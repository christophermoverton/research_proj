import os
import requests
import pandas as pd
import boto3
from io import StringIO
from datetime import datetime

# AWS S3 configuration
S3_BUCKET = 'research-project-cenergy'
CLIMATE_DIR = 'climate_data'
ENERGY_DIR = 'hourly_energy_data'
BASE_OUTPUT_DIR = os.path.abspath('research_project_data')
EIA_API_KEY = 'Your_EIA_API_KEY'

# Define date range
START_DATE = '2017-01-01'
END_DATE = '2024-12-31'
start_datetime = datetime.strptime(START_DATE, '%Y-%m-%d')
end_datetime = datetime.strptime(END_DATE, '%Y-%m-%d')

# Ensure output directories exist
os.makedirs(os.path.join(BASE_OUTPUT_DIR, CLIMATE_DIR), exist_ok=True)
os.makedirs(os.path.join(BASE_OUTPUT_DIR, ENERGY_DIR), exist_ok=True)

# Initialize S3 client
s3_client = boto3.client('s3')

def get_stations_for_states(s3_bucket, file_key, states):
    """Extract station IDs for the specified states from `hly_inventory.txt`."""
    response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    
    # Decode the response body to a string
    content = response['Body'].read().decode('utf-8')
    
    # Define column widths and names for fixed-width parsing
    colspecs = [(0, 11), (12, 20), (21, 30), (31, 38), (38, 40), (41, 71), (72, 76), (77, 82)]
    colnames = ['station_id', 'latitude', 'longitude', 'elevation', 'state', 'station_name', 'network_code', 'unknown']
    
    # Read the fixed-width file
    inventory_df = pd.read_fwf(StringIO(content), colspecs=colspecs, header=None, names=colnames)
    
    # Clean up whitespace
    inventory_df['state'] = inventory_df['state'].str.strip()
    inventory_df['station_id'] = inventory_df['station_id'].str.strip()
    
    # Filter for specified states
    stations = inventory_df[inventory_df['state'].isin(states)]['station_id'].unique()
    print(f"Found {len(stations)} stations in {states}.")
    
    return stations

def filter_and_save_chunked_data(s3_bucket, file_key, stations, output_file, date_cols):
    """Filter climate data in chunks and save to CSV."""
    response = s3_client.get_object(Bucket=s3_bucket, Key=file_key)
    
    # Extract month, day, and hour from START_DATE and END_DATE for comparison
    start_month, start_day = start_datetime.month, start_datetime.day
    end_month, end_day = end_datetime.month, end_datetime.day

    chunksize = 10**5  # Adjust chunk size as needed
    header_written = False  # Track whether the CSV header has been written
    
    for chunk in pd.read_csv(response['Body'], chunksize=chunksize):
        # Filter for station IDs
        chunk = chunk[chunk[date_cols['id_column']].isin(stations)]
        
        # Filter for rows within the specified month-day range
        # Check if the row's month and day are within the specified date range
        chunk = chunk[
            ((chunk[date_cols['month']] > start_month) | 
             ((chunk[date_cols['month']] == start_month) & (chunk[date_cols['day']] >= start_day))) &
            ((chunk[date_cols['month']] < end_month) | 
             ((chunk[date_cols['month']] == end_month) & (chunk[date_cols['day']] <= end_day)))
        ]
        
        # Append chunk to output CSV
        chunk.to_csv(output_file, mode='a', header=not header_written, index=False)
        header_written = True
        print(f"Processed and saved a chunk with {len(chunk)} rows matching stations")

def get_hourly_energy_data(state_code, start_date, end_date, length=5000):
    """Retrieve all available hourly energy data for the specified region with pagination."""
    all_data = pd.DataFrame()
    offset = 0  # Start offset for pagination
    
    while True:
        # Construct URL with current offset
        url = (
            f"https://api.eia.gov/v2/electricity/rto/region-data/data/"
            f"?api_key={EIA_API_KEY}"
            f"&frequency=hourly"
            f"&data[0]=value"
            f"&facets[respondent][]={state_code}"
            f"&start={start_date}"
            f"&end={end_date}"
            f"&sort[0][column]=period"
            f"&sort[0][direction]=desc"
            f"&offset={offset}"
            f"&length={length}"
        )

        response = requests.get(url)
        
        try:
            data = response.json()
            records = data.get('response', {}).get('data', [])
            
            if not records:
                print(f"No more data found for {state_code}. Pagination complete.")
                break
            
            # Append the new records to the all_data DataFrame
            all_data = pd.concat([all_data, pd.DataFrame(records)], ignore_index=True)
            print(f"Retrieved {len(records)} records for {state_code} with offset {offset}.")
            
            # Increment offset for the next batch
            offset += length
        
        except (requests.exceptions.JSONDecodeError, KeyError) as e:
            print(f"Error fetching energy data for state {state_code}: {e}")
            break
    
    return all_data

def main():
    # Define the states and associated regions
    states = ['CA', 'TX', 'FL', 'NY']
    regions = ['CAL', 'FLA', 'NY', 'TEX']
    
    # Step 1: Get list of stations in specified states
    station_list = get_stations_for_states(S3_BUCKET, 'hly_inventory.txt', states)
    
    # Step 2: Process each large climate data file in chunks
    climate_files = {
        'hly-degh-normal.csv': 'degh_filtered.csv',
        'hly-hidx-normal.csv': 'hidx_filtered.csv',
        'hly-wchl-normal.csv': 'wchl_filtered.csv'
    }
    date_columns = {'id_column': 'GHCN_ID', 'month': 'month', 'day': 'day', 'hour': 'hour'}
    
    for file_name, output_file_name in climate_files.items():
        output_file = os.path.join(BASE_OUTPUT_DIR, CLIMATE_DIR, output_file_name)
        print(f"Processing {file_name} and saving filtered data to {output_file}")
        filter_and_save_chunked_data(S3_BUCKET, file_name, station_list, output_file, date_columns)
    
    # Step 3: Collect hourly energy data for each region and save to files
    for region in regions:
        energy_data = get_hourly_energy_data(region, START_DATE, END_DATE)
        if not energy_data.empty:
            output_file = os.path.join(BASE_OUTPUT_DIR, ENERGY_DIR, f"{region}_hourly_energy_data.csv")
            energy_data.to_csv(output_file, index=False)
            print(f"Hourly energy data for {region} saved to {output_file}")
        else:
            print(f"No hourly energy data available for {region}")

if __name__ == "__main__":
    main()
