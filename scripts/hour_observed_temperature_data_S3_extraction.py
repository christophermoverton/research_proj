import os
import pandas as pd
import boto3
from datetime import datetime
from io import StringIO

# S3 Configuration
S3_BUCKET = 'research-project-cenergy'
S3_BASE_DIR = 'ghcnd_all/ghcnd_all'
OUTPUT_BUCKET = S3_BUCKET  # If saving back to the same bucket
OUTPUT_KEY = 'research_project_data/climate_data/observed_weather_data.csv'

# Initialize boto3 S3 client
s3_client = boto3.client('s3')

# Define target fields
FIELDS = ['TOBS', 'PRCP']  # Specify variables of interest

def load_stations_from_csv(csv_path):
    """Load station information from `high_coverage_stations.csv`."""
    stations_df = pd.read_csv(csv_path)
    print(f"Loaded {len(stations_df)} stations from {csv_path}")
    return stations_df

def fetch_s3_file(bucket, key):
    """Fetch file from S3 and return as string content."""
    response = s3_client.get_object(Bucket=bucket, Key=key)
    return response['Body'].read().decode('utf-8')

def parse_dly_file_from_s3(station_id):
    """Parse the `.dly` file for specific variables (TOBS, PRCP) from S3."""
    s3_key = f"{S3_BASE_DIR}/{station_id}.dly"
    data_rows = []
    
    try:
        file_content = fetch_s3_file(S3_BUCKET, s3_key)
        for line in file_content.splitlines():
            element = line[17:21].strip()  # Get the element type (e.g., PRCP, TOBS)
            
            # Only process lines with required elements
            if element in FIELDS:
                year = line[11:15]
                month = line[15:17]
                
                # Extract 31-day values for the month
                for day in range(31):
                    value = line[21 + day * 8:26 + day * 8].strip()  # Read each 5-character field
                    measurement_flag = line[26 + day * 8:27 + day * 8].strip()
                    
                    # Skip missing values indicated by -9999
                    if value != '-9999':
                        date = f"{year}-{month}-{str(day + 1).zfill(2)}"
                        data_rows.append({
                            'station_id': station_id,
                            'date': date,
                            'element': element,
                            'value': int(value),
                            'flag': measurement_flag
                        })
    except s3_client.exceptions.NoSuchKey:
        print(f"File not found in S3: {s3_key}")
    
    return pd.DataFrame(data_rows)

def main():
    # Load station data from CSV
    stations_csv_path = "high_coverage_tobs_stations.csv"  # Local or S3 CSV
    stations_df = load_stations_from_csv(stations_csv_path)
    
    # Initialize an empty DataFrame to collect all parsed data
    all_data = pd.DataFrame()

    # Process each station individually
    for _, row in stations_df.iterrows():
        station_id = row['station_id'].split(":")[-1]  # Strip prefix, leaving just station ID
        state = row['state']
        
        print(f"Processing data for station: {station_id} in state: {state}")
        
        # Parse .dly file from S3 for the station
        weather_data = parse_dly_file_from_s3(station_id)
        
        if not weather_data.empty:
            # Add state to data
            weather_data['state'] = state
            weather_data['date'] = pd.to_datetime(weather_data['date'])
            
            # Pivot the data to have each element (TOBS, PRCP) as a separate column
            pivoted_data = weather_data.pivot_table(
                index=['date', 'station_id', 'state'],
                columns='element',
                values='value',
                aggfunc='first'
            ).reset_index()
            
            # Ensure "TOBS" and "PRCP" columns are present, fill with NaN if missing
            for column in FIELDS:
                if column not in pivoted_data.columns:
                    pivoted_data[column] = pd.NA
            
            # Flatten columns after pivoting
            pivoted_data.columns.name = None
            
            # Append pivoted data to the collection DataFrame
            all_data = pd.concat([all_data, pivoted_data], ignore_index=True)
        else:
            print(f"No data available for station {station_id} in state {state}.")

    # Write the final aggregated data to S3 as a CSV
    if not all_data.empty:
        csv_buffer = StringIO()
        all_data.to_csv(csv_buffer, index=False)
        s3_client.put_object(Bucket=OUTPUT_BUCKET, Key=OUTPUT_KEY, Body=csv_buffer.getvalue())
        print(f"Observed weather data for all stations saved to S3 at {OUTPUT_BUCKET}/{OUTPUT_KEY}")
    else:
        print("No data processed for any station.")

if __name__ == "__main__":
    main()
