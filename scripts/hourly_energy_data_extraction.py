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
START_DATE = '2022-01-01'
END_DATE = '2023-12-31'
start_datetime = datetime.strptime(START_DATE, '%Y-%m-%d')
end_datetime = datetime.strptime(END_DATE, '%Y-%m-%d')

# Ensure output directories exist
os.makedirs(os.path.join(BASE_OUTPUT_DIR, CLIMATE_DIR), exist_ok=True)
os.makedirs(os.path.join(BASE_OUTPUT_DIR, ENERGY_DIR), exist_ok=True)



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
    # Define the regions
    regions = ['CAL', 'FLA', 'NY', 'TEX']
    
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
