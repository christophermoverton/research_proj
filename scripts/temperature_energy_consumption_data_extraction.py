import os
import requests
import pandas as pd
from datetime import datetime, timedelta

# API Keys
EIA_API_KEY = 'Your_EIA_API_KEY'
NOAA_API_KEY = 'Your_NOAA_API_TOKEN'

# Configuration for states, date range, and directories
STATES = {'CO': 'Colorado', 'TX': 'Texas', 'CA': 'California'}
START_DATE = '2022-01-01'
END_DATE = '2023-12-31'

# Define main directory paths
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'temperature_energy_dataset')
CONSUMPTION_DIR = os.path.join(BASE_DIR, 'energy_consumption_data')
TEMPERATURE_DIR = os.path.join(BASE_DIR, 'temperature_data')

# Ensure main directories exist
def ensure_directory(path):
    os.makedirs(path, exist_ok=True)

ensure_directory(CONSUMPTION_DIR)
ensure_directory(TEMPERATURE_DIR)

# Helper function to split date range into 6-month intervals
def split_date_range(start_date, end_date, interval_months=2):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []
    
    while start < end:
        interval_end = start + timedelta(days=interval_months * 30)
        ranges.append((start.strftime("%Y-%m-%d"), min(interval_end, end).strftime("%Y-%m-%d")))
        start = interval_end + timedelta(days=1)
    
    return ranges

# Function to retrieve monthly electricity consumption data by state within each 6-month period
def get_monthly_consumption_by_state(state_code, start_date, end_date, length=5000):
    url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
    all_data = pd.DataFrame()
    
    for period_start, period_end in split_date_range(start_date, end_date):
        params = {
            "api_key": EIA_API_KEY,
            "frequency": "monthly",
            "data[0]": "total-consumption",
            "facets[location][]": state_code,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "start": period_start,
            "end": period_end,
            "offset": 0,
            "length": length
        }
        
        response = requests.get(url, params=params)
        try:
            data = response.json()
            records = data.get('response', {}).get('data', [])
            if records:
                all_data = pd.concat([all_data, pd.DataFrame(records)], ignore_index=True)
        except (requests.exceptions.JSONDecodeError, KeyError):
            print(f"Error fetching data for state {state_code}: {data.get('error', 'Unknown error')}")
    
    return all_data

# Function to retrieve monthly average temperature data using GSOM with fallback to GSDN within each 6-month period
def get_monthly_average_temperature_by_state(state_fips, start_date, end_date):
    fip_code = {'CO': '08', 'TX': '48', 'CA': '06'}
    all_temperature_data = pd.DataFrame()

    for period_start, period_end in split_date_range(start_date, end_date):
        temperature_data = fetch_data_with_pagination(fip_code[state_fips], period_start, period_end, "GSOM", "TAVG")
        
        if temperature_data.empty:
            print(f"No GSOM data available for state FIPS {state_fips} from {period_start} to {period_end}. Falling back to GSDN.")
            temperature_data = fetch_data_with_pagination(fip_code[state_fips], period_start, period_end, "GHCND", "TAVG")
        
        all_temperature_data = pd.concat([all_temperature_data, temperature_data], ignore_index=True)
    
    return all_temperature_data

# Function to handle paginated data retrieval for GSOM and GSDN datasets within specified 6-month periods
def fetch_data_with_pagination(state_fips, start_date, end_date, datasetid, datatypeid):
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": NOAA_API_KEY}
    all_data = []
    offset = 0
    limit = 1000  # NOAA API max limit per request
    
    while True:
        params = {
            "datasetid": datasetid,
            "datatypeid": datatypeid,
            "locationid": f"FIPS:{state_fips}",
            "startdate": start_date,
            "enddate": end_date,
            "units": "standard",
            "limit": limit,
            "offset": offset,
        }
        
        response = requests.get(url, headers=headers, params=params)
        if response.status_code == 200:
            try:
                data = response.json()
                results = data.get('results', [])
                if not results:
                    break
                all_data.extend(results)
                offset += limit
            except requests.exceptions.JSONDecodeError:
                print(f"Error decoding JSON for dataset {datasetid} for state FIPS {state_fips}.")
                break
        else:
            print(f"Error: Received status code {response.status_code} for dataset {datasetid} and state FIPS {state_fips}.")
            break

    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

# Main function to retrieve and save data for each state within each 6-month period
def retrieve_and_save_data(states, start_date, end_date):
    for state_code, state_name in states.items():
        state_consumption_dir = os.path.join(CONSUMPTION_DIR, state_name)
        state_temperature_dir = os.path.join(TEMPERATURE_DIR, state_name)
        ensure_directory(state_consumption_dir)
        ensure_directory(state_temperature_dir)

        # Retrieve and save electricity consumption data
        consumption_data = get_monthly_consumption_by_state(state_code, start_date, end_date)
        if not consumption_data.empty:
            energy_filepath = os.path.join(state_consumption_dir, f"{state_name}_Monthly_Electricity_Consumption_{start_date[:7]}_{end_date[:7]}.csv")
            consumption_data.to_csv(energy_filepath, index=False)
            print(f"Electricity consumption data for {state_name} saved to {energy_filepath}")
        else:
            print(f"No electricity consumption data available for {state_name}")

        # Retrieve and save monthly average temperature data using GSOM with fallback to GSDN
        temperature_data = get_monthly_average_temperature_by_state(state_code, start_date, end_date)
        if not temperature_data.empty:
            temperature_filepath = os.path.join(state_temperature_dir, f"{state_name}_Monthly_Avg_Temperature_{start_date[:7]}_{end_date[:7]}.csv")
            temperature_data.to_csv(temperature_filepath, index=False)
            print(f"Temperature data for {state_name} saved to {temperature_filepath}")
        else:
            print(f"No temperature data available for {state_name}")

# Example usage
retrieve_and_save_data(STATES, START_DATE, END_DATE)
