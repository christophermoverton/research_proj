import os
import requests
import pandas as pd

# API Keys
EIA_API_KEY = 'Your_EIA_API_KEY'
NOAA_API_KEY = 'Your_NOAA_API_TOKEN'

# Configuration for states, date range, and directories
STATES = {'CO': 'Colorado', 'TX': 'Texas', 'CA': 'California'}
START_DATE = '2022-01-01'
END_DATE = '2022-12-31'

# Define main directory paths
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'temperature_energy_dataset')
ENERGY_DIR = os.path.join(BASE_DIR, 'energy_data')
TEMPERATURE_DIR = os.path.join(BASE_DIR, 'temperature_data')

# Ensure main directories exist
def ensure_directory(path):
    os.makedirs(path, exist_ok=True)

ensure_directory(ENERGY_DIR)
ensure_directory(TEMPERATURE_DIR)

# Function to retrieve monthly electricity generation data by state
def get_monthly_generation_by_state(state_code, start_date, end_date, length=5000):
    url = "https://api.eia.gov/v2/electricity/electric-power-operational-data/data/"
    params = {
        "api_key": EIA_API_KEY,
        "frequency": "monthly",
        "data[0]": "generation",
        "facets[location][]": state_code,
        "sort[0][column]": "period",
        "sort[0][direction]": "desc",
        "offset": 0,
        "length": length
    }
    if start_date:
        params["start"] = start_date
    if end_date:
        params["end"] = end_date

    response = requests.get(url, params=params)
    try:
        data = response.json()
        records = data.get('response', {}).get('data', [])
        return pd.DataFrame(records) if records else pd.DataFrame()
    except (requests.exceptions.JSONDecodeError, KeyError):
        print(f"Error fetching data for state {state_code}: {data.get('error', 'Unknown error')}")
        return pd.DataFrame()

# Function to retrieve monthly average temperature data using GSOM, with fallback to GSDN if needed
def get_monthly_average_temperature_by_state(state_fips, start_date, end_date):
    # Attempt to retrieve data from GSOM
    fip_code = {'CO': '08', 'TX':'48', 'CA':'06'}
    temperature_data = fetch_data_with_pagination(fip_code[state_fips], start_date, end_date, "GSOM", "TAVG")
    
    # If no GSOM data, fallback to GSDN
    if temperature_data.empty:
        print(f"No GSOM data available for state FIPS {state_fips}. Falling back to GSDN.")
        temperature_data = fetch_data_with_pagination(fip_code[state_fips], start_date, end_date, "GHCND", "TAVG")
    
    return temperature_data

# Function to handle paginated data retrieval for GSOM and GSDN datasets
def fetch_data_with_pagination(state_fips, start_date, end_date, datasetid, datatypeid):
    url = "https://www.ncdc.noaa.gov/cdo-web/api/v2/data"
    headers = {"token": NOAA_API_KEY}
    all_data = []
    offset = 0
    limit = 1000  # NOAA API max limit per request
    
    while True:
        params = {
            "datasetid": datasetid,         # Dataset: GSOM or GSDN
            "datatypeid": datatypeid,       # Data type: TAVG for average temperature
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
                    break  # No more data to retrieve; exit the loop
                all_data.extend(results)
                offset += limit  # Increment the offset for the next page
            except requests.exceptions.JSONDecodeError:
                print(f"Error decoding JSON for dataset {datasetid} for state FIPS {state_fips}.")
                break
        else:
            print(f"Error: Received status code {response.status_code} for dataset {datasetid} and state FIPS {state_fips}.")
            break

    return pd.DataFrame(all_data) if all_data else pd.DataFrame()

# Main function to retrieve and save data for each state
def retrieve_and_save_data(states, start_date, end_date):
    for state_code, state_name in states.items():
        state_energy_dir = os.path.join(ENERGY_DIR, state_name)
        state_temperature_dir = os.path.join(TEMPERATURE_DIR, state_name)
        ensure_directory(state_energy_dir)
        ensure_directory(state_temperature_dir)

        # Retrieve and save electricity generation data
        generation_data = get_monthly_generation_by_state(state_code, start_date, end_date)
        if not generation_data.empty:
            energy_filepath = os.path.join(state_energy_dir, f"{state_name}_Monthly_Electricity_Generation_{start_date[:7]}_{end_date[:7]}.csv")
            generation_data.to_csv(energy_filepath, index=False)
            print(f"Electricity generation data for {state_name} saved to {energy_filepath}")
        else:
            print(f"No electricity generation data available for {state_name}")

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
