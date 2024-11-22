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
BASE_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'temperature_sales_dataset')
SALES_DIR = os.path.join(BASE_DIR, 'energy_sales_data')
SALES_TEMPERATURE_DIR = os.path.join(BASE_DIR, 'sales_temperature_data')

# Ensure main directories exist
def ensure_directory(path):
    os.makedirs(path, exist_ok=True)

ensure_directory(SALES_DIR)
ensure_directory(SALES_TEMPERATURE_DIR)

# Helper function to split date range into 6-month intervals
def split_date_range(start_date, end_date, interval_months=6):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    ranges = []
    
    while start < end:
        interval_end = start + timedelta(days=interval_months * 30)
        ranges.append((start.strftime("%Y-%m-%d"), min(interval_end, end).strftime("%Y-%m-%d")))
        start = interval_end + timedelta(days=1)
    
    return ranges

# Function to retrieve monthly electricity sales data by state within each 6-month period
def get_monthly_sales_by_state(state_code, start_date, end_date, length=5000):
    all_data = pd.DataFrame()
    
    for period_start, period_end in split_date_range(start_date, end_date):
        # Construct the API URL with query parameters for each date range
        url = (
            f"https://api.eia.gov/v2/electricity/retail-sales/data/"
            f"?api_key={EIA_API_KEY}"
            f"&frequency=monthly"
            f"&data[0]=sales"
            f"&facets[stateid][]={state_code}"
            f"&start={period_start}"
            f"&end={period_end}"
            f"&sort[0][column]=period"
            f"&sort[0][direction]=desc"
            f"&offset=0"
            f"&length={length}"
        )
        
        response = requests.get(url)
        try:
            data = response.json()
            records = data.get('response', {}).get('data', [])
            if records:
                all_data = pd.concat([all_data, pd.DataFrame(records)], ignore_index=True)
                print(f"Sales data request completed for {state_code} from {period_start} to {period_end}. Records retrieved: {len(records)}")
            else:
                print(f"Sales data request for {state_code} from {period_start} to {period_end} completed with no data.")
        except (requests.exceptions.JSONDecodeError, KeyError):
            print(f"Error fetching sales data for state {state_code}: {data.get('error', 'Unknown error')}")
    
    return all_data

# Function to retrieve monthly average temperature data using GSOM with fallback to GSDN within each 6-month period
def get_monthly_average_temperature_by_state(state_fips, start_date, end_date):
    fip_code = {'CO': '08', 'TX': '48', 'CA': '06'}
    all_temperature_data = pd.DataFrame()

    for period_start, period_end in split_date_range(start_date, end_date, 2):
        temperature_data = fetch_data_with_pagination(fip_code[state_fips], period_start, period_end, "GSOM", "TAVG")
        
        if temperature_data.empty:
            print(f"No GSOM data available for state FIPS {state_fips} from {period_start} to {period_end}. Falling back to GSDN.")
            temperature_data = fetch_data_with_pagination(fip_code[state_fips], period_start, period_end, "GHCND", "TAVG")
        
        if not temperature_data.empty:
            print(f"Temperature data request completed for {state_fips} from {period_start} to {period_end}. Records retrieved: {len(temperature_data)}")
        else:
            print(f"Temperature data request for {state_fips} from {period_start} to {period_end} completed with no data.")
        
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
                    print(f"Completed temperature data request for {state_fips} with no more records.")
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
        state_sales_dir = os.path.join(SALES_DIR, state_name)
        state_sales_temperature_dir = os.path.join(SALES_TEMPERATURE_DIR, state_name)
        ensure_directory(state_sales_dir)
        ensure_directory(state_sales_temperature_dir)

        # Retrieve and save electricity sales data
        sales_data = get_monthly_sales_by_state(state_code, start_date, end_date)
        if not sales_data.empty:
            sales_filepath = os.path.join(state_sales_dir, f"{state_name}_Monthly_Electricity_Sales_{start_date[:7]}_{end_date[:7]}.csv")
            sales_data.to_csv(sales_filepath, index=False)
            print(f"Electricity sales data for {state_name} saved to {sales_filepath}")
        else:
            print(f"No electricity sales data available for {state_name}")

        # Retrieve and save monthly average temperature data using GSOM with fallback to GSDN
        temperature_data = get_monthly_average_temperature_by_state(state_code, start_date, end_date)
        if not temperature_data.empty:
            temperature_filepath = os.path.join(state_sales_temperature_dir, f"{state_name}_Monthly_Avg_Temperature_{start_date[:7]}_{end_date[:7]}.csv")
            temperature_data.to_csv(temperature_filepath, index=False)
            print(f"Temperature data for {state_name} saved to {temperature_filepath}")
        else:
            print(f"No temperature data available for {state_name}")

# Example usage
retrieve_and_save_data(STATES, START_DATE, END_DATE)
