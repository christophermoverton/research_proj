import os
import requests
import pandas as pd

# EIA API Key
api_key = 'Your_EIA_API_KEY'

# Define the subregions to sample (ID and Name)
subregions = {
    '4001': 'ISNE_Maine',
    '4002': 'ISNE_New_Hampshire',
    '4003': 'ISNE_Vermont',
    '4004': 'ISNE_Connecticut',
    '4005': 'ISNE_Rhode_Island',
    '4006': 'ISNE_Southeast_Mass',
    '4007': 'ISNE_Western_Central_Mass',
    '4008': 'ISNE_Northeast_Mass',
    # Additional PJM subregions for variety
    'AE': 'PJM_Atlantic_Electric',
    'BC': 'PJM_Baltimore_Gas_Electric',
    'CE': 'PJM_ComEd_Commonwealth_Edison',
    'AEP': 'PJM_AEP_American_Electric_Power',
    'AP': 'PJM_Allegheny_Power',
    'DPL': 'PJM_Delmarva_Power',
    'DOM': 'PJM_Dominion_Virginia_Power',
    'DEOK': 'PJM_Duke_Energy_Ohio_Kentucky',
    'DUQ': 'PJM_Duquesne_Light_Company',
    'JC': 'PJM_Jersey_Central_Power_Light',
    'VEA':'CISO_Valley_Electric_Association',
    'EAST':'ERCO_East',
    'NCEN':'ERCO_North_Central',
    'NRTH':'ERCO_North',
    'FWEST':'ERCO_Far_West',
    'COAS' :'ERCO_Coast',
    'SOUT' :'ERCO_South',
    'SCE' : 'CISO_Southern_California_Edison',
    'SDGE' : 'CISO_San_Diego_Gas_Electric',
    'PGAE' : 'CISO_Pacific_Gas_Electric',
    'LES' : 'SWPP_Lincoln_Electric_System',
    'WAUE' : 'SWPP_Western_Area_Power_Upper_Great_Plains_East'
}

# Function to retrieve data for a specific subregion with pagination
def get_hourly_demand_by_subregion(subregion_id, start_date, end_date):
    url = f"https://api.eia.gov/v2/electricity/rto/region-sub-ba-data/data/?api_key={api_key}"
    all_data = pd.DataFrame()  # Initialize empty DataFrame to store all pages
    offset = 0  # Start with the first page
    length = 5000  # Maximum records per page, as per EIA API

    while True:
        params = {
            "frequency": "hourly",
            "data[0]": "value",
            "facets[subba][]": subregion_id,
            "start": start_date,
            "end": end_date,
            "sort[0][column]": "period",
            "sort[0][direction]": "desc",
            "offset": offset,
            "length": length
        }

        # Make the API request
        response = requests.get(url, params=params)
        data = response.json()
        
        # Check for data and handle errors
        if 'response' in data and 'data' in data['response']:
            records = data['response']['data']
            page_df = pd.DataFrame(records)
            all_data = pd.concat([all_data, page_df], ignore_index=True)

            # If less data was returned than requested, we're done
            if len(records) < length:
                break
            # Otherwise, move to the next page
            offset += length
        else:
            print("Error fetching data for subregion", subregion_id, ":", data.get('error', 'Unknown error'))
            break

    return all_data

# Set up date range for 12 months (e.g., 2022)
start_date = '2022-01-01T00'  # Example start date
end_date = '2022-12-31T23'    # Example end date

# Create main dataset directory if it doesn't exist
main_dir = 'dataset'
os.makedirs(main_dir, exist_ok=True)

# Loop through each subregion, retrieve data, and save to its own directory
for subregion_id, subregion_name in subregions.items():
    # Create a subdirectory for each subregion
    subregion_dir = os.path.join(main_dir, subregion_name)
    os.makedirs(subregion_dir, exist_ok=True)
    
    # Retrieve data for the subregion
    demand_data = get_hourly_demand_by_subregion(subregion_id, start_date, end_date)
    
    # Save to CSV if data was retrieved successfully
    if not demand_data.empty:
        filename = f"{subregion_name}_Hourly_Electricity_Demand_{start_date[:4]}.csv"
        filepath = os.path.join(subregion_dir, filename)
        demand_data.to_csv(filepath, index=False)
        print(f"Data for {subregion_name} saved to {filepath}.")
    else:
        print(f"No data available for subregion {subregion_name}.")
