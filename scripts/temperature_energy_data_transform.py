import os
import pandas as pd

# Define the main directory for datasets
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'temperature_energy_dataset'))
dataset_dir = os.path.join(main_dir, 'dataset')
os.makedirs(dataset_dir, exist_ok=True)

# Define paths for each state
states = {
    'CA': {
        'name': 'California',
        'temperature_file': os.path.join(main_dir, 'temperature_data', 'California', 'California_Monthly_Avg_Temperature_2022-01_2022-12.csv'),
        'energy_file': os.path.join(main_dir, 'energy_data', 'California', 'California_Monthly_Electricity_Generation_2022-01_2022-12.csv')
    },
    'TX': {
        'name': 'Texas',
        'temperature_file': os.path.join(main_dir, 'temperature_data', 'Texas', 'Texas_Monthly_Avg_Temperature_2022-01_2022-12.csv'),
        'energy_file': os.path.join(main_dir, 'energy_data', 'Texas', 'Texas_Monthly_Electricity_Generation_2022-01_2022-12.csv')
    },
    'CO': {
        'name': 'Colorado',
        'temperature_file': os.path.join(main_dir, 'temperature_data', 'Colorado', 'Colorado_Monthly_Avg_Temperature_2022-01_2022-12.csv'),
        'energy_file': os.path.join(main_dir, 'energy_data', 'Colorado', 'Colorado_Monthly_Electricity_Generation_2022-01_2022-12.csv')
    }
}

# Initialize an empty DataFrame to store the final combined dataset
combined_data = pd.DataFrame()

# Process each state
for state_code, paths in states.items():
    # Load temperature data if available
    if not os.path.isfile(paths['temperature_file']):
        print(f"Temperature file not found for {paths['name']}: {paths['temperature_file']}")
        continue
    
    temperature_data = pd.read_csv(paths['temperature_file'])
    temperature_data['date'] = pd.to_datetime(temperature_data['date'], errors='coerce')
    temperature_data.dropna(subset=['date'], inplace=True)
    
    # Convert datetime to a period representing the month and year (e.g., '2022-01')
    temperature_data['month'] = temperature_data['date'].dt.to_period('M')
    temperature_data['state'] = state_code
    #print(temperature_data.tail())
    # Aggregate to get monthly average temperature by state
    monthly_temp = temperature_data.groupby(['state','month'])['value'].mean().reset_index()
    monthly_temp.rename(columns={'value': 'avg_temperature_celsius'}, inplace=True)
    #print(monthly_temp.head(12))
    # Load energy data if available
    if not os.path.isfile(paths['energy_file']):
        print(f"Energy file not found for {paths['name']}: {paths['energy_file']}")
        continue
    
    energy_data = pd.read_csv(paths['energy_file'])
    energy_data['period'] = pd.to_datetime(energy_data['period'], errors='coerce')
    energy_data.dropna(subset=['period'], inplace=True)
    
    # Convert datetime to a period representing the month and year
    energy_data['month'] = energy_data['period'].dt.to_period('M')
    energy_data['state'] = state_code
    print(energy_data['sectorid'].unique())

    # Filter and aggregate energy generation data by state
    energy_data = energy_data[energy_data['sectorid'] == 99]
    print(energy_data.head())
    monthly_energy = energy_data.groupby(['state', 'month'])['generation'].sum().reset_index()
    monthly_energy.rename(columns={'generation': 'total_generation_thousand_mwh'}, inplace=True)

    # Merge temperature and energy data on state and month
    merged_data = pd.merge(monthly_temp, monthly_energy, on=['state', 'month'], how='inner')

    # Append to the combined dataset
    combined_data = pd.concat([combined_data, merged_data], ignore_index=True)

# Save the resulting dataset
output_file_path = os.path.join(dataset_dir, 'monthly_energy_temperature_by_state.csv')
combined_data.to_csv(output_file_path, index=False)

print(f"Aggregated dataset saved to {output_file_path}")
