import os
import pandas as pd
from datetime import datetime

# Define date range
START_DATE = '2022-01-01'
END_DATE = '2023-12-31'
start_period = pd.to_datetime(START_DATE).to_period('M')
end_period = pd.to_datetime(END_DATE).to_period('M')

# Define main directory paths
main_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'temperature_energy_dataset'))
dataset_dir = os.path.join(main_dir, 'dataset')
os.makedirs(dataset_dir, exist_ok=True)

# Define paths for each state with consumption files instead of generation
states = {
    'CA': {
        'name': 'California',
        'temperature_file': os.path.join(main_dir, 'temperature_data', 'California', f'California_Monthly_Avg_Temperature_{START_DATE[:7]}_{END_DATE[:7]}.csv'),
        'energy_file': os.path.join(main_dir, 'energy_consumption_data', 'California', f'California_Monthly_Electricity_Consumption_{START_DATE[:7]}_{END_DATE[:7]}.csv')
    },
    'TX': {
        'name': 'Texas',
        'temperature_file': os.path.join(main_dir, 'temperature_data', 'Texas', f'Texas_Monthly_Avg_Temperature_{START_DATE[:7]}_{END_DATE[:7]}.csv'),
        'energy_file': os.path.join(main_dir, 'energy_consumption_data', 'Texas', f'Texas_Monthly_Electricity_Consumption_{START_DATE[:7]}_{END_DATE[:7]}.csv')
    },
    'CO': {
        'name': 'Colorado',
        'temperature_file': os.path.join(main_dir, 'temperature_data', 'Colorado', f'Colorado_Monthly_Avg_Temperature_{START_DATE[:7]}_{END_DATE[:7]}.csv'),
        'energy_file': os.path.join(main_dir, 'energy_consumption_data', 'Colorado', f'Colorado_Monthly_Electricity_Consumption_{START_DATE[:7]}_{END_DATE[:7]}.csv')
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
    
    # Filter data within the specified date range
    temperature_data['month'] = temperature_data['date'].dt.to_period('M')
    temperature_data = temperature_data[(temperature_data['month'] >= start_period) & (temperature_data['month'] <= end_period)]
    
    # Aggregate to get monthly average temperature by state
    temperature_data['state'] = state_code
    monthly_temp = temperature_data.groupby(['state', 'month'])['value'].mean().reset_index()
    monthly_temp.rename(columns={'value': 'avg_temperature_celsius'}, inplace=True)
    
    # Load energy consumption data if available
    if not os.path.isfile(paths['energy_file']):
        print(f"Energy consumption file not found for {paths['name']}: {paths['energy_file']}")
        continue
    
    energy_data = pd.read_csv(paths['energy_file'])
    energy_data['period'] = pd.to_datetime(energy_data['period'], errors='coerce')
    energy_data.dropna(subset=['period'], inplace=True)
    
    # Filter data within the specified date range
    energy_data['month'] = energy_data['period'].dt.to_period('M')
    energy_data = energy_data[(energy_data['month'] >= start_period) & (energy_data['month'] <= end_period)]
    energy_data['state'] = state_code

    # Filter and aggregate energy consumption data by state
    energy_data = energy_data[energy_data['sectorid'] == 99]  # Assuming sectorid 99 is relevant
    monthly_energy = energy_data.groupby(['state', 'month'])['consumption'].sum().reset_index()
    monthly_energy.rename(columns={'consumption': 'total_consumption_thousand_mwh'}, inplace=True)

    # Merge temperature and energy data on state and month
    merged_data = pd.merge(monthly_temp, monthly_energy, on=['state', 'month'], how='inner')

    # Append to the combined dataset
    combined_data = pd.concat([combined_data, merged_data], ignore_index=True)

# Save the resulting dataset
output_file_path = os.path.join(dataset_dir, 'monthly_energy_temperature_by_state.csv')
combined_data.to_csv(output_file_path, index=False)

print(f"Aggregated dataset saved to {output_file_path}")
