import pandas as pd
import os

# File paths
local_folder = "/media/christopher/Extreme SSD/research_project_data/storm_events_data"
input_file_path = os.path.join(local_folder, "filtered_combined_storm_events_by_tags.csv")
output_file_path = os.path.join(local_folder, "transformed_storm_events.csv")

# Columns to keep
columns_to_keep = [
    "BEGIN_YEARMONTH", "BEGIN_DAY", "BEGIN_TIME", "END_YEARMONTH", "END_DAY", "END_TIME",
    "STATE", "STATE_FIPS", "EVENT_TYPE", "INJURIES_DIRECT", "INJURIES_INDIRECT",
    "DEATHS_DIRECT", "DEATHS_INDIRECT", "DAMAGE_PROPERTY", "DAMAGE_CROPS",
    "MAGNITUDE", "MAGNITUDE_TYPE", "FLOOD_CAUSE", "CATEGORY", "TOR_F_SCALE"
]

# State abbreviations for mapping
state_abbreviation_mapping = {
    "CALIFORNIA": "CA",
    "FLORIDA": "FL",
    "TEXAS": "TX",
    "NEW YORK": "NY"
}

# Function to filter and transform the data
def filter_and_transform_storm_events(input_file, output_file, columns_to_keep, state_mapping):
    try:
        print(f"Processing file: {input_file}")
        
        # Load the data
        data = pd.read_csv(input_file, dtype=str)
        
        # Filter columns
        filtered_data = data.loc[:, columns_to_keep].copy()  # Ensure we are working with a proper copy
        
        # Create the 'date' column in 'yyyymmdd' format
        filtered_data.loc[:, 'date'] = (
            filtered_data['BEGIN_YEARMONTH'] + 
            filtered_data['BEGIN_DAY'].str.zfill(2)
        )
        
        # Normalize BEGIN_TIME and extract the hour
        def extract_hour(time_str):
            if pd.isna(time_str):
                return None
            # Ensure time is four characters (e.g., '0750' instead of '750')
            time_str = time_str.zfill(4)
            return time_str[:2]
        
        filtered_data.loc[:, 'hour'] = filtered_data['BEGIN_TIME'].apply(extract_hour)
        
        # Map STATE to state codes (CA, FL, TX, NY)
        filtered_data.loc[:, 'state_code'] = filtered_data['STATE'].map(state_mapping)
        
        # Save the transformed data to a new CSV
        filtered_data.to_csv(output_file, index=False)
        
        print(f"Transformed data saved to: {output_file}")
    
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

# Apply the function
filter_and_transform_storm_events(input_file_path, output_file_path, columns_to_keep, state_abbreviation_mapping)
