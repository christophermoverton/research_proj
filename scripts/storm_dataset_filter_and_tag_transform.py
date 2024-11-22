import pandas as pd
import os

# File paths and states
local_folder = "/media/christopher/Extreme SSD/research_project_data/storm_events_data"
target_states = ["CALIFORNIA", "TEXAS", "FLORIDA", "NEW YORK"]

# Define the output file paths
combined_output_path = os.path.join(local_folder, "combined_storm_events_2019_2024.csv")
tags_output_path = os.path.join(local_folder, "severe_weather_tags.txt")

# Initialize an empty DataFrame to combine all filtered data
combined_data = pd.DataFrame()

# Initialize a set to store unique severe weather tags
unique_tags = set()

# Function to process storm event details files
def process_storm_event_file(file_name):
    global combined_data
    global unique_tags
    
    file_path = os.path.join(local_folder, file_name)
    
    try:
        print(f"Processing file: {file_name}")
        
        # Load the CSV
        data = pd.read_csv(file_path, dtype=str)
        
        # Filter for target states
        filtered_data = data[data['STATE'].isin(target_states)]
        
        # Append filtered data to the combined DataFrame
        combined_data = pd.concat([combined_data, filtered_data], ignore_index=True)
        
        # Collect unique values from the EVENT_TYPE column
        if 'EVENT_TYPE' in data.columns:
            unique_tags.update(data['EVENT_TYPE'].unique())
        
        print(f"Finished processing: {file_name}")
    
    except Exception as e:
        print(f"Error processing file {file_name}: {e}")

# List of files to process
storm_event_files = [
    "StormEvents_details-ftp_v1.0_d2019_c20240117.csv",
    "StormEvents_details-ftp_v1.0_d2020_c20240620.csv",
    "StormEvents_details-ftp_v1.0_d2021_c20240716.csv",
    "StormEvents_details-ftp_v1.0_d2022_c20240716.csv",
    "StormEvents_details-ftp_v1.0_d2023_c20241017.csv",
    "StormEvents_details-ftp_v1.0_d2024_c20241017.csv"
]

# Process each file
for storm_file in storm_event_files:
    process_storm_event_file(storm_file)

# Save the combined data to a CSV
combined_data.to_csv(combined_output_path, index=False)
print(f"Combined data saved to: {combined_output_path}")

# Save the unique severe weather tags to a text file
with open(tags_output_path, 'w') as tag_file:
    for tag in sorted(unique_tags):
        tag_file.write(tag + '\n')
print(f"Severe weather tags saved to: {tags_output_path}")
