import pandas as pd
import os

# File paths
local_folder = "/media/christopher/Extreme SSD/research_project_data/storm_events_data"
input_file_path = os.path.join(local_folder, "combined_storm_events_2019_2024.csv")
output_file_path = os.path.join(local_folder, "event_magnitudes.csv")

# Condensed severe weather tags for power impact
severe_weather_tags = [
    "Blizzard",
    "Coastal Flood",
    "Cold/Wind Chill",
    "Debris Flow",
    "Drought",
    "Excessive Heat",
    "Extreme Cold/Wind Chill",
    "Flash Flood",
    "Flood",
    "Freezing Fog",
    "Hail",
    "Heavy Rain",
    "Heavy Snow",
    "High Wind",
    "Hurricane",
    "Ice Storm",
    "Lightning",
    "Marine High Wind",
    "Storm Surge/Tide",
    "Strong Wind",
    "Thunderstorm Wind",
    "Tornado",
    "Tropical Depression",
    "Tropical Storm",
    "Wildfire",
    "Winter Storm"
]

# Function to list event magnitudes
def list_event_magnitudes(input_file, output_file, tags):
    try:
        print(f"Processing file: {input_file}")
        
        # Load the combined CSV
        data = pd.read_csv(input_file, dtype=str)
        
        # Filter for events that match the condensed severe weather tags
        filtered_data = data[data['EVENT_TYPE'].isin(tags)]
        
        # Select relevant columns
        if 'MAGNITUDE' in filtered_data.columns and 'MAGNITUDE_TYPE' in filtered_data.columns:
            magnitude_data = filtered_data[['EVENT_TYPE', 'MAGNITUDE', 'MAGNITUDE_TYPE']].dropna()
        else:
            magnitude_data = filtered_data[['EVENT_TYPE', 'MAGNITUDE']].dropna()
        
        # Save the magnitude data to a CSV
        magnitude_data.to_csv(output_file, index=False)
        
        print(f"Magnitude data saved to: {output_file}")
    
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

# Extract and save event magnitudes
list_event_magnitudes(input_file_path, output_file_path, severe_weather_tags)
