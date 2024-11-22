import pandas as pd
import os

# File paths
local_folder = "/media/christopher/Extreme SSD/research_project_data/storm_events_data"
input_file_path = os.path.join(local_folder, "combined_storm_events_2019_2024.csv")
filtered_events_output_path = os.path.join(local_folder, "filtered_combined_storm_events_by_tags.csv")

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

# Function to filter events by tags from a single combined file
def filter_combined_events_by_tags(input_file, output_file, tags):
    try:
        print(f"Processing file: {input_file}")
        
        # Load the combined CSV
        data = pd.read_csv(input_file, dtype=str)
        
        # Filter for events that match the condensed severe weather tags
        filtered_data = data[data['EVENT_TYPE'].isin(tags)]
        
        # Save the filtered data to a new CSV
        filtered_data.to_csv(output_file, index=False)
        
        print(f"Filtered data saved to: {output_file}")
    
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

# Filter the combined storm events file
filter_combined_events_by_tags(input_file_path, filtered_events_output_path, severe_weather_tags)
