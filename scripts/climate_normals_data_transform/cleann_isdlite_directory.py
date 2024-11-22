import pandas as pd
import os
import shutil
import glob

# Define paths
csv_path = '/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/us_stations.csv'       # Path to the CSV file with station metadata
target_dir = '/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/isdlite'  # Directory where original ISD Lite files are located
output_dir = '/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/cleaned_isdlite'        # Directory to store copied ISD Lite files

# Read the station metadata CSV file
stations_df = pd.read_csv(csv_path)

# Ensure the output directory exists
os.makedirs(output_dir, exist_ok=True)

# Iterate over each row in the DataFrame
for _, row in stations_df.iterrows():
    station_id = row['station_id']
    wban = row['wban']
    
    # Define the pattern to match files in the target directory
    pattern = os.path.join(target_dir, f"{station_id}-{wban}-*")
    
    # Find all matching files
    matching_files = glob.glob(pattern)
    
    # Copy each matching file to the output directory
    for file_path in matching_files:
        shutil.copy(file_path, output_dir)

print("Matching files copied successfully.")
