import pandas as pd

# Define file paths
metadata_path = "/home/christopher/research_proj/scripts/cleaned_ghcn_stations.txt"  # Path to GHCN station metadata file
daily_selection_output_path = "/home/christopher/research_proj/scripts/representative_daily_stations.csv"  # Output for daily data collection stations

# Step 1: Load the GHCN metadata with adjusted column names
# Specify the column names based on the format provided
column_names = ['ID', 'Latitude', 'Longitude', 'Elevation', 'State', 'Location']
metadata_df = pd.read_csv(metadata_path, delim_whitespace=True, header=None, names=column_names, engine="python")

# Step 2: Filter out rows where 'Location' contains "CLOSED"
metadata_df = metadata_df[~metadata_df['Location'].str.contains("CLOSED", case=False, na=False)]
# Step 2: Apply additional criteria for representative selection
# Ensure geographic diversity by selecting a set number of representative stations per state
# Example: Select up to 5 representative stations per state
representative_df = metadata_df.groupby('State').head(5).reset_index(drop=True)

# Optional: Sort by Latitude and Elevation for additional geographic diversity
representative_df = representative_df.sort_values(by=['Latitude', 'Elevation'])

# Step 3: Save the representative daily collection list
representative_df.to_csv(daily_selection_output_path, index=False)

print(f"Representative daily station list saved to {daily_selection_output_path}")
