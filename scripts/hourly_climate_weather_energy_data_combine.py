import pandas as pd
import os

# Define the base directory
base_directory = "/media/christopher/Extreme SSD/"

# Paths to the files and directories
weather_climate_file = base_directory + "final_transformed_data/weather_climate_combined_all_years.csv"
energy_data_dir = base_directory + "research_project_data/hourly_energy_data/"

# Mapping of respondent to state codes
respondent_to_state = {
    "CAL": "CA",  # California
    "FLA": "FL",  # Florida
    "NY": "NY",   # New York
    "TEX": "TX"   # Texas
}

try:
    # Load the weather-climate combined data
    print("Step 1: Loading weather-climate data...")
    weather_climate_df = pd.read_csv(weather_climate_file)
    print(f"Loaded weather-climate data with {len(weather_climate_df)} rows.")

    # Process all energy data files
    print("Step 2: Processing energy data files...")
    all_energy_files = [
        f for f in os.listdir(energy_data_dir) if f.endswith(".csv")
    ]
    energy_dfs = []

    for energy_file in all_energy_files:
        print(f"Processing energy file: {energy_file}...")
        energy_df = pd.read_csv(os.path.join(energy_data_dir, energy_file))

        # Transform period to year, date, and hour
        print(" - Transforming 'period' to year, date, and hour...")
        energy_df['year'] = energy_df['period'].str[:4].astype(int)
        energy_df['date'] = energy_df['period'].str[:10].str.replace("-", "").astype(int)
        energy_df['hour'] = energy_df['period'].str[11:13].astype(int)

        # Map respondent to state codes
        print(" - Mapping 'respondent' to state codes...")
        energy_df['state'] = energy_df['respondent'].map(respondent_to_state)

        # Append to list
        energy_dfs.append(energy_df)

    # Combine all energy data
    print("Step 3: Combining energy data...")
    energy_df_combined = pd.concat(energy_dfs, ignore_index=True)
    print(f" - Combined energy data contains {len(energy_df_combined)} rows.")

    # Join the energy data with the weather-climate data
    print("Step 4: Joining datasets...")
    merged_df = pd.merge(
        energy_df_combined,
        weather_climate_df,
        on=['year', 'date', 'hour', 'state'],
        how='left'
    )
    print(f" - Join complete. Resulting dataset has {len(merged_df)} rows.")

    # Save the final dataset
    output_file = base_directory + "final_transformed_data/energy_weather_climate_combined.csv"
    print("Step 5: Saving the final dataset...")
    merged_df.to_csv(output_file, index=False)
    print(f"Final dataset saved to {output_file}")

except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
