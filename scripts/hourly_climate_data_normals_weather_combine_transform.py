import pandas as pd
import os

# Define the base directory
base_directory = "/media/christopher/Extreme SSD/"

# Paths to the directories and files
weather_data_dir = base_directory + "final_transformed_data/"
climate_data_file = base_directory + "research_project_data/climate_data/joined_climate_data.csv"

try:
    # Load the climate data
    print("Step 1: Loading climate data...")
    climate_df = pd.read_csv(climate_data_file)
    print(f"Loaded climate data with {len(climate_df)} rows.")

    # Transform climate data to include 'date' column
    print("Step 2: Transforming climate data format...")
    repeated_climate_dfs = []
    for year in range(2017, 2025):  # Iterate over years 2017-2024
        temp_climate_df = climate_df.copy()
        temp_climate_df['date'] = (
            str(year) +
            climate_df['month'].astype(str).str.zfill(2) +
            climate_df['day'].astype(str).str.zfill(2)
        ).astype(int)
        repeated_climate_dfs.append(temp_climate_df)

    # Combine climate data for all years
    climate_df = pd.concat(repeated_climate_dfs, ignore_index=True)
    climate_df = climate_df.drop(columns=['month', 'day'])
    print(f" - Climate data expanded for all years. Total rows: {len(climate_df)}")

    # Process all weather files year by year
    print("Step 3: Processing weather data files...")
    all_weather_files = [
        f for f in os.listdir(weather_data_dir)
        if f.startswith("weather_data_final_transform_") and f.endswith("_hourly.csv")
    ]
    merged_dfs = []

    for weather_file in all_weather_files:
        year = int(weather_file.split("_")[4])  # Extract year from filename
        print(f"Processing weather file for year {year}: {weather_file}...")
        
        # Load the weather data
        weather_df = pd.read_csv(os.path.join(weather_data_dir, weather_file))
        print(f" - Loaded weather data with {len(weather_df)} rows.")
        
        # Convert temperature to Fahrenheit
        weather_df['temperature'] = (weather_df['temperature'] * 9/5) + 32
        print(f" - Converted temperature to Fahrenheit.")

        # Merge with climate data
        merged_df = pd.merge(
            weather_df,
            climate_df,
            on=['date', 'hour', 'state'],
            how='left'
        )
        print(f" - Merged data for year {year}. Resulting rows: {len(merged_df)}")
        
        # Store merged DataFrame
        merged_dfs.append(merged_df)

    # Combine all yearly merged DataFrames
    print("Step 4: Combining all yearly datasets...")
    final_merged_df = pd.concat(merged_dfs, ignore_index=True)
    print(f" - Combined dataset contains {len(final_merged_df)} rows.")

    # Save the final combined dataset
    output_file = weather_data_dir + "weather_climate_combined_all_years.csv"
    print("Step 5: Saving the final dataset...")
    final_merged_df.to_csv(output_file, index=False)
    print(f"Final dataset saved to {output_file}")

except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
