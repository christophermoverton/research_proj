import pandas as pd

# Define the base directory
base_directory = "/media/christopher/Extreme SSD/research_project_data/climate_data/"

# Paths to the files
filtered_climate_file = base_directory + "filtered_climate_data_with_state_chunks.csv"
combined_climate_file = base_directory + "combined_climate_data.csv"

try:
    # Load the filtered climate data
    print("Step 1: Loading filtered climate data with state...")
    filtered_climate_df = pd.read_csv(filtered_climate_file)
    print(f"Loaded filtered climate data with {len(filtered_climate_df)} rows.")

    # Remove rows with -9999 in aggregate average columns
    print("Step 2: Removing rows with -9999 values in key columns...")
    filtered_climate_df = filtered_climate_df[
        (filtered_climate_df['HLY-TEMP-NORMAL'] != -9999) &
        (filtered_climate_df['HLY-TEMP-10PCTL'] != -9999) &
        (filtered_climate_df['HLY-TEMP-90PCTL'] != -9999)
    ]
    print(f" - Remaining rows after cleanup: {len(filtered_climate_df)}")

    # Perform the aggregation
    print("Step 3: Grouping and aggregating climate data...")
    aggregated_df = (
        filtered_climate_df
        .groupby(['month', 'day', 'hour', 'state'], as_index=False)
        .agg({
            'HLY-TEMP-NORMAL': 'mean',
            'HLY-TEMP-10PCTL': 'mean',
            'HLY-TEMP-90PCTL': 'mean'
        })
    )
    print(" - Aggregation complete.")

    # Load the combined climate data
    print("Step 4: Loading combined climate data...")
    combined_climate_df = pd.read_csv(combined_climate_file)
    print(f"Loaded combined climate data with {len(combined_climate_df)} rows.")

    # Perform the join
    print("Step 5: Joining aggregated data with combined climate data...")
    joined_df = pd.merge(
        combined_climate_df,
        aggregated_df,
        on=['month', 'day', 'hour', 'state'],
        how='left'
    )
    print(" - Join operation complete.")

    # Save the joined dataset
    output_file = base_directory + "joined_climate_data.csv"
    print("Step 6: Saving the joined dataset...")
    joined_df.to_csv(output_file, index=False)
    print(f"Joined dataset saved to {output_file}")

except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
