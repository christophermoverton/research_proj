import pandas as pd

# Define the base directory
base_directory = "/media/christopher/Extreme SSD/"

# Paths to the files
stations_file = base_directory + "filtered_stations2.csv"
climate_data_file = base_directory + "research_project_data/climate_data/hly-temp-allall.csv"

# Chunk size for processing
chunk_size = 100000  # Adjust based on your memory capacity

try:
    # Load the stations file
    print("Step 1: Loading stations data...")
    stations_df = pd.read_csv(stations_file)
    print(f"Loaded stations data with {len(stations_df)} rows.")

    # Split 'station_id' by ':' and take the right part
    print("Step 2: Processing station IDs and extracting state...")
    stations_df['station_id'] = stations_df['station_id'].str.split(':').str[-1].str.strip()

    # Extract necessary columns (station_id and state)
    stations_df = stations_df[['station_id', 'state']].drop_duplicates()
    print(f"Extracted {len(stations_df)} unique station IDs with states.")

    # Extract unique station IDs for filtering
    station_ids = stations_df['station_id'].unique().tolist()
    print("Step 3: Prepared unique station IDs for filtering.")

    # Initialize a counter for processed rows
    total_processed_rows = 0
    total_filtered_rows = 0
    chunk_count = 0

    # Process the climate data in chunks
    print("Step 4: Processing climate data in chunks...")
    filtered_chunks = []

    for chunk in pd.read_csv(climate_data_file, chunksize=chunk_size, header=0, names=[
        "GHCN_ID", "month", "day", "hour",
        "HLY-TEMP-NORMAL", "meas_flag_HLY-TEMP-NORMAL", "comp_flag_HLY-TEMP-NORMAL", "years_HLY-TEMP-NORMAL",
        "HLY-TEMP-10PCTL", "meas_flag_HLY-TEMP-10PCTL", "comp_flag_HLY-TEMP-10PCTL", "years_HLY-TEMP-10PCTL",
        "HLY-TEMP-90PCTL", "meas_flag_HLY-TEMP-90PCTL", "comp_flag_HLY-TEMP-90PCTL", "years_HLY-TEMP-90PCTL"
    ]):
        chunk_count += 1
        total_processed_rows += len(chunk)

        print(f"Processing chunk {chunk_count} with {len(chunk)} rows...")
        filtered_chunk = chunk[chunk['GHCN_ID'].isin(station_ids)]
        filtered_chunks.append(filtered_chunk)

        filtered_rows = len(filtered_chunk)
        total_filtered_rows += filtered_rows
        print(f" - Filtered {filtered_rows} rows in this chunk. Total filtered so far: {total_filtered_rows}")

    print(f"Step 5: Finished processing chunks. Total rows processed: {total_processed_rows}")
    print(f" - Total filtered rows: {total_filtered_rows}")

    # Combine all filtered chunks into a single DataFrame
    print("Step 6: Combining filtered chunks into one DataFrame...")
    filtered_climate_df = pd.concat(filtered_chunks, ignore_index=True)
    print(f"Combined filtered climate data contains {len(filtered_climate_df)} rows.")

    # Merge with station data to include state information
    print("Step 7: Merging climate data with station state information...")
    merged_df = filtered_climate_df.merge(
        stations_df, 
        left_on='GHCN_ID', 
        right_on='station_id', 
        how='left'
    )

    # Drop redundant column if needed
    merged_df = merged_df.drop(columns=['station_id'])
    print(" - Merged data contains state information.")

    # Save the merged climate data to a new CSV file
    output_file = base_directory + "filtered_climate_data_with_state_chunks.csv"
    print("Step 8: Saving the final dataset...")
    merged_df.to_csv(output_file, index=False)
    print(f"Filtered climate data with state saved to {output_file}")

except FileNotFoundError as e:
    print(f"Error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
