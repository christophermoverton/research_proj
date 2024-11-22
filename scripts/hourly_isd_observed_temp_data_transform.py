import os
import pandas as pd
import numpy as np

# Directory where yearly CSV files are stored
yearly_data_dir = "/media/christopher/Extreme SSD/yearly_data"
output_dir = "/media/christopher/Extreme SSD/transformed_data"  # Directory to store transformed files
os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists

# Path to the stations list file
stations_file = "/media/christopher/Extreme SSD/stations_list.csv"

# List of columns to aggregate and calculate the average
aggregation_columns = ["temperature", "wind_speed", "sea_level_pressure", "precip_depth_mm", "relative_humidity"]

# Read the stations data to map station IDs to states if needed
try:
    print("Loading station data...")
    stations_df = pd.read_csv(stations_file)
    stations_df["station_id"] = stations_df["USAF"].astype(str).str.zfill(6) + "-" + stations_df["WBAN"].astype(str).str.zfill(5)
    stations_df = stations_df[["station_id", "STATE"]].rename(columns={"STATE": "state"})
    print("Station data loaded successfully.")
except FileNotFoundError:
    print(f"Error: {stations_file} not found. Ensure the file exists and try again.")
    exit(1)

# Define data types to enforce consistent types and avoid dtype warnings
dtype = {
    "year": "str",  # Using string for consistency
    "date": "str",
    "time": "str",
    "station_id": "str",
    "state": "str",
    "temperature": "float64",
    "wind_speed": "float64",
    "sea_level_pressure": "float64",
    "precip_depth_mm": "float64",
    "relative_humidity": "float64"
}

# Process each yearly data file in chunks
for filename in os.listdir(yearly_data_dir):
    if filename.endswith(".csv") and "weather_data" in filename:
        # Extract the year from the filename
        year = filename.split("_")[2].split(".")[0]
        print(f"\nProcessing file: {filename} for year {year}")

        # Path for the temporary file to store intermediate results
        temp_file = os.path.join(output_dir, f"temp_{year}.csv")

        # Process the file in chunks
        file_path = os.path.join(yearly_data_dir, filename)
        chunk_size = 200000  # Define chunk size based on available memory and file size
        chunk_number = 1  # Initialize chunk counter

        # Initialize temporary file by writing headers
        with open(temp_file, 'w') as f:
            # Create a blank DataFrame with columns for initialization
            columns = ["year", "date", "time", "state"] + aggregation_columns
            pd.DataFrame(columns=columns).to_csv(f, index=False)

        for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=dtype, low_memory=False):
            print(f"  Processing chunk {chunk_number}...")

            # Replace 9999.9 with NaN in sea_level_pressure to handle missing values
            chunk["sea_level_pressure"] = chunk["sea_level_pressure"].replace(9999.9, np.nan)

            # Merge with stations data to add the state column if necessary
            if "state" not in chunk.columns:
                print("    Adding state information to chunk...")
                chunk = chunk.merge(stations_df, on="station_id", how="left")

            # Impute missing values using median for faster processing (column-wise)
            for col in aggregation_columns:
                if chunk[col].isna().all():
                    continue  # Skip entirely NaN columns to avoid warnings
                chunk[col] = chunk[col].fillna(chunk[col].median())

            # Drop rows only if all aggregation columns are NaN
            chunk_cleaned = chunk.dropna(subset=aggregation_columns, how='all')

            # Group by year, date, time, and state, and calculate the average for each specified column
            chunk_grouped = (
                chunk_cleaned.groupby(["year", "date", "time", "state"])[aggregation_columns]
                .mean()
                .reset_index()
            )

            # Append the processed chunk to the temporary file
            with open(temp_file, 'a') as f:
                chunk_grouped.to_csv(f, header=False, index=False)
            print(f"  Chunk {chunk_number} processed and written to temporary file.")

            # Increment chunk counter and clear the chunk from memory
            chunk_number += 1
            del chunk, chunk_cleaned, chunk_grouped

        # Final aggregation on the temporary file after all chunks are processed
        print(f"Final aggregation for year {year} from temporary file...")
        temp_data = pd.read_csv(temp_file)

        final_aggregated_data = (
            temp_data.groupby(["year", "date", "time", "state"])[aggregation_columns]
            .mean()
            .reset_index()
        )

        # Save the final aggregated data to the output file
        output_file = os.path.join(output_dir, f"weather_data_transform_{year}.csv")
        final_aggregated_data.to_csv(output_file, index=False)
        print(f"Transformed data for year {year} saved to {output_file}")

        # Remove the temporary file to free up disk space
        os.remove(temp_file)
        print(f"Temporary file {temp_file} deleted.")
