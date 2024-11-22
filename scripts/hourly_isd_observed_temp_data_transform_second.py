import os
import pandas as pd

# Directory where the first round of transformed CSV files are stored
transformed_data_dir = "/media/christopher/Extreme SSD/transformed_data"
output_dir = "/media/christopher/Extreme SSD/final_transformed_data"  # Directory to store final transformed files
os.makedirs(output_dir, exist_ok=True)  # Ensure the output directory exists

# Define the columns we want to aggregate and calculate the average
aggregation_columns = ["temperature", "wind_speed", "sea_level_pressure", "precip_depth_mm", "relative_humidity"]

# Define data types to enforce consistent types and avoid dtype warnings
dtype = {
    "year": "str",  # Using string for consistency
    "date": "str",
    "time": "str",
    "state": "str",
    "temperature": "float64",
    "wind_speed": "float64",
    "sea_level_pressure": "float64",
    "precip_depth_mm": "float64",
    "relative_humidity": "float64"
}

# Function to convert time to hour bucket
def convert_time_to_hour_bucket(time_str):
    try:
        minutes = int(time_str[-2:])  # Extract the last two digits (minutes)
        hours = int(time_str[:-2]) if len(time_str) > 2 else 0  # Extract the first digits (hour part)

        # Round to the next hour if minutes >= 50
        if minutes >= 50:
            hours += 1

        # If the hour exceeds 24, cap it at 24
        if hours == 25:
            hours = 24

        return hours
    except ValueError:
        print(f"Invalid time format: {time_str}")
        return 0

# Process each transformed data file in chunks
for filename in os.listdir(transformed_data_dir):
    if filename.endswith(".csv") and "transform" in filename:
        # Extract the year from the filename
        year = filename.split("_")[3].split(".")[0]
        print(f"\nProcessing file: {filename} for year {year}")

        # Path for the temporary file to store intermediate results
        temp_file = os.path.join(output_dir, f"temp_final_{year}.csv")

        # Process the file in chunks
        file_path = os.path.join(transformed_data_dir, filename)
        chunk_size = 100000  # Define chunk size based on available memory and file size
        chunk_number = 1  # Initialize chunk counter

        # Initialize temporary file by writing headers
        with open(temp_file, 'w') as f:
            columns = ["year", "date", "hour", "state"] + aggregation_columns
            pd.DataFrame(columns=columns).to_csv(f, index=False)

        for chunk in pd.read_csv(file_path, chunksize=chunk_size, dtype=dtype, low_memory=False):
            print(f"  Processing chunk {chunk_number}...")

            # Convert time to hourly buckets
            chunk["hour"] = chunk["time"].apply(convert_time_to_hour_bucket)

            # Group by year, date, hour, and state, and calculate the average for each specified column
            chunk_grouped = (
                chunk.groupby(["year", "date", "hour", "state"])[aggregation_columns]
                .mean()
                .reset_index()
            )

            # Append the processed chunk to the temporary file
            with open(temp_file, 'a') as f:
                chunk_grouped.to_csv(f, header=False, index=False)
            print(f"  Chunk {chunk_number} processed and written to temporary file.")

            # Increment chunk counter and clear the chunk from memory
            chunk_number += 1
            del chunk, chunk_grouped

        # Final aggregation on the temporary file after all chunks are processed
        print(f"Final aggregation for year {year} from temporary file...")
        temp_data = pd.read_csv(temp_file)

        final_aggregated_data = (
            temp_data.groupby(["year", "date", "hour", "state"])[aggregation_columns]
            .mean()
            .reset_index()
        )

        # Save the final aggregated data to the output file
        output_file = os.path.join(output_dir, f"weather_data_final_transform_{year}_hourly.csv")
        final_aggregated_data.to_csv(output_file, index=False)
        print(f"Final transformed data for year {year} saved to {output_file}")

        # Remove the temporary file to free up disk space
        os.remove(temp_file)
        print(f"Temporary file {temp_file} deleted.")
