import os
import pandas as pd

# Define file paths
base_dir = "/media/christopher/Extreme SSD/research_project_data/climate_data"
input_file = os.path.join(base_dir, "hidx_filtered.csv")
output_file = os.path.join(base_dir, "hidx_filtered_aggregated.csv")

# Columns to group by and to aggregate
group_by_columns = ["state", "month", "day", "hour"]
aggregation_column = "HLY-HIDX-NORMAL"

# Load the data with the specified column types to avoid dtype warnings
dtype = {
    "GHCN_ID": "str",
    "month": "int",
    "day": "int",
    "hour": "int",
    "HLY-HIDX-NORMAL": "float64",
    "meas_flag_HLY-HIDX-NORMAL": "str",
    "comp_flag_HLY-HIDX-NORMAL": "str",
    "years_HLY-HIDX-NORMAL": "int",
    "station_id": "str",
    "state": "str"
}

# Read the data
print("Loading data...")
try:
    data = pd.read_csv(input_file, dtype=dtype)
    print("Data loaded successfully.")
except FileNotFoundError:
    print(f"Error: {input_file} not found. Ensure the file exists and try again.")
    exit(1)

# Drop rows where HLY-HIDX-NORMAL is -9999
print("Dropping rows with HLY-HIDX-NORMAL = -9999...")
data = data[data[aggregation_column] != -9999]

# Group by state, month, day, and hour, then calculate the average of HLY-HIDX-NORMAL
print("Aggregating data...")
aggregated_data = (
    data.groupby(group_by_columns)[aggregation_column]
    .mean()  # Average is equivalent to mean here
    .reset_index()
)

# Save the aggregated data to a new CSV file
print("Saving aggregated data...")
aggregated_data.to_csv(output_file, index=False)
print(f"Aggregated data saved to {output_file}")
