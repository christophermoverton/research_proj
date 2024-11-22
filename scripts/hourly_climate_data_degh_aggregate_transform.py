import os
import pandas as pd

# Define file paths
base_dir = "/media/christopher/Extreme SSD/research_project_data/climate_data"
input_file = os.path.join(base_dir, "degh_filtered.csv")
output_file = os.path.join(base_dir, "degh_filtered_aggregated.csv")

# Columns to group by and to aggregate
group_by_columns = ["month", "day", "hour", "state"]
aggregation_columns = ["HLY-CLDH-NORMAL", "HLY-HTDH-NORMAL"]

# Load the data with specified data types to avoid dtype warnings
dtype = {
    "GHCN_ID": "str",
    "month": "int",
    "day": "int",
    "hour": "int",
    "HLY-CLDH-NORMAL": "float64",
    "meas_flag_HLY-CLDH-NORMAL": "str",
    "comp_flag_HLY-CLDH-NORMAL": "str",
    "years_HLY-CLDH-NORMAL": "int",
    "HLY-HTDH-NORMAL": "float64",
    "meas_flag_HLY-HTDH-NORMAL": "str",
    "comp_flag_HLY-HTDH-NORMAL": "str",
    "years_HLY-HTDH-NORMAL": "int",
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

# Drop rows where HLY-CLDH-NORMAL or HLY-HTDH-NORMAL are -9999
print("Dropping rows with -9999 in HLY-CLDH-NORMAL or HLY-HTDH-NORMAL...")
data = data[(data["HLY-CLDH-NORMAL"] != -9999) & (data["HLY-HTDH-NORMAL"] != -9999)]

# Group by month, day, hour, and state, then calculate the average of HLY-CLDH-NORMAL and HLY-HTDH-NORMAL
print("Aggregating data...")
aggregated_data = (
    data.groupby(group_by_columns)[aggregation_columns]
    .mean()  # Calculate the average for each group
    .reset_index()
)

# Save the aggregated data to a new CSV file
print("Saving aggregated data...")
aggregated_data.to_csv(output_file, index=False)
print(f"Aggregated data saved to {output_file}")
