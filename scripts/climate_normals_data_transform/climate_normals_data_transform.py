import pandas as pd
import os
from glob import glob

# Define the path to the ISD-Lite folder
isdlite_folder = "/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/isdlite"

# Define column specs and column names based on ISD-Lite format
column_specs = [
    (0, 4),    # Year
    (5, 7),    # Month
    (8, 11),   # Day
    (11, 13),  # Hour
    (13, 19),  # Air Temperature
    (19, 24),  # Dew Point Temperature
    (25, 31),  # Sea Level Pressure
    (31, 37),  # Wind Direction
    (37, 43),  # Wind Speed Rate
    (43, 49),  # Sky Condition Total Coverage Code
    (49, 55),  # Liquid Precipitation Depth - One Hour
    (55, 61)   # Liquid Precipitation Depth - Six Hour
]

column_names = [
    "Year", "Month", "Day", "Hour",
    "Air_Temperature", "Dew_Point", "Sea_Level_Pressure",
    "Wind_Direction", "Wind_Speed", "Sky_Condition",
    "Precipitation_One_Hour", "Precipitation_Six_Hour"
]

# List of ISD-Lite files in the directory
file_paths = glob(os.path.join(isdlite_folder, "*"))

# Loop through each file
for file_path in file_paths:
    file_name = os.path.basename(file_path)
    csv_file_path = os.path.join(isdlite_folder, f"{file_name}.csv")
    
    # Read the ISD-Lite fixed-width file with specified column widths
    df = pd.read_fwf(file_path, colspecs=column_specs, names=column_names)
    
    # Apply scaling for relevant columns and handle missing values according to ISD-Lite format
    scaling_factors = {
        "Air_Temperature": 10,
        "Dew_Point": 10,
        "Wind_Speed": 10
    }
    
    # Scale the specified columns and replace missing values (-9999) with NaN
    for col, scale in scaling_factors.items():
        df[col] = pd.to_numeric(df[col], errors='coerce').replace(-9999, pd.NA) / scale

    # Handle special cases for Wind Direction and Calm Winds
    df["Wind_Direction"] = df["Wind_Direction"].replace(-9999, pd.NA)
    df["Wind_Speed"] = df["Wind_Speed"].replace(0, pd.NA)  # Calm winds coded as 0

    # Handle Sky Condition, replacing -9999 as NaN for unknown coverage
    df["Sky_Condition"] = df["Sky_Condition"].replace(-9999, pd.NA)

    # Handle trace precipitation by setting -1 as a trace value (0.1 mm)
    df["Precipitation_One_Hour"] = df["Precipitation_One_Hour"].replace(-1, 0.1).replace(-9999, pd.NA)
    df["Precipitation_Six_Hour"] = df["Precipitation_Six_Hour"].replace(-1, 0.1).replace(-9999, pd.NA)

    # Save the dataframe to a CSV file in the same folder
    df.to_csv(csv_file_path, index=False)
    print(f"File {file_name} has been successfully saved as {csv_file_path}")
