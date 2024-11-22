import os
import pandas as pd
import numpy as np

# Base directory for ISD data
base_dir = "/media/christopher/Extreme SSD/"
stations_file = os.path.join(base_dir, "stations_list.csv")
output_dir = os.path.join(base_dir, "yearly_data")  # Directory to store yearly CSV files

# Create output directory if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Load station IDs from stations_list.csv and create a combined identifier "USAF-WBAN"
try:
    stations_df = pd.read_csv(stations_file)
    stations_df["station_id"] = stations_df["USAF"].astype(str).str.zfill(6) + "-" + stations_df["WBAN"].astype(str).str.zfill(5)
    station_ids = stations_df["station_id"].tolist()
    print(f"Loaded {len(station_ids)} station IDs from {stations_file}")
    print(f"Sample station IDs: {station_ids[:5]}")  # Print a sample of loaded station IDs for verification
except FileNotFoundError:
    print(f"Error: {stations_file} not found. Ensure the file exists and try again.")
    exit(1)

# Define the specific directories to search for ISD files
directories = [
    "isd_2017_c20201205T220841",
    "isd_2018_c20201205T083740",
    "isd_2019_c20201023T133240",
    "isd_2020_c20210226T222709",
    "isd_2021_c20220203T204923",
    "isd_2022_c20230109T224155",
    "isd_2023_c20240105T134534",
    "isd_2024_c20241113T114142"
]

# Function to parse each ISD file and return a DataFrame
def parse_isd_file(filepath, station_ids):
    # Define column positions based on ISD specifications with 0-based indexing
    colspecs = [
        (0, 4),     # Total variable characters
        (4, 10),    # USAF ID
        (10, 15),   # WBAN ID
        (15, 23),   # Observation date
        (23, 27),   # Observation time
        (65, 69),   # Wind speed (tenths of m/s)
        (87, 92),   # Air temperature (tenths of degrees Celsius)
        (92, 93),   # Air temperature quality code
        (93, 98),   # Dew point temperature (tenths of degrees Celsius)
        (99, 104),  # Sea level pressure (tenths of hPa)
        (104, 105)  # Sea level pressure quality code
    ]
    col_names = [
        "variable_characters", "usaf_id", "wban_id", "date", "time",
        "wind_speed", "temperature", "temp_quality", "dew_point", "sea_level_pressure", "pressure_quality"
    ]

    try:
        # Read data using fixed-width column specifications, skipping bad lines if needed
        df = pd.read_fwf(filepath, colspecs=colspecs, names=col_names, na_values=["+9999", "-9999"], error_bad_lines=False)
    except Exception as e:
        print(f"Error reading file {filepath}: {e}")
        return pd.DataFrame()  # Return empty DataFrame if parsing fails

    # Create a combined station ID "USAF-WBAN" for filtering
    df["station_id"] = df["usaf_id"].astype(str).str.zfill(6) + "-" + df["wban_id"].astype(str).str.zfill(5)

    # Convert date and time to string format to ensure consistent type for merging
    df["date"] = df["date"].astype(str)
    df["time"] = df["time"].astype(str)

    # Filter by combined USAF-WBAN ID directly
    filtered_df = df[df["station_id"].isin(station_ids)]

    # Ensure 'filtered_df' has data before proceeding
    if filtered_df.empty:
        print(f"No matching data found in {filepath} for specified station IDs.")
        return pd.DataFrame()  # Return empty DataFrame to avoid further processing

    # Convert relevant columns to numeric, handling errors gracefully
    numeric_columns = ["temperature", "dew_point", "wind_speed", "sea_level_pressure"]
    for col in numeric_columns:
        filtered_df[col] = pd.to_numeric(filtered_df[col], errors='coerce')

    # Convert temperature, wind speed, dew point, and pressure to their respective units
    try:
        filtered_df["temperature"] = filtered_df["temperature"] / 10.0
        filtered_df["dew_point"] = filtered_df["dew_point"] / 10.0
        filtered_df["wind_speed"] = filtered_df["wind_speed"] / 10.0
        filtered_df["sea_level_pressure"] = filtered_df["sea_level_pressure"] / 10.0
    except Exception as e:
        print(f"Error converting units in file {filepath}: {e}")
        return pd.DataFrame()  # Return empty DataFrame if conversion fails

    # Calculate relative humidity
    try:
        # Use the formula to calculate relative humidity
        filtered_df["relative_humidity"] = 100 * (np.exp((17.625 * filtered_df["dew_point"]) / (filtered_df["dew_point"] + 243.04)) / 
                                                  np.exp((17.625 * filtered_df["temperature"]) / (filtered_df["temperature"] + 243.04)))
    except Exception as e:
        print(f"Error calculating relative humidity in file {filepath}: {e}")
        return pd.DataFrame()  # Return empty DataFrame if calculation fails

    # Select relevant columns
    main_data = filtered_df[["date", "time", "station_id", "temperature", "dew_point", "relative_humidity", 
                             "temp_quality", "wind_speed", "sea_level_pressure", "pressure_quality"]].copy()

    # Ensure date and time columns are of string type in main_data
    main_data["date"] = main_data["date"].astype(str)
    main_data["time"] = main_data["time"].astype(str)

    # Attempt to extract precipitation data if available
    precip_data = []
    with open(filepath, 'r') as file:
        lines = file.readlines()
        for line in lines:
            # Extract and zero-pad USAF and WBAN IDs to create combined station ID
            usaf_id = line[4:10].zfill(6)
            wban_id = line[10:15].zfill(5)
            station_id = f"{usaf_id}-{wban_id}"

            if station_id in station_ids:
                # Check for 'AA1' indicator in the line to confirm presence of precipitation data
                aa1_index = line.find('AA1')
                if aa1_index != -1:
                    # Ensure sufficient length before extracting fields to avoid out-of-bounds errors
                    if len(line) > aa1_index + 11:
                        try:
                            # Extract period quantity and precipitation depth
                            period_quantity = int(line[aa1_index + 3:aa1_index + 5])  # Hours
                            precip_depth = int(line[aa1_index + 5:aa1_index + 9]) / 10.0  # Depth in mm
                            precip_data.append({
                                "station_id": station_id,
                                "date": line[15:23],  # date portion
                                "time": line[23:27],  # time portion
                                "precip_period_hours": period_quantity,
                                "precip_depth_mm": precip_depth
                            })
                        except ValueError:
                            print(f"Error parsing precipitation data in file {filepath} for line: {line}")
                    else:
                        print(f"Line too short for precipitation parsing in file {filepath}: {line}")

    # If precipitation data exists, create DataFrame and ensure 'date' and 'time' are strings for merging
    if precip_data:
        precip_df = pd.DataFrame(precip_data)
        precip_df["date"] = precip_df["date"].astype(str)  # Ensure date is string for consistent merging
        precip_df["time"] = precip_df["time"].astype(str)  # Ensure time is string for consistent merging
        full_data = pd.merge(main_data, precip_df, on=["station_id", "date", "time"], how="left")
    else:
        full_data = main_data

    return full_data


# Process each file individually and append results to the correct year-based CSV
for directory in directories:
    directory_path = os.path.join(base_dir, directory)
    
    if not os.path.exists(directory_path):
        print(f"Directory {directory_path} does not exist. Skipping.")
        continue
    
    for filename in os.listdir(directory_path):
        filepath = os.path.join(directory_path, filename)
        # Extract the combined station ID prefix "USAF-WBAN" from the filename
        usaf_id_prefix = filename[:6].zfill(6)
        wban_id_prefix = filename[7:12].zfill(5)
        station_id_prefix = f"{usaf_id_prefix}-{wban_id_prefix}"
        
        if station_id_prefix in station_ids:
            try:
                station_data = parse_isd_file(filepath, station_ids)
                if not station_data.empty:
                    # Extract the year from the date column (assuming date format is YYYYMMDD)
                    station_data["year"] = station_data["date"].str[:4]
                    
                    # Write each chunk to the appropriate yearly file
                    for year, year_data in station_data.groupby("year"):
                        year_file = os.path.join(output_dir, f"weather_data_{year}.csv")
                        # Append data to the year file
                        year_data.to_csv(year_file, mode='a', header=not os.path.exists(year_file), index=False)
                print(f"Processed and appended file: {filename}")
            except Exception as e:
                print(f"Error processing file {filename}: {e}")
