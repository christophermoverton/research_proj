import os
import pandas as pd
from datetime import datetime

# Define base location for station files and output directory
BASE_LOC = '/home/christopher/Downloads/ghcnd_all/ghcnd_all'
BASE_OUTPUT_DIR = os.path.abspath('research_project_data')
CLIMATE_DIR = 'climate_data'
os.makedirs(os.path.join(BASE_OUTPUT_DIR, CLIMATE_DIR), exist_ok=True)

# Define target fields
FIELDS = ['TOBS', 'PRCP']  # Specify variables of interest

def load_stations_from_csv(csv_path):
    """Load station information from `high_coverage_stations.csv`."""
    stations_df = pd.read_csv(csv_path)
    print(f"Loaded {len(stations_df)} stations from {csv_path}")
    return stations_df

def parse_dly_file(file_path, station_id):
    """Parse the `.dly` file for specific variables (TOBS, PRCP)."""
    # Initialize an empty list to collect rows
    data_rows = []
    
    try:
        with open(file_path, 'r') as file:
            for line in file:
                # Each line represents one element (e.g., PRCP, TOBS) for one month
                element = line[17:21].strip()  # Get the element type (e.g., PRCP, TOBS)
                
                # Only process lines with required elements
                if element in FIELDS:
                    year = line[11:15]
                    month = line[15:17]
                    
                    # Extract 31-day values for the month
                    for day in range(31):
                        value = line[21 + day * 8:26 + day * 8].strip()  # Read each 5-character field
                        measurement_flag = line[26 + day * 8:27 + day * 8].strip()
                        
                        # Skip missing values indicated by -9999
                        if value != '-9999':
                            date = f"{year}-{month}-{str(day + 1).zfill(2)}"
                            data_rows.append({
                                'station_id': station_id,
                                'date': date,
                                'element': element,
                                'value': int(value),
                                'flag': measurement_flag
                            })
    
    except FileNotFoundError:
        print(f"File not found: {file_path}")
    
    return pd.DataFrame(data_rows)

def main():
    # Load station data from CSV
    stations_csv_path = "high_coverage_tobs_stations.csv"  # Path to pre-generated filtered_stations.csv
    stations_df = load_stations_from_csv(stations_csv_path)
    
    # Prepare output file and write headers
    output_file = os.path.join(BASE_OUTPUT_DIR, CLIMATE_DIR, "observed_weather_data.csv")
    header_written = False
    
    # Process each station individually
    for _, row in stations_df.iterrows():
        station_id = row['station_id'].split(":")[-1]  # Strip prefix, leaving just station ID
        state = row['state']
        file_path = os.path.join(BASE_LOC, f"{station_id}.dly")
        
        print(f"Processing data for station: {station_id} in state: {state}")
        
        # Parse .dly file for the station
        weather_data = parse_dly_file(file_path, station_id)
        
        if not weather_data.empty:
            # Add state to data
            weather_data['state'] = state
            weather_data['date'] = pd.to_datetime(weather_data['date'])
            
            # Pivot the data to have each element (TOBS, PRCP) as a separate column
            pivoted_data = weather_data.pivot_table(
                index=['date', 'station_id', 'state'],
                columns='element',
                values='value',
                aggfunc='first'
            ).reset_index()
            
            # Ensure "TOBS" and "PRCP" columns are present, fill with NaN if missing
            for column in FIELDS:
                if column not in pivoted_data.columns:
                    pivoted_data[column] = pd.NA
            
            # Flatten columns after pivoting
            pivoted_data.columns.name = None
            
            # Write data to CSV file
            mode = 'a' if header_written else 'w'
            pivoted_data.to_csv(output_file, mode=mode, header=not header_written, index=False)
            header_written = True  # Set header_written to True after the first write
        else:
            print(f"No data available for station {station_id} in state {state}.")

    print(f"Observed weather data for all stations saved incrementally to {output_file}")

if __name__ == "__main__":
    main()
