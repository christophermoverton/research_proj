import pandas as pd

# Define the file path
file_path = '/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/stations-history.txt'

column_specs = [
    (0, 6),    # station_id
    (7, 12),    # wban
    (13, 43),   # station_name
    (43, 48),  # country
    (48, 51),  # state
    (51, 57),  # call
    (57, 65),  # lat
    (65, 74),  # lon
    (74, 82),  # elev
    (82, 91),  # begin
    (91, 100)  # end
  
]
# Define column names based on the sample data
column_names = [
    'station_id', 'wban', 'station_name', 'country', 'state', 'call', 
    'latitude', 'longitude', 'elevation', 'begin', 'end'
]


# Read the text file with fixed-width formatting
df = pd.read_fwf(file_path, colspecs=column_specs, names=column_names)

# Filter to include only US stations
us_stations = df[df['country'] == 'US']

# Save the filtered DataFrame to CSV
csv_path = '/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/us_stations.csv'
us_stations.to_csv(csv_path, index=False)

print(f"Filtered CSV file saved as: {csv_path}")
