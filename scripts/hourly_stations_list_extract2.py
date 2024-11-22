import requests
import pandas as pd
from datetime import datetime

# NOAA API Configuration
NOAA_API_TOKEN = 'Your_NOAA_API_TOKEN'  # Replace with your NOAA API token
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/stations"
HEADERS = {'token': NOAA_API_TOKEN}

# Define date range, FIPS codes for each state, and dataset filter
START_DATE = '2017-01-01'
END_DATE = '2024-12-31'
MIN_COVERAGE = 0.9  # Minimum data coverage threshold
DATASET_ID = 'GHCND'  # Specify GHCND dataset
STATE_FIPS_CODES = {
    'CA': '06',  # California
    'TX': '48', 'FL': '12', 'NY': '36'
}

def read_ghcnd_inventory(file_path='/home/christopher/research_proj/ghcnd-inventory.txt'):
    """Read GHCND inventory and filter for stations with TOBS data."""
    inventory = pd.read_csv(
        file_path, 
        delim_whitespace=True, 
        names=['station_id', 'latitude', 'longitude', 'element', 'start_year', 'end_year']
    )
    print(inventory.head())
    # Filter for stations with 'TOBS' in the element column
    tobs_stations = inventory[inventory['element'] == 'TOBS']['station_id'].unique()
    return set(tobs_stations)

def fetch_stations_for_state(fips_code, start_date, end_date, min_coverage, dataset_id, tobs_stations):
    """Fetch all stations for a given state FIPS code and filter by dataset, coverage, date range, and TOBS availability."""
    all_stations = []
    offset = 1
    limit = 1000  # Maximum allowed by NOAA API
    
    while True:
        params = {
            'locationid': f'FIPS:{fips_code}',
            'datasetid': dataset_id,
            'limit': limit,
            'offset': offset
        }
        
        response = requests.get(BASE_URL, headers=HEADERS, params=params)
        
        try:
            response.raise_for_status()
            data = response.json()
            results = data.get('results', [])
            
            if not results:
                print(f"No more data available for FIPS {fips_code}. Pagination complete.")
                break
            
            # Filter stations based on date range, data coverage, and TOBS availability
            for station in results:
                station_id = station['id'].split(':')[-1]  # Extract the station number only
                mindate = station.get('mindate')
                maxdate = station.get('maxdate')
                datacoverage = station.get('datacoverage', 0)
                
                # Ensure the station's data coverage, date range, and TOBS inclusion meet requirements
                if (datacoverage >= min_coverage and mindate and maxdate and station_id in tobs_stations):
                    mindate_dt = datetime.strptime(mindate, '%Y-%m-%d')
                    maxdate_dt = datetime.strptime(maxdate, '%Y-%m-%d')
                    start_date_dt = datetime.strptime(start_date, '%Y-%m-%d')
                    end_date_dt = datetime.strptime(end_date, '%Y-%m-%d')
                    
                    # Check if station's date range fully covers the requested date range
                    if mindate_dt <= start_date_dt and maxdate_dt >= end_date_dt:
                        all_stations.append(station)
            
            print(f"Retrieved {len(results)} stations with offset {offset} for FIPS {fips_code}")
            offset += limit
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for FIPS {fips_code}: {e}")
            break
    
    return all_stations

def format_station_data(stations, state_abbr):
    """Format station data to include only relevant details and the state abbreviation."""
    formatted_stations = []
    for station in stations:
        station_id = f"COOP:{station['id'].split(':')[-1]}"  # Extract the station number in COOP format
        station_name = station.get('name', 'Unknown')
        latitude = station.get('latitude')
        longitude = station.get('longitude')
        mindate = station.get('mindate')
        maxdate = station.get('maxdate')
        datacoverage = station.get('datacoverage')
        
        formatted_stations.append({
            'station_id': station_id,
            'state': state_abbr,
            'name': station_name,
            'latitude': latitude,
            'longitude': longitude,
            'mindate': mindate,
            'maxdate': maxdate,
            'datacoverage': datacoverage
        })
    return formatted_stations

def main():
    # Read GHCND inventory and filter for stations with TOBS data
    tobs_stations = read_ghcnd_inventory()
    print(f"Total stations with TOBS in GHCND inventory: {len(tobs_stations)}")
    
    # Collect all formatted station data for the specified states
    all_formatted_stations = []
    
    for state_abbr, fips_code in STATE_FIPS_CODES.items():
        print(f"Fetching stations for {state_abbr} with FIPS code {fips_code}")
        stations = fetch_stations_for_state(fips_code, START_DATE, END_DATE, MIN_COVERAGE, DATASET_ID, tobs_stations)
        formatted_stations = format_station_data(stations, state_abbr)
        all_formatted_stations.extend(formatted_stations)
    
    # Convert to DataFrame and display or save results
    stations_df = pd.DataFrame(all_formatted_stations)
    print(f"Total high-coverage TOBS stations retrieved: {len(stations_df)}")
    
    # Optionally save to CSV
    output_file = "high_coverage_tobs_stations.csv"
    stations_df.to_csv(output_file, index=False)
    print(f"Filtered TOBS station data saved to {output_file}")

if __name__ == "__main__":
    main()
