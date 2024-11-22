import requests
import pandas as pd

# NOAA API Configuration
NOAA_API_TOKEN = 'Your_NOAA_API_TOKEN' # Replace with your NOAA API token
BASE_URL = "https://www.ncei.noaa.gov/cdo-web/api/v2/stations"
HEADERS = {'token': NOAA_API_TOKEN}

# Define the FIPS codes for each state
STATE_FIPS_CODES = {
    'CA': '06',  # California
    'TX': '48',  # Texas
    'FL': '12',  # Florida
    'NY': '36'   # New York
}

def fetch_stations_for_state(fips_code):
    """Fetch all stations for a given state FIPS code using NOAA's station endpoint."""
    all_stations = []
    offset = 1
    limit = 1000  # Maximum allowed by NOAA API
    
    while True:
        params = {
            'locationid': f'FIPS:{fips_code}',
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
            
            all_stations.extend(results)
            print(f"Retrieved {len(results)} stations with offset {offset} for FIPS {fips_code}")
            offset += limit
        
        except requests.exceptions.RequestException as e:
            print(f"Request failed for FIPS {fips_code}: {e}")
            break
    
    return all_stations

def format_station_data(stations, state_abbr):
    """Format station data to include only the station ID in COOP format and the state abbreviation."""
    formatted_stations = []
    for station in stations:
        # Ensure station ID format as 'COOP:station_number' and add state abbreviation
        station_id = f"COOP:{station['id'].split(':')[-1]}"  # Extract the station number
        station_name = station.get('name', 'Unknown')
        formatted_stations.append({
            'station_id': station_id,
            'state': state_abbr,
            'name': station_name,
            'latitude': station.get('latitude'),
            'longitude': station.get('longitude')
        })
    return formatted_stations

def main():
    # Collect all formatted station data for the specified states
    all_formatted_stations = []
    
    for state_abbr, fips_code in STATE_FIPS_CODES.items():
        print(f"Fetching stations for {state_abbr} with FIPS code {fips_code}")
        stations = fetch_stations_for_state(fips_code)
        formatted_stations = format_station_data(stations, state_abbr)
        all_formatted_stations.extend(formatted_stations)
    
    # Convert to DataFrame and display or save results
    stations_df = pd.DataFrame(all_formatted_stations)
    print(f"Total stations retrieved: {len(stations_df)}")
    
    # Optionally save to CSV
    output_file = "filtered_stations2.csv"
    stations_df.to_csv(output_file, index=False)
    print(f"Station data saved to {output_file}")

if __name__ == "__main__":
    main()
