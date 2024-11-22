import pandas as pd
from datetime import datetime

# Define the base directory and the path to isd-history.csv
base_dir = "/media/christopher/Extreme SSD/"
history_file = f"{base_dir}/isd-history.csv"

# Define the states to filter by and the start/end dates
states = ["CA", "TX", "FL", "NY"]
start_date = "20170101"  # Start date in YYYYMMDD format
end_date = "20241231"    # End date in YYYYMMDD format

# Function to filter stations based on state and date range
def filter_stations(history_file, states, start_date, end_date):
    # Read the isd-history.csv file
    df = pd.read_csv(history_file)

    # Ensure BEGIN and END are treated as dates for filtering
    df["BEGIN"] = pd.to_datetime(df["BEGIN"], format="%Y%m%d")
    df["END"] = pd.to_datetime(df["END"], format="%Y%m%d")

    # Convert start_date and end_date to datetime objects for comparison
    start_date = datetime.strptime(start_date, "%Y%m%d")
    end_date = datetime.strptime(end_date, "%Y%m%d")

    # Filter for stations in specified states with data availability in the specified date range
    filtered_df = df[
        (df["STATE"].isin(states)) &
        (df["BEGIN"] <= end_date) &
        (df["END"] >= start_date)
    ]

    # Select only the columns relevant for the station list output
    station_list = filtered_df[["USAF", "WBAN", "STATION NAME", "CTRY", "STATE", "ICAO", "LAT", "LON", "ELEV(M)", "BEGIN", "END"]]

    # Save to CSV
    output_file = f"{base_dir}/stations_list.csv"
    station_list.to_csv(output_file, index=False)
    print(f"Filtered station list saved to {output_file}")

# Run the function
filter_stations(history_file, states, start_date, end_date)
