import os
import pandas as pd
from sklearn.impute import KNNImputer
import numpy as np

# Define base directory paths relative to the project root
PROJECT_ROOT = os.path.abspath(os.path.dirname(__file__))  # Adjusts to the directory of this script
DATA_DIR = os.path.join(PROJECT_ROOT, "../research_project_data", "climate_data")

# Define the input and output file paths
input_file = os.path.join(DATA_DIR, "observed_weather_data.csv")  # Path to the original data
output_file = os.path.join(DATA_DIR, "filtered_imputed_tobs_observed_weather_data.csv")  # Path to save the filtered and imputed data

# Define the date range for filtering
start_date = '2022-01-01'
end_date = '2023-12-31'

# Chunk size for processing (adjust as needed)
chunk_size = 70000
total_records_processed = 0  # To keep track of the total records processed

def remove_stations_with_excessive_missing(chunk, threshold=50):
    """Remove stations with more than `threshold` missing TOBS records."""
    missing_tobs_counts = chunk['TOBS'].isna().groupby(chunk['station_id']).sum()
    stations_to_remove = missing_tobs_counts[missing_tobs_counts > threshold].index
    return chunk[~chunk['station_id'].isin(stations_to_remove)]

# KNN Imputation function for TOBS only
def knn_impute_tobs(chunk, n_neighbors=3):
    """Impute missing values in TOBS using K-Nearest Neighbors, for gaps of 7 or fewer missing values."""
    if chunk['TOBS'].notna().sum() > 0 and chunk['TOBS'].isna().sum() > 0:
        imputer = KNNImputer(n_neighbors=n_neighbors)
        imputed_values = imputer.fit_transform(chunk[['TOBS']])
        if len(imputed_values) == len(chunk):
            chunk = chunk.copy()
            chunk['TOBS'] = imputed_values.flatten()
    return chunk

# Moving average imputation function for TOBS only
def moving_average_impute_tobs(chunk, window=7):
    """Apply a moving average over a seven-day window to further smooth TOBS data."""
    chunk['TOBS'] = chunk['TOBS'].fillna(chunk['TOBS'].rolling(window=window, min_periods=1, center=True).mean())
    return chunk

# Conversion functions for TOBS and PRCP values
def convert_tobs_to_fahrenheit(chunk):
    """Convert TOBS from tenths of Celsius to Fahrenheit."""
    chunk['TOBS'] = chunk['TOBS'].apply(lambda x: (x / 10) * 1.8 + 32 if pd.notna(x) else x)
    return chunk

def convert_prcp_to_inches(chunk):
    """Convert PRCP from millimeters to inches."""
    chunk['PRCP'] = chunk['PRCP'].apply(lambda x: x * 0.0393701 if pd.notna(x) else x)
    return chunk

# Prepare output file with headers only once
header_written = False

# Process data in chunks
for i, chunk in enumerate(pd.read_csv(input_file, parse_dates=['date'], chunksize=chunk_size), start=1):
    # Filter by date range
    chunk = chunk[(chunk['date'] >= start_date) & (chunk['date'] <= end_date)]
    
    # Check if the chunk is empty after filtering
    if chunk.empty:
        print(f"Skipping empty chunk {i}.")
        continue  # Skip to the next chunk if this one is empty
    
    print(f"Processing chunk {i} with {len(chunk)} records in date range {start_date} to {end_date}")
    
    # Remove stations with more than 50 missing TOBS records
    chunk = remove_stations_with_excessive_missing(chunk, threshold=50)
    
    # Check if the chunk is empty after removing stations
    if chunk.empty:
        print(f"Skipping chunk {i} after removing stations with excessive missing TOBS records.")
        continue  # Skip to the next chunk if all stations were removed
    
    # Sort chunk by station and date for smooth imputation
    chunk = chunk.sort_values(by=['station_id', 'date'])
    
    # Apply KNN Imputation to TOBS
    chunk = knn_impute_tobs(chunk)
    
    # Apply Moving Average Imputation to TOBS
    chunk = moving_average_impute_tobs(chunk)
    
    # Convert TOBS from tenths of Celsius to Fahrenheit
    chunk = convert_tobs_to_fahrenheit(chunk)
    
    # Convert PRCP from millimeters to inches
    chunk = convert_prcp_to_inches(chunk)
    
    # Save the processed chunk to the output file incrementally
    mode = 'a' if header_written else 'w'
    chunk.to_csv(output_file, mode=mode, index=False, header=not header_written)
    header_written = True  # Ensure header is only written once
    
    # Update total records processed and print status
    total_records_processed += len(chunk)
    print(f"Chunk {i} processed. Total records processed so far: {total_records_processed}")

print(f"All chunks processed. Filtered and imputed TOBS data saved to {output_file}")
print(f"Total records processed: {total_records_processed}")
