import pandas as pd
import os

# File paths
base_directory = "/media/christopher/Extreme SSD/final_transformed_data/"
input_file_path = os.path.join(base_directory, "energy_weather_climate_combined.csv")
output_file_path = os.path.join(base_directory, "cleaned_energy_weather_climate_combined.csv")

# Columns to check for empty values
columns_to_check = [
    "temperature",
    "wind_speed",
    "sea_level_pressure",
    "precip_depth_mm",
    "relative_humidity",
    "HLY-CLDH-NORMAL",
    "HLY-HTDH-NORMAL",
    "HLY-HIDX-NORMAL",
    "HLY-WCHL-NORMAL",
    "HLY-TEMP-NORMAL",
    "HLY-TEMP-10PCTL",
    "HLY-TEMP-90PCTL"
]

# Columns to drop
columns_to_drop = ["period", "respondent", "respondent-name", "type-name", "value-units"]

# Function to clean the data
def clean_combined_data(input_file, output_file, columns_to_check, columns_to_drop):
    try:
        print(f"Processing file: {input_file}")
        
        # Load the data
        data = pd.read_csv(input_file, dtype=str)  # Load as string to avoid type errors
        
        # Drop rows where all specified columns are NaN or empty
        cleaned_data = data.dropna(subset=columns_to_check, how='all')
        
        # Drop specified columns
        cleaned_data = cleaned_data.drop(columns=columns_to_drop, errors='ignore')
        
        # Save the cleaned data to a new CSV
        cleaned_data.to_csv(output_file, index=False)
        
        print(f"Cleaned data saved to: {output_file}")
    
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

# Apply the function
clean_combined_data(input_file_path, output_file_path, columns_to_check, columns_to_drop)
