import pandas as pd
import os

# File paths
base_directory = "/media/christopher/Extreme SSD/research_project_data"
input_file_path = os.path.join(base_directory, "SAGDP", "combined_sagdp_data.csv")
output_file_path = os.path.join(base_directory, "SAGDP", "cleaned_sagdp_data.csv")

# Columns to drop
columns_to_drop = ["GeoFIPS", "GeoName", "Region", "TableName", "LineCode", "IndustryClassification","Unit"]

# Function to remove specified columns
def clean_sagdp_data(input_file, output_file, columns_to_drop):
    try:
        print(f"Processing file: {input_file}")
        
        # Load the data
        data = pd.read_csv(input_file, dtype=str)
        
        # Drop the specified columns
        cleaned_data = data.drop(columns=columns_to_drop, errors='ignore')
        
        # Save the cleaned data to a new CSV
        cleaned_data.to_csv(output_file, index=False)
        
        print(f"Cleaned data saved to: {output_file}")
    
    except Exception as e:
        print(f"Error processing file {input_file}: {e}")

# Apply the function
clean_sagdp_data(input_file_path, output_file_path, columns_to_drop)
