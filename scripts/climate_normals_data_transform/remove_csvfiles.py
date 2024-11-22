import os
from glob import glob

def delete_csv_files(directory):
    # Define the path pattern for all CSV files in the directory
    csv_files = glob(os.path.join(directory, "*.csv"))
    
    # Check if there are any CSV files in the directory
    if not csv_files:
        print("No CSV files found in the specified directory.")
        return
    
    # Loop through each file and delete it
    for file_path in csv_files:
        try:
            os.remove(file_path)
            print(f"Deleted: {file_path}")
        except Exception as e:
            print(f"Error deleting {file_path}: {e}")

# Usage example
directory_path = "/home/christopher/research_proj/datasets/climate_normals_datasets/extracted/isdlite"
delete_csv_files(directory_path)