import pandas as pd
import os
from glob import glob

# Define the relative path to the data folder
data_folder = "/home/christopher/research_proj/datasets/cbecs_data"

# Collect all Excel file paths that start with 'c' or 'e'
file_paths = sorted(glob(os.path.join(data_folder, '[ce]*.xlsx')))

# Check if files are found
if not file_paths:
    print("No files found in the specified directory with patterns 'c*.xlsx' or 'e*.xlsx'.")
else:
    # Loop through each file and save it as a CSV
    for file_path in file_paths:
        # Read each file, assuming first row as header
        df = pd.read_excel(file_path, header=0)

        # Create a CSV filename by replacing .xlsx with .csv
        csv_file_path = file_path.replace(".xlsx", ".csv")

        # Save the dataframe to a CSV file
        df.to_csv(csv_file_path, index=False)
        print(f"File {file_path} has been successfully saved as {csv_file_path}")
