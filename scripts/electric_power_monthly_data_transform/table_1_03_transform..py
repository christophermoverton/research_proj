import pandas as pd
import boto3
import io
import re

# S3 configurations
bucket_name = 'research-project-cenergy'
output_base_key = 'electric_power_monthly_data/processed_'

# List of month names in the correct order
months = ["january", "february", "march", "april", "may", "june", 
          "july", "august", "september", "october", "november", "december"]

# List of table filenames to process
table_files = [
    "1_03_A", "1_03_B", "1_04_A", "1_04_B", "1_05_A", "1_05_B",
    "1_06_A", "1_06_B", "1_07_A", "1_07_B", "1_08_A", "1_08_B",
    "1_10_A", "1_10_B", "1_11_A", "1_11_B", "1_17_A", 
    "1_17_B", "1_18_A", "1_18_B"
]

# Initialize S3 client
s3 = boto3.client('s3')

# Track processed (month, year, table) to avoid redundant data
processed_months_years = set()
for month in months[9:]:
    for table_file in table_files:
        processed_months_years.add((month,2024,table_file))
# Initialize a dictionary to store DataFrames for each table file
all_data = {table_file: [] for table_file in table_files}

# Function to load Excel file from S3
def load_excel_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        xls = pd.ExcelFile(io.BytesIO(file_content))
        df = pd.read_excel(xls, sheet_name=0, header=5)
        return df
    except s3.exceptions.NoSuchKey:
        print(f"File not found: {key}")
        return None

# Function to process a single table and return the transformed DataFrame
def process_table(df):
    # Rename 'Census Division\nand State' column to 'Census Division and State'
    df = df.rename(columns={'Census Division\nand State': 'Census Division and State'})

    # Filter out rows starting with and after "U.S. Total"
    us_total_index = df[df['Census Division and State'] == 'U.S. Total'].index
    if not us_total_index.empty:
        df = df.loc[:us_total_index[0] - 1]

    # Drop the "Percentage Change" column
    df = df.drop(columns=["Percentage\nChange"], errors="ignore")

    # Melt the DataFrame to bring all monthly columns into one column
    df_melted = df.melt(
        id_vars=["Census Division and State"],
        var_name="Original Month-Year Column",
        value_name="Monthly Power Generated"
    )

    # Extract month, year, sector type, and subsector from the column names
    def extract_date_sector(row):
        # Extract month and year from the 'Original Month-Year Column' using regex
        match = re.search(r'(\w+)\s(\d{4})', row["Original Month-Year Column"])
        if match:
            month = match.group(1)
            year = int(match.group(2))
        else:
            month = None
            year = None

        sector_type = None
        subsector = None

        # Assign Sector Type and Subsector based on the column's position
        col_index = df.columns.get_loc(row["Original Month-Year Column"])

        if col_index in [1, 2, 3]:  # First columns for "All Sectors"
            sector_type = "All"
        elif col_index in [4, 5]:  # Electric Power Sector -> Electric Utilities
            sector_type = "Electric Power"
            subsector = "Electric Utilities"
        elif col_index in [6, 7]:  # Electric Power Sector -> Independent Power Producers
            sector_type = "Electric Power"
            subsector = "Independent Power Producers"
        elif col_index in [8, 9]:  # Commercial Sector
            sector_type = "Commercial"
        elif col_index in [10, 11]:  # Industrial Sector
            sector_type = "Industrial"

        return pd.Series([month, year, sector_type, subsector])

    # Apply the function to create 'Month', 'Year', 'Sector Type', and 'Subsector' columns
    df_melted[['Month', 'Year', 'Sector Type', 'Subsector']] = df_melted.apply(extract_date_sector, axis=1)

    # Drop the temporary 'Original Month-Year Column' as it’s no longer needed
    df_melted = df_melted.drop(columns=["Original Month-Year Column"])

    return df_melted

# Iterate over each year, month, and table file to collect data
for year in range(2024, 2017, -1):  # Descending order from 2024 to 2018
    for month_name in months:
        for table_file in table_files:
            # Check if this (month, year, table) has already been processed
            if (month_name, year, table_file) in processed_months_years:
                continue

            # Construct the S3 key for this file
            file_key = f"electric_power_monthly_data/{month_name}{year}/Table_{table_file}.xlsx"
            
            # Load the data
            df = load_excel_from_s3(bucket_name, file_key)
            
            if df is not None:
                # Process the table
                df_melted = process_table(df)
                
                # Get unique years from the melted DataFrame
                unique_years = df_melted["Year"].unique()
                
                # For each unique year, add (month, year, table) to processed_months_years
                for unique_year in unique_years:
                    processed_months_years.add((month_name, unique_year, table_file))
                
                # Append data to the correct list based on the table file
                all_data[table_file].append(df_melted)
                
                print(f"Processed data for {month_name} {year} from Table_{table_file}")

# Concatenate and save separate DataFrames for each table
for table_file, data_list in all_data.items():
    if data_list:
        merged_df = pd.concat(data_list, ignore_index=True)
        output_file_key = f"{output_base_key}Table_{table_file}.csv"
        csv_buffer = io.StringIO()
        merged_df.to_csv(csv_buffer, index=False)
        s3.put_object(Bucket=bucket_name, Key=output_file_key, Body=csv_buffer.getvalue())
        print(f"Merged data for Table {table_file} saved to {output_file_key} in the S3 bucket '{bucket_name}'.")
    else:
        print(f"No data collected for Table {table_file}.")
