import pandas as pd
import boto3
import io
import re

# S3 configurations
bucket_name = 'research-project-cenergy'
file_key_template = 'electric_power_monthly_data/{month_name}{year}/Table_6_02_A.xlsx'
output_combined_key = 'electric_power_monthly_data/combined_6_02_A.csv'

# Initialize S3 client
s3 = boto3.client('s3')

# List of months in order
months = ["january", "february", "march", "april", "may", "june", 
          "july", "august", "september", "october", "november", "december"]

# Function to load and process a single table from S3
def load_and_process_table(bucket, month_name, year):
    file_key = file_key_template.format(month_name=month_name, year=year)
    try:
        response = s3.get_object(Bucket=bucket, Key=file_key)
        file_content = response['Body'].read()
    except s3.exceptions.NoSuchKey:
        print(f"File not found: {file_key}")
        return None
    
    # Load Excel file using the specified header rows for multi-level columns
    xls = pd.ExcelFile(io.BytesIO(file_content))
    df = pd.read_excel(xls, sheet_name=0, header=[1, 2])
    
    # Flatten the MultiIndex columns by combining levels
    df.columns = [' '.join(col).strip().replace('\n', ' ') for col in df.columns]
    
    # Rename 'Census Division and State' column if it has variations due to formatting
    df = df.rename(columns={'Census Division\nand State': 'Census Division and State', 
                            'Census Division and State': 'Census Division and State'})

    # Locate the first occurrence of 'U.S. Total' and exclude rows after it
    us_total_index = df[df['Census Division and State'].str.startswith('U.S. Total', na=False)].index
    if not us_total_index.empty:
        df = df.loc[:us_total_index[0] - 1]

    # Melt the DataFrame while keeping 'Census Division and State' intact
    df_melted = df.melt(id_vars=["Census Division and State"], var_name="Technology_Month_Year", value_name="Capacity")
    
    # Extract 'Technology', 'Month', and 'Year' from the 'Technology_Month_Year' column
    def extract_technology_month_year(col_name):
        col_name = col_name.replace('\n', ' ')  # Remove any newline characters
        match = re.match(r"(.*?)\s(\w+)\s(\d{4})", col_name)
        if match:
            technology, month, year = match.groups()
            return pd.Series([technology.strip(), month.lower(), int(year)])  # Ensure month is lowercase
        return pd.Series([None, None, None])

    # Apply extraction to create 'Technology', 'Month', and 'Year' columns
    df_melted[['Technology', 'Month', 'Year']] = df_melted["Technology_Month_Year"].apply(extract_technology_month_year)
    
    # Drop the helper column 'Technology_Month_Year' as it is no longer needed
    df_melted = df_melted.drop(columns=["Technology_Month_Year"])
    
    # Derive unique (month, year) pairs from the melted DataFrame
    unique_months_years = set(zip(df_melted['Month'], df_melted['Year']))
    
    return df_melted, unique_months_years

# Collect all data for combined DataFrame
all_data = []
processed_months_years = set()
for month in months[9:]:
    processed_months_years.add((month, str(2024)))
# Iterate over each year and month, starting from 2024 down to 2018
for year in range(2024, 2017, -1):  # From 2024 down to 2018
    for month_name in months:
        # Check if this (month, year) has already been processed
        if (month_name, year) in processed_months_years:
            continue
        
        # Process the table for the current month and year
        result = load_and_process_table(bucket_name, month_name, year)
        
        # If data was loaded successfully, add it to the list
        if result is not None:
            df, unique_months_years = result
            all_data.append(df)
            
            # Update the processed_months_years set with actual derived months and years
            processed_months_years.update(unique_months_years)
            
            print(f"Processed data for {month_name} {year}.")

# Combine all DataFrames if data was collected
if all_data:
    combined_df = pd.concat(all_data, ignore_index=True)
    
    # Save the combined DataFrame to S3
    csv_buffer = io.StringIO()
    combined_df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=output_combined_key, Body=csv_buffer.getvalue())
    print(f"Combined data saved to {output_combined_key} in the S3 bucket '{bucket_name}'.")
else:
    print("No data collected.")
