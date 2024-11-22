import pandas as pd
import boto3
import io
from datetime import datetime

# Define S3 bucket name
bucket_name = 'research-project-cenergy'

# Initialize S3 client
s3 = boto3.client('s3')

# List of table codes and the corresponding filenames
table_codes = ["1_01", "1_01_A", "1_02_A", "1_02_B", "1_02_C", "1_02_D"]

# Define starting folder (latest) and decrement settings
start_year = 2024
start_month = 'september'
decrement_years = 2

# Month mapping to handle month conversion to integer
month_mapping = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12
}

# Function to process and filter the table from Excel file content
def process_table(file_content):
    xls = pd.ExcelFile(io.BytesIO(file_content))
    df = pd.read_excel(xls, sheet_name=0, header=3)

    collect_data = False
    current_year = None
    filtered_rows = []

    for _, row in df.iterrows():
        period = row['Period']
        
        if isinstance(period, str) and period.startswith("Year "):
            if period == "Year to...":
                collect_data = False
                break
            try:
                current_year = int(period.split()[1])
                collect_data = True
                continue
            except (IndexError, ValueError):
                continue

        if isinstance(period, str) and period.startswith("Rolling"):
            collect_data = False
            break

        if collect_data and isinstance(period, str) and period in [
            "January", "February", "March", "April", "May", "June", 
            "July", "August", "Sept", "October", "November", "December"
        ]:
            row['Year'] = current_year
            filtered_rows.append(row)

    filtered_df = pd.DataFrame(filtered_rows)
    return filtered_df

# Function to collect data for a specific table code and merge
def collect_and_merge_data(table_code):
    all_data = []
    processed_years = set()  # Set to keep track of processed years for this table

    current_year = start_year
    current_month = start_month.lower()
    current_month_num = month_mapping.get(current_month)

    while True:
        folder_name = f"{current_month}{current_year}"
        table_filename = f"Table_{table_code}.xlsx"
        s3_key = f"electric_power_monthly_data/{folder_name}/{table_filename}"
        
        try:
            response = s3.get_object(Bucket=bucket_name, Key=s3_key)
            file_content = response['Body'].read()
            
            filtered_df = process_table(file_content)
            
            if not filtered_df.empty:
                unique_years_in_file = set(filtered_df['Year'].unique())
                new_years = unique_years_in_file - processed_years

                if new_years:
                    new_data_df = filtered_df[filtered_df['Year'].isin(new_years)]
                    all_data.append(new_data_df)
                    processed_years.update(new_years)  # Mark new years as processed
                    print(f"Added data for years {new_years} from {s3_key}")
                else:
                    print(f"No new years to add from {s3_key}, skipping.")

        except s3.exceptions.NoSuchKey:
            print(f"File not found: {s3_key}, stopping collection for {table_code}.")
            break
        except Exception as e:
            print(f"Error processing {s3_key}: {e}")
            break

        current_year -= decrement_years
        current_month = "february"
        
        if current_year < 2018:
            print(f"Reached the end of available folders for {table_code}.")
            break
    
    if all_data:
        merged_df = pd.concat(all_data, ignore_index=True)
        return merged_df
    else:
        print(f"No data collected for table {table_code}.")
        return pd.DataFrame()

# Main function to process all tables
def main():
    for table_code in table_codes:
        print(f"Processing table {table_code}...")
        merged_df = collect_and_merge_data(table_code)

        if not merged_df.empty:
            # Convert the merged DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            output_file_key = f"electric_power_monthly_data/merged_Table_{table_code}.csv"
            merged_df.to_csv(csv_buffer, index=False)
            
            # Save the CSV back to S3
            s3.put_object(Bucket=bucket_name, Key=output_file_key, Body=csv_buffer.getvalue())
            
            print(f"Merged data for table {table_code} saved to {output_file_key} in the S3 bucket '{bucket_name}'.")

if __name__ == "__main__":
    main()
