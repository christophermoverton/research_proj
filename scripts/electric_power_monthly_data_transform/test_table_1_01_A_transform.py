import pandas as pd
import boto3
import io

# Define S3 bucket and file details
bucket_name = 'research-project-cenergy'
input_file_key = 'electric_power_monthly_data/may2023/Table_1_01_A.xlsx'
output_file_key = 'electric_power_monthly_data/may2023/csv_files/Table_1_1_A_filtered.csv'

# Initialize S3 client
s3 = boto3.client('s3')

def process_table_1_1_A(file_content):
    # Load the Excel file from the S3 content
    xls = pd.ExcelFile(io.BytesIO(file_content))
    
    # Read the Excel file using only the 4th row as the header (header=3)
    df = pd.read_excel(xls, sheet_name=0, header=3)
    
    # Print columns to verify they loaded correctly
    print("Columns:", df.columns)

    # Initialize variables for filtering and year assignment
    collect_data = False
    current_year = None
    filtered_rows = []

    # Iterate over rows to apply custom filtering logic
    for _, row in df.iterrows():
        period = row['Period']  # Adjust based on actual column name if needed
        
        # Start collecting after "Year *" and extract the year
        if isinstance(period, str) and period.startswith("Year "):
            # Stop collecting if we encounter "Year to..."
            if isinstance(period, str) and period.startswith("Year to"):
                collect_data = False
                break  # Stop collection entirely at "Year to..."

            # Try to split and get the year as an integer for "Year XXXX" entries
            try:
                current_year = int(period.split()[1])  # Extract year as an integer
                collect_data = True
                continue  # Skip the "Year" row itself
            except (IndexError, ValueError):
                # If there's no valid year after "Year", skip this row
                print(f"Skipping row with invalid 'Year' format in 'Period': {period}")
                continue

        # Stop collecting data at "Rolling *"
        if isinstance(period, str) and period.startswith("Rolling"):
            collect_data = False
            break  # Stop collection entirely after this point

        # If collecting data and period is in month format, add the year column
        if collect_data and isinstance(period, str) and period in [
            "January", "February", "March", "April", "May", "June", 
            "July", "August", "Sept", "October", "November", "December"
        ]:
            row['Year'] = current_year  # Set the current year for each row
            filtered_rows.append(row)   # Collect the row

    # Convert filtered rows to a DataFrame
    filtered_df = pd.DataFrame(filtered_rows)
    return filtered_df


def main():
    # Download the Excel file from S3
    response = s3.get_object(Bucket=bucket_name, Key=input_file_key)
    file_content = response['Body'].read()
    
    # Process the file to filter data and add a year column
    filtered_df = process_table_1_1_A(file_content)

    # Convert the DataFrame to CSV in memory
    csv_buffer = io.StringIO()
    filtered_df.to_csv(csv_buffer, index=False)
    
    # Save the CSV back to S3
    s3.put_object(Bucket=bucket_name, Key=output_file_key, Body=csv_buffer.getvalue())
    
    print(f"Filtered data saved to {output_file_key} in the S3 bucket '{bucket_name}'.")

# Run the main function
if __name__ == "__main__":
    main()
