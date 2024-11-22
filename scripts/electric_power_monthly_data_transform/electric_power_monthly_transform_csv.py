import boto3
import pandas as pd
import io

# Initialize S3 client
s3 = boto3.client('s3')

# Define the S3 bucket name and folder path
bucket_name = 'research-project-cenergy'
folder_path = 'electric_power_monthly_data/may2023/'

# List of table codes to process
table_codes = [
    "1.1", "1.1.A", "1.2.A", "1.2.B", "1.2.C", "1.2.D",
    "1.3.A", "1.3.B", "1.4.A", "1.4.B", "1.5.A", "1.5.B",
    "1.6.A", "1.6.B", "1.7.A", "1.7.B", "1.8.A", "1.8.B",
    "1.10.A", "1.10.B", "1.11.A", "1.11.B", "1.17.A", 
    "1.17.B", "1.18.A", "1.18.B", "5.3", "5.4.A", "5.4.B",
    "5.5.A", "5.5.B", "5.6.A", "5.6.B", "6.2.A", "6.2.B",
    "6.2.C", "6.7.A", "6.7.B", "6.7.C", "6.1", "7.1"
]

def find_header_row(df):
    """
    Finds the header row by locating the first row with data,
    assumes header rows have strings (non-numeric) as column names.
    """
    for i, row in df.iterrows():
        if all(isinstance(cell, str) for cell in row):  # Header row is all strings
            return i
    return 0  # Default to the first row if no header row found

def convert_to_csv(table_code):
    # Create S3 key for the .xlsx file
    s3_key = f"{folder_path}Table_{table_code.replace('.', '_')}.xlsx"

    # Download the file from S3 into a BytesIO object
    response = s3.get_object(Bucket=bucket_name, Key=s3_key)
    file_content = response['Body'].read()
    
    # Read the Excel file
    xls = pd.ExcelFile(io.BytesIO(file_content))

    # Process each sheet in the workbook
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name, header=None)
        
        # Find the header row
        header_row = find_header_row(df)
        
        # Read again with header row set
        df = pd.read_excel(io.BytesIO(file_content), sheet_name=sheet_name, header=header_row)
        
        # Convert to CSV format
        csv_key = f"{folder_path}csv_files/{table_code}/{sheet_name}.csv"
        csv_buffer = io.StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload CSV back to S3
        s3.put_object(Bucket=bucket_name, Key=csv_key, Body=csv_buffer.getvalue())
        print(f"Converted and uploaded: {csv_key}")

# Iterate through each table and convert to CSV
for table_code in table_codes:
    try:
        convert_to_csv(table_code)
    except Exception as e:
        print(f"Error processing table {table_code}: {e}")
