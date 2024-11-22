import pandas as pd
import boto3
import io
import re

# S3 configurations
bucket_name = 'research-project-cenergy'

# Arbitrary month and year to use for all files (e.g., "january 2024")
single_month = "january"
single_year = 2024

# List of table filenames to process
table_files = [
    "1_03_A", "1_03_B", "1_04_A", "1_04_B", "1_05_A", "1_05_B",
    "1_06_A", "1_06_B", "1_07_A", "1_07_B", "1_08_A", "1_08_B",
    "1_10_A", "1_10_B", "1_11_A", "1_11_B", "1_17_A", 
    "1_17_B", "1_18_A", "1_18_B"
]

# Initialize S3 client
s3 = boto3.client('s3')

# Dictionary to store the extracted text for each table file
extracted_text = {}

# Function to load the first row of an Excel file from S3
def load_first_row_from_s3(bucket, key):
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        file_content = response['Body'].read()
        xls = pd.ExcelFile(io.BytesIO(file_content))
        
        # Load only the first row of the first sheet
        df = pd.read_excel(xls, sheet_name=0, header=None, nrows=1)
        return df.iloc[0, 0]  # Return the first cell value in the first row
    except s3.exceptions.NoSuchKey:
        print(f"File not found: {key}")
        return None

# Function to extract text between 'from' and a newline using regex
def extract_text_after_from_before_newline(text):
    print(text)
    match = re.search(r'from\s+(.*?)(\n|$)', text, re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return None

# Only process one file per table type
for table_file in table_files:
    # Construct the S3 key for this file (using "january 2024" for all files)
    file_key = f"electric_power_monthly_data/{single_month}{single_year}/Table_{table_file}.xlsx"
    
    # Load the first row of the file
    first_row_text = load_first_row_from_s3(bucket_name, file_key)
    
    if first_row_text:
        # Extract the text after 'from' and before newline
        extracted_value = extract_text_after_from_before_newline(first_row_text)
        
        # Store the result in the dictionary if extraction is successful
        if extracted_value:
            extracted_text[table_file] = extracted_value
            print(f"Extracted text for Table_{table_file}: {extracted_value}")
        else:
            print(f"No text found after 'from' in Table_{table_file}")

# Optionally, save the results to a CSV file
output_csv_key = 'electric_power_monthly_data/extracted_text_summary.csv'
csv_buffer = io.StringIO()
pd.DataFrame(list(extracted_text.items()), columns=['Table File', 'Extracted Text']).to_csv(csv_buffer, index=False)

# Upload the summary CSV to S3
s3.put_object(Bucket=bucket_name, Key=output_csv_key, Body=csv_buffer.getvalue())
print(f"Summary of extracted text saved to {output_csv_key} in the S3 bucket '{bucket_name}'.")
