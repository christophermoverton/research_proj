import pandas as pd
import boto3
import io

# S3 configurations
bucket_name = 'research-project-cenergy'
extracted_text_summary_key = 'electric_power_monthly_data/extracted_text_summary.csv'
output_merged_a_key = 'electric_power_monthly_data/merged_A_tables.csv'
output_merged_b_key = 'electric_power_monthly_data/merged_B_tables.csv'

# List of table files to process, as per provided list
table_files = [
    "1_03_A", "1_03_B", "1_04_A", "1_04_B", "1_05_A", "1_05_B",
    "1_06_A", "1_06_B", "1_07_A", "1_07_B", "1_08_A", "1_08_B",
    "1_10_A", "1_10_B", "1_11_A", "1_11_B", "1_17_A", 
    "1_17_B", "1_18_A", "1_18_B"
]

# Initialize S3 client
s3 = boto3.client('s3')

# Load the extracted_text_summary.csv file from S3
def load_extracted_text_summary(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    extracted_text_summary = pd.read_csv(io.BytesIO(response['Body'].read()))
    # Create a dictionary mapping 'Table File' to 'Extracted Text' for easy lookup
    extracted_text_mapping = dict(zip(extracted_text_summary['Table File'], extracted_text_summary['Extracted Text']))
    return extracted_text_mapping

# Load the extracted text mapping
extracted_text_mapping = load_extracted_text_summary(bucket_name, extracted_text_summary_key)

# Function to load CSV and add 'Energy_Source' column to each DataFrame
def load_and_add_energy_source(bucket, file_key, table_file_name):
    response = s3.get_object(Bucket=bucket, Key=file_key)
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    
    # Remove 'processed_' prefix to match with the table file in extracted_text_summary
    stripped_table_name = table_file_name.replace("processed_Table_", "").replace(".csv", "")
    energy_source = extracted_text_mapping.get(stripped_table_name, "All")
    print(energy_source)
    # Add 'Energy_Source' column to the DataFrame
    df['Energy_Source'] = energy_source
    return df

# Separate lists for A and B tables
table_files_a = [f"processed_Table_{name}.csv" for name in table_files if name.endswith("_A")]
table_files_b = [f"processed_Table_{name}.csv" for name in table_files if name.endswith("_B")]

# Collect all data for A and B tables separately
all_data_a = []
all_data_b = []

# Process and merge A version tables
for table_file in table_files_a:
    file_key = f"electric_power_monthly_data/{table_file}"
    df_a = load_and_add_energy_source(bucket_name, file_key, table_file)
    all_data_a.append(df_a)

# Process and merge B version tables
for table_file in table_files_b:
    file_key = f"electric_power_monthly_data/{table_file}"
    df_b = load_and_add_energy_source(bucket_name, file_key, table_file)
    all_data_b.append(df_b)

# Concatenate all A version DataFrames
if all_data_a:
    merged_df_a = pd.concat(all_data_a, ignore_index=True)
    # Convert to CSV in memory and upload to S3
    output_a_buffer = io.StringIO()
    merged_df_a.to_csv(output_a_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=output_merged_a_key, Body=output_a_buffer.getvalue())
    print(f"Merged A tables saved to {output_merged_a_key} in the S3 bucket '{bucket_name}'.")

# Concatenate all B version DataFrames
if all_data_b:
    merged_df_b = pd.concat(all_data_b, ignore_index=True)
    # Convert to CSV in memory and upload to S3
    output_b_buffer = io.StringIO()
    merged_df_b.to_csv(output_b_buffer, index=False)
    s3.put_object(Bucket=bucket_name, Key=output_merged_b_key, Body=output_b_buffer.getvalue())
    print(f"Merged B tables saved to {output_merged_b_key} in the S3 bucket '{bucket_name}'.")
