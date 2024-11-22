import boto3
import zipfile
import os
import shutil  # Import shutil for directory cleanup

# Initialize S3 client
s3 = boto3.client('s3')

def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    """
    Uploads a single file to S3.
    
    Parameters:
    - local_file_path: str - The local path of the file to upload.
    - bucket_name: str - The name of the S3 bucket.
    - s3_key: str - The destination S3 key (path).
    """
    print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
    s3.upload_file(local_file_path, bucket_name, s3_key)

def decompress_and_upload_zip_file(local_zip_path, bucket_name, s3_prefix, base_local_directory):
    """
    Decompresses a local .zip file and uploads its contents to S3, preserving directory structure.
    
    Parameters:
    - local_zip_path: str - The local path of the .zip file.
    - bucket_name: str - The name of the S3 bucket.
    - s3_prefix: str - The S3 path (prefix) where the decompressed files should be uploaded.
    - base_local_directory: str - The base directory to preserve the relative path for S3.
    """
    
    # Directory to extract files temporarily
    extraction_path = "/tmp/extracted_content"
    os.makedirs(extraction_path, exist_ok=True)
    
    # Extract the .zip file
    print(f"Extracting {local_zip_path}...")
    with zipfile.ZipFile(local_zip_path, 'r') as zip_ref:
        zip_ref.extractall(extraction_path)
    
    # Upload each extracted file to S3, preserving directory structure
    for root, _, files in os.walk(extraction_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            
            # Calculate S3 key based on relative path from the original base directory
            relative_path = os.path.relpath(local_file_path, start=extraction_path)
            s3_key = os.path.join(s3_prefix, os.path.relpath(local_zip_path, base_local_directory).rsplit('.zip', 1)[0], relative_path).replace("\\", "/")
            
            # Upload file to S3
            upload_file_to_s3(local_file_path, bucket_name, s3_key)
    
    # Clean up the entire extraction directory
    shutil.rmtree(extraction_path)
    print(f"{local_zip_path} extracted and uploaded to S3.")

def process_all_local_zip_files(local_directory, bucket_name, s3_prefix):
    """
    Finds all .zip files in a specified local directory, decompresses each one, 
    and uploads its contents to an S3 bucket, preserving directory structure.
    
    Parameters:
    - local_directory: str - The local directory containing .zip files.
    - bucket_name: str - The name of the S3 bucket.
    - s3_prefix: str - The S3 directory (prefix) to upload the decompressed files.
    """
    
    # Walk through the local directory to find all .zip files
    zip_files = []
    for root, _, files in os.walk(local_directory):
        for file in files:
            if file.endswith('.zip'):
                zip_files.append(os.path.join(root, file))
    
    print(f"Found {len(zip_files)} .zip files in {local_directory}.")

    # Process each .zip file
    for zip_file_path in zip_files:
        print(f"\nProcessing {zip_file_path}...")
        decompress_and_upload_zip_file(zip_file_path, bucket_name, s3_prefix, local_directory)

# Usage
local_directory = "/home/christopher/research_proj/datasets/electric_power_monthly_data"  # Local directory containing .zip files
bucket_name = "research-project-cenergy"
s3_prefix = "electric_power_monthly_data"  # The S3 prefix (folder) to store decompressed files

process_all_local_zip_files(local_directory, bucket_name, s3_prefix)
