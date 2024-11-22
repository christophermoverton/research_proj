import os
import boto3
from pathlib import Path

# Define local directory and S3 parameters
local_directory = Path('/home/christopher/Downloads/ghcnd_all')
bucket_name = 'research-project-cenergy'
s3_directory = 'ghcnd_all'  # S3 directory where the folder will be stored

# Initialize S3 client
s3_client = boto3.client('s3')

def upload_directory_to_s3(directory, bucket, s3_prefix):
    """Uploads all files in a directory to an S3 bucket with a specified prefix."""
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = Path(root) / file
            s3_key = f"{s3_prefix}/{file_path.relative_to(directory)}"
            print(f"Uploading {file_path} to s3://{bucket}/{s3_key}...")
            s3_client.upload_file(str(file_path), bucket, s3_key)
            print(f"Uploaded {file_path} to {s3_key}")

# Upload the local directory to S3
upload_directory_to_s3(local_directory, bucket_name, s3_directory)

print("All files uploaded successfully.")
