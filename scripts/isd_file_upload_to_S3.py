import os
import tarfile
import shutil
import boto3
from pathlib import Path

# Define paths and S3 parameters
download_dir = Path('/home/christopher/Downloads')
archive_file = download_dir / 'isd_2023_c20240105T134534.tar.gz'
bucket_name = 'research-project-cenergy'
s3_directory = 'isd'

# Initialize S3 client
s3_client = boto3.client('s3')

def extract_tar_gz(file_path, extract_to):
    """Extracts a .tar.gz file to the specified directory."""
    if tarfile.is_tarfile(file_path):
        with tarfile.open(file_path, 'r:gz') as tar:
            print(f"Extracting {file_path} to {extract_to}...")
            tar.extractall(path=extract_to)
            print("Extraction complete.")
    else:
        print(f"{file_path} is not a valid .tar.gz file.")

def upload_directory_to_s3(directory, bucket, s3_prefix):
    """Uploads all files in a directory to an S3 bucket with a specified prefix."""
    for root, _, files in os.walk(directory):
        for file in files:
            file_path = Path(root) / file
            s3_key = f"{s3_prefix}/{file_path.relative_to(directory)}"
            print(f"Uploading {file_path} to s3://{bucket}/{s3_key}...")
            s3_client.upload_file(str(file_path), bucket, s3_key)
            print(f"Uploaded {file_path} to {s3_key}")

def cleanup_local_files(directory):
    """Deletes a specified directory and its contents."""
    try:
        shutil.rmtree(directory)
        print(f"Cleaned up local files in {directory}")
    except Exception as e:
        print(f"Failed to delete {directory}: {e}")

# Step 1: Extract the .tar.gz file
extract_dir = download_dir / 'isd_2023'  # Temporary extraction directory
extract_tar_gz(archive_file, extract_dir)

# Step 2: Upload extracted files to S3
upload_directory_to_s3(extract_dir, bucket_name, s3_directory)

# Step 3: Clean up extracted files from local storage
cleanup_local_files(extract_dir)

print("All files uploaded and local cleanup complete.")
