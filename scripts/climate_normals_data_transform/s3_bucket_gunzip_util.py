import boto3
import tarfile
import os
import shutil

# Set up S3 client
s3 = boto3.client('s3')

def upload_directory_to_s3(local_path, bucket_name, s3_path):
    """
    Recursively uploads files from a local directory to an S3 bucket, preserving the directory structure.

    Parameters:
    - local_path: str - The local directory path to upload.
    - bucket_name: str - The name of the S3 bucket.
    - s3_path: str - The S3 path (prefix) where the files should be uploaded.
    """
    for root, _, files in os.walk(local_path):
        for file in files:
            local_file_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_file_path, local_path)
            s3_file_path = os.path.join(s3_path, relative_path).replace("\\", "/")  # Handle Windows paths
            
            print(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_file_path}")
            s3.upload_file(local_file_path, bucket_name, s3_file_path)

def unzip_s3_file_with_directories(bucket_name, gz_key):
    """
    Downloads a .tar.gz file from S3, extracts it, and uploads the contents back to S3, 
    preserving the directory structure.

    Parameters:
    - bucket_name: str - The name of the S3 bucket.
    - gz_key: str - The S3 key (path) of the .tar.gz file.
    """
    
    # Temporary local paths for download and extraction
    gz_filename = "/tmp/temp.tar.gz"
    extraction_path = "/tmp/unzipped_content"
    
    # Ensure the extraction path exists
    os.makedirs(extraction_path, exist_ok=True)
    
    # Download the .tar.gz file from S3
    print(f"Downloading {gz_key} from bucket {bucket_name}...")
    s3.download_file(bucket_name, gz_key, gz_filename)
    
    # Extract the tar.gz file while preserving the directory structure
    print(f"Extracting {gz_key}...")
    with tarfile.open(gz_filename, 'r:gz') as tar:
        tar.extractall(path=extraction_path)
    
    # Upload extracted files back to S3, preserving directory structure
    upload_directory_to_s3(extraction_path, bucket_name, os.path.dirname(gz_key))
    
    # Clean up local files
    os.remove(gz_filename)
    shutil.rmtree(extraction_path)  # Remove entire extraction directory
    
    print("File unzipped, extracted, and uploaded successfully.")

# Usage
bucket_name = "research-project-cenergy"
gz_key = "us-climate-normals_2006-2020_v1.0.0_hourly_multivariate_by-variable_c20211019.tar.gz"  # path in S3 including directories

unzip_s3_file_with_directories(bucket_name, gz_key)
