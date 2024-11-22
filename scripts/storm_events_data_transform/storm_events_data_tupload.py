import boto3
import gzip
import os

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

def decompress_and_upload_gz_file(local_gz_path, bucket_name, s3_prefix):
    """
    Decompresses a local .gz file and uploads the decompressed file to S3.
    
    Parameters:
    - local_gz_path: str - The local path of the .gz file.
    - bucket_name: str - The name of the S3 bucket.
    - s3_prefix: str - The S3 path (prefix) where the decompressed file should be uploaded.
    """
    
    # Determine the local decompressed file path
    decompressed_file_path = local_gz_path.rsplit('.gz', 1)[0]
    
    # Decompress the .gz file
    print(f"Decompressing {local_gz_path}...")
    with gzip.open(local_gz_path, 'rb') as gz_file:
        with open(decompressed_file_path, 'wb') as decompressed_file:
            decompressed_file.write(gz_file.read())
    
    # Determine the S3 key for the decompressed file, preserving directory structure
    relative_path = os.path.relpath(decompressed_file_path, start=os.path.dirname(s3_prefix))
    s3_key = os.path.join(s3_prefix, relative_path).replace("\\", "/")  # Ensure compatibility with S3
    
    # Upload decompressed file to S3
    upload_file_to_s3(decompressed_file_path, bucket_name, s3_key)
    
    # Optionally, clean up the decompressed local file
    os.remove(decompressed_file_path)
    print(f"{local_gz_path} decompressed and uploaded to s3://{bucket_name}/{s3_key}")

def process_all_local_gz_files(local_directory, bucket_name, s3_prefix):
    """
    Finds all .gz files in a specified local directory, decompresses each one, 
    and uploads the decompressed files to an S3 bucket, preserving directory structure.
    
    Parameters:
    - local_directory: str - The local directory containing .gz files.
    - bucket_name: str - The name of the S3 bucket.
    - s3_prefix: str - The S3 directory (prefix) to upload the decompressed files.
    """
    
    # Walk through the local directory to find all .gz files
    gz_files = []
    for root, _, files in os.walk(local_directory):
        for file in files:
            if file.endswith('.gz'):
                gz_files.append(os.path.join(root, file))
    
    print(f"Found {len(gz_files)} .gz files in {local_directory}.")

    # Process each .gz file
    for gz_file_path in gz_files:
        print(f"\nProcessing {gz_file_path}...")
        decompress_and_upload_gz_file(gz_file_path, bucket_name, s3_prefix)

# Usage
local_directory = "/home/christopher/research_proj/datasets/storm_events_data"  # Local directory containing .gz files
bucket_name = "research-project-cenergy"
s3_prefix = "storm_events_dataset"  # The S3 prefix (folder) where decompressed files will be stored

process_all_local_gz_files(local_directory, bucket_name, s3_prefix)
