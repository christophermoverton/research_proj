import boto3
import os

# Configure your S3 bucket and folder
bucket_name = "research-project-cenergy"
s3_folder = "storm_events_dataset/datasets/storm_events_data"
local_folder = "/media/christopher/Extreme SSD/research_project_data/storm_events_data"

# Ensure local folder exists
os.makedirs(local_folder, exist_ok=True)

# Initialize S3 client
s3_client = boto3.client("s3")

# File mappings based on screenshots
files_to_download = [
    #"StormEvents_details-ftp_v1.0_d2019_c20240117.csv",
    "StormEvents_details-ftp_v1.0_d2020_c20240620.csv",
    "StormEvents_details-ftp_v1.0_d2021_c20240716.csv",
    "StormEvents_details-ftp_v1.0_d2022_c20240716.csv",
    "StormEvents_details-ftp_v1.0_d2023_c20241017.csv",
    "StormEvents_details-ftp_v1.0_d2024_c20241017.csv",
    "StormEvents_locations-ftp_v1.0_d2020_c20240620.csv",
    "StormEvents_locations-ftp_v1.0_d2021_c20240716.csv",
    "StormEvents_locations-ftp_v1.0_d2022_c20240716.csv",
    "StormEvents_locations-ftp_v1.0_d2023_c20241017.csv",
    "StormEvents_locations-ftp_v1.0_d2024_c20241017.csv"
]

# Function to download files from S3
def download_files(files):
    for file_name in files:
        s3_key = f"{s3_folder}/{file_name}"
        local_file_path = os.path.join(local_folder, file_name)
        
        try:
            print(f"Downloading {file_name}...")
            s3_client.download_file(bucket_name, s3_key, local_file_path)
            print(f"Downloaded: {file_name}")
        except Exception as e:
            print(f"Failed to download {file_name}. Error: {e}")

# Download the files
download_files(files_to_download)
