#!/bin/bash

# Directory where the datasets are stored
DATASET_DIR="datasets"

# Check if the directory exists
if [ -d "$DATASET_DIR" ]; then
  echo "Unzipping datasets in $DATASET_DIR..."

  # Loop through each .zip file in the directory
  for zip_file in "$DATASET_DIR"/*.zip; do
    # Check if the file exists (to avoid errors if no .zip files are found)
    if [ -f "$zip_file" ]; then
      # Get the base name of the file (without the .zip extension)
      base_name=$(basename "$zip_file" .zip)
      
      # Create a directory for this dataset
      mkdir -p "$DATASET_DIR/$base_name"
      
      # Unzip the file into its respective directory
      unzip -o "$zip_file" -d "$DATASET_DIR/$base_name"
      
      echo "Unzipped $zip_file to $DATASET_DIR/$base_name"
    else
      echo "No zip files found in $DATASET_DIR."
      exit 1
    fi
  done
  echo "All zip files unzipped successfully!"
else
  echo "Directory $DATASET_DIR does not exist."
  exit 1
fi
