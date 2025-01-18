import boto3
import os
import logging

# Initialize S3 client
s3 = boto3.client('s3')

def upload_files_to_s3(local_directory, s3_bucket, s3_folder):
    """
    Upload files from a local directory to S3 bucket.
    
    Parameters:
    - local_directory: Path to the local directory with raw data files.
    - s3_bucket: The S3 bucket name.
    - s3_folder: The folder within the S3 bucket where files should be uploaded.
    """
    # Iterate through files in the local directory
    for file_name in os.listdir(local_directory):
        file_path = os.path.join(local_directory, file_name)
        
        if os.path.isfile(file_path):  # Check if it's a file
            # Upload each file to S3
            s3.upload_file(file_path, s3_bucket, f"{s3_folder}/{file_name}")
            print(f"Uploaded {file_name} to s3://{s3_bucket}/{s3_folder}/{file_name}")
# Example usage
local_directory = r"C:\Users\DELL\Documents\unzipped_folder"  # Local folder containing raw files
s3_bucket = 'my-etl-project-bucket-guvi'  # Your S3 bucket name
s3_folder = 'raw'  # S3 folder where files will be uploaded

# Upload files to S3
upload_files_to_s3(local_directory, s3_bucket, s3_folder)
