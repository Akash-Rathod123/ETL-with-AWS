import boto3
import os
import logging

# Initialize S3 client
s3 = boto3.client('s3')

def download_files_from_s3(s3_bucket, s3_folder, local_directory):
    """
    Download files from an S3 bucket to a local directory.
    
    Parameters:
    - s3_bucket: The S3 bucket name.
    - s3_folder: The folder within the S3 bucket where files are stored.
    - local_directory: Path to the local directory where files should be downloaded.
    """
    # Ensure the local directory exists
    if not os.path.exists(local_directory):
        os.makedirs(local_directory)

    # List objects in the S3 folder
    objects = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)
    
    # Download each file in the folder
    for obj in objects.get('Contents', []):
        file_key = obj['Key']
        file_name = os.path.basename(file_key)  # Get the file name only

        if file_name:  # Skip empty keys (directories)
            local_file_path = os.path.join(local_directory, file_name)
            try:
                s3.download_file(s3_bucket, file_key, local_file_path)
                print(f"Downloaded {file_name} to {local_file_path}")
            except Exception as e:
                print(f"Error downloading {file_name}: {str(e)}")

# Example usage
local_directory = r"C:\Users\DELL\Documents\unzipped_folder\downloaded"  # Local folder for downloads
s3_bucket = 'my-etl-project-bucket-guvi'  # Your S3 bucket name
s3_folder = 'raw'  # S3 folder where files were uploaded

# Download files from S3
download_files_from_s3(s3_bucket, s3_folder, local_directory)
