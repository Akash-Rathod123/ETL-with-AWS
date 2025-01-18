import pandas as pd
import boto3
import os
import logging
# Initialize S3 client
s3 = boto3.client('s3')

def transform_data(df):
    """
    Perform transformation on the DataFrame to convert units and clean the data.
    - Convert 'height' from inches to meters.
    - Convert 'weight' from pounds to kilograms.
    
    Parameters:
    df: The input DataFrame with 'height' in inches and 'weight' in pounds.
    
    Returns:
    The transformed DataFrame with new columns for 'height_meters' and 'weight_kg'.
    """
    df['height_meters'] = df['height'] * 0.0254  # Convert inches to meters
    df['weight_kg'] = df['weight'] * 0.453592   # Convert pounds to kilograms
    return df

def save_to_csv(df, local_path):
    """
    Save the transformed DataFrame to a local CSV file.
    
    Parameters:
    df: The DataFrame to save.
    local_path: The local path where the CSV file will be saved.
    """
    df.to_csv(local_path, index=False)
    print(f"Transformed data saved to {local_path}")

def upload_to_s3(local_file_path, s3_bucket, s3_folder):
    """
    Upload a file to an S3 bucket.
    
    Parameters:
    local_file_path: The path to the file to upload.
    s3_bucket: The S3 bucket name.
    s3_folder: The folder within the S3 bucket where the file should be uploaded.
    """
    file_name = os.path.basename(local_file_path)
    s3_key = f"{s3_folder}/{file_name}"
    
    # Upload the file
    s3.upload_file(local_file_path, s3_bucket, s3_key)
    print(f"Uploaded {file_name} to s3://{s3_bucket}/{s3_key}")

# Example usage
data = {
    'name': ['Alice', 'Bob', 'Charlie'],
    'height': [65, 72, 68],  # Heights in inches
    'weight': [150, 180, 160]  # Weights in pounds
}

df = pd.DataFrame(data)

# Step 1: Transform the data
transformed_df = transform_data(df)

# Step 2: Save the transformed data locally
local_directory = r"C:\Users\DELL\Documents\transformed_data"
local_file_path = os.path.join(local_directory, 'transformed_data.csv')

# Ensure the directory exists
os.makedirs(local_directory, exist_ok=True)

# Save DataFrame to CSV
save_to_csv(transformed_df, local_file_path)

# Step 3: Upload the transformed data to S3
s3_bucket = 'my-etl-project-bucket-guvi'  # Your S3 bucket name
s3_folder = 'transformed'  # Folder in S3

upload_to_s3(local_file_path, s3_bucket, s3_folder)
