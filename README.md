# ETL-with-AWS
Enhanced ETL Workflow with Python, AWS S3, RDS, and Glue for Data Engineers
Overview
This project demonstrates the ETL process of extracting data from raw files, transforming it, and loading it into an Amazon RDS MySQL database. The main steps include:

Extracting data from S3.
Transforming data (unit conversions).
Loading data into RDS MySQL.

**Setup**
Install dependencies:
pip install pandas boto3 mysql-connector-python


Setup AWS credentials: Use aws configure to set up your AWS credentials.

**Dataset** :wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip

Steps:

Step 1: Gather Data Files
Open a terminal and download the dataset:
Use the wget command to download the dataset containing multiple file formats.
wget https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMDeveloperSkillsNetwork-PY0221EN-SkillsNetwork/labs/module%206/Lab%20-%20Extract%20Transform%20Load/data/source.zip
Unzip the downloaded file:
Expand-Archive -Path source.zip -DestinationPath ./unzipped_folder


##**Run the scripts**:

Upload raw data to S3: Use the Local_to_s3.py script.
Download files from S3: Use the Downlod_s3_to_local.py script.
Transform data: Apply transformations like converting height and weight units.
Upload transformed data to S3: Use the transformation_Back2_s3.py script.
Load data into RDS MySQL: Use the s3_to_RDS.py script.
logs
data_pipeline.log

