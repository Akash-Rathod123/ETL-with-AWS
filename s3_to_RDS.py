import pandas as pd
import mysql.connector
from mysql.connector import Error
import os
import logging

# Function to create the table
def create_table(connection, db_table):
    cursor = connection.cursor()
    create_table_query = f"""
    CREATE TABLE IF NOT EXISTS {db_table} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        name VARCHAR(255),
        height_meters FLOAT,
        weight_kg FLOAT
    );
    """
    cursor.execute(create_table_query)
    print(f"Table `{db_table}` is created or already exists.")

# Function to load DataFrame into RDS MySQL
def load_data_to_rds(df, db_endpoint, db_user, db_password, db_name, db_table):
    connection = None
    try:
        # Establish a connection to the RDS database
        connection = mysql.connector.connect(
            host=db_endpoint,
            user=db_user,
            password=db_password,
            database=db_name
        )

        if connection.is_connected():
            cursor = connection.cursor()

            # Create the table if it doesn't exist
            create_table(connection, db_table)

            # Prepare the SQL insert query
            insert_query = f"""
            INSERT INTO {db_table} (name, height_meters, weight_kg)
            VALUES (%s, %s, %s)
            """

            # Iterate through the DataFrame and insert each row into the RDS table
            for index, row in df.iterrows():
                cursor.execute(insert_query, (row['name'], row['height_meters'], row['weight_kg']))

            # Commit the transaction
            connection.commit()
            print(f"Data successfully loaded into {db_table}.")

    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    
    finally:
        if connection and connection.is_connected():
            cursor.close()
            connection.close()
            print("MySQL connection is closed.")

# Step 1: Define the local file path
local_directory = r"C:\Users\DELL\Documents\transformed_data"
local_file_path = os.path.join(local_directory, 'transformed_data.csv')

# Load the transformed CSV into a DataFrame
transformed_df = pd.read_csv(local_file_path)

# Step 2: Define RDS connection parameters
db_endpoint = 'endpoint'
db_user = 'admin'
db_password = 'XYZ'
db_name = 'database-1'
db_table = 'transformed_data123'

# Step 3: Load the data into RDS
load_data_to_rds(transformed_df, db_endpoint, db_user, db_password, db_name, db_table)
