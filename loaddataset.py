# Load Ecommerce into dataframe csv
import os
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging as log
import pandas as pd
from google.cloud import bigquery


# Set up the folder
folder_path = '/Users/mohanjawahar/DataScience/data/module2data/'
file_list = list(Path(folder_path).glob("*.csv"))

# Set up the client
# The client automatically picks up the credentials from the environment variable
client = bigquery.Client()

# Define your project, dataset, and table details
project_id = 'pilot-488720'  # Replace with your GCP project ID
dataset_id = 'ecommerce'  # Replace with your desired dataset name
# table_id = 'ecommerce'  # Replace with your desired table name


# Construct the table name from the CSV file name
def construct_table_name(file_name):
    start_delimiter = "olist_"
    end_delimiter = "_dataset"

    # Find the index of the start delimiter
    # Adding the length of the delimiter ensures we start the slice *after* it
    start_index = file_name.find(start_delimiter) + len(start_delimiter)

    # Find the index of the end delimiter, starting the search from start_index
    end_index = file_name.find(end_delimiter, start_index)

    # Extract the substring using slicing
    if start_index != -1 and end_index != -1:
        result = file_name[start_index:end_index]
        return result
    else:
        print("Delimiters not found or misplaced.")


# Construct the full table reference
# table_ref = f"{project_id}.{dataset_id}.{table_id}"
def get_schema_for_table(table_id):
    schemas = {
        "product_category_translation": [
            bigquery.SchemaField("product_category_name", "STRING"),
            bigquery.SchemaField("product_category_name_english", "STRING"),
        ],
    }
    return schemas.get(table_id)


# Configure the load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Skip the header row
    #    autodetect=True,      # Automatically detect schema and data types
    # To fix the missing quote characters when uploading csv file
    allow_quoted_newlines=True,
    field_delimiter=",",
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
)

# Create the dataset if it does not exist
try:
    client.create_dataset(dataset_id, timeout=30)  # API request
    print(f"Dataset {dataset_id} created")
except Exception as e:
    print(f"Dataset {dataset_id} already exists or an error occurred: {e}")

# Load the CSV data into BigQuery in loop
for file_path in file_list:
    file_name = str(file_path).split("/")
    # print(f"File Name: {file_name}")
    table_id = construct_table_name(file_name[-1])
    # Construct the full table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    with open(file_path, "rb") as source_file:
        # Initiate the load job
        schema = get_schema_for_table(table_id)
        if schema:
            job_config.schema = schema
        else:
            job_config.autodetect = True

        load_job = client.load_table_from_file(
            source_file,
            table_ref,
            job_config=job_config
        )  # Make an API request

        print(f"Starting load job {load_job.job_id}.")
        load_job.result()  # Wait for the job to complete

        # Print the result
        destination_table = client.get_table(table_ref)
        print(f"Loaded {destination_table.num_rows} rows into {table_ref}.")
