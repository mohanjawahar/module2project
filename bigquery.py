# Load SGJobsDB into dataframe csv
import os
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging as log
import pandas as pd
from google.cloud import bigquery

# Set up the client
# The client automatically picks up the credentials from the environment variable
client = bigquery.Client()

# Define your project, dataset, and table details
project_id = 'pilot-488720'  # Replace with your GCP project ID
dataset_id = 'olist'  # Replace with your desired dataset name
table_id = 'ecommerce'  # Replace with your desired table name
# Replace with the path to your CSV file
file_path = '/Users/mohanjawahar/DataScience/data/module2data/olist_customers_dataset.csv'

# Construct the full table reference
table_ref = f"{project_id}.{dataset_id}.{table_id}"

# Configure the load job
job_config = bigquery.LoadJobConfig(
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,  # Skip the header row
    autodetect=True,      # Automatically detect schema and data types
)

# Create the dataset if it does not exist
try:
    client.create_dataset(dataset_id, timeout=30)  # API request
    print(f"Dataset {dataset_id} created")
except Exception as e:
    print(f"Dataset {dataset_id} already exists or an error occurred: {e}")

# Load the CSV data into BigQuery
with open(file_path, "rb") as source_file:
    # Initiate the load job
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
