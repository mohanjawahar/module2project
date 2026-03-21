# Load SGJobsDB into dataframe csv
import os
from pathlib import Path
import matplotlib.pyplot as plt
import seaborn as sns
import logging as log
import pandas as pd
from google.cloud import bigquery

'''
def load_csv_file(filepath):
    try:
        # Try reading with default parameters
        df = pd.read_csv(filepath)
        print(f"Successfully loaded {filepath}")
        print(f"Shape: {df.shape}")
        print(f"Columns: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        print(f"Error: File '{filepath}' not found")
    except pd.errors.EmptyDataError:
        print("Error: File is empty")
    except Exception as e:
        print(f"Error loading file: {str(e)}")
        # Try different encodings
        for encoding in ['latin1', 'cp1252', 'iso-8859-1']:
            try:
                df = pd.read_csv(filepath, encoding=encoding)
                print(f"Successfully loaded with {encoding} encoding")
                return df
            except:
                continue
    return None


# Main
df = load_csv_file(
    '/Users/mohanjawahar/DataScience/data/module2data/olist_customers_dataset.csv')
if df is not None:
    # Display first few rows
    print(df.head())

    # Basic info
    print(df.info(memory_usage=True))
    # Statistical summary
    print(df.describe())



def load_csvs_into_dfs(folder_path):
    """
    Reads all CSV files in a given folder and loads them into a dictionary of pandas DataFrames.

    Args:
        folder_path (str): The path to the folder containing the CSV files.

    Returns:
        dict: A dictionary where keys are filenames (without extension) 
              and values are pandas DataFrames.
    """
    # Use pathlib to find all CSV files recursively (adjust glob pattern as needed)
    file_list = list(Path(folder_path).glob("*.csv"))

    if not file_list:
        print(f"No CSV files found in: {folder_path}")
        return {}

    # Dictionary to store DataFrames
    dfs = {}

    for file_path in file_list:
        # Get the filename without the extension to use as the dictionary key
        filename = os.path.splitext(os.path.basename(file_path))[0]

        # Read the CSV file into a pandas DataFrame
        try:
            df = pd.read_csv(file_path)
            dfs[filename] = df
            print(f"Loaded {file_path} into DataFrame '{filename}'")
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")

    return dfs


# Example usage:
# Replace 'your_folder_path' with the actual path to your folder
your_folder_path = '/Users/mohanjawahar/DataScience/data/module2data'
all_dataframes = load_csvs_into_dfs(your_folder_path)

# Access individual DataFrames using their filename as a key:
print(all_dataframes['olist_customers_dataset'].head())
print(all_dataframes['olist_customers_dataset'].info())

# bigquery 

from google.cloud import bigquery


def load_data_from_gcs(dataset_id, table_id, gcs_uri, source_format):
    """Loads data from GCS into a BigQuery table."""
    bigquery_client = bigquery.Client()

    # Construct the full table ID
    table_ref = bigquery_client.dataset(dataset_id).table(table_id)

    # Configure the load job
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = source_format
    job_config.autodetect = True  # Optional: auto-detect schema for CSV/JSON
    # Overwrite table (or use WRITE_APPEND)
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

    # Start the load job
    load_job = bigquery_client.load_table_from_uri(
        gcs_uri,
        table_ref,
        job_config=job_config
    )

    print(f"Starting load job {load_job.job_id}...")
    load_job.result()  # Waits for the job to complete
    print(f"Job finished. Loaded {load_job.output_rows} rows into {table_id}.")

# Example usage (using the GCS URI from Step 1):
# load_data_from_gcs("my_dataset", "my_table", "gs://my-bigquery-bucket/data_folder/large_file.parquet", bigquery.SourceFormat.PARQUET)
'''


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
