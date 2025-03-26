from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import pandas as pd
import io
import os

# Azure Storage Account details
AZURE_STORAGE_CONNECTION_STRING = "<your-azure-blob-connection-string>"
AZURE_DATALAKE_CONNECTION_STRING = "<your-azure-datalake-connection-string>"
BLOB_CONTAINER_NAME = "dataset"
DATALAKE_FILESYSTEM_NAME = "processed-data"

# Initialize Blob Storage and Data Lake Clients
blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
datalake_service_client = DataLakeServiceClient.from_connection_string(AZURE_DATALAKE_CONNECTION_STRING)

# Get blob container client
blob_container_client = blob_service_client.get_container_client(BLOB_CONTAINER_NAME)

def extract_data():
    """Extract CSV files from Azure Blob Storage."""
    csv_files = []
    for blob in blob_container_client.list_blobs():
        if blob.name.endswith(".csv"):
            blob_client = blob_container_client.get_blob_client(blob.name)
            csv_data = blob_client.download_blob().readall()
            df = pd.read_csv(io.BytesIO(csv_data))
            csv_files.append((blob.name, df))
    return csv_files

def transform_data(csv_files):
    """Perform simple data transformations (e.g., cleaning, formatting)."""
    transformed_data = []
    for filename, df in csv_files:
        df.dropna(inplace=True)  # Remove missing values
        df.columns = [col.strip().lower().replace(" ", "_") for col in df.columns]  # Normalize column names
        transformed_data.append((filename, df))
    return transformed_data

def load_data(transformed_data):
    """Load transformed data into Azure Data Lake Storage."""
    filesystem_client = datalake_service_client.get_file_system_client(DATALAKE_FILESYSTEM_NAME)
    
    for filename, df in transformed_data:
        file_client = filesystem_client.get_file_client(f"processed_{filename}")
        data = df.to_csv(index=False)
        file_client.upload_data(data, overwrite=True)

# Run ETL Process
if __name__ == "__main__":
    extracted_data = extract_data()
    transformed_data = transform_data(extracted_data)
    load_data(transformed_data)
    print("ETL Process Completed Successfully!")
