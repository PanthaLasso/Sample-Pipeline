# Sample-Pipeline

Explanation:
Extract: Reads all CSV files from the Azure Blob Storage "dataset" container.

Transform: Cleans missing values and standardizes column names.

Load: Saves the cleaned data as CSV in Azure Data Lake.

You'll need to replace <your-azure-blob-connection-string> and <your-azure-datalake-connection-string> with actual credentials.
