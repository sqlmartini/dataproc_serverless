from google.cloud import storage
from azure.storage.blob import BlobServiceClient

def gcs_to_azure_blob(
    gcs_bucket_name,
    gcs_blob_name,
    azure_connection_string,
    azure_blob_container_name,
    azure_blob_name=None
):
    """
    Reads a file from Google Cloud Storage (GCS) and writes it to Azure Blob Storage.

    Args:
        gcs_bucket_name (str): The name of the GCS bucket.
        gcs_blob_name (str): The name of the GCS blob (file path).
        azure_connection_string (str): The connection string for your Azure storage account.
        azure_blob_container_name (str): The name of the Azure Blob Storage container.
        azure_blob_name (str, optional): The name to use for the blob in Azure Blob Storage. 
                                         If not provided, the GCS blob name will be used.

    Returns:
        None
    """

    if azure_blob_name is None:
        azure_blob_name = gcs_blob_name

    # Download from GCS
    gcs_client = storage.Client()
    gcs_bucket = gcs_client.bucket(gcs_bucket_name)
    gcs_blob = gcs_bucket.blob(gcs_blob_name)
    file_content = gcs_blob.download_as_bytes()

    # Upload to Azure Blob Storage
    blob_service_client = BlobServiceClient.from_connection_string(azure_connection_string)
    blob_client = blob_service_client.get_blob_client(container=azure_blob_container_name, blob=azure_blob_name)
    blob_client.upload_blob(file_content)

    print(f"File '{gcs_blob_name}' copied from GCS bucket '{gcs_bucket_name}' to Azure Blob Storage container '{azure_blob_container_name}' as '{azure_blob_name}'")

if __name__ == "__main__":
    
    # Set variables
    gcs_bucket_name = ""
    gcs_blob_name = ""
    azure_connection_string = ""
    azure_blob_container_name = ""
    azure_blob_name = ""    

    # Call function
    gcs_to_azure_blob(gcs_bucket_name, gcs_blob_name, azure_connection_string, azure_blob_container_name, azure_blob_name)