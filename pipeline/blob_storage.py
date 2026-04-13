from azure.storage.blob import BlobServiceClient
import io
from config import BLOB_CONN_STR, BLOB_CONTAINER

CONN_STR = BLOB_CONN_STR
CONTAINER = BLOB_CONTAINER

def _get_blob_service_client():
    return BlobServiceClient.from_connection_string(CONN_STR)

def upload_to_blob(df, blob_name: str):
    client = _get_blob_service_client()
    container_client = client.get_container_client(CONTAINER)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    container_client.upload_blob(
        name=blob_name,
        data=csv_buffer.getvalue(),
        overwrite=True
    )
    print(f"Uploaded {blob_name} to Blob Storage")

def check_blob_connection():
    client = _get_blob_service_client()
    container_client = client.get_container_client(CONTAINER)
    container_client.get_container_properties()
    return True
