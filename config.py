import os
from dotenv import load_dotenv
load_dotenv()

BLOB_CONN_STR   = os.getenv("AZURE_BLOB_CONN_STR")
BLOB_CONTAINER  = os.getenv("AZURE_BLOB_CONTAINER")
SQL_SERVER      = os.getenv("SQL_SERVER")
SQL_DATABASE    = os.getenv("SQL_DATABASE")
SQL_USER        = os.getenv("SQL_USER")
SQL_PASSWORD    = os.getenv("SQL_PASSWORD")
TICKER          = os.getenv("TICKER", "META")