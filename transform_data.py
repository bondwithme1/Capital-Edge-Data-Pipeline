import os
import pandas as pd
from io import StringIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
RAW_CONTAINER = "financial-raw"
PROCESSED_CONTAINER = "financial-processed"

def load_raw_from_blob(symbol="AAPL"):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    raw_container = blob_service.get_container_client(RAW_CONTAINER)

    today = datetime.now().strftime("%Y-%m-%d")
    blob_path = f"{symbol}/{today}/raw_data.csv"

    blob = raw_container.get_blob_client(blob_path)
    raw_data = blob.download_blob().readall().decode("utf-8")

    return pd.read_csv(StringIO(raw_data))

def clean_transform(df):
    df.columns = [col.replace(" ", "_").lower() for col in df.columns]
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values("date")
    df["daily_return"] = df["adjusted_close"].pct_change()
    df = df.drop_duplicates()
    return df

def upload_processed(df, symbol="AAPL"):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    processed_container = blob_service.get_container_client(PROCESSED_CONTAINER)

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{symbol}/{today}/processed_data.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    processed_container.upload_blob(filename, csv_bytes, overwrite=True)

def main():
    df = load_raw_from_blob("AAPL")
    df_clean = clean_transform(df)
    upload_processed(df_clean)

if __name__ == "__main__":
    main()
