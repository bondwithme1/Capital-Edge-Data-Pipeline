import os
import pandas as pd
import sqlalchemy
from io import StringIO
from datetime import datetime
from azure.storage.blob import BlobServiceClient

AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
DB_CONN = os.getenv("DB_CONN_STRING")
CONTAINER = "financial-processed"

def load_processed(symbol="AAPL"):
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = blob_service.get_container_client(CONTAINER)

    today = datetime.now().strftime("%Y-%m-%d")
    blob_path = f"{symbol}/{today}/processed_data.csv"

    blob = container.get_blob_client(blob_path)
    processed_data = blob.download_blob().readall().decode("utf-8")

    return pd.read_csv(StringIO(processed_data))

def get_latest_db_date(engine):
    query = "SELECT MAX(date) FROM stock_prices"
    latest = pd.read_sql(query, engine).iloc[0, 0]
    return latest

def incremental_insert(df, engine):
    latest = get_latest_db_date(engine)
    df["date"] = pd.to_datetime(df["date"])

    if latest:
        df_new = df[df["date"] > latest]
    else:
        df_new = df

    df_new.to_sql("stock_prices", engine, if_exists="append", index=False)

def main():
    engine = sqlalchemy.create_engine(DB_CONN)
    df = load_processed("AAPL")
    incremental_insert(df, engine)

if __name__ == "__main__":
    main()
