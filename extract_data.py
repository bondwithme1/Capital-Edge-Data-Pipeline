import os
import requests
import pandas as pd
from datetime import datetime
from azure.storage.blob import BlobServiceClient

ALPHA_VANTAGE_API_KEY = os.getenv("ALPHA_VANTAGE_API_KEY")
AZURE_CONNECTION_STRING = os.getenv("AZURE_CONNECTION_STRING")
CONTAINER_NAME = "financial-raw"

def fetch_stock_data(symbol="AAPL"):
    """Fetches daily stock data from Alpha Vantage API."""
    url = (
        "[alphavantage.co](https://www.alphavantage.co/query)"
        f"function=TIME_SERIES_DAILY_ADJUSTED&symbol={symbol}&apikey={ALPHA_VANTAGE_API_KEY}"
    )

    response = requests.get(url)
    response.raise_for_status()

    data = response.json()["Time Series (Daily)"]
    df = pd.DataFrame(data).T.reset_index()
    df.columns = ["date"] + list(df.columns[1:])
    df["symbol"] = symbol
    return df

def upload_to_blob(df, symbol="AAPL"):
    """Upload raw API data to Azure Blob Storage with date partition."""
    blob_service = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
    container = blob_service.get_container_client(CONTAINER_NAME)

    today = datetime.now().strftime("%Y-%m-%d")
    filename = f"{symbol}/{today}/raw_data.csv"

    csv_bytes = df.to_csv(index=False).encode("utf-8")
    container.upload_blob(filename, csv_bytes, overwrite=True)

def main():
    df = fetch_stock_data(symbol="AAPL")
    upload_to_blob(df, symbol="AAPL")

if __name__ == "__main__":
    main()
