import yfinance as yf
import pandas as pd
from datetime import datetime
from etl.common.db import engine


def ingest_prices(
    ticker: str,
    start: str,
    end: str
):
    df = yf.download(ticker, start=start, end=end)

    if df.empty:
        raise ValueError("No data downloaded")

    # handle multi-index columns from yfinance
    df.columns = df.columns.get_level_values(0)
    df.columns = df.columns.str.lower()

    # keep date as index
    df.index.name = "date"

    # add metadata
    df["ticker"] = ticker
    df["ingested_at"] = datetime.utcnow().isoformat()

    df = df[[
        "ticker", "open", "high", "low", "close", "volume", "ingested_at"
    ]]

    # write to bronze
    df.to_sql(
        "bronze_price_raw",
        engine,
        if_exists="append",
        index=True,
        index_label="date"
    )

    print(f"✅ Bronze ingestion completed for {ticker}")


if __name__ == "__main__":
    ingest_prices(
        ticker="AAPL",
        start="2024-01-01",
        end="2024-06-01"
    )
