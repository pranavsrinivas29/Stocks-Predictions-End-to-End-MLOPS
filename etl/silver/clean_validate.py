import pandas as pd
import pandera as pa
from pandera import Column, DataFrameSchema, Check
from etl.common.db import engine


# -----------------------------
# Pandera schema
# -----------------------------
silver_schema = DataFrameSchema(
    {
        "ticker": Column(str, nullable=False),
        "date": Column(pa.DateTime, nullable=False),

        "open": Column(float, Check.gt(0)),
        "high": Column(float, Check.gt(0)),
        "low": Column(float, Check.gt(0)),
        "close": Column(float, Check.gt(0)),

        "volume": Column(int, Check.ge(0)),
    },
    strict=True
)


def clean_and_validate():
    # ---------------------------------
    # Load only NEW rows from Bronze
    # ---------------------------------
    query = """
    SELECT *
    FROM bronze_price_raw
    WHERE date NOT IN (
        SELECT date FROM silver_price_clean
    )
    """

    df = pd.read_sql(query, engine, parse_dates=["date"])

    if df.empty:
        print("ℹ️ No new rows to process in Silver layer")
        return

    # ---------------------------------
    # Basic cleaning
    # ---------------------------------
    df = df.drop_duplicates(subset=["ticker", "date"])
    df = df.sort_values(["ticker", "date"])

    # ---------------------------------
    # OHLC logical consistency
    # ---------------------------------
    df = df[
        (df["high"] >= df["open"]) &
        (df["high"] >= df["close"]) &
        (df["low"] <= df["open"]) &
        (df["low"] <= df["close"])
    ]

    # ---------------------------------
    # Select Silver columns
    # ---------------------------------
    df = df[
        ["ticker", "date", "open", "high", "low", "close", "volume"]
    ]

    # ---------------------------------
    # Pandera validation
    # ---------------------------------
    df = silver_schema.validate(df, lazy=True)

    # ---------------------------------
    # Append to Silver
    # ---------------------------------
    df.to_sql(
        "silver_price_clean",
        engine,
        if_exists="append",
        index=False
    )

    print(f"✅ Silver layer appended {len(df)} rows")


if __name__ == "__main__":
    clean_and_validate()
