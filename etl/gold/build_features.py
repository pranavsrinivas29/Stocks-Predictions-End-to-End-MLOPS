import pandas as pd
from etl.common.db import engine
from features.feature_engineering import build_feature_table


def build_gold_features():
    # -------------------------------------------------
    # Load FULL Silver history (for rolling context)
    # -------------------------------------------------
    silver_df = pd.read_sql(
        "SELECT * FROM silver_price_clean",
        engine,
        parse_dates=["date"]
    )
    print("Silver rows:", len(silver_df))
    print(silver_df.head(12))
    if silver_df.empty:
        print("ℹ️ Silver table empty")
        return

    silver_df = silver_df.sort_values("date")
    silver_df = silver_df.set_index("date")

    # -------------------------------------------------
    # Build features using FULL history
    # -------------------------------------------------
    features_df = build_feature_table(silver_df)
    print("Feature rows:", len(features_df))
    print(features_df.head())
    
    features_df = features_df.reset_index()
    features_df["ticker"] = silver_df["ticker"].iloc[0]

    # -------------------------------------------------
    # Load existing Gold dates
    # -------------------------------------------------
    try:
        gold_dates = pd.read_sql(
            "SELECT date FROM gold_price_features",
            engine,
            parse_dates=["date"]
        )["date"]
    except Exception:
        gold_dates = pd.Series(dtype="datetime64[ns]")

    # -------------------------------------------------
    # Keep ONLY new feature rows
    # -------------------------------------------------
    features_df = features_df[~features_df["date"].isin(gold_dates)]

    if features_df.empty:
        print("ℹ️ No new rows to append to Gold")
        return

    # -------------------------------------------------
    # Append to Gold
    # -------------------------------------------------
    features_df.to_sql(
        "gold_price_features",
        engine,
        if_exists="append",
        index=False
    )

    print(f"✅ Gold layer appended {len(features_df)} rows")


if __name__ == "__main__":
    build_gold_features()
