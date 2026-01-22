import pandas as pd
import numpy as np


def add_price_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["return"] = df["close"].pct_change()
    df["log_return"] = np.log(df["close"] / df["close"].shift(1))

    return df


def add_lag_features(df: pd.DataFrame, lags=(1, 3)) -> pd.DataFrame:
    df = df.copy()

    for lag in lags:
        df[f"return_lag_{lag}"] = df["log_return"].shift(lag)
        df[f"close_lag_{lag}"] = df["close"].shift(lag)

    return df


def add_rolling_features(df: pd.DataFrame, windows=(1, 3)) -> pd.DataFrame:
    df = df.copy()

    for window in windows:
        df[f"rolling_mean_{window}"] = df["log_return"].rolling(window).mean()
        df[f"rolling_std_{window}"] = df["log_return"].rolling(window).std()

    return df


def add_moving_averages(df: pd.DataFrame, windows=(3, 10)) -> pd.DataFrame:
    df = df.copy()

    for window in windows:
        df[f"sma_{window}"] = df["close"].rolling(window).mean()
        df[f"ema_{window}"] = df["close"].ewm(span=window, adjust=False).mean()

    return df


def add_volume_features(df: pd.DataFrame, window=10) -> pd.DataFrame:
    df = df.copy()

    df["volume_change"] = df["volume"].pct_change()
    df[f"volume_rolling_mean_{window}"] = df["volume"].rolling(window).mean()

    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    df["day_of_week"] = df.index.dayofweek
    df["week_of_year"] = df.index.isocalendar().week.astype(int)
    df["month"] = df.index.month
    df["is_month_end"] = df.index.is_month_end.astype(int)

    return df


def build_feature_table(df: pd.DataFrame) -> pd.DataFrame:
    if not isinstance(df.index, pd.DatetimeIndex):
        raise ValueError("Index must be DatetimeIndex")

    df = df.sort_index()

    df = add_price_features(df)
    df = add_lag_features(df)
    df = add_rolling_features(df)
    df = add_moving_averages(df)
    df = add_volume_features(df)
    df = add_time_features(df)

    #df = df.dropna()

    return df
