"""Time/latency helpers shared across the AQ and HMS streams."""
from datetime import datetime, timedelta

import numpy as np
import pandas as pd


def offset_hour(df, offset=0):
    df["utc_ts"] = pd.to_datetime(df.utc_ts, utc=True)
    df.utc_ts = df.utc_ts + pd.Timedelta(offset, unit="h")
    return df


def add_latency(df):
    # convert to datetime type if not already
    if pd.api.types.is_string_dtype(df.utc_ts):
        df["utc_ts"] = pd.to_datetime(df.utc_ts, utc=True)

    # drop NaT
    df = df.dropna(subset=["utc_ts"])

    df["latency_mins"] = (pd.Timestamp.utcnow() - df.utc_ts) / pd.Timedelta(minutes=1)
    df.latency_mins = df.latency_mins
    df.latency_mins = df.latency_mins.astype("int")
    return df


def add_status(df):
    df["status"] = 0
    df["status"] = np.where((df["latency_mins"] > 160) & (df["latency_mins"] <= 250), 1, df["status"])
    df["status"] = np.where((df["latency_mins"] > 250), 2, df["status"])
    return df


def parse_hms_smoke_datetime(time_str):
    """Parse HMS smoke datetime format (YYYYDDD HHMM) to a datetime object."""
    split = time_str.split(" ")
    yearday = split[0]
    hourmin = split[1]
    date = datetime.strptime(str(yearday), "%Y%j")
    hour = int(hourmin[:2])
    minute = int(hourmin[2:])
    date = date + timedelta(hours=hour)
    date = date + timedelta(minutes=minute)
    return date
