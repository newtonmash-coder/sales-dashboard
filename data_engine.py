# ==========================================================
# data_engine.py
# Reads from LOCAL parquet files (written by preload.py).
# Zero Drive API calls at runtime → instant load.
# ==========================================================

import os
import re
import pandas as pd
import streamlit as st
from datetime import datetime

DATA_DIR      = "data_cache"
SALES_FILE    = os.path.join(DATA_DIR, "sales.parquet")
TARGETS_FILE  = os.path.join(DATA_DIR, "targets.parquet")


def _files_ready():
    return os.path.exists(SALES_FILE)


def extract_month_year(filename):
    m = re.match(r"(\d+).*?(\d{4})", filename)
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def is_current_month(month, year):
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# LOAD FROM DISK — cached so it only reads once per session
# ----------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def load_sales_data():
    if not _files_ready():
        return pd.DataFrame()
    return pd.read_parquet(SALES_FILE)


@st.cache_data(ttl=300, show_spinner=False)
def load_targets_data():
    if not os.path.exists(TARGETS_FILE):
        return pd.DataFrame()
    return pd.read_parquet(TARGETS_FILE)


# ----------------------------------------------------------
# TIME FEATURES
# ----------------------------------------------------------
def create_time_features(df):
    if df.empty:
        return df
    df["Date"]    = pd.to_datetime(df["Date"], errors="coerce")
    df["Year"]    = df["Date"].dt.year
    df["Month"]   = df["Date"].dt.month
    df["Quarter"] = df["Date"].dt.quarter
    df = df.sort_values("Date")
    df["MTD"] = df.groupby(["Year", "Month"])["Sales"].cumsum()
    df["QTD"] = df.groupby(["Year", "Quarter"])["Sales"].cumsum()
    df["YTD"] = df.groupby(["Year"])["Sales"].cumsum()
    return df


# ----------------------------------------------------------
# MERGE
# ----------------------------------------------------------
def merge_sales_with_targets(sales, targets):
    if targets.empty:
        sales["Sales_Targets"] = 0
        return sales

    possible_keys = ["Distributor", "DT_Name", "FSR_code",
                     "FSR", "Brand", "Month_File", "Year_File"]
    merge_keys = [k for k in possible_keys
                  if k in sales.columns and k in targets.columns]

    merged = sales.merge(targets, on=merge_keys, how="left", suffixes=("", "_target"))
    if "Sales_Targets" in merged.columns:
        merged["Sales_Targets"] = merged["Sales_Targets"].fillna(0)
    else:
        merged["Sales_Targets"] = 0
    return merged