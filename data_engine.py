# ==========================================================
# data_engine.py
# FIXED VERSION — works with CSV sales and target files
# ==========================================================

import pandas as pd
import re
from datetime import datetime

from drive_loader import (
    list_files_in_folder,
    read_drive_file
)

# ----------------------------------------------------------
# GOOGLE DRIVE FOLDER ID
# ----------------------------------------------------------
FOLDER_ID = "1tX9kPXQK3WQvQVAIF0YambVHJyh34qeL"


# ----------------------------------------------------------
# CLEAN / STANDARDIZE RAW COLUMNS
# ----------------------------------------------------------
def clean_columns(df):
    rename_map = {
        "NetSales":    "Sales",
        "Distibutor":  "Distributor",
        "Outlet_code": "outlet_code",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Strip whitespace from all string columns
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()

    return df


# ----------------------------------------------------------
# EXTRACT MONTH & YEAR FROM FILE NAME
# e.g. "1 Sales 2024.csv" → month=1, year=2024
# ----------------------------------------------------------
def extract_month_year(filename):
    m = re.match(r"(\d+).*?(\d{4})", filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


# ----------------------------------------------------------
# IS CURRENT MONTH FILE?
# The file whose month/year matches today is "live"
# ----------------------------------------------------------
def is_current_month(month, year):
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# LOAD ALL SALES FILES
# Old months → cached 24 hrs | Current month → cached 5 min
# ----------------------------------------------------------
def load_sales_data():
    import streamlit as st

    files = list_files_in_folder(FOLDER_ID)
    sales_files = [f for f in files if "sales" in f["name"].lower()]

    if not sales_files:
        st.error("No sales files found in the Google Drive folder.")
        return pd.DataFrame()

    all_data = []

    for f in sales_files:
        month, year = extract_month_year(f["name"])
        live = is_current_month(month, year)

        print(f"Loading {'[LIVE] ' if live else ''}Sales: {f['name']}")

        try:
            # Use appropriate TTL cache based on whether file is current month
            if live:
                df = _load_file_live(f["id"])
            else:
                df = _load_file_cached(f["id"])

            df = clean_columns(df)
            df["Month_File"]  = month
            df["Year_File"]   = year
            df["SourceFile"]  = f["name"]
            df["IsLive"]      = live

            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

            all_data.append(df)

        except Exception as e:
            st.warning(f"Could not load sales file '{f['name']}': {e}")
            continue

    if not all_data:
        st.error("No sales data could be loaded.")
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


# ----------------------------------------------------------
# LOAD ALL TARGET FILES
# ----------------------------------------------------------
def load_targets_data():
    import streamlit as st

    files = list_files_in_folder(FOLDER_ID)
    target_files = [f for f in files if "target" in f["name"].lower()]

    if not target_files:
        st.warning("No target files found in the Google Drive folder.")
        return pd.DataFrame()

    all_data = []

    for f in target_files:
        month, year = extract_month_year(f["name"])
        print(f"Loading Target: {f['name']}")

        try:
            df = _load_file_cached(f["id"])
            df = clean_columns(df)
            df["Month_File"] = month
            df["Year_File"]  = year
            df["SourceFile"] = f["name"]
            all_data.append(df)

        except Exception as e:
            st.warning(f"Could not load target file '{f['name']}': {e}")
            continue

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


# ----------------------------------------------------------
# CACHED LOADERS
# _load_file_cached  → 24 hours  (old months, targets)
# _load_file_live    → 5 minutes (current month)
# ----------------------------------------------------------
import streamlit as st

@st.cache_data(ttl=86400, show_spinner=False)
def _load_file_cached(file_id):
    return read_drive_file(file_id)

@st.cache_data(ttl=300, show_spinner=False)
def _load_file_live(file_id):
    return read_drive_file(file_id)


# ----------------------------------------------------------
# CREATE TIME FEATURES
# ----------------------------------------------------------
def create_time_features(df):
    if df.empty:
        return df

    df["Date"]    = pd.to_datetime(df["Date"], errors="coerce")
    df["Year"]    = df["Date"].dt.year
    df["Month"]   = df["Date"].dt.month
    df["Quarter"] = df["Date"].dt.quarter

    # Cumulative sales within each period
    df = df.sort_values("Date")

    df["MTD"] = df.groupby(["Year", "Month"])["Sales"].cumsum()
    df["QTD"] = df.groupby(["Year", "Quarter"])["Sales"].cumsum()
    df["YTD"] = df.groupby(["Year"])["Sales"].cumsum()

    return df


# ----------------------------------------------------------
# MERGE SALES + TARGETS
# ----------------------------------------------------------
def merge_sales_with_targets(sales, targets):
    if targets.empty:
        # No targets yet — add empty columns so app doesn't crash
        sales["Sales_Targets"] = 0
        return sales

    # Determine shared merge keys (only use columns that exist in both)
    possible_keys = ["Distributor", "DT_Name", "FSR_code", "FSR", "Brand", "Month_File", "Year_File"]
    merge_keys = [k for k in possible_keys if k in sales.columns and k in targets.columns]

    merged = sales.merge(
        targets,
        on=merge_keys,
        how="left",
        suffixes=("", "_target")
    )

    # Fill missing targets with 0
    if "Sales_Targets" in merged.columns:
        merged["Sales_Targets"] = merged["Sales_Targets"].fillna(0)
    else:
        merged["Sales_Targets"] = 0

    return merged