# ==========================================================
# data_engine.py
# FIXED — uses recursive folder search so files inside
#          subfolders (e.g. "Spread", "Targets_Spreads")
#          are found automatically
# ==========================================================

import pandas as pd
import re
from datetime import datetime

import streamlit as st

from drive_loader import (
    list_all_files_recursive,
    read_drive_file
)

# ----------------------------------------------------------
# ROOT GOOGLE DRIVE FOLDER ID
# This is the TOP-LEVEL folder that contains both the
# "Spread" subfolder (sales) and "Targets_Spreads" subfolder
# Set this to the ID of "Rawdata 2024" folder
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
# "1 sales 2024.csv"    → month=1,  year=2024
# "1 Targets 2024.csv"  → month=1,  year=2024
# ----------------------------------------------------------
def extract_month_year(filename):
    m = re.match(r"(\d+).*?(\d{4})", filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


# ----------------------------------------------------------
# IS CURRENT MONTH FILE?
# ----------------------------------------------------------
def is_current_month(month, year):
    if month is None or year is None:
        return False
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# CACHED FILE LOADERS
# Old months / targets → cache 24 hours (never change)
# Current month sales  → cache 5 minutes (daily updates)
# ----------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def _load_file_cached(file_id):
    return read_drive_file(file_id)

@st.cache_data(ttl=300, show_spinner=False)
def _load_file_live(file_id):
    return read_drive_file(file_id)


# ----------------------------------------------------------
# LOAD ALL SALES FILES
# Searches all subfolders recursively — finds files even if
# they are inside a subfolder like "Spread"
# ----------------------------------------------------------
def load_sales_data():
    # Get ALL files across all subfolders
    all_files = list_all_files_recursive(FOLDER_ID)

    # Filter: files whose name contains "sales" (case-insensitive)
    sales_files = [
        f for f in all_files
        if "sales" in f["name"].lower()
    ]

    if not sales_files:
        st.error(
            "No sales files found. Make sure your files are named like "
            "'1 sales 2024.csv' and are in the correct Google Drive folder."
        )
        return pd.DataFrame()

    # Show which files were found (helps with debugging)
    st.sidebar.markdown("**Sales files found:**")
    for f in sorted(sales_files, key=lambda x: x["name"]):
        st.sidebar.caption(f"• {f['name']}")

    all_data = []

    for f in sales_files:
        month, year = extract_month_year(f["name"])
        live = is_current_month(month, year)

        print(f"Loading {'[LIVE] ' if live else ''}Sales: {f['name']}")

        try:
            df = _load_file_live(f["id"]) if live else _load_file_cached(f["id"])

            df = clean_columns(df)
            df["Month_File"] = month
            df["Year_File"]  = year
            df["SourceFile"] = f["name"]
            df["IsLive"]     = live

            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")

            all_data.append(df)

        except Exception as e:
            st.warning(f"Could not load sales file '{f['name']}': {e}")
            continue

    if not all_data:
        st.error("Sales files were found but could not be read.")
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


# ----------------------------------------------------------
# LOAD ALL TARGET FILES
# Searches all subfolders recursively — finds files even if
# they are inside "Targets_Spreads" subfolder
# ----------------------------------------------------------
def load_targets_data():
    all_files = list_all_files_recursive(FOLDER_ID)

    # Filter: files whose name contains "target" (case-insensitive)
    target_files = [
        f for f in all_files
        if "target" in f["name"].lower()
    ]

    if not target_files:
        st.warning(
            "No target files found. Make sure your files are named like "
            "'1 Targets 2024.csv' and are in the correct Google Drive folder."
        )
        return pd.DataFrame()

    # Show which target files were found
    st.sidebar.markdown("**Target files found:**")
    for f in sorted(target_files, key=lambda x: x["name"]):
        st.sidebar.caption(f"• {f['name']}")

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
# CREATE TIME FEATURES (MTD, QTD, YTD cumulative columns)
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
# MERGE SALES + TARGETS
# Only merges on columns that actually exist in both files
# ----------------------------------------------------------
def merge_sales_with_targets(sales, targets):
    if targets.empty:
        sales["Sales_Targets"] = 0
        return sales

    possible_keys = [
        "Distributor", "DT_Name", "FSR_code",
        "FSR", "Brand", "Month_File", "Year_File"
    ]
    merge_keys = [
        k for k in possible_keys
        if k in sales.columns and k in targets.columns
    ]

    merged = sales.merge(
        targets,
        on=merge_keys,
        how="left",
        suffixes=("", "_target")
    )

    if "Sales_Targets" in merged.columns:
        merged["Sales_Targets"] = merged["Sales_Targets"].fillna(0)
    else:
        merged["Sales_Targets"] = 0

    return merged