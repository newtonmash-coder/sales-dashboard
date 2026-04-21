# ==========================================================
# data_engine.py
# OPTIMISED — entire dataset cached as one unit so Render
# never re-downloads files on each page interaction
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
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    return df


# ----------------------------------------------------------
# EXTRACT MONTH & YEAR FROM FILE NAME
# ----------------------------------------------------------
def extract_month_year(filename):
    m = re.match(r"(\d+).*?(\d{4})", filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    return None, None


def is_current_month(month, year):
    if month is None or year is None:
        return False
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# STEP 1 — LIST FILES ONCE, CACHED FOR 1 HOUR
# Avoids hitting Drive API on every page interaction
# ----------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def _get_file_list():
    """
    Fetches the full file list from Drive once per hour.
    Returns (sales_files, target_files) as lists of dicts.
    """
    all_files    = list_all_files_recursive(FOLDER_ID)
    sales_files  = [f for f in all_files if "sales"  in f["name"].lower()]
    target_files = [f for f in all_files if "target" in f["name"].lower()]
    return sales_files, target_files


# ----------------------------------------------------------
# STEP 2a — LOAD ONE OLD/COMPLETED SALES FILE
# Cached forever (24h) — completed months never change
# ----------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def _load_sales_file_cached(file_id, filename):
    df = read_drive_file(file_id)
    month, year = extract_month_year(filename)
    df = clean_columns(df)
    df["Month_File"] = month
    df["Year_File"]  = year
    df["SourceFile"] = filename
    df["IsLive"]     = False
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


# ----------------------------------------------------------
# STEP 2b — LOAD CURRENT MONTH SALES FILE
# Cached for 5 minutes — refreshes with your daily updates
# ----------------------------------------------------------
@st.cache_data(ttl=300, show_spinner=False)
def _load_sales_file_live(file_id, filename):
    df = read_drive_file(file_id)
    month, year = extract_month_year(filename)
    df = clean_columns(df)
    df["Month_File"] = month
    df["Year_File"]  = year
    df["SourceFile"] = filename
    df["IsLive"]     = True
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


# ----------------------------------------------------------
# STEP 2c — LOAD ONE TARGET FILE
# Cached for 24 hours — targets don't change daily
# ----------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def _load_target_file_cached(file_id, filename):
    df = read_drive_file(file_id)
    month, year = extract_month_year(filename)
    df = clean_columns(df)
    df["Month_File"] = month
    df["Year_File"]  = year
    df["SourceFile"] = filename
    return df


# ----------------------------------------------------------
# STEP 3 — COMBINE ALL SALES FILES
# Each file is individually cached — only missing/expired
# files are re-downloaded, not the whole dataset
# ----------------------------------------------------------
def load_sales_data():
    sales_files, _ = _get_file_list()

    if not sales_files:
        st.error(
            "No sales files found. Make sure your files are named like "
            "'1 sales 2024.csv' and are in the Google Drive folder."
        )
        return pd.DataFrame()

    # Show file list in sidebar
    with st.sidebar.expander("📂 Sales files loaded", expanded=False):
        for f in sorted(sales_files, key=lambda x: x["name"]):
            st.caption(f"• {f['name']}")

    all_data = []

    for f in sales_files:
        month, year = extract_month_year(f["name"])
        live = is_current_month(month, year)
        try:
            if live:
                df = _load_sales_file_live(f["id"], f["name"])
            else:
                df = _load_sales_file_cached(f["id"], f["name"])
            all_data.append(df)
        except Exception as e:
            st.warning(f"Skipped '{f['name']}': {e}")
            continue

    if not all_data:
        st.error("Sales files were found but could not be read.")
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


# ----------------------------------------------------------
# STEP 4 — COMBINE ALL TARGET FILES
# ----------------------------------------------------------
def load_targets_data():
    _, target_files = _get_file_list()

    if not target_files:
        st.warning(
            "No target files found. Make sure your files are named like "
            "'1 Targets 2024.csv'."
        )
        return pd.DataFrame()

    with st.sidebar.expander("🎯 Target files loaded", expanded=False):
        for f in sorted(target_files, key=lambda x: x["name"]):
            st.caption(f"• {f['name']}")

    all_data = []

    for f in target_files:
        try:
            df = _load_target_file_cached(f["id"], f["name"])
            all_data.append(df)
        except Exception as e:
            st.warning(f"Skipped target '{f['name']}': {e}")
            continue

    if not all_data:
        return pd.DataFrame()

    return pd.concat(all_data, ignore_index=True)


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