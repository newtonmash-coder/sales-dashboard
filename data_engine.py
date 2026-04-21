# ==========================================================
# data_engine.py
# Loads files one-by-one with progress callback so the
# Render connection stays alive during cold start
# ==========================================================

import pandas as pd
import re
from datetime import datetime

import streamlit as st
from drive_loader import list_all_files_recursive, read_drive_file

FOLDER_ID = "1tX9kPXQK3WQvQVAIF0YambVHJyh34qeL"


# ----------------------------------------------------------
# HELPERS
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


def extract_month_year(filename):
    m = re.match(r"(\d+).*?(\d{4})", filename)
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def is_current_month(month, year):
    if not month or not year:
        return False
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# PER-FILE CACHES
# Each file is cached individually by file_id.
# Old months: 24 h | Current month: 5 min
# ----------------------------------------------------------
@st.cache_data(ttl=86400, show_spinner=False)
def _fetch_file_cached(file_id):
    return read_drive_file(file_id)

@st.cache_data(ttl=300, show_spinner=False)
def _fetch_file_live(file_id):
    return read_drive_file(file_id)


def _process_raw(raw_df, filename, live):
    df = clean_columns(raw_df.copy())
    month, year = extract_month_year(filename)
    df["Month_File"] = month
    df["Year_File"]  = year
    df["SourceFile"] = filename
    df["IsLive"]     = live
    if "Date" in df.columns:
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
    return df


# ----------------------------------------------------------
# GET FILE LISTS — cached 1 hour
# ----------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def _get_file_list():
    all_files    = list_all_files_recursive(FOLDER_ID)
    sales_files  = [f for f in all_files if "sales"  in f["name"].lower()]
    target_files = [f for f in all_files if "target" in f["name"].lower()]
    return sales_files, target_files


# ----------------------------------------------------------
# LOAD SALES — streams files one by one, updates progress
# progress_cb: callable(current, total, filename)
# ----------------------------------------------------------
def load_sales_data(progress_cb=None):
    sales_files, _ = _get_file_list()

    if not sales_files:
        return pd.DataFrame(), []

    frames  = []
    errors  = []
    total   = len(sales_files)

    for i, f in enumerate(sorted(sales_files, key=lambda x: x["name"]), 1):
        if progress_cb:
            progress_cb(i, total, f["name"], "sales")
        month, year = extract_month_year(f["name"])
        live = is_current_month(month, year)
        try:
            raw = _fetch_file_live(f["id"]) if live else _fetch_file_cached(f["id"])
            frames.append(_process_raw(raw, f["name"], live))
        except Exception as e:
            errors.append(f"Sales '{f['name']}': {e}")

    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), errors


# ----------------------------------------------------------
# LOAD TARGETS — streams files one by one
# ----------------------------------------------------------
def load_targets_data(progress_cb=None):
    _, target_files = _get_file_list()

    if not target_files:
        return pd.DataFrame(), []

    frames = []
    errors = []
    total  = len(target_files)

    for i, f in enumerate(sorted(target_files, key=lambda x: x["name"]), 1):
        if progress_cb:
            progress_cb(i, total, f["name"], "targets")
        try:
            raw = _fetch_file_cached(f["id"])
            df  = clean_columns(raw.copy())
            month, year = extract_month_year(f["name"])
            df["Month_File"] = month
            df["Year_File"]  = year
            df["SourceFile"] = f["name"]
            frames.append(df)
        except Exception as e:
            errors.append(f"Target '{f['name']}': {e}")

    return (pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()), errors


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

    possible_keys = ["Distributor", "DT_Name", "FSR_code", "FSR", "Brand", "Month_File", "Year_File"]
    merge_keys    = [k for k in possible_keys if k in sales.columns and k in targets.columns]

    merged = sales.merge(targets, on=merge_keys, how="left", suffixes=("", "_target"))
    if "Sales_Targets" in merged.columns:
        merged["Sales_Targets"] = merged["Sales_Targets"].fillna(0)
    else:
        merged["Sales_Targets"] = 0
    return merged