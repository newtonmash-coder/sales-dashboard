# ==========================================================
# data_engine.py
# FINAL UPDATED VERSION
# Keeps all original logic + adds raw column cleanup
# ==========================================================

import pandas as pd
import re
from dateutil.relativedelta import relativedelta
from datetime import datetime

from drive_loader import (
    list_files_in_folder,
    download_excel_file,
    download_csv_file
)

# ----------------------------------------------------------
# GOOGLE DRIVE FOLDER ID
# ----------------------------------------------------------
FOLDER_ID = "11QmN5FWNu5XDEVMPh75Q0eGHtn0Czlyt"


# ----------------------------------------------------------
# CLEAN / STANDARDIZE RAW COLUMNS
# ----------------------------------------------------------
def clean_columns(df):

    rename_map = {
        "NetSales": "Sales",
        "Distibutor": "Distributor",
        "Outlet_code": "outlet_code"
    }

    df = df.rename(columns=rename_map)

    return df


# ----------------------------------------------------------
# EXTRACT MONTH & YEAR FROM FILE NAME
# Example:
# 1 Sales 2024.xlsx = Month 1 / Year 2024
# ----------------------------------------------------------
def extract_month_year(filename):

    m = re.match(r"(\d+).*?(\d{4})", filename)

    if m:
        month = int(m.group(1))
        year = int(m.group(2))
        return month, year

    return None, None


# ----------------------------------------------------------
# LOAD ALL SALES FILES
# ----------------------------------------------------------
def load_sales_data():

    files = list_files_in_folder(FOLDER_ID)

    sales_files = [
        f for f in files
        if "sales" in f["name"].lower()
    ]

    all_data = []

    for f in sales_files:

        print("Loading Sales File:", f["name"])

        df = download_excel_file(f["id"])

        # clean raw columns
        df = clean_columns(df)

        # extract month/year from filename
        month, year = extract_month_year(f["name"])

        df["Month_File"] = month
        df["Year_File"] = year
        df["SourceFile"] = f["name"]

        # date cleanup
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(
                df["Date"],
                errors="coerce"
            )

        all_data.append(df)

    final_df = pd.concat(
        all_data,
        ignore_index=True
    )

    return final_df


# ----------------------------------------------------------
# LOAD ALL TARGET FILES
# ----------------------------------------------------------
def load_targets_data():

    files = list_files_in_folder(FOLDER_ID)

    target_files = [
        f for f in files
        if "target" in f["name"].lower()
    ]

    all_data = []

    for f in target_files:

        print("Loading Target File:", f["name"])

        df = download_excel_file(f["id"])

        # clean raw columns
        df = clean_columns(df)

        # extract month/year
        month, year = extract_month_year(f["name"])

        df["Month_File"] = month
        df["Year_File"] = year
        df["SourceFile"] = f["name"]

        all_data.append(df)

    final_df = pd.concat(
        all_data,
        ignore_index=True
    )

    return final_df


# ----------------------------------------------------------
# CREATE TIME FEATURES
# ----------------------------------------------------------
def create_time_features(df):

    df["Date"] = pd.to_datetime(
        df["Date"],
        errors="coerce"
    )

    df["Year"] = df["Date"].dt.year
    df["Month"] = df["Date"].dt.month
    df["Quarter"] = df["Date"].dt.quarter

    # cumulative metrics
    df["MTD"] = df.groupby(
        ["Year", "Month"]
    )["Sales"].cumsum()

    df["QTD"] = df.groupby(
        ["Year", "Quarter"]
    )["Sales"].cumsum()

    df["YTD"] = df.groupby(
        ["Year"]
    )["Sales"].cumsum()

    return df


# ----------------------------------------------------------
# MERGE SALES + TARGETS
# ----------------------------------------------------------
def merge_sales_with_targets(sales, targets):

    merged = sales.merge(
        targets,
        on=[
            "Distributor",
            "DT_Name",
            "FSR_code",
            "FSR",
            "Brand",
            "Month_File",
            "Year_File"
        ],
        how="left",
        suffixes=("", "_target")
    )

    return merged