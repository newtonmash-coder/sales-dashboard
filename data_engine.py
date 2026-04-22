# ==========================================================
# data_engine.py
# Reads from disk if available, downloads inline if not.
# Handles Render ephemeral filesystem correctly.
# ==========================================================

import os
import re
import json
import pandas as pd
import streamlit as st
from io import BytesIO
from datetime import datetime

DATA_DIR     = "/tmp/data_cache"   # /tmp persists within a session on Render
SALES_FILE   = os.path.join(DATA_DIR, "sales.parquet")
TARGETS_FILE = os.path.join(DATA_DIR, "targets.parquet")

FOLDER_ID    = "1tX9kPXQK3WQvQVAIF0YambVHJyh34qeL"

MIME_SHEET   = "application/vnd.google-apps.spreadsheet"
MIME_FOLDER  = "application/vnd.google-apps.folder"

os.makedirs(DATA_DIR, exist_ok=True)


# ----------------------------------------------------------
# DRIVE SERVICE
# ----------------------------------------------------------
@st.cache_resource
def get_drive_service():
    import os, json
    from google.oauth2 import service_account
    from googleapiclient.discovery import build

    creds_dict = json.loads(os.environ["GOOGLE_CREDS"])
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


# ----------------------------------------------------------
# DRIVE HELPERS
# ----------------------------------------------------------
def _list_folder(folder_id):
    svc = get_drive_service()
    res = svc.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,modifiedTime)",
        pageSize=1000
    ).execute()
    return res.get("files", [])


def _list_recursive(folder_id):
    all_files = []
    for item in _list_folder(folder_id):
        if item["mimeType"] == MIME_FOLDER:
            all_files.extend(_list_recursive(item["id"]))
        else:
            all_files.append(item)
    return all_files


def _download_bytes(file_id, mime):
    from googleapiclient.http import MediaIoBaseDownload
    svc = get_drive_service()
    if mime == MIME_SHEET:
        req = svc.files().export_media(fileId=file_id, mimeType="text/csv")
    else:
        req = svc.files().get_media(fileId=file_id)
    buf = BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf


def _read_csv_safe(buf, name):
    for enc in ["utf-8", "latin-1", "windows-1252", "utf-8-sig"]:
        try:
            buf.seek(0)
            return pd.read_csv(buf, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode '{name}' with any encoding.")


def _read_file(file_id, name, mime):
    buf = _download_bytes(file_id, mime)
    if mime == MIME_SHEET or name.lower().endswith(".csv"):
        return _read_csv_safe(buf, name)
    try:
        return pd.read_excel(buf, engine="openpyxl")
    except Exception:
        return _read_csv_safe(buf, name)


def _clean_columns(df):
    rename_map = {
        "NetSales":    "Sales",
        "Distibutor":  "Distributor",
        "Outlet_code": "outlet_code",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].astype(str).str.strip()
    return df


def _extract_month_year(filename):
    m = re.match(r"(\d+).*?(\d{4})", filename)
    return (int(m.group(1)), int(m.group(2))) if m else (None, None)


def _is_current_month(month, year):
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# GET FILE LIST (cached 1 hour to avoid repeated API calls)
# ----------------------------------------------------------
@st.cache_data(ttl=3600, show_spinner=False)
def _get_file_list():
    all_files    = _list_recursive(FOLDER_ID)
    sales_files  = [f for f in all_files if "sales"  in f["name"].lower()]
    target_files = [f for f in all_files if "target" in f["name"].lower()]
    return sales_files, target_files


# ----------------------------------------------------------
# DOWNLOAD AND BUILD — called once, result saved to /tmp
# Progress updates keep the WebSocket alive during download
# ----------------------------------------------------------
def build_and_cache(status_placeholder, progress_bar):
    """
    Downloads all files from Drive, saves parquet to /tmp.
    Returns (sales_df, targets_df).
    Uses status/progress placeholders to keep UI alive.
    """
    sales_files, target_files = _get_file_list()
    total = len(sales_files) + len(target_files)

    if total == 0:
        return pd.DataFrame(), pd.DataFrame()

    sales_frames   = []
    targets_frames = []
    done_count     = 0
    errors         = []

    # --- Sales ---
    for f in sorted(sales_files, key=lambda x: x["name"]):
        done_count += 1
        pct = int(done_count / total * 100)
        month, year = _extract_month_year(f["name"])
        live = _is_current_month(month, year)
        tag  = "🔴 LIVE" if live else "📁"
        status_placeholder.info(f"{tag} Loading: **{f['name']}** ({done_count}/{total})")
        progress_bar.progress(pct)

        try:
            df = _read_file(f["id"], f["name"], f.get("mimeType", ""))
            df = _clean_columns(df)
            df["Month_File"] = month
            df["Year_File"]  = year
            df["SourceFile"] = f["name"]
            df["IsLive"]     = live
            if "Date" in df.columns:
                df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
            sales_frames.append(df)
        except Exception as e:
            errors.append(f"Sales '{f['name']}': {e}")

    # --- Targets ---
    for f in sorted(target_files, key=lambda x: x["name"]):
        done_count += 1
        pct = int(done_count / total * 100)
        status_placeholder.info(f"🎯 Loading target: **{f['name']}** ({done_count}/{total})")
        progress_bar.progress(pct)

        try:
            df = _read_file(f["id"], f["name"], f.get("mimeType", ""))
            df = _clean_columns(df)
            month, year = _extract_month_year(f["name"])
            df["Month_File"] = month
            df["Year_File"]  = year
            df["SourceFile"] = f["name"]
            targets_frames.append(df)
        except Exception as e:
            errors.append(f"Target '{f['name']}': {e}")

    for err in errors:
        st.warning(err)

    sales_df   = pd.concat(sales_frames,   ignore_index=True) if sales_frames   else pd.DataFrame()
    targets_df = pd.concat(targets_frames, ignore_index=True) if targets_frames else pd.DataFrame()

    # Save to /tmp so subsequent page interactions read from disk
    if not sales_df.empty:
        sales_df.to_parquet(SALES_FILE, index=False)
    if not targets_df.empty:
        targets_df.to_parquet(TARGETS_FILE, index=False)

    return sales_df, targets_df


# ----------------------------------------------------------
# READ FROM DISK (fast — used after first load)
# ----------------------------------------------------------
def read_from_disk():
    sales_df   = pd.read_parquet(SALES_FILE)   if os.path.exists(SALES_FILE)   else pd.DataFrame()
    targets_df = pd.read_parquet(TARGETS_FILE) if os.path.exists(TARGETS_FILE) else pd.DataFrame()
    return sales_df, targets_df


def disk_cache_exists():
    return os.path.exists(SALES_FILE)


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