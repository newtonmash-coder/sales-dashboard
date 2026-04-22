#!/usr/bin/env python3
# ==========================================================
# preload.py
# Runs ONCE at server startup (before Streamlit starts).
# Downloads all Google Drive files → saves as local parquet.
# App then reads from disk instantly — no Drive calls on load.
# ==========================================================

import os
import sys
import json
import re
import time
import pandas as pd
from io import BytesIO
from datetime import datetime

print("=" * 60)
print("PRELOAD: Starting data download from Google Drive...")
print("=" * 60)

# ----------------------------------------------------------
# Auth
# ----------------------------------------------------------
try:
    from google.oauth2 import service_account
    from googleapiclient.discovery import build
    from googleapiclient.http import MediaIoBaseDownload

    creds_json = os.environ.get("GOOGLE_CREDS", "")
    if not creds_json:
        print("PRELOAD ERROR: GOOGLE_CREDS env variable not set.")
        sys.exit(0)  # Don't crash — let app show error message

    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    service = build("drive", "v3", credentials=creds)
    print("PRELOAD: Google Drive authenticated OK")

except Exception as e:
    print(f"PRELOAD ERROR: Auth failed: {e}")
    sys.exit(0)

# ----------------------------------------------------------
# Config
# ----------------------------------------------------------
FOLDER_ID      = "1tX9kPXQK3WQvQVAIF0YambVHJyh34qeL"
DATA_DIR       = "data_cache"
SALES_FILE     = os.path.join(DATA_DIR, "sales.parquet")
TARGETS_FILE   = os.path.join(DATA_DIR, "targets.parquet")
MANIFEST_FILE  = os.path.join(DATA_DIR, "manifest.json")

MIME_SHEET  = "application/vnd.google-apps.spreadsheet"
MIME_FOLDER = "application/vnd.google-apps.folder"

os.makedirs(DATA_DIR, exist_ok=True)

# ----------------------------------------------------------
# Helpers
# ----------------------------------------------------------
def list_folder(folder_id):
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,modifiedTime)",
        pageSize=1000
    ).execute()
    return results.get("files", [])


def list_recursive(folder_id):
    all_files = []
    for item in list_folder(folder_id):
        if item["mimeType"] == MIME_FOLDER:
            all_files.extend(list_recursive(item["id"]))
        else:
            all_files.append(item)
    return all_files


def download_bytes(file_id, mime):
    if mime == MIME_SHEET:
        req = service.files().export_media(fileId=file_id, mimeType="text/csv")
    else:
        req = service.files().get_media(fileId=file_id)
    buf = BytesIO()
    dl = MediaIoBaseDownload(buf, req)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf


def read_csv_safe(buf, name):
    for enc in ["utf-8", "latin-1", "windows-1252", "utf-8-sig"]:
        try:
            buf.seek(0)
            return pd.read_csv(buf, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode '{name}'")


def read_file(file_id, name, mime):
    buf = download_bytes(file_id, mime)
    if mime == MIME_SHEET or name.lower().endswith(".csv"):
        return read_csv_safe(buf, name)
    try:
        return pd.read_excel(buf, engine="openpyxl")
    except Exception:
        return read_csv_safe(buf, name)


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
    now = datetime.now()
    return month == now.month and year == now.year


# ----------------------------------------------------------
# Check if re-download is needed
# Current month file always re-downloads.
# Old files only re-download if Drive modifiedTime changed.
# ----------------------------------------------------------
def load_manifest():
    if os.path.exists(MANIFEST_FILE):
        with open(MANIFEST_FILE) as f:
            return json.load(f)
    return {}


def save_manifest(manifest):
    with open(MANIFEST_FILE, "w") as f:
        json.dump(manifest, f)


# ----------------------------------------------------------
# MAIN DOWNLOAD
# ----------------------------------------------------------
print("PRELOAD: Listing files in Google Drive...")
try:
    all_files = list_recursive(FOLDER_ID)
except Exception as e:
    print(f"PRELOAD ERROR: Could not list files: {e}")
    sys.exit(0)

sales_files  = [f for f in all_files if "sales"  in f["name"].lower()]
target_files = [f for f in all_files if "target" in f["name"].lower()]

print(f"PRELOAD: Found {len(sales_files)} sales files, {len(target_files)} target files")

manifest = load_manifest()
sales_frames   = []
targets_frames = []

# --- Sales ---
for f in sorted(sales_files, key=lambda x: x["name"]):
    name  = f["name"]
    fid   = f["id"]
    mtime = f.get("modifiedTime", "")
    mime  = f.get("mimeType", "")
    month, year = extract_month_year(name)
    live = is_current_month(month, year)

    cached_mtime = manifest.get(fid, {}).get("modifiedTime", "")
    skip = (not live) and (cached_mtime == mtime) and os.path.exists(SALES_FILE)

    print(f"  {'[LIVE]' if live else '[OLD] '} {name} ... ", end="", flush=True)

    try:
        df = read_file(fid, name, mime)
        df = clean_columns(df)
        df["Month_File"] = month
        df["Year_File"]  = year
        df["SourceFile"] = name
        df["IsLive"]     = live
        if "Date" in df.columns:
            df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        sales_frames.append(df)
        manifest[fid] = {"modifiedTime": mtime, "name": name}
        print(f"OK ({len(df)} rows)")
    except Exception as e:
        print(f"ERROR: {e}")

# --- Targets ---
for f in sorted(target_files, key=lambda x: x["name"]):
    name  = f["name"]
    fid   = f["id"]
    mime  = f.get("mimeType", "")
    month, year = extract_month_year(name)

    print(f"  [TGT ] {name} ... ", end="", flush=True)

    try:
        df = read_file(fid, name, mime)
        df = clean_columns(df)
        df["Month_File"] = month
        df["Year_File"]  = year
        df["SourceFile"] = name
        targets_frames.append(df)
        print(f"OK ({len(df)} rows)")
    except Exception as e:
        print(f"ERROR: {e}")

# --- Save ---
if sales_frames:
    sales_df = pd.concat(sales_frames, ignore_index=True)
    sales_df.to_parquet(SALES_FILE, index=False)
    print(f"PRELOAD: Sales saved → {SALES_FILE} ({len(sales_df):,} rows)")
else:
    print("PRELOAD WARNING: No sales data downloaded.")

if targets_frames:
    tgt_df = pd.concat(targets_frames, ignore_index=True)
    tgt_df.to_parquet(TARGETS_FILE, index=False)
    print(f"PRELOAD: Targets saved → {TARGETS_FILE} ({len(tgt_df):,} rows)")
else:
    print("PRELOAD WARNING: No target data downloaded.")

save_manifest(manifest)
print("PRELOAD: Complete.")
print("=" * 60)
