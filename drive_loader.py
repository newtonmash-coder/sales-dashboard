# ==========================================================
# drive_loader.py
# FIXED VERSION — auto-detects CSV vs Excel correctly
# ==========================================================

import os
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import pandas as pd
from io import BytesIO
import streamlit as st


# ----------------------------------------------------------
# GOOGLE DRIVE AUTH
# Reads GOOGLE_CREDS from Render Environment Variable
# ----------------------------------------------------------
@st.cache_resource
def get_drive_service():
    creds_json = os.environ["GOOGLE_CREDS"]
    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    service = build("drive", "v3", credentials=creds)
    return service


# ----------------------------------------------------------
# LIST FILES IN FOLDER
# ----------------------------------------------------------
def list_files_in_folder(folder_id):
    service = get_drive_service()
    query = f"'{folder_id}' in parents and trashed=false"
    results = service.files().list(
        q=query,
        fields="files(id,name,mimeType,modifiedTime,size)",
        pageSize=1000
    ).execute()
    return results.get("files", [])


# ----------------------------------------------------------
# DOWNLOAD RAW FILE BYTES
# ----------------------------------------------------------
def download_file(file_id):
    service = get_drive_service()

    # First check if it's a Google Sheet (needs export)
    file_meta = service.files().get(
        fileId=file_id,
        fields="mimeType,name"
    ).execute()

    mime = file_meta.get("mimeType", "")

    # Google Sheets → export as CSV
    if mime == "application/vnd.google-apps.spreadsheet":
        request = service.files().export_media(
            fileId=file_id,
            mimeType="text/csv"
        )
    else:
        request = service.files().get_media(fileId=file_id)

    file_data = BytesIO()
    downloader = MediaIoBaseDownload(file_data, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    file_data.seek(0)
    return file_data, file_meta.get("name", "")


# ----------------------------------------------------------
# SMART READ — detects CSV vs Excel from filename/mimeType
# ----------------------------------------------------------
def read_drive_file(file_id):
    """
    Downloads a file from Google Drive and returns a DataFrame.
    Automatically handles: .csv, .xlsx, .xls, Google Sheets.
    """
    file_data, filename = download_file(file_id)
    name_lower = filename.lower()

    # Try CSV first (covers .csv and Google Sheets exported as CSV)
    if name_lower.endswith(".csv"):
        try:
            return pd.read_csv(file_data)
        except Exception as e:
            raise ValueError(f"Failed to read CSV '{filename}': {e}")

    # Try Excel formats
    if name_lower.endswith(".xlsx") or name_lower.endswith(".xls"):
        try:
            return pd.read_excel(file_data, engine="openpyxl")
        except Exception:
            # Fallback: maybe it was saved as CSV with xlsx name
            file_data.seek(0)
            try:
                return pd.read_csv(file_data)
            except Exception as e:
                raise ValueError(f"Failed to read '{filename}' as Excel or CSV: {e}")

    # Unknown extension — try CSV then Excel
    try:
        return pd.read_csv(file_data)
    except Exception:
        file_data.seek(0)
        try:
            return pd.read_excel(file_data, engine="openpyxl")
        except Exception as e:
            raise ValueError(f"Could not read '{filename}' as CSV or Excel: {e}")


# ----------------------------------------------------------
# KEPT FOR BACKWARD COMPATIBILITY
# ----------------------------------------------------------
def download_excel_file(file_id):
    return read_drive_file(file_id)

def download_csv_file(file_id):
    return read_drive_file(file_id)

def read_google_file(file_id, filename):
    return read_drive_file(file_id)