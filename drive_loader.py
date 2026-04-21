# ==========================================================
# drive_loader.py
# FIXED — handles Google Sheets (export), real CSVs,
#          and searches subfolders recursively
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
# MIME TYPE CONSTANTS
# ----------------------------------------------------------
MIME_GOOGLE_SHEET  = "application/vnd.google-apps.spreadsheet"
MIME_GOOGLE_FOLDER = "application/vnd.google-apps.folder"


# ----------------------------------------------------------
# GOOGLE DRIVE AUTH
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
# LIST FILES IN A SINGLE FOLDER
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
# LIST ALL FILES RECURSIVELY (searches subfolders too)
# Fixes: sales files inside subfolder "Spread" were not found
# ----------------------------------------------------------
def list_all_files_recursive(folder_id):
    """
    Returns all non-folder files under folder_id,
    including files inside any subfolders (any depth).
    """
    all_files = []
    items = list_files_in_folder(folder_id)

    for item in items:
        if item["mimeType"] == MIME_GOOGLE_FOLDER:
            # Recurse into subfolder
            sub_files = list_all_files_recursive(item["id"])
            all_files.extend(sub_files)
        else:
            all_files.append(item)

    return all_files


# ----------------------------------------------------------
# DOWNLOAD FILE BYTES
# Google Sheets must use export_media (get_media = 403 error)
# Real files use get_media directly
# ----------------------------------------------------------
def download_file_bytes(file_id):
    """Returns (BytesIO data, filename, is_google_sheet)"""
    service = get_drive_service()

    file_meta = service.files().get(
        fileId=file_id,
        fields="mimeType,name"
    ).execute()

    mime     = file_meta.get("mimeType", "")
    filename = file_meta.get("name", "")

    if mime == MIME_GOOGLE_SHEET:
        # Google Sheet — export as CSV (downloading directly = 403)
        request  = service.files().export_media(
            fileId=file_id,
            mimeType="text/csv"
        )
        is_sheet = True
    else:
        # Real uploaded file — download bytes directly
        request  = service.files().get_media(fileId=file_id)
        is_sheet = False

    buf = BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = downloader.next_chunk()
    buf.seek(0)

    return buf, filename, is_sheet


# ----------------------------------------------------------
# READ ANY DRIVE FILE → pandas DataFrame
# ----------------------------------------------------------
def read_drive_file(file_id):
    buf, filename, is_sheet = download_file_bytes(file_id)
    name_lower = filename.lower()

    # Google Sheets exported as CSV — try multiple encodings
    if is_sheet:
        for encoding in ["utf-8", "latin-1", "windows-1252", "utf-8-sig"]:
            try:
                buf.seek(0)
                return pd.read_csv(buf, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                raise ValueError(f"Google Sheet '{filename}' could not be read: {e}")
        raise ValueError(f"Google Sheet '{filename}' could not be decoded with any known encoding.")

    # Real .csv file — try multiple encodings (Windows Excel saves as latin-1)
    if name_lower.endswith(".csv"):
        for encoding in ["utf-8", "latin-1", "windows-1252", "utf-8-sig"]:
            try:
                buf.seek(0)
                return pd.read_csv(buf, encoding=encoding)
            except UnicodeDecodeError:
                continue
            except Exception as e:
                raise ValueError(f"CSV '{filename}' could not be read: {e}")
        raise ValueError(f"CSV '{filename}' could not be decoded with any known encoding.")

    # Real Excel file
    if name_lower.endswith(".xlsx") or name_lower.endswith(".xls"):
        try:
            return pd.read_excel(buf, engine="openpyxl")
        except Exception:
            buf.seek(0)
            try:
                return pd.read_csv(buf)
            except Exception as e:
                raise ValueError(f"'{filename}' could not be read as Excel or CSV: {e}")

    # Unknown extension — try CSV with multiple encodings, then Excel
    for encoding in ["utf-8", "latin-1", "windows-1252", "utf-8-sig"]:
        try:
            buf.seek(0)
            return pd.read_csv(buf, encoding=encoding)
        except UnicodeDecodeError:
            continue
        except Exception:
            break
    buf.seek(0)
    try:
        return pd.read_excel(buf, engine="openpyxl")
    except Exception as e:
        raise ValueError(f"Cannot read '{filename}': {e}")


# ----------------------------------------------------------
# BACKWARD-COMPATIBLE ALIASES
# ----------------------------------------------------------
def download_excel_file(file_id):
    return read_drive_file(file_id)

def download_csv_file(file_id):
    return read_drive_file(file_id)

def read_google_file(file_id, filename):
    return read_drive_file(file_id)