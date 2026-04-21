# ==========================================================
# drive_loader.py
# Handles Google Sheets export, real CSVs, subfolders
# ==========================================================

import os
import json

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

import pandas as pd
from io import BytesIO
import streamlit as st

MIME_GOOGLE_SHEET  = "application/vnd.google-apps.spreadsheet"
MIME_GOOGLE_FOLDER = "application/vnd.google-apps.folder"


@st.cache_resource
def get_drive_service():
    creds_json = os.environ["GOOGLE_CREDS"]
    creds_dict = json.loads(creds_json)
    creds = service_account.Credentials.from_service_account_info(
        creds_dict,
        scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)


def list_files_in_folder(folder_id):
    service = get_drive_service()
    results = service.files().list(
        q=f"'{folder_id}' in parents and trashed=false",
        fields="files(id,name,mimeType,modifiedTime,size)",
        pageSize=1000
    ).execute()
    return results.get("files", [])


def list_all_files_recursive(folder_id):
    all_files = []
    for item in list_files_in_folder(folder_id):
        if item["mimeType"] == MIME_GOOGLE_FOLDER:
            all_files.extend(list_all_files_recursive(item["id"]))
        else:
            all_files.append(item)
    return all_files


def download_file_bytes(file_id):
    service  = get_drive_service()
    meta     = service.files().get(fileId=file_id, fields="mimeType,name").execute()
    mime     = meta.get("mimeType", "")
    filename = meta.get("name", "")

    if mime == MIME_GOOGLE_SHEET:
        request  = service.files().export_media(fileId=file_id, mimeType="text/csv")
        is_sheet = True
    else:
        request  = service.files().get_media(fileId=file_id)
        is_sheet = False

    buf = BytesIO()
    dl  = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _, done = dl.next_chunk()
    buf.seek(0)
    return buf, filename, is_sheet


def _read_csv_multi_encoding(buf, filename):
    for enc in ["utf-8", "latin-1", "windows-1252", "utf-8-sig"]:
        try:
            buf.seek(0)
            return pd.read_csv(buf, encoding=enc)
        except UnicodeDecodeError:
            continue
    raise ValueError(f"Cannot decode '{filename}' with any known encoding.")


def read_drive_file(file_id):
    buf, filename, is_sheet = download_file_bytes(file_id)
    name_lower = filename.lower()

    if is_sheet or name_lower.endswith(".csv"):
        return _read_csv_multi_encoding(buf, filename)

    if name_lower.endswith(".xlsx") or name_lower.endswith(".xls"):
        try:
            return pd.read_excel(buf, engine="openpyxl")
        except Exception:
            return _read_csv_multi_encoding(buf, filename)

    # Unknown — try CSV then Excel
    try:
        return _read_csv_multi_encoding(buf, filename)
    except Exception:
        buf.seek(0)
        return pd.read_excel(buf, engine="openpyxl")


# Backward compat
def download_excel_file(file_id): return read_drive_file(file_id)
def download_csv_file(file_id):   return read_drive_file(file_id)
def read_google_file(file_id, _): return read_drive_file(file_id)