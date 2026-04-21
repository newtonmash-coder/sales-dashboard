# ==========================================================
# drive_loader.py
# FINAL PROFESSIONAL VERSION (RENDER ENV READY)
# Keeps ALL original features + fixes auth method
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
        scopes=[
            "https://www.googleapis.com/auth/drive.readonly"
        ]
    )

    service = build(
        "drive",
        "v3",
        credentials=creds
    )

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
# DOWNLOAD RAW FILE
# ----------------------------------------------------------
def download_file(file_id):

    service = get_drive_service()

    request = service.files().get_media(
        fileId=file_id
    )

    file_data = BytesIO()

    downloader = MediaIoBaseDownload(
        file_data,
        request
    )

    done = False

    while done is False:
        status, done = downloader.next_chunk()

    file_data.seek(0)

    return file_data


# ----------------------------------------------------------
# READ EXCEL
# ----------------------------------------------------------
def download_excel_file(file_id):

    file_data = download_file(file_id)

    df = pd.read_excel(
        file_data,
        engine="openpyxl"
    )

    return df


# ----------------------------------------------------------
# READ CSV
# ----------------------------------------------------------
def download_csv_file(file_id):

    file_data = download_file(file_id)

    df = pd.read_csv(file_data)

    return df


# ----------------------------------------------------------
# AUTO READ FILE TYPE
# ----------------------------------------------------------
def read_google_file(file_id, filename):

    name = filename.lower()

    if name.endswith(".csv"):
        return download_csv_file(file_id)

    return download_excel_file(file_id)