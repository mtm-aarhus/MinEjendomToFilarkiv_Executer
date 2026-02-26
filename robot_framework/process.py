"""This module contains the main process of the robot."""

from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
from OpenOrchestrator.database.queues import QueueElement

import pyodbc
import sqlite3
import os
import json
import pandas as pd
import time
from datetime import datetime
import requests
import uuid
import re
from GetFilarkivAcessToken import GetFilarkivToken


# pylint: disable-next=unused-argument
def process(orchestrator_connection: OrchestratorConnection, queue_element: QueueElement | None = None) -> None:
    """Do the primary process of the robot."""
    orchestrator_connection.log_trace("Running process.")


    # ------------------- CONFIG -------------------

    BASE_FOLDER = r"D:\MinEjendomToFilarkiv"

    # Ensure folder exists
    os.makedirs(BASE_FOLDER, exist_ok=True)

    SQLITE_PATH = os.path.join(BASE_FOLDER, "minejendom2filarkiv.db")

    conn = sqlite3.connect(SQLITE_PATH)
    cur = conn.cursor()

    FilarkivURL = orchestrator_connection.get_constant("FilarkivURL").value
    Filarkiv_access_token = GetFilarkivToken(orchestrator_connection)
    # ------- Henter kø-elementer ------------------
        # ---- Henter Kø-elementer ----
    queue = json.loads(queue_element.data)


    DocumentId = queue.get("DocumentId")
    DocumentTitle = queue.get("DocumentTitle")
    FileName = queue.get("FileName")
    FileExtension = queue.get("FileExtension")
    IsScannedPage = queue.get("IsScannedPage")
    CaseId = queue.get("CaseId")
    FilArkivCaseId = queue.get("FilArkivCaseId")
    CaseNumber = queue.get("CaseNumber")
    CaseTitle = queue.get("CaseTitle")
    IgnoreCase = queue.get("IgnoreCase")
    FilePath = queue.get("FilePath")
    securityClassificationLevel = queue.get("securityClassificationLevel")


    # ------------------------------------------------
    # SQLite Update Function
    # ------------------------------------------------

    def update_sqlite_document(SQLITE_PATH, DocumentId, FilarkivDocumentId, FilarkivFileId, UploadedAt):
        conn = sqlite3.connect(SQLITE_PATH)
        cur = conn.cursor()

        cur.execute("""
            UPDATE MinEjendom_Documents
            SET FilArkivDocumentId = ?,
                FilArkivFileId = ?,
                UploadedAt = ?
            WHERE Id = ?
        """, (
            FilarkivDocumentId,
            FilarkivFileId,
            UploadedAt,
            DocumentId
        ))

        conn.commit()
        conn.close()
    # ------ Create document, create file and upload document-----------------------


    def upload_to_filarkiv_NoneSensitive(FilarkivURL, FilarkivCaseID, Filarkiv_access_token, title, file_path, DocumentType, orchestrator_connection,DocumentId,FileName,DocumentNumber,Documenttype):
        Filarkiv_DocumentID = None  # Ensure it is initialized

        DocumentDate = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        data = {
            "caseId": FilarkivCaseID,
            "securityClassificationLevel": 0,
            "title": title,
            "documentNumber": DocumentNumber,
            "documentDate": DocumentDate,
            "direction": 0,
            "documentReference": f"minejendom:{DocumentId}-{Documenttype}"
        }
        response = requests.post(f"{FilarkivURL}/Documents", headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, data=json.dumps(data))
        if response.status_code in [200, 201]:
            Filarkiv_DocumentID = response.json().get("id")
            #orchestrator_connection.log_info(f"Anvender følgende Filarkiv_DocumentID: {Filarkiv_DocumentID}")
        else:
            orchestrator_connection.log_info(f"Failed to create document. Response: {response.text}")

        if Filarkiv_DocumentID is None:
            #orchestrator_connection.log_info("Fejl: Filarkiv_DocumentID blev ikke genereret. Afbryder processen.")
            return False
        
        extension = f".{DocumentType}"
        mime_type = {
            ".txt": "text/plain", ".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif", ".doc": "application/msword", ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xls": "application/vnd.ms-excel", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".csv": "text/csv",
            ".json": "application/json", ".xml": "application/xml"
        }.get(extension, "application/octet-stream")
        
        FileName += extension
        #orchestrator_connection.log_info(f"Anvender følgende DocumentID: {Filarkiv_DocumentID}")
        response = requests.post(f"{FilarkivURL}/Files", headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, json={"documentId": Filarkiv_DocumentID, "fileName": FileName, "sequenceNumber": 1,"fileReference":f"minejendom:{CaseId}-{DocumentId}-{Documenttype}", "mimeType": mime_type})
        if response.status_code in [200, 201]:
            FileID = response.json().get('id')
            #orchestrator_connection.log_info(f"FileID: {FileID}")
        else:
            #orchestrator_connection.log_info(f"Failed to create file metadata. {response.text}")
            return False
        
        url = f"https://core.filarkiv.dk/api/v1/FileIO/Upload/{FileID}"
        if not os.path.exists(file_path):
            orchestrator_connection.log_info(f"Error: File not found at {file_path}")
        else:
            with open(file_path, 'rb') as file:
                files = [('file', (FileName, file, mime_type))]
                response = requests.post(url, headers={"Authorization": f"Bearer {Filarkiv_access_token}"}, files=files)
                if response.status_code in [200, 201]:
                    print("File uploaded successfully.")
                else:
                    #orchestrator_connection.log_info(f"Failed to upload file. Status Code: {response.status_code} - deleting file + document")
                    url = f"https://core.filarkiv.dk/api/v1/Files"
                    data = {"id": FileID}
                    response = requests.delete(url, headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, data=json.dumps(data))
                    #orchestrator_connection.log_info(f"File deletion status code: {response.status_code}")

                    url = f"https://core.filarkiv.dk/api/v1/Documents"
                    data = {"id": Filarkiv_DocumentID}
                    response = requests.delete(url, headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, data=json.dumps(data))
                    #orchestrator_connection.log_info(f"Document deletion status code: {response.status_code}")
                    return False
        return True, Filarkiv_DocumentID, FileID, DocumentDate


    def upload_to_filarkiv_Sensitive(FilarkivURL, FilarkivCaseID, Filarkiv_access_token, title, file_path, DocumentType, orchestrator_connection,DocumentId,FileName,DocumentNumber,Documenttype):
        Filarkiv_DocumentID = None  # Ensure it is initialized

        DocumentDate = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        data = {
            "caseId": FilarkivCaseID,
            "securityClassificationLevel": 1,
            "title": title,
            "documentNumber": DocumentNumber,
            "documentDate": DocumentDate,
            "direction": 0,
            "documentReference": f"minejendom:{DocumentId}-{Documenttype}"
        }
        response = requests.post(f"{FilarkivURL}/Documents", headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, data=json.dumps(data))
        if response.status_code in [200, 201]:
            Filarkiv_DocumentID = response.json().get("id")
            #orchestrator_connection.log_info(f"Anvender følgende Filarkiv_DocumentID: {Filarkiv_DocumentID}")
        else:
            orchestrator_connection.log_info(f"Failed to create document. Response: {response.text}")

        if Filarkiv_DocumentID is None:
            #orchestrator_connection.log_info("Fejl: Filarkiv_DocumentID blev ikke genereret. Afbryder processen.")
            return False
        
        extension = f".{DocumentType}"
        mime_type = {
            ".txt": "text/plain", ".pdf": "application/pdf", ".jpg": "image/jpeg", ".jpeg": "image/jpeg", ".png": "image/png",
            ".gif": "image/gif", ".doc": "application/msword", ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            ".xls": "application/vnd.ms-excel", ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".csv": "text/csv",
            ".json": "application/json", ".xml": "application/xml"
        }.get(extension, "application/octet-stream")
        FileName += extension
        #orchestrator_connection.log_info(f"Anvender følgende DocumentID: {Filarkiv_DocumentID}")
        response = requests.post(f"{FilarkivURL}/Files", headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, json={"documentId": Filarkiv_DocumentID, "fileName": FileName, "sequenceNumber": 1,"fileReference":f"minejendom:{CaseId}-{DocumentId}-{Documenttype}", "mimeType": mime_type})
        if response.status_code in [200, 201]:
            FileID = response.json().get('id')
            #orchestrator_connection.log_info(f"FileID: {FileID}")
        else:
            #orchestrator_connection.log_info(f"Failed to create file metadata. {response.text}")
            return False
        
        url = f"https://core.filarkiv.dk/api/v1/FileIO/Upload/{FileID}"
        if not os.path.exists(file_path):
            orchestrator_connection.log_info(f"Error: File not found at {file_path}")
        else:
            with open(file_path, 'rb') as file:
                files = [('file', (FileName, file, mime_type))]
                response = requests.post(url, headers={"Authorization": f"Bearer {Filarkiv_access_token}"}, files=files)
                if response.status_code in [200, 201]:
                    print("File uploaded successfully.")
                else:
                    #orchestrator_connection.log_info(f"Failed to upload file. Status Code: {response.status_code} - deleting file + document")
                    url = f"https://core.filarkiv.dk/api/v1/Files"
                    data = {"id": FileID}
                    response = requests.delete(url, headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, data=json.dumps(data))
                    #orchestrator_connection.log_info(f"File deletion status code: {response.status_code}")

                    url = f"https://core.filarkiv.dk/api/v1/Documents"
                    data = {"id": Filarkiv_DocumentID}
                    response = requests.delete(url, headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, data=json.dumps(data))
                    #orchestrator_connection.log_info(f"Document deletion status code: {response.status_code}")
                    return False
        return True, Filarkiv_DocumentID, FileID, DocumentDate


    def is_document_uploaded(FilarkivURL,FilarkivCaseId, FileName, Filarkiv_access_token):

        url = f"{FilarkivURL}/Files?caseId={FilarkivCaseId}&orderBy=documentDate&noPaging=true&skipTotalCount=false&pageSize=500"

        headers = {
            "Authorization": f"Bearer {Filarkiv_access_token}"
        }

        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            files = response.json()

            print(f"Looking for: {FileName}")

            for file in files:
                api_filename = file.get("fileName", "")
                print(f"- {api_filename}")

                # Remove extension from API filename
                api_name_without_ext = os.path.splitext(api_filename)[0].strip().lower()

                if api_name_without_ext == FileName:
                    print("\n✅ Match found:")
                    print(json.dumps(file, indent=4))
                    return True, file.get("documentId"), file.get("id"),file.get("createdAt")

            print("\n❌ No match found.")
            return False, None, None, None

        except requests.exceptions.RequestException as e:
            print(f"API request failed: {e}")
            return False, None, None, None


    # -------------------- Main workflow ----------
    print("Henter Documentet")
    print(f"File extension er: {FileExtension}")

    CanDocumentBeConverted = False
    conversionPossible = False

    DocumentNumber = int(FileName.split("-")[-1])
    print(DocumentNumber)  
    Documenttype = 1

    # ---------------- Henter Documentet ----------------------


    #OBS! Tjek om listen skal opdateres med Ea, inden du oploader
    # List of supported file extensions 
    supported_extensions = [
        "bmp", "doc","docx", "gif","heic","heics","heif","heifs", "jpeg",
        "jpg", "msg","pdf", "png", "psd","tif", "tiff", "txt", "xls", "xlsx", "xlsm", "xlt", "xltx","ods"
    ]

    # Check if the input file extension exists in the list
    if FileExtension.lower() in supported_extensions:
        CanDocumentBeConverted = True
    else:
        CanDocumentBeConverted = False

    if CanDocumentBeConverted:
        print("Filen konverteres i Filarkiv")


    # check if document already is uploaded return true og false

    IsdocumentUploaded, filarkiv_document_id, filarkiv_file_id, created_at_from_api = is_document_uploaded(
        FilarkivURL,
        FilArkivCaseId,
        FileName,
        Filarkiv_access_token
    )

    print(f"IsdocumentUploaded is: {IsdocumentUploaded}")

    success = False
    document_date = None

    if CanDocumentBeConverted:

        if not IsdocumentUploaded:

            if securityClassificationLevel == 1:
                print("Document is sensitive")
                success, filarkiv_document_id, filarkiv_file_id, document_date = upload_to_filarkiv_Sensitive(
                    FilarkivURL, FilArkivCaseId, Filarkiv_access_token,
                    DocumentTitle, FilePath,
                    FileExtension,
                    orchestrator_connection,
                    DocumentId, FileName, DocumentNumber, Documenttype
                )
            else:
                print("Document is not sensitive")
                success, filarkiv_document_id, filarkiv_file_id, document_date = upload_to_filarkiv_NoneSensitive(
                    FilarkivURL, FilArkivCaseId, Filarkiv_access_token,
                    DocumentTitle, FilePath,
                    FileExtension,
                    orchestrator_connection,
                    DocumentId, FileName, DocumentNumber, Documenttype
                )
            uploaded_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        else:
            print("Document already exists in Filarkiv.")
            success = True   # Important: treat as success
            uploaded_at = datetime.fromisoformat(created_at_from_api.split(".")[0]).strftime("%Y-%m-%d %H:%M:%S")

        # ✅ Update database ONLY if success
        if success and filarkiv_document_id and filarkiv_file_id:


            update_sqlite_document(
                SQLITE_PATH,
                DocumentId,
                filarkiv_document_id,
                filarkiv_file_id,
                uploaded_at
            )

            print("SQLite updated successfully.")

        else:
            print("Upload failed — database NOT updated.")

    else:
        print("Document type not supported — nothing done.")
