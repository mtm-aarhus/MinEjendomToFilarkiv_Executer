import pyodbc
import sqlite3
import os
from OpenOrchestrator.orchestrator_connection.connection import OrchestratorConnection
orchestrator_connection = OrchestratorConnection("MinEjenDomToFilarkiv_Dispatcher", os.getenv('OpenOrchestratorSQL'),os.getenv('OpenOrchestratorKey'), None,None)
import json
import pyodbc
import pandas as pd
import time
from datetime import datetime
import requests
import uuid
import re

from GetFilarkivAcessToken import GetFilarkivToken

FilarkivURL = orchestrator_connection.get_constant("FilarkivURL").value
Filarkiv_access_token = GetFilarkivToken(orchestrator_connection)

# ------- Henter kø-elementer ------------------

DocumentId = "7495621"
DocumentTitle = "Intern færdigmelding"
FileName = "3189.06-0009"
FileExtension = "pdf"
IsScannedPage = 0
CaseId = 3581
FilArkivCaseId = "B90C95F8-A37A-4428-B70B-188CE16565E1"
CaseNumber = "3189-06"
CaseTitle = "Opførelse af enfamiliehus med integreret carport"
IgnoreCase = 0
FilePath = "\\\\adm.aarhuskommune.dk\\MTM\\Byggesag\\Byggesag_2016_202X\\2017\\8\\975201\\3189-06\\9438560b-759e-48a5-8dde-65a757c42868.pdf"
securityClassificationLevel = 0

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
    response = requests.post(f"{FilarkivURL}/Files", headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, json={"documentId": Filarkiv_DocumentID, "fileName": FileName, "sequenceNumber": 0,"fileReference":f"minejendom:{CaseId}-{DocumentId}-{Documenttype}", "mimeType": mime_type})
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
                orchestrator_connection.log_info("File uploaded successfully.")
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
    return True


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
    response = requests.post(f"{FilarkivURL}/Files", headers={"Authorization": f"Bearer {Filarkiv_access_token}", "Content-Type": "application/json"}, json={"documentId": Filarkiv_DocumentID, "fileName": FileName, "sequenceNumber": 0,"fileReference":f"minejendom:{CaseId}-{DocumentId}-{Documenttype}", "mimeType": mime_type})
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
                orchestrator_connection.log_info("File uploaded successfully.")
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
    return True


# ------ Add basic data ------------------------


# -------------------- Main workflow ----------
print("Henter Documentet")
print(f"File extension er: {FileExtension}")

CanDocumentBeConverted = False
conversionPossible = False

DocumentNumber = int(FileName.split("-")[-1])
print(DocumentNumber)  # 0
Documenttype = 0

# ---------------- Henter Documentet ----------------------


#OBS! Tjek om listen skal opdateres med Ea, inden du oploader
# List of supported file extensions
supported_extensions = [
    "bmp", "doc","docx", "gif","heic","heics","heif","heifs", "jpeg",
    "jpg", "msg","pdf", "png", "psd","tif", "tiff", "txt", 
]

# Check if the input file extension exists in the list
if FileExtension.lower() in supported_extensions:
    CanDocumentBeConverted = True
else:
    CanDocumentBeConverted = False

if CanDocumentBeConverted:
    print("Filen konverteres i Filarkiv")


if CanDocumentBeConverted:
    if securityClassificationLevel == 1:
        print("Document is sensitive")
        success  = upload_to_filarkiv_Sensitive(
            FilarkivURL, FilArkivCaseId, Filarkiv_access_token,
            DocumentTitle, FilePath,
            FileExtension,
            orchestrator_connection,
            DocumentId, FileName,DocumentNumber,Documenttype
        )
    else:
        print("Document is not sensitive")
        success  = upload_to_filarkiv_NoneSensitive(
            FilarkivURL, FilArkivCaseId, Filarkiv_access_token,
            DocumentTitle, FilePath,
            FileExtension,
            orchestrator_connection,
            DocumentId,FileName,DocumentNumber,Documenttype
        )

    if success:
        #os.remove(FilePath) --- skal ikke fjernes hvis det kan gøres uden at downloade lokalt inden opload.
        print("Documentet er oploaded til Filarkiv")