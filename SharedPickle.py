import io
import json
import os
import pickle

from google.cloud import storage
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from dotenv import load_dotenv
from pathlib import Path, WindowsPath, PosixPath

load_dotenv()


# Constants
DIRECTORY_YEAR = "2024"
PICKLE_NAME = f"{DIRECTORY_YEAR}_pickle_test.pickle"
PICKLE_NAME = f"{DIRECTORY_YEAR}_pickle_test_1.pickle"

SHARED_DRIVE_ID = '0ACxUDm7mZyTVUk9PVA'
PICKLE_FILE_ID = '1h9fu1mZa9pzQLDOIjEpejBz8zKpbMtyG'
PICKLE_FILE_ID = '1l20dBjwVjR0tjHk0ksbYxBXt_Jfkiorb'
# CREDENTIALS_FILE = r'C:\Users\odolan\PycharmProjects\SlackBot\client_secret_creds.json'

SCOPES = ['https://www.googleapis.com/auth/drive']
class CustomUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module == 'pathlib' and name == 'WindowsPath':
            return Path  # Replace WindowsPath with a generic Path (PosixPath on Unix systems)
        return super().find_class(module, name)

def custom_load(file_obj):
    return CustomUnpickler(file_obj).load()

def open_pickle(file_id=PICKLE_FILE_ID):
    try:
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        fh.seek(0)
        data = custom_load(fh)  # Use the custom loader to handle WindowsPath
        return data
    except Exception as e:
        print(f"Error reading from Shared Drive: {e}")
        return None
def get_drive_service():
    credentials_info = {
          "type": "service_account",
          "project_id": "rich-meridian-430023-j1",
          "private_key_id": "66cbc6d4279f474fe9c83e1ada738099fec5b251",
          "private_key": "-----BEGIN PRIVATE "
                         "KEY-----\nMIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQCpP1qKCDDGQcjo"
                         "\n83Wub6pu8g6uwtGU1vKPqhugPwo8kCqA3Rykynq4rHPGVDeHAPo/Iu+t90RyCvMd\nvzNh0SXkKKWqSh9HJvP1"
                         "/Taip3cS7YdNTiUxC/us5r00g+8npOaiThBnAh+FXKpd\nn/ASutz5yZ21AF2yqMrhvnIutFcf/E4OZn"
                         "+MgHQN1dpcD7+P0jgX9I6QZmpNG1ig\nKhHauVf5ry/35KPConycYgFaUDOnMsif/CeYRoN6L7ZJ9kUCrkUxZ"
                         "+tnZaUJ3Puw\noMLf6KR6YY7HOXQTSaUZAjYkxrv6aGnYC37cMx47WkmvpwdKZJYH4hzuA1xnwzn8"
                         "\nwy6w35ilAgMBAAECggEAAm+YPsDX7N6RBPNOGAzg49hliDPjHtSKKLGu1Jtbqxv7\nFKA6E5AbfJF02B"
                         "+pre6Aa4y17OfQayDHt3+jPm7rb/F60uzeruA7Zii3Ete8sb/L\n8PulMuPEg0xN4FXeyRAJRsA/YbAo4ns"
                         "/M3pEEwzv9cNmWu7Oqm3d/6pFS/FKCLqL\norSN0wLW2dNZ9FasFmLuSAGOGxFyW"
                         "+zzYQl5Hlybnuk0mIV8WwxhpbCRCy3NgyM3\ntU0pYoaBwA3Z/zY"
                         "+G91t7G66shKi5u7EZtiI7VGSMrkSY3zBUeA7gAaPEw1Lvm8Y"
                         "\nEWSLLjefNXJW8cTtgYh1GGHjp11tSLlGE3VWTORAcQKBgQDltAB9fEuMsQ7/l+7Z"
                         "\n0WI7a41f3EgKNDw2jjaxDTfq49VTEyQmpuTiVbbSmRRaKOWtSmoSaqwLdLbx10Om\nc1YOh1"
                         "/kiNpaaFqJPvGQjHtgn8fcYhAeCXUiUzctnYRLSgx3dvbGxpCoZ13VVK21"
                         "\n8EIrOSyTsaeq8S5BzmyxtXBC9QKBgQC8n432yE7+7f1l8Z8v9Nk+2Edl5oRxOuqV\nwjLYD5kttTYR5B5"
                         "+uj4xsvfKVlfR6QkYNT3DBzq5jGYYOejhbm08msqWW+jdPLSi\nLh9Y55zkPZa/vuOAnOp1z52L21yM2jL27cXJ"
                         "/nu82VVM55hI4aRQ7ppi1MvgpXNw\nE+ZUNbRQ8QKBgH4OGxrCJD+wRvfS6/vS4SKUsj"
                         "/CBjK7WbPitXbSNzaLE12Eqpkf\ni4n92deWtEmKGgjQRoeWzJV41pC/PlvQ/Y/5kJE83P8yN0UMKsrVnTt4U9jIY"
                         "+nn\n7MUKf8Rjpd8fYtoIigKpo2cXWrIgxzeKAvXvaVwf6VBxDJ6GZrXbSSElAoGAPvKE"
                         "\nXvokGsFzkkTbWha9NVLaKPCP/HWr+cRwUViLRwy1ea0GXEZtIQrX1MeR0TSS22hR\nLzfHakqne6g"
                         "/xpOiktoZh6ougT6UDZeU0Iei/SxslZrvs2kqeZyKuDTBoyPiZDOf\nkTSDONfStrKHSLM8seGe1iKr01GDv8B0Wl"
                         "/9yBECgYEA0ivupeVyehM1dRqJqxZy"
                         "\nQyn7QKSTP4Ulaw3NiLYx4mQiCgQKpdgrGcB7g8S8cKmlgZ9aXhZA1SdnM3Bm5fz8\n8CwVNQHZgzjq+ZbTt7"
                         "/WzRLiGm5KLtbx2QRk7jljL/DQ5pD76pddq7QwV20spUtQ\nt6QBEuRN3lcSlSec3Sqpt5g=\n-----END PRIVATE "
                         "KEY-----\n",
          "client_email": "slack-bot-service-account@rich-meridian-430023-j1.iam.gserviceaccount.com",
          "client_id": "104920020437657199869",
          "auth_uri": "https://accounts.google.com/o/oauth2/auth",
          "token_uri": "https://oauth2.googleapis.com/token",
          "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
          "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/slack-bot-service-account%40rich-meridian-430023-j1.iam.gserviceaccount.com",
          "universe_domain": "googleapis.com"
    }

    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=['https://www.googleapis.com/auth/drive'])

    service = build('drive', 'v3', credentials=credentials)
    return service

# Use the service
service = get_drive_service()

def list_files():
    try:
        # Call the Drive v3 API
        results = service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name)", supportsAllDrives=True, includeItemsFromAllDrives=True).execute()
        items = results.get('files', [])
        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(f"{item['name']} ({item['id']})")
    except Exception as e:
        print(f"An error occurred: {e}")

def list_shared_drive_files(drive_id = SHARED_DRIVE_ID):
    try:
        # List files in the shared drive
        results = service.files().list(
            corpora='drive',
            driveId=drive_id,
            includeItemsFromAllDrives=True,
            supportsAllDrives=True,
            fields="nextPageToken, files(id, name)"
        ).execute()
        items = results.get('files', [])

        if not items:
            print('No files found.')
            return
        print('Files:')
        for item in items:
            print(f"{item['name']} ({item['id']})")
    except HttpError as error:
        print(f'An error occurred: {error}')



def download_pickle(file_id = PICKLE_FILE_ID):
    """
    Function to download a pickle file from Google Drive and load its content.
    """
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)

    done = False
    while done is False:
        status, done = downloader.next_chunk()
        print(f"Download {int(status.progress() * 100)}%.")

    fh.seek(0)
    return pickle.load(fh)


def write_pickle(data, filename: str = PICKLE_NAME, file_id=PICKLE_FILE_ID):
    """
    Function to upload a pickle file to Google Drive, completely replacing the old data.
    """
    try:
        # Serialize data to a pickle in memory
        pickle_data = io.BytesIO()
        pickle.dump(data, pickle_data)
        pickle_data.seek(0)  # Go to the start of the BytesIO buffer

        media = MediaIoBaseUpload(pickle_data, mimetype='application/octet-stream', resumable=True)

        if file_id:
            # Update the existing file
            file = service.files().update(
                fileId=file_id,
                media_body=media,
                supportsAllDrives=True,
                fields='id'
            ).execute()
        else:
            # Create a new file
            file_metadata = {
                'name': filename,
                'parents': [SHARED_DRIVE_ID],
                'driveId': SHARED_DRIVE_ID
            }
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                supportsAllDrives=True,
                fields='id'
            ).execute()

        print(f"Pickle {'updated' if file_id else 'created'} in Shared Drive with file ID: {file.get('id')}")
        return file.get('id')
    except Exception as e:
        print(f"Error writing to Shared Drive: {e}")
        return None
def show_pickle(filename: str = PICKLE_NAME):
    """
        Function to print out the contents of the pickle.

        :return:
    """
    data = open_pickle(file_id=PICKLE_FILE_ID)
    print("PICKLE CONTENTS")
    for d in data:
        d.to_string()

def get_grower(grower_name: str):
    """
    Function to get a grower object from the pickle

    :param grower_name: String of grower name
    :return: Grower object
    """
    growers = open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            return grower

def get_field(field_name: str, grower_name: str = ''):
    """
    Function to get a field

    :param field_name: String for the field name
    :param grower_name: Optional parameter of the string for the grower name
    :return: Field object of the field
    """
    if grower_name:
        grower = get_grower(grower_name)
        for field in grower.fields:
            if field.name == field_name:
                return field
    else:
        growers = open_pickle()
        for grower in growers:
            for field in grower.fields:
                if field.name == field_name:
                    return field

# Example usage in your Slack bot
def slack_bot(request):
    # Your existing Slack bot code here...
    growers = open_pickle()
    # ... rest of your code ...
    write_pickle(growers)
    pass


# Entry point for Google Cloud Functions
def main(request):
    return slack_bot(request)
# 
# open_pickle()
# show_pickle()
list_files()
list_shared_drive_files()