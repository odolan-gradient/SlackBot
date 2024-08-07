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

# Constants
DIRECTORY_YEAR = "2024"
PICKLE_NAME = f"{DIRECTORY_YEAR}_pickle_test.pickle"

# Google Cloud Storage setup
storage_client = storage.Client()
bucket_name = 'gradient-pickle'
bucket = storage_client.bucket(bucket_name)
cloud_pickle_path = f"Pickle/{PICKLE_NAME}"

SHARED_DRIVE_ID = '0ACxUDm7mZyTVUk9PVA'
PICKLE_FILE_ID = '1h9fu1mZa9pzQLDOIjEpejBz8zKpbMtyG'
CREDENTIALS_FILE = r'C:\Users\odolan\PycharmProjects\SlackBot\client_secret_creds.json'

SCOPES = ['https://www.googleapis.com/auth/drive']

def get_drive_service():
    # Get the JSON string from the environment variable
    credentials_json = os.environ.get("GOOGLE_CREDENTIALS_JSON")
    if not credentials_json:
        raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable not set")

    # Load the credentials as a JSON object
    credentials_info = json.loads(credentials_json)

    # Create a service account credentials object
    credentials = service_account.Credentials.from_service_account_info(credentials_info, scopes=SCOPES)

    # Build the Drive service
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

def open_pickle(file_id=PICKLE_FILE_ID):
    try:
        request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while done is False:
            status, done = downloader.next_chunk()

        fh.seek(0)
        data = pickle.load(fh)
        return data
    except Exception as e:
        print(f"Error reading from Shared Drive: {e}")
        return None

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


def upload_pickle(data, filename: str = PICKLE_NAME, file_id=PICKLE_FILE_ID):
    """
    Function to upload a pickle file to Google Drive, overwriting if file_id is provided.
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

open_pickle()