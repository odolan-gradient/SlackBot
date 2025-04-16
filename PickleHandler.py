import os
import pickle

from google.cloud import storage
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Your existing constants
DIRECTORY_YEAR = "2025"
PICKLE_DIRECTORY = f"H:\\Shared drives\\Stomato\\{DIRECTORY_YEAR}\\Pickle\\"
PICKLE_NAME = f"{DIRECTORY_YEAR}_pickle.pickle"
PICKLE_PATH = PICKLE_DIRECTORY + PICKLE_NAME

# Google Cloud Storage setup
storage_client = storage.Client()
bucket_name = 'gradient-pickle'
bucket = storage_client.bucket(bucket_name)
cloud_pickle_path = f"Pickle/{PICKLE_NAME}"



# Google Drive Creds
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']
SERVICE_ACCOUNT_FILE = "C://Users//odolan//PycharmProjects//SlackBot//rich-meridian-430023-j1-686ca31bc69d.json"
credentials = service_account.Credentials.from_service_account_file(
    SERVICE_ACCOUNT_FILE, scopes=SCOPES)

# Build the Drive API service
service = build('drive', 'v3', credentials=credentials)

def list_shared_drives():
    try:
        # Call the Drive API to list shared drives
        results = service.drives().list().execute()
        drives = results.get('drives', [])

        if not drives:
            print('No shared drives found.')
        else:
            print('Shared Drives:')
            for drive in drives:
                print(f"Name: {drive['name']}, ID: {drive['id']}")

    except Exception as e:
        print(f'An error occurred: {e}')
def list_shared_drive_files(drive_id):
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

def sync_pickle_to_cloud():
    """
    Synchronize the local pickle file with Google Cloud Storage.
    """
    local_pickle = PICKLE_PATH
    cloud_blob = bucket.blob(cloud_pickle_path)

    # Check if local pickle exists
    if not os.path.exists(local_pickle):
        print("Local pickle does not exist. Nothing to sync.")
        return

    # Get local file's last modified time
    local_modified_time = os.path.getmtime(local_pickle)

    # Check if cloud pickle exists
    if cloud_blob.exists():
        if cloud_blob.updated is not None:
            cloud_modified_time = cloud_blob.updated.timestamp()
            # Compare modification times
            if local_modified_time > cloud_modified_time:
                # Local file is newer, upload to cloud
                cloud_blob.upload_from_filename(local_pickle)
                print("Local pickle is newer. Uploaded to Google Cloud Storage.")
            else:
                print("Cloud pickle is up-to-date. No action needed.")
    else:
        # Cloud pickle doesn't exist or hasnt been uploaded to yet, upload local pickle
        cloud_blob.upload_from_filename(local_pickle)
        print("Uploaded local pickle to Google Cloud Storage.")


def open_pickle(filename: str = PICKLE_NAME, specific_file_path: str = PICKLE_DIRECTORY):
    """
    Function to open a pickle and return its contents.
    Now checks both local and cloud storage.
    """
    print("Opening pickle")
    local_path = specific_file_path + filename

    if not os.path.exists(local_path): #TODO fix the not
        with open(local_path, 'rb') as f:
            content = pickle.load(f)

        # Sync to cloud after opening
        sync_pickle_to_cloud()

        return content
    else:
        print("Local pickle does not exist. Using Cloud.")
        # If local doesn't exist, try to get from cloud
        cloud_blob = bucket.blob(cloud_pickle_path)
        if cloud_blob.exists():
            content = pickle.loads(cloud_blob.download_as_bytes())

            # Save to local for future use
            with open(local_path, 'wb') as f:
                pickle.dump(content, f)

            return content
        else:
            print("Pickle not found in local or cloud storage.")
            return None


def write_pickle(data, filename: str = PICKLE_NAME, specific_file_path: str = PICKLE_DIRECTORY):
    """
    Function to write to a pickle.
    Writes to both local (if possible) and cloud storage.
    """
    local_path = specific_file_path + filename

    # Try to write to local
    try:
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        with open(local_path, 'wb') as f:
            pickle.dump(data, f)
        print(f"Pickle written to local path: {local_path}")
    except Exception as e:
        print(f"Unable to write to local path: {e}")

    # Write to cloud
    try:
        cloud_blob = bucket.blob(PICKLE_PATH)
        cloud_blob.upload_from_string(pickle.dumps(data))
        print(f"Pickle written to cloud path: {PICKLE_PATH}")
    except Exception as e:
        print(f"Error writing to cloud storage: {e}")

    # Sync to cloud (in case local write was successful but cloud write failed)
    # sync_pickle_to_cloud()

def show_pickle(filename: str = PICKLE_NAME, specific_file_path: str = PICKLE_DIRECTORY):
    """
        Function to print out the contents of the pickle.

        :return:
    """
    data = open_pickle(filename=filename, specific_file_path=specific_file_path)
    print("PICKLE CONTENTS")
    for d in data:
        d.to_string()

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

# test_pickle_name = f"{DIRECTORY_YEAR}_pickle_test.pickle"
# show_pickle(filename=test_pickle_name)
# growers = open_pickle(filename=test_pickle_name)
# for grower in growers:
#     for field in grower.fields:
#         if field.name == 'SugalSalvador':
#             field.cimis_station = '5'
# write_pickle(growers, filename=test_pickle_name)
# show_pickle(filename=test_pickle_name)
# list_shared_drives()
list_shared_drive_files('0ACxUDm7mZyTVUk9PVA')



#TODO not writing correctly to the cloud pickle bucket