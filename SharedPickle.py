import base64
import io
import json
import os
import pickle
import pytz
from Logger import Logger
from google.cloud import storage
from google.oauth2 import service_account
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from dotenv import load_dotenv
from pathlib import Path, PosixPath, PureWindowsPath
from flask import Flask, jsonify
from datetime import date, datetime, timedelta
from xml.etree import ElementTree as ET
from shapely.geometry import Point, Polygon
import time


try:
    from pathlib import WindowsPath
except ImportError:
    WindowsPath = None  # WindowsPath is not available on this system
from DBWriter import DBWriter

load_dotenv()

# Constants
DIRECTORY_YEAR = "2025"
PICKLE_NAME = f"{DIRECTORY_YEAR}_pickle_test.pickle"
PICKLE_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Pickle\\"

SHARED_DRIVE_ID = '0ACxUDm7mZyTVUk9PVA' # real
# SHARED_DRIVE_ID = '12TwRTStB7JMH7x8rg7VsMc5Dczw_9UkV'  # test
# PICKLE_FILE_ID = '1pOIU5by64wTzVRCz5UaayzaFnHyMLv9k'  # 2025 test pickle
PICKLE_FILE_ID = '1ywkb4_okDaBiMsju1nSbd6TvJjJ8XHRI' #2025
# CREDENTIALS_FILE = r'C:\Users\odolan\PycharmProjects\SlackBot\client_secret_creds.json'
#  trash pickle 1L-HCL32sR3rroEel293HAON_Q1wXqu6y, 1Kvb53U9rZoDlGRNljsfWIcdTofSkK2Zh
DXD_FOLDER = '12tW4E9EmU-f3JynckrAJq3qd_Ndd-sH6'
SCOPES = ['https://www.googleapis.com/auth/drive']

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


def get_drive_service():
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=['https://www.googleapis.com/auth/drive'])

    service = build('drive', 'v3', credentials=credentials)
    return service



class CustomUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module == "pathlib" and name == "WindowsPath":
            return PureWindowsPath
        elif module == "pathlib" and name == "PosixPath":
            return PosixPath
        return super().find_class (module, name)


def convert_to_pure_windows_path(obj):
    """Recursively convert Path objects to PureWindowsPath."""
    if isinstance(obj, (Path, PosixPath, PureWindowsPath)):
        return PureWindowsPath(*obj.parts)
    elif isinstance(obj, dict):
        return {convert_to_pure_windows_path(k): convert_to_pure_windows_path(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_to_pure_windows_path(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(convert_to_pure_windows_path(item) for item in obj)
    return obj


def open_pickle(file_id=PICKLE_FILE_ID):
    try:
        service = get_drive_service()
        # Get latest version ID first
        metadata = service.files().get(
            fileId=file_id,
            fields='name, modifiedTime',
            supportsAllDrives=True  # need for shared drives
        ).execute()


        if check_if_pickle_valid(metadata):
            # request = service.files().get_media(fileId=file_id, fields='name, modifiedTime', supportsAllDrives=True)

            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            # file_metadata = request.execute()

            fh = io.BytesIO()  # file handler - a memory buffer like a temp file
            downloader = MediaIoBaseDownload(fh, request)  # takes the api request and file handler (place to store the data)

            done = False # downloading in chunks
            while not done:
                status, done = downloader.next_chunk()

            fh.seek(0) #resets the read pointer so unpickling starts at the beginning

            # Use CustomUnpickler to load the data, treating WindowsPath as PureWindowsPath
            unpickler = CustomUnpickler(fh)
            data = unpickler.load()
            print(f'Pickle Opened')
            return data
        else:
            print(f'Error Pickle sync issue not using todays pickle')
    except Exception as e:
        print(f"Error reading from Shared Drive: {e}")
        return None

def is_recent(metadata, minutes=30):
    """True if remote file modified within N minutes of now (UTC)."""
    utc = datetime.strptime(metadata['modifiedTime'], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=pytz.utc)
    return (datetime.now(pytz.utc) - utc) <= timedelta(minutes=minutes)

def write_pickle(data, file_id=PICKLE_FILE_ID):
    try:
        # mins_to_check = 30
        # if not is_recent(data, minutes=mins_to_check):
        #     raise RuntimeError(
        #         f"Refusing to write: local copy older than {mins_to_check} minutes "
        #         f"(read at {data['modifiedTime']})."
        #     )
        service = get_drive_service()
        # Convert Path objects to PureWindowsPath before writing
        data = convert_to_pure_windows_path(data)

        # Check if current ve
        fh = io.BytesIO()
        pickle.dump(data, fh)
        fh.seek(0)

        media = MediaIoBaseUpload(fh, mimetype='application/octet-stream', resumable=True)

        file = service.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()

        print(f"Data written to Shared Drive with file ID: {file.get('id')}")
        return file.get('id')
    except Exception as e:
        print(f"Error writing to Shared Drive: {e}")
        return None

def check_if_pickle_valid(metadata):
    # Parse the UTC datetime string
    utc_time = datetime.strptime(metadata['modifiedTime'], "%Y-%m-%dT%H:%M:%S.%fZ")
    utc_time = utc_time.replace(tzinfo=pytz.utc)

    # Convert to California time (PDT in April)
    california_tz = pytz.timezone("America/Los_Angeles")
    local_time = utc_time.astimezone(california_tz)
    # print(f'Pickle timestamp: {local_time}')
    # Check if the date matches today's date in California
    today_local = datetime.now(california_tz).date()
    is_today = local_time.date() == today_local

    return is_today

def list_files():
    # List all files that the service account has access to
    try:
        service = get_drive_service()
        results = service.files().list(
            pageSize=10, fields="nextPageToken, files(id, name)", supportsAllDrives=True,
            includeItemsFromAllDrives=True).execute()
        items = results.get('files', [])
        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(f"{item['name']} ({item['id']})")
    except Exception as e:
        print(f"An error occurred: {e}")


def list_shared_drive_files(drive_id=SHARED_DRIVE_ID):
    try:
        service = get_drive_service()
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


def show_pickle():
    """
        Function to print out the contents of the pickle.

        :return:
    """
    data = open_pickle(file_id=PICKLE_FILE_ID)
    pickle_contents = "PICKLE CONTENTS\n"

    for d in data:
        pickle_contents += d.to_string()
    print(pickle_contents)
    return pickle_contents


def get_grower(grower_name: str, growers = None):
    """
    Function to get a grower object from the pickle

    :param growers:
    :param grower_name: String of grower name
    :return: Grower object
    """
    if growers is None:
        growers = open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            return grower


def get_field(field_name: str, grower_name: str = '', growers = None):
    """
    Function to get a field

    :param growers: Pickle
    :param field_name: String for the field name
    :param grower_name: Optional parameter of the string for the grower name
    :return: Field object of the field
    """
    if grower_name:
        grower = get_grower(grower_name, growers)
        for field in grower.fields:
            if field.name == field_name:
                return field
    else:
        if growers is None:
            growers = open_pickle()
        for grower in growers:
            for field in grower.fields:
                if field.name == field_name:
                    return field


def get_fields(field_names: list[str], growers: list = None):
    """
    Given a list of field names, return the matching Field objects,
    looking only once at the pickle data.
    """
    if growers is None:
        growers = open_pickle()
    name_set = set(field_names)
    result = []
    for g in growers:
        for f in g.fields:
            if f.name in name_set:
                result.append(f)
                name_set.remove(f.name)
                if not name_set:
                    return result
    return result


def get_project(field_name: str, grower_name: str = '', growers=None):
    """
    Function to get a fields project

    :param growers: Pickle
    :param field_name: String for the field name
    :param grower_name: Optional parameter of the string for the grower name
    :return: Project of the field
    """
    dbw = DBWriter()
    if grower_name:
        grower = get_grower(grower_name, growers=growers)
        for field in grower.fields:
            if field.name == field_name:
                crop_type = field.loggers[-1].crop_type
                project = dbw.get_db_project(crop_type)
                return project
    else:
        if growers is None:
            growers = open_pickle()
        for grower in growers:
            for field in grower.fields:
                if field.name == field_name:
                    crop_type = field.loggers[-1].crop_type
                    project = dbw.get_db_project(crop_type)
                    return project

def list_all_kml_files(folder_id, service):
    all_files = []
    page_token = None

    while True:
        response = service.files().list(
            q=f"'{folder_id}' in parents and mimeType='application/vnd.google-earth.kml+xml'",
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            fields="nextPageToken, files(id, name)",
            pageSize=1000,
            pageToken=page_token
        ).execute()

        all_files.extend(response.get('files', []))
        page_token = response.get('nextPageToken')
        if not page_token:
            break

    return all_files

def get_coords_from_kml_folder(field_number, folder_id="12sNfi4L4BUwQM0JYx84tXafNp_Hur9P6"):
    """
    Searches through KML files in a Google Drive folder for a field number match
    and returns the first set of coordinates found.

    :param field_number: str - the field number to search for
    :param folder_id: str - the Google Drive folder ID
    :param service: Google Drive API service instance
    :return: tuple (lat, lon) or None if not found
    """
    service = get_drive_service()
    files = list_all_kml_files(folder_id, service)

    for file in files:
        if field_number in file['name']:
            request = service.files().get_media(fileId=file['id'], supportsAllDrives=True)
            fh = io.BytesIO()
            downloader = MediaIoBaseDownload(fh, request)

            done = False
            while not done:
                _, done = downloader.next_chunk()

            fh.seek(0)
            try:
                tree = ET.parse(fh)
                root = tree.getroot()
                for elem in root.iter():
                    if elem.tag.endswith("coordinates"):
                        coords_text = elem.text.strip()
                        lon, lat, *_ = coords_text.split(",")
                        return float(lat), float(lon)
            except Exception as e:
                print(f"Failed to parse KML {file['name']}: {e}")

    return None

def get_kml_from_coordinate(lat, lon, folder_id="12sNfi4L4BUwQM0JYx84tXafNp_Hur9P6"):
    """
    Searches through KML files in a Google Drive folder and returns any KML file
    whose polygon contains the provided (lat, lon) coordinate.

    :param lat: float - latitude
    :param lon: float - longitude
    :param folder_id: str - Google Drive folder ID
    :param service: Google Drive API service instance
    :return: list of matching filenames or empty list if none found
    """
    service = get_drive_service()
    files = list_all_kml_files(folder_id, service)
    point = Point(float(lon), float(lat))  # KML uses (lon, lat)
    matching_files = []

    for file in files:
        request = service.files().get_media(fileId=file['id'], supportsAllDrives=True)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            _, done = downloader.next_chunk()

        fh.seek(0)
        try:
            tree = ET.parse(fh)
            root = tree.getroot()
            ns = {'kml': 'http://www.opengis.net/kml/2.2'}

            for placemark in root.findall('.//kml:Placemark', ns):
                for coords in placemark.findall('.//kml:coordinates', ns):
                    coords_text = coords.text.strip()
                    coord_pairs = coords_text.split()
                    polygon_points = []

                    for pair in coord_pairs:
                        parts = pair.split(',')
                        if len(parts) >= 2:
                            lon_i, lat_i = map(float, parts[:2])
                            polygon_points.append((lon_i, lat_i))

                    if len(polygon_points) >= 3:
                        polygon = Polygon(polygon_points)
                        if polygon.contains(point):
                            matching_files.append(file['name'])
                            break  # stop checking this file after first match
        except Exception as e:
            print(f"Failed to parse {file['name']}: {e}")

    return matching_files


def load_drive_file(file_id: str, filename: str):
    """
    If it’s a .pickle → use your existing open_pickle;
    if it’s anything else (e.g. .dxd/.json) → stream & json.load().
    """
    if filename.lower().endswith(".pickle"):
        return open_pickle(file_id=file_id)

    # otherwise assume JSON
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh,
                                     get_drive_service().files()
                                     .get_media(fileId=file_id, supportsAllDrives=True)
                                     )
    done = False
    while not done:
        _, done = downloader.next_chunk()
    fh.seek(0)
    return json.load(fh)


def list_files_in_folder(folder_id: str) -> list[dict]:
    """
    Returns a list of {id, name} dicts for every non-trashed file
    in the given folder (across My Drive or Shared Drives).
    """
    service    = get_drive_service()
    files_out  = []
    page_token = None

    while True:
        resp = service.files().list(
            q=f"'{folder_id}' in parents and trashed = false",
            supportsAllDrives       = True,
            includeItemsFromAllDrives = True,
            fields                  = "nextPageToken, files(id, name)",
            pageToken               = page_token
        ).execute()

        files_out.extend(resp.get("files", []))
        page_token = resp.get("nextPageToken")
        if not page_token:
            break

    return files_out

def find_file_id_in_folder(folder_id: str, file_name: str) -> str | None:
    """
    Returns the fileId of the *first* file named `file_name`
    inside the Drive folder `folder_id`, or None if not found.
    """
    svc = get_drive_service()
    # escape any single‐quotes in file_name
    safe_name = file_name.replace("'", r"\'")
    q = (
        f"name = '{safe_name}' "
        f"and '{folder_id}' in parents "
        "and trashed = false"
    )

    resp = svc.files().list(
        q=q,
        pageSize=1,
        supportsAllDrives        = True,
        includeItemsFromAllDrives = True,
        fields                   = "files(id, name)"
    ).execute()

    files = resp.get("files", [])
    return files[0]["id"] if files else None

# def get_dxd_file(dxd_name):
#     '''
#     Returns the file named `dxd_name` in Google Drive
#     :param dxd_name: should have .dxd at the end
#     :return:
#     '''
#
#     dxd_id = find_file_id_in_folder(DXD_FOLDER, dxd_name)
#     dxd_data = load_drive_file(dxd_id, dxd_name)
#     return dxd_data, dxd_id


# Example usage in your Slack bot
def slack_bot(request):
    growers = open_pickle()
    write_pickle(growers)
    pass


# Entry point for Google Cloud Functions
def main(request):
    return slack_bot(request)

def scheduled_task(request):
    """
    Function triggered by Cloud Scheduler via HTTP.
    """
    # Your scheduled task logic here
    print("Task executed at scheduled time.")
    return jsonify({"status": "success", "message": "Task executed."})

# growers = open_pickle(DXD_FOLDER)
# print()
# write_pickle(growers)
# list_shared_drive_files(DXD_FOLDER)
# show_pickle()
# get_coords_from_kml_folder('1416')
# get_kml_from_coordinate(36.862627, -120.607836)
# find_file_id_by_name('z6-01143', DXD_FOLDER)
# get_dxd_file('z6-11492.dxd')