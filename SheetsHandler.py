import json
import google.auth
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime
import sys

import SharedPickle

load_dotenv()

# Load credentials from the environment or a JSON file
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'path/to/your/credentials.json')
credentials_info = SharedPickle.credentials_info


def get_drive_service():
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info,
        scopes=['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/spreadsheets'])

    service = build('sheets', 'v4', credentials=credentials)
    return service


def log_request_to_sheet(request_name, user, info):
    """Log the request name, info, and timestamp to Google Sheets."""
    service = get_drive_service()

    spreadsheet_id = '1nXrjbexLmSwdu5yE0Q42cQwQpWeS4j0i25-_rj5wSss'
    range_name = 'Sheet1!A:D'

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert info to string if it's not already
    if not isinstance(info, str):
        try:
            info = json.dumps(info)
        except:
            info = str(info)

    values = [[request_name, user, info, timestamp]]
    body = {
        'values': values
    }

    try:
        sheet = service.spreadsheets()
        result = sheet.values().append(
            spreadsheetId=spreadsheet_id,
            range=range_name,
            valueInputOption='RAW',
            body=body
        ).execute()
        print(f"{result.get('updates').get('updatedCells')} cells updated in sheet.")
    except Exception as e:
        print(f"Error appending to sheet: {e}")
        print(f"Attempted to append: {values}")


def get_all_sheet_names(service, spreadsheet_id):
    '''
    Retrieves the names of all sheets (tabs) in the spreadsheet.
    :param service: The Google Sheets API service
    :param spreadsheet_id: The ID of the spreadsheet
    :return: A list of sheet names
    '''
    # Get the spreadsheet metadata, including all sheet names
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()

    # Extract sheet names from the metadata
    sheets = sheet_metadata.get('sheets', [])
    sheet_names = [sheet['properties']['title'] for sheet in sheets]

    return sheet_names


def billing_report_new_tab(growers):
    '''
    Creates a new sheet tab for the current month, excluding fields that have already been added in previous tabs.
    :param growers: A list of grower names
    :return: True if successful, False otherwise
    '''
    spreadsheet_id = '137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k'
    service = get_drive_service()

    # Get the current month to name the tab
    current_month = datetime.now().strftime('%B')

    # Retrieve all sheet names to track fields added in previous runs
    sheet_names = get_all_sheet_names(service, spreadsheet_id)

    # Create a set of previously recorded fields (grower.name + field.nickname)
    recorded_fields = set()
    for sheet_name in sheet_names:
        recorded_fields.update(get_existing_fields(service, spreadsheet_id, sheet_name))

    # Prepare data for the new tab, only including new fields from the list of growers
    all_grower_data = []
    for grower in growers:
        # Collect data for new fields of the current grower
        grower_data = []
        for field in grower.fields:
            unique_field_identifier = f'{grower.name}{field.nickname}'
            if unique_field_identifier not in recorded_fields:
                install_dates = ', '.join(
                    sorted(set(logger.install_date.strftime('%d-%b') for logger in field.loggers)))
                row_data = [
                    grower.region,  # Region
                    install_dates,  # Date Installed
                    grower.name,  # Grower
                    '',  # Bill Address (Blank)
                    f"'{field.nickname}",  # Field names extra apostrophe to prevent formatting by sheets (berra issue)
                    field.crop_type,  # Crop
                    field.acres,  # Acres
                    '',  # Cost (Blank for now)
                    ''  # Total (Blank for now)
                ]
                grower_data.append(row_data)

        if grower_data:
            all_grower_data.extend(grower_data)

    # If no new fields, skip creating a new tab
    if not all_grower_data:
        print(f"No new fields to add for any growers.")
        return False

    # Create a new sheet tab named after the current month (if it doesn't exist)
    new_sheet_name = current_month
    if new_sheet_name not in sheet_names:
        create_new_sheet_tab(service, spreadsheet_id, new_sheet_name)

    # Write the data to the new sheet tab
    column_headers = ['Region', 'Date Installed', 'Grower', 'Bill Address', 'Fields', 'Crop', 'Acres', 'Cost', 'Total']
    write_data_to_sheet(service, spreadsheet_id, new_sheet_name, column_headers, all_grower_data)

    print(f"New tab '{new_sheet_name}' created and data written for growers.")
    return True


def read_sheet_data(service, spreadsheet_id, sheet_name):
    '''
    Reads the data from a specific sheet (tab) in the Google Spreadsheet.
    :param service: The Google Sheets API service
    :param spreadsheet_id: The ID of the spreadsheet
    :param sheet_name: The name of the sheet/tab
    :return: A list of rows, where each row is a list of cell values
    '''
    sheet_range = f'{sheet_name}!A:Z'  # Adjust column range if necessary
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=sheet_range
    ).execute()

    # Get the rows of data
    values = result.get('values', [])

    return values


def get_existing_fields(service, spreadsheet_id, sheet_name):
    '''
    Retrieves all the existing fields in a given sheet (as unique grower_name + field.nickname).
    :param service: The Google Drive/Sheets API service
    :param spreadsheet_id: The ID of the spreadsheet
    :param sheet_name: The name of the sheet/tab
    :return: A set of strings representing fields (grower_name + field.nickname)
    '''
    # Read the sheet data and extract unique field identifiers (grower_name + field.nickname)
    sheet_data = read_sheet_data(service, spreadsheet_id, sheet_name)
    existing_fields = set()
    for row in sheet_data:
        grower_name = row[2]  # Assuming grower name is in the 3rd column (index 2)
        field_nickname = row[4]  # Assuming field nickname is in the 5th column (index 4)
        existing_fields.add(f'{grower_name}{field_nickname}')
    return existing_fields


def create_new_sheet_tab(service, spreadsheet_id, sheet_name):
    '''
    Creates a new sheet tab in the spreadsheet with the given name.
    :param service: The Google Sheets API service
    :param spreadsheet_id: The ID of the spreadsheet
    :param sheet_name: The name of the new sheet/tab
    '''
    requests = {
        "addSheet": {
            "properties": {
                "title": sheet_name
            }
        }
    }
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [requests]}
    ).execute()


def write_data_to_sheet(service, spreadsheet_id, sheet_name, column_headers, data):
    '''
    Writes data to a specific sheet in the spreadsheet.
    :param service: The Google Sheets API service
    :param spreadsheet_id: The ID of the spreadsheet
    :param sheet_name: The name of the sheet/tab
    :param column_headers: A list of column headers
    :param data: The data to write (list of lists, each representing a row)
    '''
    body = {
        'values': [column_headers] + data
    }
    sheet_range = f'{sheet_name}!A1'
    service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=sheet_range,
        valueInputOption='USER_ENTERED',
        body=body
    ).execute()


# https://docs.google.com/spreadsheets/d/137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k/edit?gid=0#gid=0
# growers = SharedPickle.open_pickle()
# billing_report_new_tab(growers)
# spreadsheet_id = '137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k'
# grower_names = get_grower_names(spreadsheet_id)
# print("Grower Names:", grower_names)
