import json
import google.auth
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime
import sys

load_dotenv()

# Load credentials from the environment or a JSON file
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
# SERVICE_ACCOUNT_FILE = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', 'path/to/your/credentials.json')
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
    :param growers: A list of grower objects
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

def billing_report_new_tab_v2(growers):
    '''
    Creates or updates sheet tabs based on the install month and year of each field's logger.
    Each field is placed in the tab corresponding to the earliest install month-year it was installed.
    :param growers: A list of grower objects
    :return: True if any data was written, False otherwise
    '''
    from collections import defaultdict
    from datetime import datetime

    spreadsheet_id = '137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k'
    service = get_drive_service()

    # Retrieve all existing sheet names and build a record of what fields have already been written
    sheet_names = get_all_sheet_names(service, spreadsheet_id)
    recorded_fields_by_sheet = {sheet_name: set(get_existing_fields(service, spreadsheet_id, sheet_name)) for sheet_name in sheet_names}

    # Track all data to be written, grouped by month-year
    data_by_month = defaultdict(list)
    month_keys = {}  # maps formatted month to sortable datetime for sorting later

    for grower in growers:
        for field in grower.fields:
            unique_field_identifier = f'{grower.name}{field.nickname}'

            # Extract install dates from loggers (if any exist)
            install_dates = [logger.install_date for logger in field.loggers if logger.install_date]
            if not install_dates:
                continue

            earliest_install_date = min(install_dates)
            month_key = earliest_install_date.strftime('%Y-%m')  # sortable key like '2025-03'
            month_name = earliest_install_date.strftime('%B %Y')  # human-friendly tab name like 'March 2025'
            month_keys[month_name] = datetime.strptime(month_key, '%Y-%m')

            if unique_field_identifier in recorded_fields_by_sheet.get(month_name, set()):
                continue  # Skip if already recorded in that month's tab

            # Format all install dates for display (not just earliest)
            formatted_dates = ', '.join(sorted(set(dt.strftime('%d-%b') for dt in install_dates)))
            row_data = [
                grower.region,
                formatted_dates,
                grower.name,
                '',  # Bill Address
                f"'{field.nickname}",
                field.crop_type,
                field.acres,
                '',  # Cost
                ''   # Total
            ]
            data_by_month[month_name].append(row_data)

    # Write out data to appropriate monthly tabs in chronological order
    wrote_any_data = False
    for month_name in sorted(data_by_month, key=lambda x: month_keys[x]):
        rows = data_by_month[month_name]
        if month_name not in sheet_names:
            create_new_sheet_tab(service, spreadsheet_id, month_name)
        column_headers = ['Region', 'Date Installed', 'Grower', 'Bill Address', 'Fields', 'Crop', 'Acres', 'Cost', 'Total']
        write_data_to_sheet(service, spreadsheet_id, month_name, column_headers, rows)
        wrote_any_data = True
        print(f"✅ Updated tab '{month_name}' with {len(rows)} new fields.")

    if not wrote_any_data:
        print("No new fields to add for any month.")
        return False

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
