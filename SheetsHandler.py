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


def get_last_used_row(service, spreadsheet_id, sheet_name):
    # Fetch the last used row
    range_ = f'{sheet_name}!A:C'  # Check the C column cuz that has the extra total acres row
    result = service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=range_
    ).execute()

    values = result.get('values', [])
    return len(values)  # The last row used is the length of the values array


def get_grower_names(spreadsheet_id):
    service = get_drive_service()

    # Call the Sheets API to get all the data
    sheet = service.spreadsheets()
    result = sheet.values().get(spreadsheetId=spreadsheet_id, range='Sheet1').execute()
    values = result.get('values', [])

    if not values:
        print('No data found.')
        return []

    # Get the sheet's properties to access gridProperties
    sheet_metadata = service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    properties = sheet_metadata['sheets'][0]['properties']
    row_count = properties['gridProperties']['rowCount']

    # Get the cell format for each row
    fields = 'sheets/data/rowData/values/userEnteredFormat/backgroundColor'
    response = service.spreadsheets().get(spreadsheetId=spreadsheet_id, ranges=['Sheet1'], fields=fields).execute()

    grower_names = []
    row_data = response['sheets'][0]['data'][0]['rowData']

    for i, value_row in enumerate(values):
        if i >= row_count or i >= len(row_data):
            break

        # Check if the row is non-empty
        if value_row:
            is_green = False

            # Check if the row has formatting data
            if i < len(row_data) and 'values' in row_data[i] and row_data[i]['values']:
                first_cell = row_data[i]['values'][0]
                if 'userEnteredFormat' in first_cell:
                    bg_color = first_cell['userEnteredFormat'].get('backgroundColor', {})
                    # Check if the background color is green
                    is_green = bg_color.get('green', 0) == 1

            # If the row is green or if it's the first non-empty row after a green row
            if is_green or (grower_names and i > 0 and not values[i - 1]):
                grower_names.append(value_row[0])  # Append the first cell of the row

    return grower_names


def create_checkbox_request(sheet_id, start_row, end_row, column):
    return {
        'setDataValidation': {
            'range': {
                'sheetId': sheet_id,
                'startRowIndex': start_row - 1,  # -1 because API is 0-indexed
                'endRowIndex': end_row,
                'startColumnIndex': ord(column) - ord('A'),
                'endColumnIndex': ord(column) - ord('A') + 1
            },
            'rule': {
                'condition': {
                    'type': 'BOOLEAN'
                },
                'showCustomUi': True
            }
        }
    }


def billing_report(grower_name):
    '''
    Returns True if successfully adds a grower name, returns False if already in the sheet
    :param grower_name:
    :return:
    '''
    spreadsheet_id = '137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k'

    grower = SharedPickle.get_grower(grower_name)
    service = get_drive_service()
    sheet_name = 'Sheet1'

    # Check if grower already in sheet
    growers_in_sheet = get_grower_names(spreadsheet_id)
    if grower_name in growers_in_sheet:
        print(f'{grower_name} already in the sheet')
        return False
    print(f'Adding {grower_name} to billing sheet')

    grower_header = [[grower.name]]
    column_labels = [
        'Grower Field Number', 'MS Field Number', 'Contracted Acres',
        'Installation Date', 'Crop', 'Discount', 'Ready to bill', 'Billed'
    ]
    last_used_row = get_last_used_row(service, spreadsheet_id, sheet_name)

    # Prepare data for each field
    data_to_append = []
    total_acres = 0
    num_fields = 0
    for field in grower.fields:
        field_name = field.name
        ms_num = field.name_ms or ''
        acres = field.acres
        installation_date = field.loggers[0].install_date.strftime('%Y-%m-%d')
        crop = field.crop_type
        discount = ''
        total_acres += float(acres)

        ready_to_bill = ''
        billed = ''

        # Append each row's data
        row_data = [
            field_name,  # Grower Field Number
            ms_num,  # MS Field Number
            acres,  # Contracted Acres
            installation_date,  # Installation Date
            crop,  # Crop
            discount,  # Discount or any additional data
            ready_to_bill,  # Ready to bill
            billed  # Billed
        ]
        num_fields += 1
        data_to_append.append(row_data)

    # Append the total contracted acres two rows below the last field row
    if num_fields > 1:
        total_row = ['', '', total_acres, '', '', '', '', '']
        data_to_append.append([])  # Empty row (two rows down)
        data_to_append.append(total_row)  # Row with the sum of Contracted Acres

    # Prepare the request body for batch updating
    body = {
        'values': grower_header + [column_labels] + data_to_append
    }

    # Append the data to the sheet
    if last_used_row != 0:
        last_used_row += 1  # Add a space between growers
    sheet_range = f'{sheet_name}!A{last_used_row + 1}'  # was causing the shifting over problem
    request = service.spreadsheets().values().append(
        spreadsheetId=spreadsheet_id,
        range=sheet_range,
        valueInputOption='USER_ENTERED',
        body=body
    )
    response = request.execute()

    # Calculate where to apply formatting (start from last_used_row)
    grower_header_row = last_used_row
    column_labels_row = last_used_row + 1
    first_data_row = last_used_row + 2
    last_data_row = first_data_row + len(data_to_append)

    # Apply formatting for the grower header (dynamically calculated)
    requests = [
        {
            "repeatCell": {
                "range": {
                    "sheetId": 0,  # Assumes the first sheet
                    "startRowIndex": grower_header_row,
                    "endRowIndex": grower_header_row + 1,  # Only this row for grower header
                    "startColumnIndex": 0,
                    "endColumnIndex": 8  # Assuming 8 columns
                },
                "cell": {
                    "userEnteredFormat": {
                        "backgroundColor": {
                            "red": 0.0,
                            "green": 1.0,
                            "blue": 0.0  # Green highlight for grower header
                        },
                        "horizontalAlignment": "CENTER",
                        "textFormat": {
                            "bold": True,
                            "fontSize": 14  # Increase font size to 14
                        }
                    }
                },
                "fields": "userEnteredFormat(backgroundColor,textFormat,horizontalAlignment)"
            }
        },
        {
            "repeatCell": {
                "range": {
                    "sheetId": 0,
                    "startRowIndex": column_labels_row,  # Second row for column labels
                    "endRowIndex": column_labels_row + 1,  # Only the row for column labels
                    "startColumnIndex": 0,
                    "endColumnIndex": 8  # Adjust if there are more columns
                },
                "cell": {
                    "userEnteredFormat": {
                        "horizontalAlignment": "CENTER",
                        "textFormat": {
                            "bold": True,
                            "fontSize": 12  # Font size 12 for column labels
                        },
                        "backgroundColor": {
                            "red": 0.9,
                            "green": 0.9,
                            "blue": 0.9  # Light gray background for column labels
                        }
                    }
                },
                "fields": "userEnteredFormat(textFormat,horizontalAlignment,backgroundColor)"
            }
        }
    ]

    # Send batch update for formatting
    batch_update_request = {
        'requests': requests
    }

    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=batch_update_request
    ).execute()

    print(f"Data appended and formatted for grower: {grower_name}")

    #                       ADD CHECKBOXES                    #

    # Calculate the range for 'Ready to Bill' and 'Billed' columns
    ready_to_bill_col = 'G'  # Assuming 'Ready to Bill' is column G
    billed_col = 'H'  # Assuming 'Billed' is column H
    start_row = first_data_row + 1  # +1 because we want to start after the header rows
    end_row = last_data_row
    if num_fields > 1:
        end_row -= 2  # Add a gap between checkboxes and total contacted acres

    # Create requests for adding checkboxes
    sheet_id = 0  # Assuming it's the first sheet
    checkbox_columns = ['G', 'H']  # 'Ready to Bill' and 'Billed' columns

    checkbox_requests = [
        create_checkbox_request(sheet_id, start_row, end_row, column)
        for column in checkbox_columns
    ]
    #  this is a list comp, the val is the first statement, so run that request for each column

    # Add the checkbox requests to the existing batch update request
    batch_update_request['requests'].extend(checkbox_requests)

    # Execute the batch update
    service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body=batch_update_request
    ).execute()

    print(f"Data appended, formatted, and checkboxes added for grower: {grower_name}")
    return True


# https://docs.google.com/spreadsheets/d/137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k/edit?gid=0#gid=0
# billing_report('Ryan Jones')
# billing_report('TOS Farms')
# billing_report('Dwelley Farms')
# spreadsheet_id = '137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k'
# grower_names = get_grower_names(spreadsheet_id)
# print("Grower Names:", grower_names)
