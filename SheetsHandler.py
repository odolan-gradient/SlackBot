import json
import google.auth
from googleapiclient.discovery import build
from google.oauth2 import service_account
from dotenv import load_dotenv
from datetime import datetime

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


def log_request_to_sheet(request_name, info):
    """Log the request name, info, and timestamp to Google Sheets."""
    service = get_drive_service()

    spreadsheet_id = '1nXrjbexLmSwdu5yE0Q42cQwQpWeS4j0i25-_rj5wSss'
    range_name = 'Sheet1!A:C'

    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # Convert info to string if it's not already
    if not isinstance(info, str):
        try:
            info = json.dumps(info)
        except:
            info = str(info)

    values = [[request_name, info, timestamp]]
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
