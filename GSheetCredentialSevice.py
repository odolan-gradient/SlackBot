import os
import pickle

from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient import discovery


class GSheetCredentialSevice(object):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None

    def __init__(self):
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

    def getCreds(self):
        creds = self.creds
        if os.path.exists('C:\\Users\\javie\\Projects\\S-TOMAto\\credentials.json'):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/javie/Projects/S-TOMAto/credentials.json"
        elif os.path.exists('C:\\Users\\javie\\PycharmProjects\\Stomato\\credentials2.json'):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/javie/PycharmProjects/Stomato/credentials.json"
        elif os.path.exists('C:\\Users\\jsalcedo\\PycharmProjects\\Stomato\\credentials2.json'):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/jsalcedo/PycharmProjects/Stomato/credentials.json"
        elif os.path.exists('C:\\Users\\jesus\\PycharmProjects\\Stomato\\credentials2.json'):
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/jesus/PycharmProjects/Stomato/credentials2.json"

        if os.path.exists('token.pickle'):
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
                self.creds = creds
        else:
            creds = None
        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    'credentials2.json', self.SCOPES)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open('token.pickle', 'wb') as token:
                pickle.dump(self.creds, token)
        self.creds = creds

    def getService(self):
        self.getCreds()
        return discovery.build('sheets', 'v4', credentials=self.creds)

