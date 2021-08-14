from kemokrw.client import ApiClient
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os.path
import time


class GoogleClient(ApiClient):
    """Clase para manejar la API de Google Sheets"""
    def __init__(self, credentials_file, token_file):
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.credentials = None
        self.service = None
        self.refresh()

    def refresh(self):
        if os.path.exists(self.token_file):
            self.credentials = Credentials.from_authorized_user_file(self.token_file, self.scopes)
        if not self.credentials or not self.credentials.valid:
            if self.credentials and self.credentials.expired and self.credentials.refresh_token:
                self.credentials.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(self.credentials_file, self.scopes)
                self.credentials = flow.run_local_server(port=0)
            with open(self.token_file, 'w') as token:
                token.write(self.credentials.to_json())
        service = build('sheets', 'v4', credentials=self.credentials)
        self.service = service.spreadsheets()

    def get(self, spreadsheet, data_range):
        counter, exception = 0, None
        while counter < 3:
            try:
                response = self.service.values().get(spreadsheetId=spreadsheet, range=data_range,
                                                     valueRenderOption='UNFORMATTED_VALUE',
                                                     dateTimeRenderOption='SERIAL_NUMBER').execute()
                values = response.get('values', [])
                return values
            except AttributeError as e:
                time.sleep(10)
                exception = e
                counter += 1
        raise exception
