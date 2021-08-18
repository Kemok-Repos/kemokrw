from kemokrw.client import ApiClient
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
import os.path
import time


class GoogleClient(ApiClient):
    """Clase GoogleClient implementacion de la clase ApiClient.

     Cumple la función de encapsular el manejador la API de Google Sheets.

    Atributos
    ---------
    scopes : list
    credentials_file : str
    token_file : str
    credentials : dict
    service : googleapiclient.discovery.build.service Object

    Métodos
    ------
    refresh():
        Actualiza el access token de acceso a la API.
    get():
        Obtiene la información cruda de una rango de celdas en una hoja de cálculo.
    update():
        Escribe información curda en un rango de celdas en una hoja de cálculo.
    update_properties():
        Actualiza el formate de las celdas de un rango en una hoja de cálculo.
     """
    def __init__(self, credentials_file, token_file):
        """Construye un objeto encapsulando la API de Google Sheets.

        Parametros
        ----------
            scopes : list
                Indica el alcance de los permisos del token de acceso a generar.
            credentials_file : str
                PATH al directorio en donde se encuentra el archivo para generar el primer refresh token.
            token_file : str
                PATH al directorio en donde se encuentra la información encriptada de autenticación de Google.
            credentials : dict
                Diccionario con la información necesaria para manejar y solicitar nuevos tokens de acceso.
            service : str
                Objeto que permite manejar una hoja de cálculo.
        """
        self.scopes = ['https://www.googleapis.com/auth/spreadsheets']
        self.credentials_file = credentials_file
        self.token_file = token_file
        self.credentials = None
        self.service = None
        self.refresh()

    def refresh(self):
        """ Método que permite la actualización de credenciales para uso de la API. """
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
        """ Método que permite la extracción de información de una hoja de cálculo. """
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

    def update(self, spreadsheet, data_range, values, valueoption='RAW'):
        """ Método que permite la actualización de información de una hoja de cálculo. """
        counter, exception = 0, None
        while counter < 3:
            try:
                body = {'values': values}
                response = self.service.spreadsheet().values().update(spreadsheetId=spreadsheet, range=data_range,
                                                                      valueInputOption=valueoption, body=body).execute()
                return response
            except AttributeError as e:
                time.sleep(10)
                exception = e
                counter += 1
        raise exception

    def update_properties(self, spreadsheet, requests):
        """ Método que permite la actualización de formato de una hoja de cálculo. Vease:
                https://developers.google.com/sheets/api/samples/formatting
        """
        counter, exception = 0, None
        while counter < 3:
            try:
                body = {'requests': requests}
                response = self.service.spreadsheet().batchUpdate(spreadsheetId=spreadsheet, body=body).execute()
                return response
            except AttributeError as e:
                time.sleep(10)
                exception = e
                counter += 1
        raise exception