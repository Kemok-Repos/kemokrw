from kemokrw.client import ApiClient
import kemokrw.config_api as config
from datetime import datetime, timedelta
import requests
import time
import json


class HubstaffClient(ApiClient):
    """Clase para manejar la API de Hubstaff.

    Atributos
    ---------
    access_token : str
    refresh_token : str
    expiration : datetime
    organization : str
    organization_id : int
    headers : dict
    filepath : str

    Métodos
    -------
    get_access_token():
        Obtiene el token de autenticación necesario para acceder a la API.
    get():
        Crea GET requests de la API. Retorna un Json con el resultado.
    """
    def __init__(self, path='HubstaffCredentials.json', refresh_token=None, organization= 'Kemok'):
        """Construye los atributos necesarios para la autenticación.

        Parametros
        ----------
            access_token : str
                Token para autenticar los requests.
            refresh_token: str
                Token para obtener un access token.
            expiration : datetime
                Fecha y hora de expiración del token.
            organization : str
                Nombre de la organización de la que se desea obtener datos.
            organization_id : int
                ID de Hubstaff de la organización de la que se desea obtener datos.
            filepath : str
                PATH al directorio donde se almacenara la información para autenticarse con la API.
            headers : dict
                Estructura de headers para realizar llamadas a la API.
        """
        self.access_token = None
        self.refresh_token = refresh_token
        self.expiration = datetime.utcnow()
        self.organization = organization
        self.organization_id = None
        self.filepath = path
        self.headers = None

        # Verifica si existe un archivo con credenciales, de lo contrario genera uno con el Persona Access Token
        try:
            file = open(path, "r")
            credentials = json.loads(file.read())
            self.access_token = credentials['access_token']
            self.expiration = datetime.fromisoformat(credentials['expiration'])
            self.headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + self.access_token}
            if credentials['refresh_token'] is not None and self.expiration < (datetime.utcnow()-timedelta(seconds=2)):
                self.refresh_token = credentials['refresh_token']
        except FileNotFoundError:
            print('No se encuentra el archivo de configuración. Intentando obtener nuevo acceso.')
            if refresh_token is None:
                raise Exception('No se puede inicializar el cliente por falta de un refresh token válido.')
        self.refresh()

        # Obtiene el token de la organizacion
        response = self.get(config.HUBSTAFF['organizations']['base_url'])
        for i in response[config.HUBSTAFF['organizations']['key']]:
            if i['name'] == self.organization:
                self.organization_id = i['id']
        print('Cliente de Hubstaff exitoso.')

    def refresh(self):
        """Genera un access_token a partir del refresh_token"""
        if self.expiration > datetime.utcnow() and self.access_token is not None:
            print('Access Token aun es valido.')
        else:
            self.headers = None
            response = self.get('https://account.hubstaff.com/.well-known/openid-configuration')
            token_endpoint = response['token_endpoint']
            response = self.post(url=token_endpoint, data={'grant_type': 'refresh_token',
                                                           'refresh_token': self.refresh_token})
            self.access_token = response['access_token']
            self.refresh_token = response['refresh_token']
            self.expiration = datetime.utcnow() + timedelta(seconds=int(response['expires_in']))
            credentials = {'access_token': self.access_token, 'refresh_token': self.refresh_token,
                           'expiration': self.expiration.isoformat()}
            config_object = json.dumps(credentials)
            file = open(self.filepath, "w")
            file.write(config_object)
            self.headers = {'Accept': 'application/json', 'Authorization': 'Bearer ' + self.access_token}

    def get(self, url, params=None):
        counter, exception = 0, None
        while counter < 3:
            try:
                response = requests.get(url, headers=self.headers, params=params)
                if response.status_code == 200:
                    return response.json()
                else:
                    time.sleep(10)
                    exception = Exception('GET '+url + ' code ' + str(response.status_code)+' : '+response.text)
                    counter += 1
            except requests.exceptions.RequestException as e:
                time.sleep(10)
                exception = e
                counter += 1
        raise exception

    def post(self, url, params=None, data=None):
        counter, exception = 0, None
        while counter < 3:
            try:
                response = requests.post(url, headers=self.headers, params=params, data=data)
                if response.status_code == 200:
                    return response.json()
                else:
                    time.sleep(10)
                    exception = Exception('POST '+url + ' code ' + str(response.status_code)+' : '+response.text)
                    counter += 1
            except requests.exceptions.RequestException as e:
                time.sleep(10)
                exception = e
                counter += 1
        raise exception
