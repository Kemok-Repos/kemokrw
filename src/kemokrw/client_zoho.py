from kemokrw.client import ApiClient
from datetime import datetime, timedelta
import requests
import time
import json


class ZohoClient(ApiClient):
    """Clase ZohoClient implementacion de la clase ApiClient.

     Cumple la función de encapsular el manejador la API de Zoho CRM.

    Atributos
    ---------
    code : str
    access_token : str
    refresh_token : str
    expiration : datetime
    headers : dict
    filepath : str

    Métodos
    -------
    refresh():
        Obtiene el token de autenticación necesario para acceder a la API.
    get():
        Crea una llamada GET a la API. Retorna un Json con el resultado.
    post():
        Crea una llamada POST a la API. Retorna un Json con el resultado.
    """
    def __init__(self, filepath='ZohoCredentials.json'):
        """Construye un objeto encapsulando la API de Zoho CRM.

        Parametros
        ----------
            access_token : str
                Token para autenticar los requests.
            access_info: dict
                Diccionario con la información necesaria para autenticar la aplicación.
            expiration : datetime
                Fecha y hora de expiración del token.
            filepath : str
                PATH al directorio donde se almacenara la información para autenticarse con la API.
            headers : dict
                Estructura de headers para realizar llamadas a la API.
        """
        self.filepath = filepath
        self.access_info = None
        self.access_token = None
        self.expiration = datetime.utcnow()
        self.headers = None

        # Verifica si existe un archivo con credenciales, de lo contrario genera uno con el código
        try:
            file = open(filepath, "r")
            self.access_info = json.loads(file.read())
            self.access_token = self.access_info['access_token']
            self.expiration = datetime.fromisoformat(self.access_info['expiration'])
            self.headers = {'Accept': 'application/json', 'Authorization': 'Zoho-oauthtoken ' + self.access_token}
        except FileNotFoundError:
            raise Exception('No se puede inicializar el cliente por falta de credenciales.')
        except KeyError:
            print('No se tienes uno de los parametros necesarios')
        self.refresh()

    def refresh(self):
        """Genera un access_token a partir del la información de acceso"""
        if self.expiration > datetime.utcnow() and self.access_token is not None:
            print('Access Token aun es valido.')
        elif 'refresh_token' in self.access_info.keys():
            self.headers = None
            self.access_info['grant_type'] = 'refresh_token'
            access_keys = ['refresh_token', 'client_id', 'client_secret', 'grant_type']
            access_params = {x: self.access_info[x] for x in access_keys}
            access_url = 'https://accounts.zoho.com/oauth/v2/token'
            response = self.post(url=access_url, data=access_params)
            if 'error' in response.keys():
                raise Exception('Error de credenciales')
            for i in response:
                self.access_info[i] = response[i]
            self.access_token = response['access_token']
            self.expiration = datetime.utcnow() + timedelta(seconds=int(response['expires_in']))
            self.access_info['expiration'] = self.expiration.isoformat()
            config_object = json.dumps(self.access_info)
            file = open(self.filepath, "w")
            file.write(config_object)
            self.headers = {'Accept': 'application/json', 'Authorization': 'Zoho-oauthtoken ' + self.access_token}
        else:
            self.headers = None
            access_url = 'https://accounts.zoho.com/oauth/v2/token'
            response = self.post(url=access_url, data=self.access_info)
            if 'error' in response.keys():
                raise Exception('Error de credenciales. Posible token vencido')
            for i in response:
                self.access_info[i] = response[i]
            self.access_token = response['access_token']
            self.expiration = datetime.utcnow() + timedelta(seconds=int(response['expires_in']))
            self.access_info['expiration'] = self.expiration.isoformat()
            config_object = json.dumps(self.access_info)
            file = open(self.filepath, "w")
            file.write(config_object)
            self.headers = {'Accept': 'application/json', 'Authorization': 'Zoho-oauthtoken ' + self.access_token}

    def get(self, url, params=None):
        """ Método que crea una llamada GET a la API. Retorna un Json con el resultado."""
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
        """ Método que crea una llamada POST a la API. Retorna un Json con el resultado."""
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
