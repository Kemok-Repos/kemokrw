from kemokrw.client import ApiClient
import kemokrw.config_api as config
from datetime import datetime, timedelta
import requests
import time
import json


class TeamworkClient(ApiClient):
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
    def __init__(self, user_token=None):
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
        self.user_token = user_token

    def get(self, url, params=None):
        counter, exception = 0, None
        while counter < 3:
            try:
                response = requests.get(url, params=params, auth=(self.user_token, '.'))
                if response.status_code == 200:
                    answer = response.json()
                    if 'x-page' in response.headers.keys():
                        answer['x-page'] = response.headers['x-page']
                    if 'x-pages' in response.headers.keys():
                        answer['x-pages'] = response.headers['x-pages']
                    if 'x-records' in response.headers.keys():
                        answer['x-records'] = response.headers['x-records']
                    return answer
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
                response = requests.post(url, params=params, data=data, auth=(self.user_token, '.'))
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
