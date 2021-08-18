from kemokrw.client import ApiClient
import requests
import time


class TeamworkClient(ApiClient):
    """Clase TeamworkClient implementacion de la clase ApiClient.

     Cumple la función de encapsular el manejador la API de Teamwork.

    Atributos
    ---------
    user_token : str

    Métodos
    -------
    get():
        Crea una llamada GET a la API. Retorna un Json con el resultado.
    post():
        Crea una llamada POST a la API. Retorna un Json con el resultado.
    """
    def __init__(self, user_token=None):
        """Construye un objeto encapsulando la API de Teamwork.

        Parametros
        ----------
            user_token : str
                Token para autenticar los requests.
        """
        self.user_token = user_token

    def get(self, url, params=None):
        """ Método que crea una llamada GET a la API. Retorna un Json con el resultado."""
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
        """ Método que crea una llamada POST a la API. Retorna un Json con el resultado."""
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
