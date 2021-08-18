from kemokrw.extract import Extract
from kemokrw.func_db import model_format_check
from kemokrw.func_api import date_range, extract_metadata, match_model, query_model_from_db
import pandas as pd


class ExtractHubstaff(Extract):
    """Clase ExtractHubstaff implementación de la clase Extract.

     Cumple la función de extraer información de la API de Hubstaff.

     Atributos
     ---------
     client : client_hubstaff.HubstaffClient Object
     url : str
     endpoint : str
     endpoint_type : str
     response_key : str
     by_list : str
     url_params : dict
     params : dict
     model : dict
     metadata : dict
     data : pandas.DataFrame Object

     Métodos
     -------
     get_model():
         Obtiene la configuración de Hubstaff de una tabla de modelos.
     get_metadata():
         Obtiene la metadata de la información extraida de Hubstaff.
     get_data():
         Obtiene la data de la API de Hubstaff.
     """
    def __init__(self, client, url, endpoint, endpoint_type, response_key, model, params=dict(), by_list=None):
        """Construye un objeto desde la API de Hubstaff.

        Parametros
        ----------
             client : client_hubstaff.HubstaffClient Object
                Cliente para manejar la API de Hubstaff.
             url : str
                URL del endpoint.
             endpoint : str
                Nombre del endpoint a extraer.
             endpoint_type : str
                Tipo de extracción a usar en endpoint.
             response_key : str
                Llave utilizada por la API para encapsular los datos.
             by_list : str
                Listado de valores a extraer en caso se requiera extraer valor puntuales.
             url_params : dict
                Listado de parametros a utilizar dentro de la url al hacer la llamada a la API.
             params : dict
                Listado de parametros a utilizar dentro del header al hacer la llamada a la API
             model : dict
                 Un diccionario con la información de columnas a extraer.
             metadata : dict
                Diccionario con el tipo (normalizado) y los chequeos realizados en cada columna para
                determinar diferencias.
             data : pandas.DataFrame Object
                 Data extraída.
        """
        self.client = client
        self.url = url
        self.endpoint = endpoint
        self.endpoint_type = endpoint_type
        self.response_key = response_key
        self.by_list = by_list
        self.url_params = {'organization_id': str(self.client.organization_id), 'id': '{id}'}
        self.model = model
        self.metadata = dict()
        self.data = pd.DataFrame()
        self.params = date_range(params)

        model_format_check(self.model)
        self.get_metadata()

    @classmethod
    def get_model(cls, client, db, model_id, params=dict(), by_list=None):
        """Método que construye un objeto de extracción desde Hubstaff a partir de un modelo en base de datos.

        Parametros
        ----------
            client : client_hubstaff.HubstaffClient Object
                Objeto que encapsula la API de Google Sheets.
            db : str
                Connection string para bases de datos en SQLAlchemy.
            model_id : int
                Id del modelo dentro de la tabla de maestro_de_modelos.
             by_list : str
                Listado de valores a extraer en caso se requiera extraer valor puntuales.
             params : dict
                Listado de parametros a utilizar dentro del header al hacer la llamada a la API
        """
        model_config = query_model_from_db(db, model_id)
        return cls(client, model_config['url'], model_config['endpoint'], model_config['endpoint_type'],
                   model_config['response_key'], model_config['model'], params, by_list)

    def get_metadata(self):
        """ Método que genera la metadata de los datos extraidos. """
        self.get_data()
        self.metadata = extract_metadata(self.model, self.data)

    def get_data(self):
        """Método que genera un Dataframe con la respuesta de la API"""
        self.data = pd.DataFrame()
        url = self.url.format(**self.url_params)
        # Manejar la paginacion
        if self.endpoint_type == 'by_id':
            data = []
            for i in self.by_list:
                response = self.client.get(url.format(id=i), params=self.params)
                data.append(response[self.response_key])
            self.data = pd.DataFrame(data)
        elif self.endpoint_type == 'by_organization':
            while True:
                response = self.client.get(url, params=self.params)
                page = pd.DataFrame(response[self.response_key])
                self.data = pd.concat([self.data, page])
                if 'pagination' not in response.keys():
                    break
                self.params['page_start_id'] = response['pagination']['next_page_start_id']
        else:
            raise Exception(str(self.endpoint_type)+' is not a valid type.')
        if 'page_start_id' in self.params.keys():
            self.params.pop('page_start_id')

        # Dar formato a la data
        self.data = match_model(self.model, self.data)
