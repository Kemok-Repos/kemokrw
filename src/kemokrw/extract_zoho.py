from kemokrw.extract import Extract
from kemokrw.func_db import model_format_check
from kemokrw.func_api import extract_metadata, match_model, query_model_from_db
import pandas as pd


class ExtractZoho(Extract):
    """Clase ExtractZoho implementación de la clase Extract.

     Cumple la función de extraer información de la API de Zoho.

     Atributos
     ---------
     client : client_zoho.ZohoClient Object
     url : str
     endpoint : str
     endpoint_type : str
     response_key : str
     by_list : str
     params : dict
     model : dict
     metadata : dict
     data : pandas.DataFrame Object

     Métodos
     -------
     get_model():
         Obtiene la configuración de Zoho de una tabla de modelos.
     get_metadata():
         Obtiene la metadata de la información extraida de Zoho.
     get_data():
         Obtiene la data de la API de Zoho.
     """
    def __init__(self, client, url, endpoint, endpoint_type, response_key, model, params=dict(), by_list=None):
        """Construye un objeto desde la API de Zoho.

        Parametros
        ----------
             client : client_zoho.ZohoClient Object
                Cliente para manejar la API de Zoho.
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
        self.model = model
        self.metadata = dict()
        self.data = pd.DataFrame()
        self.params = (params or dict())

        model_format_check(self.model)
        self.get_metadata()

    @classmethod
    def get_model(cls, client, db, model_id, params=dict(), by_list=None):
        """Construye un objeto de extracción desde Teamwork a partir de un modelo en base de datos.

        Parametros
        ----------
            client : client_google.GoogleClient Object
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
        url = self.url
        if self.endpoint_type == 'by_list':
            data = []
            for i in self.by_list:
                response = self.client.get(url.format(id=i), params=self.params)
                data.append(response[self.response_key])
            self.data = pd.DataFrame(data)
        elif self.endpoint_type == 'all':
            pages = True
            page_number = 1
            self.params["page"] = page_number
            while pages:
                response = self.client.get(url, params=self.params)
                page = pd.DataFrame(response[self.response_key])
                self.data = pd.concat([self.data, page])
                if 'info' in response.keys():
                    pages = response['info']['more_records']
                    page_number += 1
                    self.params["page"] = page_number
                else:
                    pages = False
        else:
            raise Exception(str(self.endpoint_type)+' is not a valid type.')
        if 'page' in self.params.keys():
            self.params.pop('page')

        self.data = match_model(self.model, self.data)
