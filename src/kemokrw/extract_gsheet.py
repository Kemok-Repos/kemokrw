from kemokrw.extract import Extract
from kemokrw.func_db import model_format_check
from kemokrw.func_api import extract_metadata, match_model, query_model_from_db
import pandas as pd


class ExtractGSheet(Extract):
    """Clase ExtractGSheet implementación de la clase Extract.

    Cumple la función de extraer información de una hoja de Google Sheets.

    Atributos
    ---------
    client : client_google.GoogleClient Object
    spreadsheet : str
    data_range : str
    model : dict
    metadata : dict
    data : pandas.DataFrame Object

    Métodos
    -------
    get_model():
        Obtiene la configuración de Google Sheets de una tabla de modelos.
    get_metadata():
        Obtiene la metadata de la tabla en Google Sheets.
    get_data():
        Obtiene la data de la tabla en Google Sheets.
    """
    def __init__(self, client, spreadsheet, data_range, model):
        """Construye un objeto de extracción desde Google Sheets.

        Parametros
        ----------
            client : client_google.GoogleClient Object
                Objeto que encapsula la API de Google Sheets.
            spreadsheet : str
                Id de la instancia de Google Sheets a extraer.
            data_range : str
                Rango de datos a extraer (inluye la hoja).
            model : dict
                Un diccionario con la información de columnas a extraer.
            metadata : dict
                Diccionario con el tipo (normalizado) y los chequeos realizados en cada columna para
                determinar diferencias.
            data : pandas.DataFrame Object
                Data extraída.
        """
        self.client = client
        self.spreadsheet = spreadsheet
        self.data_range = data_range
        self.model = model
        self.metadata = dict()
        self.data = pd.DataFrame()

        model_format_check(self.model)
        self.get_metadata()

    @classmethod
    def get_model(cls, client, db, model_id):
        """Construye un objeto de extracción desde Google Sheets a partir de un modelo en base de datos.

        Parametros
        ----------
            client : client_google.GoogleClient Object
                Objeto que encapsula la API de Google Sheets.
            db : str
                Connection string para bases de datos en SQLAlchemy.
            model_id : int
                Id del modelo dentro de la tabla de maestro_de_modelos.
        """
        model_config = query_model_from_db(db, model_id)
        return cls(client, model_config['spreadsheetId'], model_config['range'], model_config['model'])

    def get_metadata(self):
        """ Método que genera la metadata de los datos extraidos. """
        self.get_data()
        self.metadata = extract_metadata(self.model, self.data)

    def get_data(self):
        """Método que genera un Dataframe con la respuesta de la API"""
        self.data = pd.DataFrame()
        response = self.client.get(self.spreadsheet, self.data_range)
        table = []
        range_start = self.data_range.split("!", 1)[1]
        range_start = range_start.split(":", 1)[0]
        range_start = ''.join(e for e in range_start if not e.isnumeric())
        for i in response:
            row = dict()
            column = range_start
            for j, k in enumerate(i):
                row[column] = k
                column = chr(ord(column) + 1)
            table.append(row)

        self.data = pd.DataFrame(table)
        self.data = match_model(self.model, self.data, False)


