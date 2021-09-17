<<<<<<< HEAD
from kemokrw.extract import Extract
from kemokrw.func_db import model_format_check
from kemokrw.func_api import extract_metadata, match_model, query_model_from_db
import pandas as pd


class ExtractGSheet(Extract):
    """Clase ExtractGSheet implementación de la clase Extract.
=======
import os.path
#
from sqlalchemy import create_engine
import pandas as pd
from kemokrw.extract import Extract
from kemokrw.gsheet import *
import kemokrw.config_db as config
import re
import hashlib


class ExtractGSheet(Extract):
    """
    Clase ExtractGSheet implementación de la clase Extract.
>>>>>>> @{u}

    Cumple la función de extraer información de una hoja de Google Sheets.

    Atributos
    ---------
<<<<<<< HEAD
    client : client_google.GoogleClient Object
    spreadsheet : str
    data_range : str
    model : dict
=======
    spreadsheet_id : str
    sheet : str
    model : dict ej: {"db": "str_sql_alchemy", "table": "table1", "fields": {"field1": "tipodato", "field1": "tipodato"},
         "map_sheet_model": {"a1": "field1", "b1": "field2", "c1": "field3"},"formats":{"datetime":"%d/%m/%Y %H:%M:%S"}}

>>>>>>> @{u}
    metadata : dict
    data : pandas.DataFrame Object

    Métodos
    -------
<<<<<<< HEAD
    get_model():
        Obtiene la configuración de Google Sheets de una tabla de modelos.
=======
>>>>>>> @{u}
    get_metadata():
        Obtiene la metadata de la tabla en Google Sheets.
    get_data():
        Obtiene la data de la tabla en Google Sheets.
    """
<<<<<<< HEAD
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
=======
    model: dict


    def __init__(self, spreadsheet_id, sheet, header, model):
        self.model = model
        self.header = header
        self.spreadsheet_id = spreadsheet_id
        self.sheet = sheet
        self.metadata = None
        self.data = None
        self.connection = create_engine(model["db"])
        #self.scope = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self.scope = ['https://www.googleapis.com/auth/drive']
        self.gsheet = GSheet(g_object='spreadsheets',
                             spreadsheet_id=self.spreadsheet_id,
                             sheet=self.sheet,
                             scope=self.scope,
                             model=self.model)
        self.gsheet.gauth()
        self.MaxRowId = self.gsheet.read_countRow(self.sheet + '!' + self.header)
        print(self.MaxRowId)


    def get_metadata(self):
        col = 'descripcion'
        # falla calculo string_agg pandas
        fd = self.data["descripcion"].sort_values().astype(str).str.strip().values.sum(axis=0)
        fd_md5 = hashlib.md5(fd.encode('utf-8')).hexdigest()
        print(fd_md5)
        query = config.COLUMN_CHECK['postgresql']['text']['check_hash'].\
            format(column="descripcion", table=self.model["table"], condition="", order="order by descripcion")
        rs = self.connection.execute(query)
        str_hash = rs.fetchone()[0]
        print(str_hash)


    def hash_string(self, value):
        return (hashlib.md5(str(value).encode('utf-8')).hexdigest())


    def get_data(self):
        values = ''
        header = ''
        headers = self.gsheet.read_spreadsheet(self.sheet + '!' + self.header)[0]
        cells = re.sub('[0-9]:', '{}:'.format(str(2)), self.header)
        rango = self.sheet + '!'+cells+'99999'
        try:
            header = self.gsheet.read_spreadsheet(rango)[0]
            values = self.gsheet.read_spreadsheet(rango)

        except Exception as e:
            msg = ",  Contact Kemok Administrator"
            valor = str(e).replace('\"', '').replace(":", ' ') \
                .replace("%", '(porcentaje)')

        values, err = self.gsheet.prepare(values, header)
        self.data = pd.DataFrame(values)
        print(self.data)


    def Verify(self):
        pass


    def tranfer(self):
        self.get_data()
        columns, j = {}, 0
        for i in self.model["model"]:
            columns[j] = self.model["model"][i]["name"]
            j += 1

        self.data.rename(columns=columns, inplace=True)
        #self.get_metadata()
        self.connection.execute("DELETE FROM {0} {1}".format(self.model['table'], ''))
        self.data.to_sql(name=self.model["table"], con=self.connection, if_exists='append',
                         index=False, chunksize=10000)
>>>>>>> @{u}


