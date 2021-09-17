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

    Cumple la función de extraer información de una hoja de Google Sheets.

    Atributos
    ---------
    spreadsheet_id : str
    sheet : str
    model : dict ej: {"db": "str_sql_alchemy", "table": "table1", "fields": {"field1": "tipodato", "field1": "tipodato"},
         "map_sheet_model": {"a1": "field1", "b1": "field2", "c1": "field3"},"formats":{"datetime":"%d/%m/%Y %H:%M:%S"}}

    metadata : dict
    data : pandas.DataFrame Object

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla en Google Sheets.
    get_data():
        Obtiene la data de la tabla en Google Sheets.
    """
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


