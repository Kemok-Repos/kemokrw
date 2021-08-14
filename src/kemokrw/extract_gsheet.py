from kemokrw.extract import Extract
from kemokrw.func_api import clean_str, convert_date_from_gsheet, clean_bool, clean_numeric
import kemokrw.config_api as config
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError
import pandas as pd


class ExtractGSheet(Extract):
    """Clase ExtractGSheet implementación de la clase Extract.

    Cumple la función de extraer información de una hoja de Google Sheets.

    Atributos
    ---------
    spreadsheet_id : str
    sheet : str
    model : dict
    metadata : dict
    data : pandas.DataFrame Object

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla en Google Sheets.
    get_data():
        Obtiene la data de la tabla en Google Sheets.
    """
    def __init__(self, client, spreadsheet, data_range, model):
        self.client = client
        self.spreadsheet = spreadsheet
        self.data_range = data_range
        self.model = model
        self.metadata = None
        self.data = None
        self.get_metadata()

    @classmethod
    def query_model(cls, client, db, model_id,
                    query='SELECT configuracion FROM maestro_de_modelos WHERE id = {0} LIMIT 1;'):
        engine = create_engine(db)
        attempts = 0
        while attempts < 3:
            try:
                connection = engine.connect()
                config_query = connection.execute(query.format(model_id))
                connection.close()
                break
            except OperationalError as err:
                attempts += 1
                if attempts == 3:
                    raise err
            except DatabaseError as err:
                attempts += 1
                if attempts == 3:
                    raise err
        config = dict()
        for i in config_query:
            config = i[0]
        if config != dict():
            return cls(client, config['spreadsheetId'], config['range'], config['model'])
        else:
            raise Exception('Modelo no encontrado')

    def get_metadata(self):
        self.get_data()
        self.metadata = dict()
        self.metadata["ncols"] = len(self.model)
        if not self.data.empty:
            self.metadata["check_rows"] = len(self.data)
        else:
            self.metadata["check_rows"] = 0

        columns = dict()
        for i in self.model:
            col = dict()
            col["subtype"] = self.model[i]["type"]
            col_type = col["subtype"].upper()
            col_type = ''.join(e for e in col_type if e.isalpha() or e.isspace() or e == '[' or e == ']')
            for j in config.COLUMN_TYPES:
                if col_type in config.COLUMN_TYPES[j]:
                    col["type"] = j
            if "type" not in col.keys():
                print("*WARNING*: {} no es un tipo identificado.".format(col["subtype"]))
                col["type"] = "other"

            if col["type"] in ["numeric"] and not self.data.empty:
                col["check_sum"] = self.data[i].sum()
                if col["check_sum"]:
                    col["check_sum"] = round(col["check_sum"], 0)
            elif col["type"] in ["boolean"] and not self.data.empty:
                col["check_true"] = self.data[i].sum()
            if not self.data.empty:
                col["check_nn"] = len(self.data[i]) - self.data[i].isna().sum()
            columns[i] = col
        self.metadata["columns"] = columns

    def get_data(self):
        """Genera un Dataframe con la respuesta de la API"""
        self.data = None
        response = self.client.get(self.spreadsheet, self.data_range)
        table = []
        for i in response:
            row = dict()
            for j, k in enumerate(i):
                row['col' + str(j + 1)] = k
            table.append(row)

        self.data = pd.DataFrame(table)

        if self.model == dict():
            raise Exception('Modelo vacio')

        if not self.data.empty:

            for i in self.data.columns:
                if i not in self.model.keys():
                    print(self.data.columns)
                    raise Exception('Llave erronea: La columna "{}" no se encuentra dentro del modelo.'.format(i))
            for i in self.model:
                if self.model[i]['type'] == 'datetime64':
                    try:
                        self.data[i] = self.data[i].apply(convert_date_from_gsheet)
                    except ValueError as e:
                        print(i)
                        raise e
                elif self.model[i]['type'] == 'numeric':
                    try:
                        self.data[i] = self.data[i].apply(clean_numeric)
                        self.data[i] = pd.to_numeric(self.data[i])
                    except ValueError as e:
                        print(i)
                        raise e
                elif self.model[i]['type'] == 'str':
                    try:
                        self.data = self.data.astype({i: 'str'})
                        self.data[i] = self.data[i].apply(clean_str)
                    except ValueError as e:
                        print(i)
                        raise e
                elif self.model[i]['type'] == 'boolean':
                    try:
                        self.data = self.data.astype({i: 'str'})
                        self.data[i] = self.data[i].apply(clean_bool)
                    except ValueError as e:
                        print(i)
                        raise e
                else:
                    try:
                        self.data = self.data.astype({i: self.model[i]['type']})
                    except ValueError as e:
                        print(i)
                        raise e
            self.data.reset_index(drop=True, inplace=True)
        else:
            print('Sin datos')

