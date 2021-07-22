from kemokrw.load import Load
from kemokrw.gsheet import *
from kemokrw.func_db import *

class LoadGSheet(Load):
    """"Clase LoadGSheet implementación de la clase Load.

    Cumple la función de cargar información a una hoja de Google Sheets.

    Atributos
    ---------
    spreadsheet_id : str
    sheet : str
    model : dict
    metadata : dict

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla en Google Sheets.
    save_data():
        Almacena la data de un pandas.DataFrame Object una tabla en Google Sheets.
    """
    def __init__(self, spreadsheet_id, sheet, header, model, condition, order):
        self.spreadsheet_id = spreadsheet_id
        self.sheet = sheet
        self.header = header
        self.model = model
        self.gsheet = None
        self.scope = ['https://www.googleapis.com/auth/spreadsheets','https://www.googleapis.com/auth/drive']
        self.condition = condition
        self.order = order
        self.connection = create_engine(model["db"])
        # ['https://www.googleapis.com/auth/drive']
        try:
            self.gsheet = GSheet(g_object='sheets',
                                 spreadsheet_id=self.spreadsheet_id,
                                 sheet=self.sheet,
                                 scope=self.scope,
                                 model=self.model)

            self.gsheet.gauth()
            self.MaxRowId = self.gsheet.read_countRow(self.sheet + '!' + self.header)
            print(self.MaxRowId)
            estado = 0

        except Exception as e:
            print(str(e))

    def get_metadata(self):
        self.metadata = get_db_metadata(self.model["db"], self.dbms, self.model, self.table, self.condition, self.key)

    def get_data(self):
        """Método  para extraer data"""
        j = []
        for i in self.model["model"]:
            j.append("{0} AS {1}".format(self.model["model"][i]["name"], i))
        columns = ", ".join(j)
        query =config.TABLE_CHECK["check_rows"].format(table=self.model["table"],condition=self.condition)

        self.rows = self.connection.execute(query).fetchone()[0]

        query = config.TABLE_QUERY.format(columns=columns, table=self.model["table"],
                                          condition=self.condition, order=self.order)

        self.data = pd.read_sql(sql=query, con=self.connection)
        pd.set_option('display.max_rows', None)
        print(self.data)


    def save_data(self):
        self.get_data()
        rango = self.sheet+'!'+self.header[:-1] + str(self.rows)
        self.gsheet.update_sheet(self.data, rango)


