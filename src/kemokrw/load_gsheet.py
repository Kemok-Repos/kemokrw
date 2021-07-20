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
    def __init__(self, spreadsheet_id, sheet, scope, model):
        self.spreadsheet_id = spreadsheet_id
        self.sheet = sheet
        self.model = model
        self.gsheet = None
        self.scope = scope
        # ['https://www.googleapis.com/auth/drive']

        try:
            self.gsheet = GSheet(g_object='sheets',
                                 spreadsheet_id=self.spreadsheet_id,
                                 sheet=self.sheet,
                                 model=self.model)

            self.gsheet.gauth()
            estado = 0
        except Exception as e:
            return {"Api Response": 'fail', "Result": str(e)}

    def get_metadata(self):
        self.metadata = get_db_metadata(self.model["db"], self.dbms, self.model, self.table, self.condition, self.key)

    def get_data(self):
        pass

    def update_sheet(self, data):
        self.gsheet.update_sheet(data)
        pass


