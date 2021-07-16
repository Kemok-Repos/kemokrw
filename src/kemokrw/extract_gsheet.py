import os.path
#
from sqlalchemy import create_engine
import pandas as pd
from kemokrw.extract import Extract
from kemokrw.gsheet import *
import re


class ExtractGSheet(Extract):
    """Clase ExtractGSheet implementación de la clase Extract.

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
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self.gsheet = None

    def get_metadata(self):
        pass

    def get_data(self):
        pass

    def Verify(self):
        pass

    def tranfer(self):
        resultado, tipo, estado = {}, 'full', -1

        estado = 0
        # automata de estados. esta inicial estado fallido -1
        estados = {"-1": 'Cant Access GoogleSheet service...',
                   "0": 'ok',
                   "9": 'Fail opening google sheet service ',
                   "8": "Fail reading hearder spreadsheets id",
                   "7": 'Fail reading values spreadsheets id ',
                   "6": "Fail data saving",
                   "5": 'no data to store'}

        try:
            self.gsheet = GSheet(g_object='spreadsheets',
                                 spreadsheet_id=self.spreadsheet_id,
                                 sheet=self.sheet,
                                 model=self.model)

            self.gsheet.gauth()

        except Exception as e:
            return {"Api Response": estados[str(9)], "Result": str(e)}

        if estado != 9:
            # read headers, used to build json with extra fields
            sheetName = self.sheet
            cells = self.header
            rango = sheetName + '!' + cells
            print(rango)
            try:
                headers = self.gsheet.read_spreadsheet(rango)[0]

            except Exception as e:
                print(e)
                valor = str(e).replace("\'", '').replace('\"', '') \
                    .replace(":", ' ').replace("%", '(porcentaje)')
                msg = ",  Contact Kemok Administrator"
                return {"Api Response": estados[str(8)] + msg,
                        "Result": "8", "Err": valor}


            if estado != 8:
                # load last_read_row, and read new_rows (if any)
                if tipo == 'full':
                    index = 2  # actulizas offset
                else:
                    index = self.gsheet.last_read_row()
                index = 2
                cells = re.sub('[0-9]:', '{}:'.format(str(index)), self.header)
                rango = self.sheet + '!'+cells+'99999'
                try:
                    values = self.gsheet.read_spreadsheet(rango)

                except Exception as e:
                    msg = ",  Contact Kemok Administrator"
                    valor = str(e).replace('\"', '').replace(":", ' ') \
                        .replace("%", '(porcentaje)')

                    return {"Api Response": estados[str(7)] + msg,
                            "Result": '7', "err": valor}


                if estado != 7:
                    # convert to dictionary
                    df = pd.DataFrame(values)
                    # --print(df)
                    values, err = self.gsheet.prepare(values, headers)
                    if values != [] and err == {}:

                        salida = self.gsheet.store_db(values, tipo)
                        if salida == {}:
                            index += len(values)
                            index = self.gsheet.last_read_row(index)
                            resultado = {'registros': str(len(values))}
                        else:
                            estado = 6
                            msg = ",  Contact Kemok Administrator"
                            return {"Api Response": estados[str(estado)] + msg,
                                    "Result": resultado, 'info:': salida}

                    else:
                        return {"Api Response": "dont have data to save",
                                "Err": err}

        return {"Api Response": estados[str(estado)], "Result": resultado}



