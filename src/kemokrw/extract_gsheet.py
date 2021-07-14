import os.path
#
import pandas as pd
from kemokrw.extract import Extract
from kemokrw.gsheet import *


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

    def __init__(self, jsonconfig, milog, dblog, metadata, data):
        self.jsonconfig = jsonconfig
        self.metadata = metadata
        self.data = data
        self.SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
        self.gsheet = None
        self.milog = milog
        self.dbloger = dblog

    def get_metadata(self):
        pass

    def get_data(self):
        pass


    def Verify(self):
        pass

    def tranfer(self):
        resultado, tipo, estado = {}, 'full', -1

        # automata de estados. esta inicial estado fallido -1
        estados = {"-1": 'Cant Access GoogleSheet service...',
                   "0": 'ok',
                   "9": 'Fail opening google sheet service ',
                   "8": "Fail reading hearder spreadsheets id",
                   "7": 'Fail reading values spreadsheets id ',
                   "6": "Fail data saving",
                   "5": 'no data to store'}

        spreadsheet_id = self.jsonconfig["spreadsheet_id"]

        # ----spreadsheet_id = '1FnB5kP3cbHuLhhPQCyRtaJALPYAONgMklRMhlSavK6s'
        try:
            self.gsheet = GSheet(g_object='spreadsheets',
                                 jsonconfig=self.jsonconfig,
                                 milog=self.milog,
                                 dblog=self.dbloger)
            self.gsheet.gauth()
            estado = 0
        except Exception as e:
            return {"Api Response": estados[str(9)], "Result": str(e)}
            # milog.error(estados[estado] + str(e))
            # estado = 9

        if estado != 9:
            # read headers, used to build json with extra fields
            sheetName = self.jsonconfig['sheet_name']
            cells = self.jsonconfig['header_cells']
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
                estado = 8
                milog.error(estados[estado] + str(spreadsheet_id)
                            + 'cells:' + str(cells) + str(e))

            if estado != 8:
                # load last_read_row, and read new_rows (if any)
                if tipo == 'full':
                    index = 2  # actulizas offset
                else:
                    index = self.gsheet.last_read_row()
                index = 2
                sheetName = self.jsonconfig['sheet_name']
                cells = self.jsonconfig['data_cells'] % index
                rango = sheetName + '!' + cells
                try:
                    values = self.gsheet.read_spreadsheet(rango)

                except Exception as e:
                    msg = ",  Contact Kemok Administrator"
                    valor = str(e).replace('\"', '').replace(":", ' ') \
                        .replace("%", '(porcentaje)')

                    return {"Api Response": estados[str(7)] + msg,
                            "Result": '7', "err": valor}
                    estado = 7
                    milog.error(estados[estado] + str(spreadsheet_id) +
                                'cells:' + str(cells) + ' ' + str(e))

                if estado != 7:
                    # convert to dictionary
                    df = pd.DataFrame(values)
                    # --print(df)
                    values, err = self.gsheet.prepare(values, headers)
                    if values != [] and err == {}:
                        x = self.dbloger.get_ServiceStatus()
                        if x:
                            if x == 'stoped':
                                return {"Api Response": "gsheet Service STOPED"}
                            elif x == 'started':
                                pass
                            else:
                                return {"Api Response": "g2sheet Service bad parameters maestro_de_gsheetdb"}

                        else:
                            return {"Api Response": "Fail Maestro g2sheet"}

                        salida = self.gsheet.store(values, tipo)
                        if salida == {}:
                            index += len(values)
                            index = self.gsheet.last_read_row(index)
                            resultado = {'registros': str(len(values))}
                        else:
                            estado = 6
                            milog.error("Api Response:estado 6" + str(salida))
                            msg = ",  Contact Kemok Administrator"
                            return {"Api Response": estados[str(estado)] + msg,
                                    "Result": resultado, 'info:': salida}

                    else:
                        return {"Api Response": "dont have data to save",
                                "Err": err}

        return {"Api Response": estados[str(estado)], "Result": resultado}



