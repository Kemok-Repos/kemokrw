from sqlalchemy import create_engine
import json
import os.path
import pickle
import datetime
from glob import glob
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from yaml import load, Loader
import pprint

class GSheet():

    def __init__(self, g_object, spreadsheet_id, sheet, scope, model):
        self.g_object = g_object
        self.model = model
        self.spreadsheet_id = spreadsheet_id
        self.sheet = sheet
        self.scope = scope

    def ls4(self, path, filtro=""):
        spath = path + filtro
        return glob(spath)

    def gauth(self):
        """Connect to google API and get a `g_object`, e.g., spreadsheets."""
        # auth
        creds = None
        file_gsheet_token = os.path.dirname(__file__) + '/' + "token.pickle"
        try:
            if os.path.exists(file_gsheet_token):
                with open(file_gsheet_token, 'rb') as token:
                    creds = pickle.load(token)
        except Exception as e:
            msg = self.ls4('os.path.dirname(__file__))')
            print(str(e))

        if not creds or not creds.valid:
            if creds and creds.expired and creds.refresh_token:
                try:
                    creds.refresh(Request())
                except Exception as e:
                    valor = str(e).replace('\"', '').replace(":", ' ') \
                        .replace("%", '(porcentaje)')
                    print('Fail credential refresh: ' + valor)
                    return False
            else:
                try:
                    file_gsheet_credential = os.path.dirname(__file__) + \
                                             '/' + 'credentials_local.json'
                    flow = InstalledAppFlow.from_client_secrets_file(
                        file_gsheet_credential, self.scope)
                except Exception as e:
                    print(
                        'Fail  InstalledAppFlow file_gsheet_credential, install google-cloud-storage: ' +
                        str(file_gsheet_credential) + '... ' + str(e))

                try:
                    creds = flow.run_local_server(port=0)
                except Exception as e:
                    print('Fail flow.run_local_server  : ' + str(e))

            # Save the credentials for the next run
            try:
                with open(file_gsheet_token, 'wb') as token:
                    pickle.dump(creds, token)
            except Exception as e:
                print('Fail save Token ' + file_gsheet_token + ' ' + str(e))

        # build service
        print('build...')
        service = build('sheets', 'v4', credentials=creds)

        if hasattr(service, self.g_object):
            self.ga_ss = getattr(service, self.g_object)()
        else:
            self.ga_ss = service


    def last_read_row(self, index: int = 0):
        """Reads and stores last read row."""
        mode = 'w+' if index else 'r+'
        try:
            with open(os.path.dirname(__file__) + '/last_read_row', mode) as f:
                if index:
                    f.write(str(index))
                else:
                    index = int(f.read() or '0')
        except Exception:
            return 0
        return index

    def read_spreadsheet(self, range):
        """Simple method to read from google and return as list of lists."""
        result = self.ga_ss.values().get(spreadsheetId=self.spreadsheet_id, range=range).execute()
        print(range)
        values = result.get('values', [])
        return values

    def read_countRow(self, range):
        """Simple method to read from google and return as list of lists."""

        vrange = range[:-1]
        rows = self.ga_ss.values().\
            get(spreadsheetId=self.spreadsheet_id, range=vrange).execute().get('values', [])

        last_row = rows[-1] if rows else None
        last_row_id = len(rows)
        print(last_row_id, last_row)
        return last_row_id

    def prepare(self, values, headers,  format='%d/%m/%Y %H:%M:%S'):
        """Prepare values to ensure those can be stored in DB with sqlalchemy."""

        def escape(value, replace: dict = {'%': '%%', '\'': '\'\''}):
            escaped = str(value)
            for i, j in replace.items():
                escaped = escaped.replace(i, j)
            return escaped

        def SpanishMonthFormat(strFecha: str, format: str):
            x = format.find("%B")
            if x >= 0:
                strFecha = strFecha.lower()
                strFecha = strFecha.replace('enero', 'January').replace('febrero', 'February'). \
                    replace('marzo', 'March').replace('abril', 'April'). \
                    replace('mayo', 'May').replace('junio', 'June'). \
                    replace('julio', 'July').replace('agosto', 'August'). \
                    replace('septiembre', 'September').replace('octubre', 'October'). \
                    replace('noviembre', 'November').replace('diciembre', 'December')

            return strFecha

        prepared2 = []
        # preparanda datos
        mapping = {}
        fields ={}
        for i in self.model["model"]:
            mapping[self.model["model"][i]["name"]] = self.model["model"][i]["map_sheet"]
            if str(self.model["model"][i]["type"]).find('datetime') != -1 or \
                    str(self.model["model"][i]["type"]).find('timestamp') != -1:
                fields[self.model["model"][i]["name"]] = {"type":self.model["model"][i]["type"],
                                                          "format":self.model["model"][i]["format"]}
            else:
                fields[self.model["model"][i]["name"]] = {"type": self.model["model"][i]["type"],
                                                          "format":''}

        #
        # columnas_map = self.model['map_sheet_model'].keys()
        columnas_map = mapping.values()

        for i in values:
            dictValor = {}
            column = ['a1', 'b1', 'c1', 'd1', 'e1', 'f1', 'g1', 'h1',
                      'i1', 'j1', 'k1', 'l1', 'm1', 'n1', 'o1', 'p1',
                      'q1', 'r1', 's1', 't1', 'v1', 'u1', 'w1', 'x1', 'y1', 'z1']
            for xcol in columnas_map:
                if xcol not in column:
                    return [], \
                           {"State Api Response": "Error bad column " +
                                                  str(columnas_map[xcol])}
            col = -1
            for key in mapping:
                col = col + 1
                while column[col] not in mapping.values():
                    x, col = i.pop(0), col + 1

                if column[col] in mapping.values():
                    if str(fields[key]["type"]).lower().find('datetime') != -1 or \
                            str(fields[key]["type"]).lower().find('timestamp') != -1:
                        if i:
                            try:
                                format = fields[key]["format"]
                                val = str(i.pop(0))
                                if val != '':
                                    strdate = SpanishMonthFormat(val, format)
                                    dictValor[key] = datetime.datetime. \
                                        strptime(strdate, format)
                                else:
                                    dictValor[key] = None

                            except Exception as e:
                                stre = str(e).replace("'", "").replace('"', '')
                                print(str(e))
                                return [], \
                                       {"State Api Response": "Fail,  "
                                                              "Contact "
                                                              "Kemoks "
                                                              "Administrator -  datetime value without expected correct format",
                                        "detail": stre[:18]}
                        else:
                            dictValor[key] = None

                    elif str(fields[key]).lower().find('json') != -1:
                        dictValor[key] = escape(json.dumps(
                            {headers[j]: i[j] for j in range(len(i))}))

                    elif str(fields[key]["type"]).lower().find('numeric') != -1 or \
                            str(fields[key]["type"]).lower().find('money') != -1:
                        if i:
                            valor = escape(i.pop(0))
                            dictValor[key] = float(valor.replace(',', '').replace('%', '').replace('$', '').replace('Q', ''))
                        else:
                            dictValor[key] = None

                    elif str(fields[key]["type"]).lower().find('int') != -1:
                        if i:
                            valor = escape(i.pop(0))
                            if valor != '':
                                dictValor[key] = int(valor.replace('.', '').replace('%', '').replace('$', '').replace('Q', ''))
                            else:
                                dictValor[key] = None
                        else:
                            dictValor[key] = None
                    else:
                        if i:
                            dictValor[key] = escape(i.pop(0))
                        else:
                            dictValor[key] = None

            prepared2.append(dictValor)

        return prepared2, {}

    def store_db(self, rows, tipo):
        # load settings
        file_sql_command = os.path.dirname(__file__) + '/settings.yaml'

        try:
            with open(file_sql_command) as file:
                settings = load(file, Loader)
        except Exception as e:
            print('Falla de lectura archivo sql_command yaml' +
                        str(file_sql_command) + ' Exception:' + str(e))
            return {'err': "store 001", 'detail': str(e)}

        db = self.model["db"]

        try:
            engine = create_engine(db)
            conn = engine.connect()

        except Exception as e:
            print('Falla de inicio conexion db:' + str(db) +
                        ' Exception:' + str(e))
            valor = str(e).replace('\"', '').replace(":", ' ') \
                .replace("%", '(porcentaje)')
            return {"err": "store 002", "detail": str(valor)}

        try:
            trans = conn.begin()

        except Exception as e:
            valor = str(e).replace('\"', '').replace(":", ' ') \
                .replace("%", '(porcentaje)')
            print("Falla de inicio conexion db:" + str(db)
                        + " Exception:" + str(valor))
            return {"err": "store 003", "detail": valor}

        fail = {}
        if len(rows) > 0:
            if tipo == 'full':
                try:
                    tablas = engine.table_names()
                    if self.model["table"] in tablas:
                        sql_delete = settings['delete'] % self.model["table"]
                        conn.execute(sql_delete)
                        # ---trans.commit()
                    else:
                        print('Falla tabla ' + self.model["table"] +
                                    ' no encontrada:' + str(db))
                        valor = 'tabla ' + str(self.model["table"]) + \
                                ' No exist'
                        return {"err": "store 004", "detail": valor}

                except Exception as e:
                    x = str(e)
                    print('Falla eliminando registro tabla db:' +
                                str(db) + ' Exception:' + x)
                    valor = x.replace('\"', '').replace(":", ' ') \
                        .replace("%", '(porcentaje)')
                    return {"err": "store 005", "detail": valor}

            try:  # inserting records
                # trans = conn.begin()
                fields = '('
                settings['insert2'] = "("
                for field in self.model["map_sheet_model"]:
                    fields = fields + self.model["map_sheet_model"][field] + ','
                    settings['insert2'] = \
                        settings['insert2'] + "'%(" \
                        + self.model["map_sheet_model"][field] + ")s',"

                settings['insert2'] = settings['insert2'][:-1] + ')'
                fields = fields[:-1] + ')'

                sql_insert = 'insert into ' + self.model["table"] + fields
                sql_insert = sql_insert + ' values %s' % ',\n' \
                    .join([settings['insert2'] % row for row in rows])
                # print(sql_insert)
                sql_insert = sql_insert.replace("\'\'", 'null')
                sql_insert = sql_insert.replace("\'None\'", 'null')
                # print(sql_insert)
                conn.execute(sql_insert)
                trans.commit()
            except Exception as e:
                print('Falla ejecutando sql:' + str(sql_insert) +
                            ' Exception:' + str(e))
                x = str(e)
                i = x.find('[SQL')
                x = x[:i] + '...'
                valor = str(x).replace('\"', '').replace(":", ' ') \
                    .replace("%", '(porcentaje)').replace("\'", '')
                trans.rollback()
                return {"err": "store 006", "detail": str(valor)}

        else:
            return {"detail": "dont have data to save"}


        conn.close()
        return fail

    def update_sheet(self, data):
        self.gauth('sheets')
        #spreadsheet_id = '1ODUzm3WXzz9DItY0nXMkrqG0HLgtOLOi2BEtdCrkhIo'  # TODO: Update placeholder value.
        #spreadsheet_id = self.jsonconfig["spreadsheet_id"]
        # The A1 notation of the values to update.
        #range_ = 'primera!A1:E2'  # TODO: Update placeholder value.

        #sheet_name = self.jsonconfig['sheet_name']
        # How the input data should be interpreted.
        value_input_option = 'RAW'  # TODO: Update placeholder value.
        #values = [[500, 400, 300, 200, 100, ], [500, 400, 300, 200, 100, ], ]
        Body = {'values': data, }
        value_range_body = {}
        request = self.ga_ss.spreadsheets().values().update(spreadsheetId=self.spreadsheet_id,
                                                            range=range,
                                                            valueInputOption=value_input_option,
                                                            body=Body)
        response = request.execute()

        # TODO: Change code below to process the `response` dict:
        pprint(response)


