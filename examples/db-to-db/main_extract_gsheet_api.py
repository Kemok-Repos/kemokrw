'''
 Uilza la clase la libreria kemokrw especificamente con la clase extradt_gsheet.
 se utiliza acopla al marco de ejecución de la api y se prueba mediante gsheet.
 '''


from flask import Flask, jsonify, request
from kemokrw.extract_gsheet import *
from kemokrw.dblog import *
import logging
import json
import traceback
import sys
import os
from glob import glob
import uuid
import re

# import passboltapi
def ls4(path, filtro=""):
    spath = path + filtro
    return glob(spath)

x = 5000
sys.setrecursionlimit(x)

app = Flask(__name__)
app.config['Ambiente'] = 'debug'
#app.config['Ambiente'] = 'production'

def DbConections(dbcode):
    dbConection = []
    pwd_gsheet = '7ba40379e4597bf535dfa79f9c45b60a'
    pwd_gsheet2 ='jbfdbi%%ms.$2773lnnwn'

    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@50.116.33.86/panamacompra')
    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@45.56.113.157/guatecompras')
    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@45.79.204.111/bago')
    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@45.79.204.111/bago_caricam')
    dbConection.append('postgresql://notificaciones_marketing:'
                       '7ba40379e4597bf535dfa79f9c45b60a'
                       '@192.155.95.216/notificaciones_marketing')
    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@bacgt.cg9u5bhsoxjc.us-east-1.rds.amazonaws.com/bacgt')
    dbConection.append('postgresql://g2sheets:' + pwd_gsheet2
                       + '@172.105.156.208/aquasistemas')
    dbConection.append('postgresql://forge:68qCIg1PMdOHOkC09qsE@96.126.123.195:5432/expolandivar2021')
    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@45.79.216.118/srtendero')

    dbConection.append('postgresql://g2sheets:' + pwd_gsheet
                       + '@45.79.9.70/guatecompras2')



    # db pruebas
    dbConection.append('postgresql://admin:admin@localhost:5433/panamacompra')
    dbConection.append('postgresql://admin:admin@localhost:5433/guatecompras')
    dbConection.append('postgresql://admin:admin@localhost:5433/bago')
    dbConection.append('postgresql://admin:admin@localhost:5433/bago_caricam')
    dbConection.append('postgresql://notificaciones_marketing:'
                       '7ba40379e4597bf535dfa79f9c45b60a'
                       '@192.155.95.216/notificaciones_marketing')

    return dbConection[dbcode]


def ValidJsonConfig(jsonconfig,milog):
    keys = ["microservice_id", "api_type", "spreadsheet_id", "sheet_name",
            "header_cells", "data_cells", "model_name", "model",
            "map_sheet_model", "formats"]
    '''"microservice_id", "api_type" ,"file_gsheet_credential",
    "file_gsheet_pickle_token", "spreadsheet_id", "header_cells", "data_cells",
    "db_connection", "model", "map_sheet_model"
    '''
    valid = 0
    falta = ''
    for key in keys:
        if key not in jsonconfig:
            milog.error('No existe: ' + key + ' ')
            falta = falta + ',' + key
            valid = valid + 1
    if valid > 0:
        milog.error('Fail' + str(valid) + ' not foud key:, :' + falta)
        return {"Api Response": "Fail json request configurations:" + falta +
                                ", Contact Kemok Administrator"}
    else:
        valid = {}

    return valid


def cargarJsonConfig(archivo, milog):
    microservicio = 'falla ... configuracion'

    if not CheckPathFile(archivo, 'archivo configuracion microservconf.json',
                         milog):
        exit(0)
    else:
        milog.info('Encontrado archivo ' + archivo)
    try:
        with open(archivo) as informacion:
            microservicio = json.load(informacion)
    except Exception as e:
        print('Falla Carga archivo json' + archivo +
              ' metodo cargarJsonConfig')
        milog.error('Falla lectura ...archivo json.. '
                    + archivo + ' ' + str(e))
        print(traceback.format_exc())
        exit(0)

    # -- validar claves microservicio
    ValidJsonConfig(microservicio, milog)

    return microservicio


def CheckPathFile(file_, nombre, milog):
    sol = True
    try:
        with open(file_) as file:
            keyfile = file.read()
    except Exception as e:
        print('Falla lectura archivo ' + nombre + ' no se puede leer:' + file_)
        milog.error(
            'Falla lectura archivo metodo CheckPathFile ' + nombre +
            ' no se puede leer:' + file_ + '... ' + str(e))
        msg = ls4('os.path.dirname(__file__))')
        milog.error('directorio actual....:' + str(msg) + '-->' +
                    str(os.path.dirname(__file__)))
        sol = False
    return sol


def generarCalculoVelocidad():
    for num in range(100, 4900):
        print(str(num) + ':')
        fact = (lambda x: 1 if x == 0 else x * fact(x - 1))
        valor = fact(num)
        print(valor)
        print('Cifras:' + str(len(str(valor))))


def configLogger():
    # Create a custom logger
    logger = logging.getLogger('komokSErver')
    logger.setLevel(logging.INFO)
    gestorLogInfo = logging.FileHandler('/var/log/KemokMicroserviceServer.log')
    gestorLogInfo.setLevel(logging.INFO)
    format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - '
                               '%(message)s')
    gestorLogInfo.setFormatter(format)
    logger.addHandler(gestorLogInfo)
    return logger


def validarTipo(tipo: str):
    falla_parametro = False
    tipos = ["new_only", "upsert", "full"]
    if tipo.strip().lower() not in tipos:
        falla_parametro = True

    return falla_parametro

def ConectLog(tipo):

    if tipo == 'production':
        dbloger = DbLog(uuid.uuid1().hex, "postgresql://katadmin:$q%$=0#WyCI1.@45.56.117.5:5432/katdb")
    else:
        dbloger = DbLog(uuid.uuid1().hex, "postgresql://admin:admin@localhost:5433/etl")

    return dbloger


@app.errorhandler(404)
def page_not_found():
    return jsonify({"Api response":"ok", "response": "Kemokrw extract_gsheet service endpoint not implemented"})

@app.route("/", methods=['POST', 'GET'])
def selector_microservice():
    a = datetime.datetime.now()
    status = True
    milog = configLogger()
    dbloger = ConectLog(app.config["Ambiente"])
    jsonconfig = {}

     # continue end point
    if request.json:  # valido para post jsonconfig
        jsonconfig = request.json
        k = ValidJsonConfig(jsonconfig, milog)
        # validar
        if k == {}:
            # validating service start to continue end point
            x = dbloger.get_ServiceStatus()
            if x:
                if x == 'stoped':
                    dbloger.logger(str(jsonconfig["microservice_id"]), 'info', 'info', {"Api response": x})
                    return {"Api Response": "Kemokrw extract_gsheet Service is STOPPED"}
                elif x == 'started':
                    pass  # continue excecute endpoint
                else:
                    return {"Api Response": "Kemokrw extract_gsheet Service bat parameters maestro_de_gsheetdb"}

            else:
                return {"Api Response": "Fail Maestro g2sheet"}

            dbloger.logger('g2sheet:' + str(jsonconfig["microservice_id"]),
                           'info', 'starting', {})
            tipo = jsonconfig["api_type"]
            milog.info('microservicio :' + jsonconfig["microservice_id"])
            milog.info('Parametro de ejecucion ' + tipo)
            milog.info('ejecutando script extraccion Kemokrw extract_gsheet:' +
                       jsonconfig["microservice_id"] + '...')
            print('ejecutando script extraccion gsheet:' +
                  jsonconfig["microservice_id"])
            spreadsheet_id, sheet, model = \
                jsonconfig["spreadsheet_id"], jsonconfig['sheet_name'], \
                jsonconfig["model"]


            model = {}
            model["db"], model["table"], \
            model["fields"], \
            model["map_sheet_model"], model["formats"] = DbConections(int(jsonconfig["target"])), \
                                      jsonconfig["model_name"], \
                                      jsonconfig["model"],\
                                      jsonconfig["map_sheet_model"],  jsonconfig["formats"]

            gsheet = ExtractGSheet(spreadsheet_id=spreadsheet_id,
                                   sheet=jsonconfig["sheet_name"],
                                   header=jsonconfig["header_cells"],
                                   model=model)
            k = gsheet.tranfer() \
                if tipo in ["new_only", "full"] else status
        else:
            dbloger.logger(str(jsonconfig["microservice_id"]), 'info',
                           'error', k)

    else:
        # valido para get jsonconfig en el servidor
        milog.error('Falta parametro configuracion')
        k = {"Api Response": "Not found Configuration Parameters, "
                             "Contact Kemok Administrator"}

    b = datetime.datetime.now()
    tiempo = b - a
    k["Time"] = {'Elapsed': str(round(tiempo.total_seconds(), 4)) + 'seg'}
    if 'Api Response' in k:
        print(k)
        if k["Api Response"] == 'ok':
            dbloger.logger(str(jsonconfig["microservice_id"]), 'info',
                           'success', k)
        else:
            dbloger.logger(str(jsonconfig["microservice_id"]), 'info',
                           'error', k)
    # return jsonify(k)

    return jsonify({"Server": k, ".microservice_id":
                                 str(jsonconfig["microservice_id"])})

if __name__ == '__main__':

    dbloger = ConectLog(app.config["Ambiente"])
    milog = None
    try:
        milog = configLogger()
        r = {}
        dbloger.logger('Kemokrw server ', 'info', 'starting', r)
    except Exception as e:
        traceback.print_exc()
        milog.error('Parametro de ejecucion ' + str(e))
        print('error escribiendo archivo /var/log/KemokMicroserviceServer.log')
        exit(0)

    try:
        app.run(host="0.0.0.0", debug=False)
    except Exception as e:
        dbloger.logger('Kemokrw extract_gsheet', 'err', 'start', {'err': str(e)})

    # 1.- sin firmar
    # app.run(host="0.0.0.0", debug=False,ssl_context='adhoc')
    # 2.- auto firmado
    # app.run(host="0.0.0.0", debug=False,
    #         ssl_context = ('cert.pem', 'key.pem'))
