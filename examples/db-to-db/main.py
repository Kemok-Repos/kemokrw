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

# import passboltapi
def ls4(path, filtro=""):
    spath = path + filtro
    return glob(spath)

x = 5000
sys.setrecursionlimit(x)

app = Flask(__name__)
app.config['Ambiente'] = 'debug'
#app.config['Ambiente'] = 'production'


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

# UpdateMaestroGsheet
@app.route("/start", methods=['POST', 'GET'])
def start():
    dbloger = None
    try:
        milog = configLogger()
        dbloger = ConectLog(app.config["Ambiente"])
        dbloger.UpdateMaestroGsheet('%', 'started')
        dbloger.logger('manager Kemokrw extract_gsheet', 'info', 'info',
                       {"Api response": "Kemokrw extract_gsheet service was STARTED"})

        return jsonify({"Api response":"OK", "response":"Kemokrw extract_gsheet Service was STARTED"})
    except Exception as excep:
        dbloger.logger('manager Kemokrw extract_gsheet', 'info', 'error', {"Api response": str(excep)})
        return jsonify({"Api response": "OK", "response": str(excep)})

@app.route("/stop", methods=['POST', 'GET'])
def stop():

    dbloger = None
    try:
        status = True
        milog = configLogger()
        dbloger = ConectLog(app.config["Ambiente"])
        dbloger.UpdateMaestroGsheet('%%', 'stoped')
        dbloger.logger('manager Kemokrw extract_gsheet', 'info', 'info',
                       {"Api response": "Kemokrw extract_gsheet service was STOPPED"})

        return jsonify({"Api response": "OK", "response": "Kemokrw extract_gsheet Service was STOPPED"})
    except Exception as excep:
        dbloger.logger('manager Kemokrw extract_gsheet', 'info', 'error', {"Api response": str(excep)})
        return jsonify({"Api response": "fail", "response": str(excep)})

@app.route("/status", methods=['POST', 'GET'])
def status():
    status = True
    milog = configLogger()
    dbloger = ConectLog(app.config["Ambiente"])
    valor = dbloger.get_ServiceStatus()
    dbloger.logger('', 'info', 'info', {"Api response": "get status Kemokrw extract_gsheet ={}".format(valor)})
    return jsonify({"Api response": valor})


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

            metadata = ''
            data = ''
            gsheet = ExtractGSheet(jsonconfig=jsonconfig,
                                   metadata=metadata,
                                   data=data)

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


@app.route("/test")
def test_get():
    return "Test nice... "


@app.route("/test_json", methods=['POST'])
def test_post():
    return jsonify(request.json)

if __name__ == '__main__':

    #dbloger = ConectLog('debug')
    #dbloger = ConectLog('production')
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
