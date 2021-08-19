import kemokrw.config_api as config
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError
from datetime import datetime, date, timezone, timedelta
from pprint import pprint
import pandas as pd
import numpy as np
import json
import unidecode


def date_range(params):
    """ Vuelve una fecha, un rango de timestamps. """
    if 'date' in params.keys():
        start_date = date.fromisoformat(params['date'])
        start_date = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
        end_date = date.fromisoformat(params['date']) + timedelta(days=1)
        end_date = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc)
        params['time_slot[start]'] = start_date.isoformat()
        params['time_slot[stop]'] = end_date.isoformat()
        params.pop('date')
    return params


def extract_metadata(model, data):
    """ Obtiene la metadata de un dataframe de pandas"""
    metadata = dict()
    # Calcula el número de columnas
    metadata["ncols"] = len(model)

    # Calcula el número de filas
    if not data.empty:
        metadata["check_rows"] = len(data)
    else:
        metadata["check_rows"] = 0

    # Revisa las comprobaciones de cada columna
    columns = dict()
    for i in model:
        col = dict()
        # Obtiene el tipo normalizado de columna que es
        col["subtype"] = model[i]["type"]
        col_type = col["subtype"].upper()
        col_type = ''.join(e for e in col_type if e.isalpha() or e.isspace() or e == '[' or e == ']')
        for j in config.COLUMN_TYPES:
            if col_type in config.COLUMN_TYPES[j]:
                col["type"] = j
        if "type" not in col.keys():
            print("*WARNING*: {} no es un tipo identificado.".format(col["subtype"]))
            col["type"] = "other"
        # Comprueba la suma de las columnas numericas
        if col["type"] in ["numeric"] and not data.empty:
            col["check_sum"] = round(data[i].sum(), 0)
        # Comprueba la suma de los calores verdaderos en columnas booleanas
        elif col["type"] in ["boolean"] and not data.empty:
            col["check_true"] = data[i].sum()
        # Comprueba la suma de la cantidad de valores nulos de cada columna
        if not data.empty:
            col["check_nn"] = len(data[i]) - data[i].isna().sum()
        columns[i] = col
    metadata["columns"] = columns
    return metadata


def clean_str(x):
    """ Limpia de nulos cualquier string. """
    if x.lower() in ['#n/a', '#n/a n/a', '#na', '-1.#ind', '-1.#qnan', '-nan', '1.#ind', '1.#qnan', '<na>', 'n/a', 'na',
                     'null', 'nan', '<n/a>', '<null>', '<nan>']:
        return None
    if x.strip() in ['']:
        return None
    else:
        return x


def clean_bool(x):
    """ Convierte cualquier string a boolean. """
    x = x.lower()
    x = x.strip()
    x = unidecode.unidecode(x)
    if x in ['si', 'yes', 'true', 'verdadero', 'activo', 'afirmativo', 'positivo', '1']:
        return True
    elif x in ['no', 'false', 'falso', 'inactivo', 'negativo', '0']:
        return False
    else:
        return None


def clean_numeric(x):
    """ Convierte cualquier string a numeric limpiando cualquier formato. """
    if isinstance(x, str):
        x = ''.join(e for e in x if e.isnumeric() or e == '.')
    return x


def clean_json(x):
    """ Convierte un diccionario en un JSON serializado y limpia los nulos. """
    x = json.dumps(x)
    if x.lower() in ['nan', 'null']:
        return None
    else:
        return x


def suggest_model(data):
    """ Función que sugiere un módelo en base a un dataframe. """
    suggested_model = dict()
    for i, j in enumerate(data.columns):
        suggested_model['col' + str(i + 1)] = dict()
        suggested_model['col' + str(i + 1)]['name'] = j
        if data[j].dtypes == object:
            suggested_model['col' + str(i + 1)]['type'] = 'str'
        else:
            suggested_model['col' + str(i + 1)]['type'] = str(data[j].dtypes)
    pprint(suggested_model)


def match_model(model, data, api=True):
    """ Función que adapta un dataframe a un módelo. """
    if model == dict():
        print('Sin modelo definido. Sugerimos el siguiente:')
        suggest_model(data)
        raise Exception('Modelo vacio')

    if not data.empty:
        # Asignación del nombre de las columnas
        column_names = dict()
        for i in model:
            column_names[model[i]['name']] = i
        data.rename(column_names, axis=1, inplace=True)

        # Agregar columnas faltantes en los datos extraídos.
        for i in model:
            if i not in data.columns:
                data[i] = np.NaN
                print(model[i]['name']+' not found. Adding artificially')

        # Identificar errores de columnas faltantes en el modelo.
        for i in data.columns:
            if i not in model.keys() and not api:
                data.drop(columns=i, inplace=True)
            elif i not in model.keys() and api:
                print(data.columns)
                raise Exception('Llave erronea: La columna "{}" no se encuentra dentro del modelo.'.format(i))

        # Manejo de tipo de datos
        for i in model:
            model_type = model[i]['type'].upper()
            model_type = ''.join(e for e in model_type if e.isalpha() or e.isspace() or e == '[' or e == ']')
            if model_type in config.COLUMN_TYPES['datetime'] and api:
                try:
                    data[i] = pd.to_datetime(data[i])
                except ValueError as e:
                    print(i)
                    raise e
            elif model_type in config.COLUMN_TYPES['datetime'] and not api:
                try:
                    data[i] = data[i].apply(convert_date_from_gsheet)
                except ValueError as e:
                    print(i)
                    raise e
            elif model_type in config.COLUMN_TYPES['numeric']:
                try:
                    data[i] = data[i].apply(clean_numeric)
                    data[i] = pd.to_numeric(data[i])
                except ValueError as e:
                    print(i)
                    raise e
            elif model_type in config.COLUMN_TYPES['text']:
                try:
                    data = data.astype({i: 'str'})
                    data[i] = data[i].apply(clean_str)
                except ValueError as e:
                    print(i)
                    raise e
            elif model_type in ['DICT', 'JSON', 'JSONB']:
                try:
                    data = data.astype({i: 'object'})
                    data[i] = data[i].apply(clean_json)
                except ValueError as e:
                    print(i)
                    raise e
            elif model_type in config.COLUMN_TYPES['boolean'] and not api:
                try:
                    data = data.astype({i: 'str'})
                    data[i] = data[i].apply(clean_bool)
                except ValueError as e:
                    print(i)
                    raise e
            else:
                try:
                    data = data.astype({i: model[i]['type']})
                except ValueError as e:
                    print(i)
                    raise e
        data.reset_index(drop=True, inplace=True)
    else:
        print('Sin datos')
    return data


def convert_date_from_gsheet(x):
    """ Convierte la fecha de el formato de Google Sheet a un objeto datetime. """
    if x:
        date_part = datetime(1899, 12, 30) + timedelta(days=int(x))
        time_part = (x - int(x))*86400
        timestamp = date_part + timedelta(seconds=int(time_part))
        return timestamp
    else:
        return None


def query_model_from_db(db, model_id):
    """ Obtiene la configuración y modelo de la tabla maestro. """
    engine = create_engine(db)
    attempts = 0
    while attempts < 3:
        try:
            connection = engine.connect()
            config_query = connection.execute(config.QUERY_MODEL.format(str(model_id)))
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
    model_config = dict()
    for i in config_query:
        model_config = i[0]
    if model_config != dict():
        return model_config
    else:
        raise Exception('Modelo no encontrado')
