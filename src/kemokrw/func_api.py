from pprint import pprint
import datetime
import pandas as pd
import json
import unidecode


def clean_str(x):
    if x.lower() in ['nan', 'null', '']:
        return None
    else:
        return x


def clean_bool(x):
    x = x.lower()
    x = x.strip()
    x = unidecode.unidecode(x)
    if x in ['si', 'yes', 'true', 'verdadero', 'activo', 'afirmativo', 'positivo']:
        return True
    elif x in ['no', 'false', 'falso', 'inactivo', 'negativo']:
        return False
    else:
        return None


def clean_numeric(x):
    if isinstance(x, str):
        x = ''.join(e for e in x if e.isnumeric() or e == '.')
    return x


def clean_json(x):
    x = json.dumps(x)
    if x.lower() in ['nan', 'null']:
        return None
    else:
        return x


def suggest_model(data):
    suggested_model = dict()
    for i, j in enumerate(data.columns):
        suggested_model['col' + str(i + 1)] = dict()
        suggested_model['col' + str(i + 1)]['name'] = j
        if data[j].dtypes == object:
            suggested_model['col' + str(i + 1)]['type'] = 'str'
        else:
            suggested_model['col' + str(i + 1)]['type'] = str(data[j].dtypes)
    pprint(suggested_model)


def match_model(model, data):
    if model == dict():
        print('Sin modelo definido. Sugerimos el siguiente:')
        suggest_model(data)
        raise Exception('Modelo vacio')

    if not data.empty:
        # Cambio de nombre
        column_names = dict()
        for i in model:
            column_names[model[i]['name']] = i
        data.rename(column_names, axis=1, inplace=True)

        for i in data.columns:
            if i not in model.keys():
                print(data.columns)
                raise Exception('Llave erronea: La columna "{}" no se encuentra dentro del modelo.'.format(i))
        for i in model:
            if model[i]['type'] == 'datetime64':
                try:
                    data[i] = pd.to_datetime(data[i])
                except ValueError as e:
                    print(i)
                    raise e
            elif model[i]['type'] == 'numeric':
                try:
                    data[i] = pd.to_numeric(data[i])
                except ValueError as e:
                    print(i)
                    raise e
            elif model[i]['type'] == 'str':
                try:
                    data = data.astype({i: 'str'})
                    data[i] = data[i].apply(clean_str)
                except ValueError as e:
                    print(i)
                    raise e
            elif model[i]['type'] == 'dict':
                try:
                    data = data.astype({i: 'object'})
                    data[i] = data[i].apply(clean_json)
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
    if x:
        date_part = datetime.datetime(1899, 12, 30) + datetime.timedelta(days=int(x))
        time_part = (x - int(x))*86400
        date_part = date_part + datetime.timedelta(seconds=int(time_part))
        return date_part
    else:
        return None
