from kemokrw.extract_db import ExtractDB
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
from  kemokrw.dbclient import *
import pandas as pd
import yaml
import datetime
import kemokrw.config_db as config
import uuid

from sqlalchemy import create_engine

def registrolog(f,msg):
    f.write(msg)

def Tranfer(Key, condition):

    tuplaKey = ExtractDB(src_config['db'], src_config['table'], src_config['model'], order="order by 1", condition=condition)
    tuplaKey.get_dataId()

    longitud = tuplaKey.data.shape[0]
    n_partition = 20

    elementos = longitud // n_partition
    k = 0

    for indice in range(0, longitud - elementos, elementos):
        k += 1
        registrolog(f, str(datetime.datetime.now()) + ' --- PARTICION {}--- \n'.format(k))
        print(str(datetime.datetime.now())+' --- PARTICION {}--- \n'.format(k))
        idKeyoffset, idKeylimit = tuplaKey.data.iloc[indice, 0], \
                                  tuplaKey.data.iloc[indice + elementos - 1, 0]

        ExtractCondition = 'where {} >= {} and  {} <= {}'.format(Key, idKeyoffset, Key, idKeylimit) \
            if k != n_partition else 'where {} >= {} '.format(Key, idKeyoffset)

        print(str(indice) + ' '+str(indice + elementos))
        print(ExtractCondition)
        src = ExtractDB(src_config['db'], src_config['table'], src_config['model'], order="", condition=ExtractCondition)
        dst = LoadDB(dst_config['db'], dst_config['table'], dst_config['model'], order="", condition=ExtractCondition)
        src.get_data()

        trf = BasicTransfer(src, dst)
        trf.transfer(4)
        del trf
        del src
        del dst

if __name__ == "__main__":

    f = open('registro_tiempo.log', 'w')
    key = "idfacturadetalle"
    pack = 1000000
    # Cargar configuración ejemplo de una base de datos de origen
    with open('ejemplo_origen.yaml') as file:
        src_config = yaml.load(file, Loader=yaml.FullLoader)

    # Cargar configuración ejemplo de una base de datos de destino
    with open('ejemplo_destino.yaml') as file:
        dst_config = yaml.load(file, Loader=yaml.FullLoader)

    # calulo de particiones pack compatibles com pandas
    query = config.TABLE_CHECK["check_rows"].format(table=src_config['table'], condition="")
    db = DbClient(uuid.uuid1().hex, "postgresql+psycopg2://postgres:maitreya1234@localhost:5433/srtendero")
    longitud = db.ejecutar(query)[0]
    condition = {}
    if longitud > 1500000:
        # limite de manejo de pandas 2M
        IdKeys = {}
        indice=1
        valor_old = 0;
        for i in range(0, longitud, pack):
            query = config.TABLE_QUERY_MAX.format(table=src_config['table'],
                                                    key=key,
                                                    offset=str(i),
                                                    limit=str(i + pack))
            valor = db.ejecutar(query)
            valor = valor[0]
            if valor != valor_old:
                condition[str(indice)] = 'where {} >= {} and {} <{}'.format(key, valor_old, key, valor)
            else:
                break
            valor_old= valor
            print(condition)
            indice += 1

    if condition == {}:
        Tranfer(Key=key, condition="")
    else:
        for i in condition:
            registrolog(f, str(datetime.datetime.now()) + ' --- PARTICION PANDA {}--- \n'.format(i))
            Tranfer(Key=key, condition=condition[i])






