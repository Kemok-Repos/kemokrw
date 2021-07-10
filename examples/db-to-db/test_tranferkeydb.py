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

def Tranfer(Key, condition,total_Tp, tp,npartition):
    a1 = datetime.datetime.now()
    print(str(datetime.datetime.now()) +  ' Calculating Sub Table partitions..')
    tuplaKey = ExtractDB(src_config['db'], src_config['table'], src_config['model'], order="order by 1", condition=condition,key=Key)
    tuplaKey.get_dataId()

    longitud = tuplaKey.data.shape[0]
    n_partition = npartition

    elementos = longitud // n_partition
    k = 0
    ExtractCondition = {}
    valor_old = 0
    indice = 0
    b1 = datetime.datetime.now()
    tiempo= b1 - a1
    seg_tspIndex = tiempo.total_seconds()
    print(str(datetime.datetime.now()) + ' Procesed Time Elapsed: ' + str(round(seg_tspIndex, 4)) + 'seg')

    for indice in range(0, longitud - elementos, elementos):
        k += 1
        idKeyoffset, idKeylimit = tuplaKey.data.iloc[indice, 0], \
                                  tuplaKey.data.iloc[indice + elementos - 1, 0]

        ExtractCondition[str(k)] = 'where {} >= {} and  {} <= {}'.format(Key, idKeyoffset, Key, idKeylimit)
        print(str(datetime.datetime.now()) + ' Sub Table partition {}--- {}'.format(k, ExtractCondition[str(k)]))

    if indice < longitud:
        ExtractCondition[str(k + 1)] = 'where {} >= {} and  {} <= {}'.format(Key,tuplaKey.data.iloc[indice + elementos, 0] , Key, tuplaKey.data.iloc[longitud - 1, 0])
        print(str(datetime.datetime.now()) + ' Sub Table partition {}--- {}'.format(k + 1, ExtractCondition[str(k + 1)]))

    print('Sub partitions Calculated.. procceing')
    del tuplaKey

    for indice in ExtractCondition:
        a = datetime.datetime.now()
        stri = 'Procesing Sub table partition {} --- {} \n'.format(indice, ExtractCondition[str(indice)])
        print(stri)
        src = ExtractDB(src_config['db'], src_config['table'], src_config['model'], order="", condition=ExtractCondition[str(indice)], key=Key)
        dst = LoadDB(dst_config['db'], dst_config['table'], dst_config['model'], order="", condition=ExtractCondition[str(indice)], key=Key, src_lc_collation=src.src_lc_monetary)
        src.get_data()

        trf = BasicTransfer(src, dst)
        trf.transfer(4)
        del trf
        del src
        del dst
        tsp = len(ExtractCondition)
        b = datetime.datetime.now()
        tiempo , seg = b - a, tiempo.total_seconds()
        print(str(datetime.datetime.now()) + ' Procesed Time Elapsed: ' + str(round(seg, 4)) + 'seg')
        #t_estimado = (total_tp - tp) * (tsp - int(indice)) * (seg + seg_tspIndex) / 60
        #print(str(datetime.datetime.now()) + ' Time Estimated: ' + str(round(t_estimado, 4)) + ' Min')

if __name__ == "__main__":
    a = datetime.datetime.now()
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
    db = DbClient(uuid.uuid1().hex, src_config["db"])
    longitud = db.ejecutar(query)[0]
    condition = {}
    print(str(datetime.datetime.now()) +  ' Calculating Table partitions..')
    if longitud > 1500000:
        # limite de manejo de pandas 2M
        IdKeys = {}
        indice=1
        valor_old = 0
        for i in range(0, longitud, pack):
            indice += 1
            query = config.TABLE_QUERY_MAX.format(table=src_config['table'],
                                                    key=key,
                                                    offset=str(i),
                                                    limit=str(pack))
            valor = db.ejecutar(query)
            valor = valor[0]
            if valor != valor_old:
                condition[str(indice)] = 'where {} >= {} and {} <{}'.format(key, valor_old, key, valor)
            else:
                break
            valor_old = valor
            print(str(datetime.datetime.now()) + ' Table partition {} --- {}'.format(indice - 1  , condition[str(indice)]))

    #print(str(datetime.datetime.now())+ "partition table calulated...")
    condition[str(indice)].replace('<', '<=')
    if condition == {}:
        Tranfer(Key=key, condition="")
    else:
        total_tp = len(condition)
        for i in condition:
            stri = str(datetime.datetime.now())+' Procesing table Partition  No. {} --- {} '.format(int(i) -1, condition[i])
            #registrolog(f, str(datetime.datetime.now()) + stri)
            print(stri)
            Tranfer(Key=key, condition=condition[i],total_Tp=total_tp, tp=int(i) - 1,npartition=5)
            b = datetime.datetime.now()
            tiempo = b - a
            print(str(datetime.datetime.now()) + ' Total Time Elapsed: ' + str(round(tiempo.total_seconds(), 4)) + 'seg')






