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

if __name__ == "__main__":

    f = open('registro_tiempo.log', 'w')


    # Cargar configuración ejemplo de una base de datos de origen
    with open('ejemplo_origen.yaml') as file:
        src_config = yaml.load(file, Loader=yaml.FullLoader)

    # Cargar configuración ejemplo de una base de datos de destino
    with open('ejemplo_destino.yaml') as file:
        dst_config = yaml.load(file, Loader=yaml.FullLoader)

    # calulo de porciones


    query = config.COLUMN_CHECK["postgresql"]["numeric"]["check_nn"].format(column="*", table="erp_gom_ubicacion",
                                      condition="", order="", offset="", limit="")


    db = DbClient(uuid.uuid1().hex, "postgresql+psycopg2://srtendero:FFk39$.A210%@45.79.216.118:5432/srtendero")
    longitud = db.ejecutar(query)[0]
    n_portion = 4
    elementos = longitud // n_portion
    partition_src = {}
    k = 0
    for indice in range(0, longitud - elementos, elementos):
        k += 1
        offset="offset "+str(indice)
        limit ="limit " + str(indice + elementos - 1)
        partition_src[str(k)] = ExtractDB(src_config['db'], src_config['table'], src_config['model'], order="order by 1")
        print(partition_src[str(k)].data)
        print('porcion ' + str(indice) + ':' + str(indice + elementos - 1))
        print(partition_src[str(k)])




    registrolog(f, str(datetime.datetime.now())+' --- inicio ejecucion--- \n')
    # Crear objeto de extracción
    registrolog(f, str(datetime.datetime.now())+' inicio ExtractDB src \n')
    src = ExtractDB(src_config['db'], src_config['table'], src_config['model'], order="order by 1")
    registrolog(f, str(datetime.datetime.now()) + ' Fin ExtractDB src \n')
    print(src.metadata)

    # Crear objeto de carga
    registrolog(f, str(datetime.datetime.now()) + ' inicio ExtractDB dst \n')
    dst = ExtractDB(dst_config['db'], dst_config['table'], dst_config['model'], order="order by 1")
    registrolog(f, str(datetime.datetime.now()) + ' inicio ExtractDB dst \n')
    print(dst.metadata)

    registrolog(f, str(datetime.datetime.now()) + ' inicio src.get_data \n')
    src.get_data()
    registrolog(f, str(datetime.datetime.now()) + ' inicio dst.get_data \n')
    dst.get_data()
    registrolog(f, str(datetime.datetime.now()) + ' inicio src.data.merge(dst.data.. \n')
    df = src.data.merge(dst.data, how='outer', indicator='union')
    registrolog(f, str(datetime.datetime.now()) + ' inicio filtro left_only.... \n')
    df_faltantes = df[df.union == 'left_only']
    print(df_faltantes)
    print(str(datetime.datetime.now()) + ' fin filtro left_only \n')

    #verify dataframe set
    longitud = src.data.shape[0]
    print(df)
    print(longitud)
    n_portion = 4
    elementos = longitud // n_portion
    k = 0
    partition = {}
    for indice in range(0, longitud - elementos, elementos):
        partition[str(k)] = {}
        print('porcion '+str(indice) + ':'+str(indice + elementos -1 ))
        partition[str(k)] = src.data.iloc[indice:indice + elementos, :]
        print(partition[str(k)])
        resultado = pd.DataFrame(partition[str(k)]) if indice == 0 else pd.concat([resultado, partition[str(k)]], axis=0)


    longitud2 = resultado.shape[0]
    print(longitud2)
    df = resultado.merge(src.data, how='outer', indicator='union')
    registrolog(f, str(datetime.datetime.now()) + ' inicio filtro left_only.... \n')
    df_faltantes = df[df.union == 'left_only']
    print(df_faltantes)

    # Crear objeto de transferencia
    #trf = BasicTransfer(src, dst)
    #trf.transfer(2)
