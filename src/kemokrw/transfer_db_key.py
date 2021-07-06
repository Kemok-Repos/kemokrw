from kemokrw.transfer import Transfer
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

class DbKeyTransfer(Transfer):

    def __init__(self, src_config, dst_config, key, max_transfer=0):
        self.max_tranfer = max_transfer
        self.pack = 1000000
        self.src_config = src_config
        self.dst_config = dst_config
        self.key = key

    def prepare(self):
        f = open('registro_tiempo.log', 'w')

        # calulo de particiones pack compatibles com pandas
        query = config.TABLE_CHECK["check_rows"].format(table=self.src_config['table'], condition="")
        db = DbClient(uuid.uuid1().hex, self.src_config['db'])
        longitud = db.ejecutar(query)[0]
        condition = {}
        if longitud > 1500000:
            # limite de manejo de pandas 2M
            indice = 1
            valor_old = 0
            for i in range(0, longitud, self.pack):
                query = config.TABLE_QUERY_MAX.format(table=self.src_config['table'],
                                                      key=self.key,
                                                      offset=str(i),
                                                      limit=str(i + self.pack))
                valor = db.ejecutar(query)
                valor = valor[0]
                if valor != valor_old:
                    condition[str(indice)] = 'where {} >= {} and {} <{}'.format(self.key, valor_old, self.key, valor)
                else:
                    break
                valor_old = valor
                indice += 1

        if condition == {}:
            self.Tranfer(condition="")
        else:
            for i in condition:
                self.Tranfer(condition=condition[i])


    def Tranfer(self, condition):

        tuplaKey = ExtractDB(self.src_config['db'], self.src_config['table'], self.src_config['model'],
                             order="order by 1", condition=condition)
        tuplaKey.get_dataId()
        longitud = tuplaKey.data.shape[0]
        n_partition = 20
        elementos = longitud // n_partition
        k = 0

        for indice in range(0, longitud - elementos, elementos):
            k += 1
            idKeyoffset, idKeylimit = tuplaKey.data.iloc[indice, 0], \
                                      tuplaKey.data.iloc[indice + elementos - 1, 0]

            ExtractCondition = 'where {} >= {} and  {} <= {}'.format(self.Key, idKeyoffset, self.Key, idKeylimit) \
                if k != n_partition else 'where {} >= {} '.format(self.Key, idKeyoffset)

            src = ExtractDB(self.src_config['db'], self.src_config['table'], self.src_config['model'], order="",
                            condition=ExtractCondition)
            dst = LoadDB(self.dst_config['db'], self.dst_config['table'], self.dst_config['model'], order="",
                         condition=ExtractCondition)
            src.get_data()
            trf = BasicTransfer(src, dst)
            trf.transfer(2)

