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

class DbKeyTransfer():
    # documentacion inicial.
    """Clase DbKeyTransfer implementación de la clase Transfer.
        La clase se encarga de transferir y asegurar información de base de datos a base de datos
        usando el siguiente algoritmo y una columna(s) de llave primaria (Tablas transaccionales o maestras).
        Fase de actualización
        1. Transferir todos los registros nuevos del origen al destino.
        Fase de verificación
        2. Fragmentar la tabla de origen en paquetes. El tamaño debe ser un parametro.
        3. Realizar verificaciones de cada segmento (ver el método verify() de la clase BasicTransfer)
        Fase de Transferencia
        1. Transferir todos los paquetes identificados.
        2. Almacenar el listado de los segmentos transferidos dentro de la fase de actualizacion y de transferencia
            dentro del atributo updated.
        Atributos
        ---------
        src_dict : dict
            Información necesaria para describir una tabla [db, table, model, condition]
        dst_dict : dict
            Información necesaria para describir una tabla [db, table, model, condition]
        key_column : list
            Columas de la llave primaria
        package_size : int
        updated_ranges : list
        Métodos
        -------
        verify():
            Verifica si la fuente y el destino son compatibles.
            Verifica si el destino es igual a la fuente.
        transfer(retires=0):
            Tranfiere los datos de la fuente a el destino.
        """
        # propuesta de implementación DbKeyTransfer basada en basictranfer.
        # UML: https://lucid.app/lucidchart/5c4d839e-6ec6-450d-988a-7eb71a48c264/edit?beaconFlowId=7200D524CFEBC447&page=P2mReXmc01Ko#

    def __init__(self, src_config, dst_config, key, max_transfer=0,nSubPartition = 2):
        self.max_tranfer = max_transfer
        self.nSubPartition = nSubPartition
        # Unidad medida bloque lectura panda
        self.pack = 1000000
        self.src_config = src_config
        self.dst_config = dst_config
        self.key = key


    def tranfer(self):
        a = datetime.datetime.now()
        f = open('registro_tiempo.log', 'w')

        # calculo de particiones pack compatibles com pandas
        query = config.TABLE_CHECK["check_rows"].format(table=self.src_config['table'], condition="")
        db = DbClient(uuid.uuid1().hex, self.src_config["db"])
        longitud = db.ejecutar(query)[0]
        condition = {}
        print(str(datetime.datetime.now()) + ' Calculating Table partitions..')
        # particionado de tabla limitacion estructural PANDA
        if longitud > 1500000:
            # limite de manejo de pandas 2M
            IdKeys = {}
            indice = 1
            valor_old = 0
            for i in range(0, longitud, self.pack):
                indice += 1
                query = config.TABLE_QUERY_MAX.format(table=self.src_config['table'],
                                                      key=self.key,
                                                      offset=str(i),
                                                      limit=str(self.pack))
                valor = db.ejecutar(query)
                valor = valor[0]
                if valor != valor_old:
                    condition[str(indice)] = 'where {} >= {} and {} <{}'.\
                        format(self.key, valor_old, self.key, valor)
                else:
                    break
                valor_old = valor
                print(str(datetime.datetime.now()) + ' Table partition {} --- {}'.
                      format(indice - 1, condition[str(indice)]))

        condition[str(indice)].replace('<', '<=')
        del db
        if condition == {}:
            self.TranferPartitions(Key=self.key, condition="")
        else:
            total_tp = len(condition)
            for i in condition:
                stri = str(datetime.datetime.now()) + \
                       ' Procesing table Partition  No. {} --- {} '.\
                           format(int(i) - 1, condition[i])

                # registrolog(f, str(datetime.datetime.now()) + stri)
                print(stri)
                self.TranferPartitions(Key=self.key, condition=condition[i],
                                       total_Tp=total_tp, tp=int(i) - 1)

                b = datetime.datetime.now()
                tiempo = b - a
                print(str(datetime.datetime.now()) + ' Total Time Elapsed: ' + str(
                    round(tiempo.total_seconds(), 4)) + 'seg')


    def TranferPartitions(self, Key, condition, total_Tp, tp):
        a = datetime.datetime.now()
        src = ExtractDB(self.src_config["db"], self.src_config["table"],
                        self.src_config["model"], order="",
                        condition=condition, key=Key,
                        id_passbolt='eaef93b8-5016-46a6-9c80-5b254b68e09a')


        dst = LoadDB(self.dst_config["db"], self.dst_config["table"],
                     self.dst_config["model"], order="",
                     condition=condition,
                     key=Key, src_lc_collation=src.src_lc_monetary)

        trf = BasicTransfer(src, dst)
        trf.transfer(4)
        del trf
        del src
        del dst

        b = datetime.datetime.now()
        tiempo = b - a
        seg = tiempo.total_seconds()
        print(str(datetime.datetime.now()) + ' Processed Time Elapsed: ' +
              str(round(seg, 4)) + 'seg')
