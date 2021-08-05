import re

from kemokrw.extract_db import ExtractDB
import datetime
import uuid
import re
import kemokrw.config_db as config
from kemokrw.dbclient import *
from kemokrw.extract_db import ExtractDB
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer


class DbDateTransfer():
    """Clase DbDateTransfer implementación de la clase Transfer.

    La clase se encarga de transferir y asegurar información de base de datos a base de datos
    usando el siguiente algoritmo y una columna de fecha (Tablas transaccionales o con fecha de modificación).

    Fase de actualización

    1. Transferir todos los registros nuevos del origen al destino. Si la columna esta en formato fecha (sin hora),
        es necesario transferir tambien la información del último día.

    Fase de verificación

    2. Fragmentar la tabla de origen en porciones de tiempo. Estas deben de poder ser parametrizables para mejorar la
        eficiencia.
        Ej. Años anteriores segmentar por año.
            Meses anteriores segmentar por mes.
            Mes actual segmentar por día.

    3. Realizar verificaciones de cada segmento (ver el método verify() de la clase BasicTransfer)
    4. Si se encuentran diferencias en un segmento mayor a un día (Ej. fallo de verificación en 2018),
        framentar y verificar hasta encontrar los días con diferencias.

    Fase de Transferencia

    1. Transferir todos los segmentos identificados.
    2. Almacenar el listado de los segmentos transferidos dentro de la fase de actualizacion y de transferencia
        dentro del atributo updated.

    Las transferencias implementadas en esta clase debén de tomar un máximo de registros por segmento. En caso se
    exceda, el metodo hara "paquetes" para hacer una transferencia controlada.

    Atributos
    ---------
    src_dict : dict
        Información necesaria para describir una tabla [db, table, model, condition]
    dst_dict : dict
        Información necesaria para describir una tabla [db, table, model, condition]
    date_column : string
        Columa de fecha y hora por la cual hacer el la transferencia
    max_transfer : int
    updated_days : list

    Métodos
    -------
    verify():
        Verifica si la fuente y el destino son compatibles.
        Verifica si el destino es igual a la fuente.
    transfer(retires=0, max_tranfer=1000):
        Tranfiere los datos de la fuente a el destino.
    """

    def __init__(self, src_config, dst_config, max_transfer=0, Multiprocessing=False):
        self.max_tranfer = max_transfer
        # Unidad medida bloque lectura panda
        self.pack = 1000000
        self.src_config = src_config
        self.dst_config = dst_config
        self.date_column = self.src_config['date_column']
        self.key_column = self.src_config['key_column']
        self.workerPar = None
        # multiprocessing option
        self.multiprocessing = Multiprocessing
        self.current_year = str(datetime.datetime.now().year)
        self.current_month = datetime.datetime.now().month
        self.current_month = '0' + str(self.current_month) if self.current_month < 10 else str(self.current_month)
        self.current_day = datetime.datetime.now().day
        self.current_day = '0' + str(self.current_day) if self.current_day < 10 else str(self.current_day)

    def tranfer(self, workerPar=None, years_old=False, months_old=False, days_old=False, partitionDate=None):
        self.workerPar = workerPar
        a = datetime.datetime.now()

        # años anteriores segmenta por año
        patern = ''
        if partitionDate == 'years_old':
            oper_year = "<"
            query = config.TABLE_DATE_YEAR.format(table=self.src_config['table'],
                                                  key=self.date_column,
                                                  oper=oper_year,
                                                  year=self.current_year)
            patern = 'YYYY'

        # meses  anteriores segmenta por mes
        elif partitionDate == 'months_old':
            oper_month, patern = "<", 'YYYY-MM'
            query = config.TABLE_DATE_MONTH.format(table=self.src_config['table'],
                                                   key=self.date_column,
                                                   oper=oper_month,
                                                   year=self.current_year,
                                                   patern=patern,
                                                   month=self.current_month)

        # mes actual segmente por dia
        elif partitionDate == 'month':
            oper_month, patern = "<", 'YYYY-MM-DD'
            # test.......... 07
            self.current_month = '07'
            query = config.TABLE_DATE_MONTH.format(table=self.src_config['table'],
                                                   key=self.date_column,
                                                   oper=oper_month,
                                                   year=self.current_year,
                                                   patern=patern,
                                                   month=self.current_month)
        else:
            raise Exception("fill partitionDate, "
                            "implemented to years_old, months_old and month")

        # creating partitions
        db = DbClient(uuid.uuid1().hex, self.src_config["db"])
        part = str(db.ejecutar(query)[0]).split(',')
        condicion = []
        a1 = datetime.datetime.now()
        for i, k in enumerate(part):
            a = datetime.datetime.now()
            if re.match(r'\d{4}$', k):
                condicion.append("where extract(year from {}) = '{}'".
                                 format(self.date_column, k))
            elif re.match(r'\d{4}-\d{2}$', k):
                condicion.append("where  extract(year from {date_column}) = "
                                 "'{year}' and extract(month "
                                 "from {date_column}) = '{month}'".
                                 format(date_column=self.date_column,
                                        year=k[:4],
                                        month=k[5:]))

            elif re.match(r'\d{4}-\d{2}-\d{2}$',k):
                condicion.append("where extract(day from {}) = '{}'".
                                 format(self.date_column, k))

            else:
                raise Exception ('falla de formato')


            # condicion.append(" where TO_CHAR({},'{}')='{}'".format(self.date_column, patern, k))
            print(condicion[i])
            self.TranferPartitions(Key=self.key_column, condition=condicion[i])

        b = datetime.datetime.now()
        tiempo = b - a1
        print(str(datetime.datetime.now()) + 'Total Time Elapsed: ' + str(
            round(tiempo.total_seconds(), 4)) + 'seg')

    def TranferPartitions(self, Key, condition, total_Tp=None, tp=None):
        a = datetime.datetime.now()
        src = ExtractDB(self.src_config["db"], self.src_config["table"],
                        self.src_config["model"], order="",
                        condition=condition, key=Key,
                        id_passbolt=self.src_config['id_passbolt'])

        dst = LoadDB(self.dst_config["db"], self.dst_config["table"],
                     self.dst_config["model"], order="",
                     condition=condition,
                     key=Key, src_lc_collation=src.src_lc_monetary)

        trf = BasicTransfer(src, dst)
        trf.transfer(4)
        del trf
        del dst

        b = datetime.datetime.now()
        tiempo = b - a
        seg = tiempo.total_seconds()
        print(str(datetime.datetime.now()) + ' Processed Time Elapsed: ' +
              str(round(seg, 4)) + 'seg')
