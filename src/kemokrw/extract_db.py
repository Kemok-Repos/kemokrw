from kemokrw.extract import Extract
from sqlalchemy import create_engine
<<<<<<< HEAD
from sqlalchemy.exc import OperationalError, DatabaseError
from kemokrw.func_db import get_db_metadata, model_format_check
from kemokrw.func_api import query_model_from_db
=======
from kemokrw.func_db import get_db_metadata, model_format_check, get_db_collation
>>>>>>> @{u}
import kemokrw.config_db as config
from kredential.kredential import discover_credId, json_to_sqlalchemy, discover_full
import pandas as pd


class ExtractDB(Extract):
    """ Clase ExtractDB implementación de la clase Extract.

    Cumple la función de extraer información de bases de datos.

    Atributos
    ---------
    db : str
    dbms : str
    table : str
    model : dict
    condition : str
    order : str
    metadata : dict
    data : pandas.DataFrame Object

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla a extraer.
    get_data():
        Obtiene la data de la tabla a extraer.
    """

<<<<<<< HEAD
    def __init__(self, db, table, model, condition="", order=""):
        """ Construye un objeto de extracción desde base de datos.
=======
    def __init__(self, db, table, model, condition="", order="", key="", id_passbolt=None, config_passbolt=''):
        """Construye los atributos necesarios para la lectura de la información.
>>>>>>> @{u}

        Parametros
        ----------
            db : str
                Connection string para bases de datos en SQLAlchemy.
            dbms : str
                Manejador de base de dato (se obtiene del connection string).
            table : str
                Tabla que se va a extraer con el objeto.
            model : dict
                Un diccionario con la información de columnas a extraer.
            condition : str
                Condición de "WHERE" a usar en la extracción.
            order : str
                Columas por las que se ordena el query.
            metadata : dict
                Diccionario con el tipo (normalizado) y los chequeos realizados en cada columna para
                determinar diferencias.
            data : pandas.DataFrame Object
                Data extraída.
        """

        if id_passbolt:
            cred = discover_credId(id_passbolt, config_passbolt)
            db = json_to_sqlalchemy(cred)
            self.dbms = 'postgresql'
        else:
            self.dbms = db.split('+')[0]

        self.db = create_engine(db)
        self.table = table
        self.model = model
        self.condition = condition
        self.order = order
        self.metadata = dict()
<<<<<<< HEAD
        self.data = pd.DataFrame()
=======
        self.data = None
        self.key = key
        self.src_lc_monetary = ''
>>>>>>> @{u}

        # Inicializa metadata
        model_format_check(self.model)
        self.get_metadata()
        self.get_collation()


    @classmethod
<<<<<<< HEAD
    def get_model(cls, db, model_id, condition="", order=""):
        pass

    @classmethod
    def query_model(cls, db, table, condition="", order="", include_columns=None, exclude_columns=None, xconfig=""):
        """ Construye un objeto de extracción desde base de datos usando un modelo generado automaticamente.

        Parametros
        ----------
            db : str
                Connection string para bases de datos en SQLAlchemy.
            table : str
                Tabla que se va a extraer con el objeto.
            condition : str
                Condición de "WHERE" a usar en la extracción.
            order : str
                Columas por las que se ordena el query.
            include_columnas: str
                Columnas a incluir dentro del modelo obtenido.
            exclude_columnas: str
                Columnas a excluir dentro del modelo obtenido.
            xconfig: str
                Condicional dentro del query para incluir o excluir columnas. Ej. 'AND a = b'
        """
        dbms = db.split('+')[0]
        model = dict()
        engine = create_engine(db)
        attempts = 0
        while attempts < 3:
            try:
                connection = engine.connect()
                data = pd.read_sql(sql=config.MODEL_QUERY[dbms].format(table, xconfig), con=connection)
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

        data.sort_values(['ordinal_position'], ascending=True, ignore_index=True, inplace=True)

        if include_columns is not None and exclude_columns is None:
            data = data[data['column_name'].isin(include_columns)]
        elif include_columns is None and exclude_columns is not None:
            data = data[~data['column_name'].isin(exclude_columns)]
        elif include_columns is not None and exclude_columns is not None:
            columns = [x for x in include_columns if x not in exclude_columns]
            data = data[data['column_name'].isin(columns)]
        data.reset_index(inplace=True, drop=True)
        for index, row in data.iterrows():
            model['col' + str(index + 1)] = {'name': row['column_name'], 'type': row['data_type']}
        return cls(db, table, model, condition, order)
        
    def get_metadata(self):
        """ Método que actualiza la metadata de la tabla de extracción. """
        self.metadata = get_db_metadata(self.db, self.dbms, self.model, self.table, self.condition)
=======
    def from_passbolt(cls, passbolt_id, table, model, condition="", order=""):
        """Construye los atributos necesarios para la lectura de la información desde la API de passbolt."""
        cred = discover_credId(passbolt_id)
        return json_to_sqlalchemy(cred)

    def get_collation(self):
        self.src_lc_monetary = get_db_collation(self.db, self.dbms, 'lc_monetary')

    
    def get_metadata(self):
        """Método que actualiza la metadata de la tabla de extracción"""
        self.metadata = get_db_metadata(self.db, self.dbms, self.model, self.table, self.condition, self.key)
>>>>>>> @{u}

    def get_data(self):
        """ Método que para extraer data. """
        self.data = pd.DataFrame()

        j = []
        for i in self.model:
            j.append("{0} AS {1}".format(self.model[i]["name"], i))

        columns = ", ".join(j)

        query = config.TABLE_QUERY.format(columns=columns, table=self.table,
                                          condition=self.condition, order=self.order)
<<<<<<< HEAD
        attempts = 0
        while attempts < 3:
            try:
                connection = self.db.connect()
                self.data = pd.read_sql(sql=query, con=connection)
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
=======

        connection = self.db.connect()
        self.data = pd.read_sql(sql=query, con=connection)
        pd.set_option('display.max_rows', None)

        connection.close()




>>>>>>> @{u}
