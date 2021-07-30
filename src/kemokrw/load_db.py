from kemokrw.load import Load
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError
from kemokrw.func_db import get_db_metadata
import kemokrw.config_db as config
import pandas as pd


class LoadDB(Load):
    """"Clase LoadDB implementación de la clase Load.

    Cumple la función de cargar información a una bases de datos.

    Atributos
    ---------
    db : str
    dbms : str
    table : str
    model : dict
    condition : str
    order : str
    chuncksize : int
    metadata : dict

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla destino.
    save_data():
        Almacena la data de un pandas.DataFrame Object en una base de datos.
    """

    def __init__(self, db, table, model, condition="", order="", chunksize=1000):
        """Construye los atributos necesarios para almacenar la información.

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
        """
        self.db = create_engine(db)
        self.dbms = db.split('+')[0]
        self.table = table
        self.model = model
        self.condition = condition
        self.order = order
        self.chunksize = chunksize
        self.metadata = None

        # Inicializa metadata
        self.get_metadata()

    @classmethod
    def from_passbolt(cls, passbolt_id, table, model, condition="", order=""):
        """Construye los atributos necesarios para la lectura de la información desde la API de passbolt."""
        pass
        
    @classmethod
    def query_model(cls, db, table, condition="", order="", include_columns=None, exclude_columns=None, xconfig=""):
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
                print(err)

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
        """Método que actualiza la metadata de la tabla de extracción"""
        self.metadata = get_db_metadata(self.db, self.dbms, self.model, self.table, self.condition)

    def save_data(self, data):
        """Almacenar la información de un DataFrame.

        Parametros
        ----------
            data : pandas.DataFrame Object
                Objeto con la información por almacenar
        """
        connection = self.db.connect()
        connection.execute("DELETE FROM {0} {1}".format(self.table, self.condition))
        names = []


        column_names = dict()
        for i in self.model:
            column_names[i] = self.model[i]['name']
        data.rename(column_names, axis=1, inplace=True)
        data.to_sql(name=self.table, con=connection, if_exists='append', index=False,
                    chunksize=self.chunksize)
        connection.close()

    @staticmethod
    def built_connection_string(login, password, host, port, schema):
        con_string = "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}"
        return con_string.format(login, password, host, port, schema)
