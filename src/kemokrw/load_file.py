from kemokrw.load import Load
import pandas as pd


class LoadExcel(Load):
    """"Clase LoadDB implementación de la clase Load.

    Cumple la función de cargar información a documento de excel.

    Atributos
    ---------
    path : str
    sheet : str
    model : dict
    metadata : dict
    data : DataFrame

    Métodos
    -------
    get_metadata():
        Obtiene la metadata de la tabla destino.
    save_data():
        Almacena la data de un pandas.DataFrame Object en una base de datos.
    """

    def __init__(self, excel_writer, path, sheet, model):
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
        
    def get_metadata(self):
        """Método que actualiza la metadata del archivo almacenado"""
        self.metadata = get_db_metadata(self.db, self.dbms, self.model, self.table, self.condition)

    def save_data(self, data):
        """Almacenar la información de un DataFrame.

        Parametros
        ----------
            data : pandas.DataFrame Object
                Objeto con la información por almacenar
        """
        column_names = dict()
        for i in self.model:
            column_names[i] = self.model[i]['name']
        data.rename(column_names, axis=1, inplace=True)

        attempts = 0
        while attempts < 3:
            try:
                connection = self.db.connect()
                connection.execute("DELETE FROM {0} {1}".format(self.table, self.condition))
                data.to_sql(name=self.table, con=connection, if_exists='append', index=False,
                            chunksize=self.chunksize)
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

    @staticmethod
    def built_connection_string(login, password, host, port, schema):
        con_string = "postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}"
        return con_string.format(login, password, host, port, schema)
