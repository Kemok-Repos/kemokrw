from kemokrw.extract import Extract
from sqlalchemy import create_engine
from kemokrw.func_db import get_db_metadata, model_format_check, get_db_collation
import kemokrw.config_db as config
from kredential.kredential import discover_credId, json_to_sqlalchemy, discover_full
import pandas as pd


class ExtractDB(Extract):
    """Clase ExtractDB implementación de la clase Extract.

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

    def __init__(self, db, table, model, condition="", order="", key="", id_passbolt=None, config_passbolt=''):
        """Construye los atributos necesarios para la lectura de la información.

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
        self.data = None
        self.key = key
        self.src_lc_monetary = ''

        # Inicializa metadata
        model_format_check(self.model)
        self.get_metadata()
        self.get_collation()


    @classmethod
    def from_passbolt(cls, passbolt_id, table, model, condition="", order=""):
        """Construye los atributos necesarios para la lectura de la información desde la API de passbolt."""
        cred = discover_credId(passbolt_id)
        return json_to_sqlalchemy(cred)

    def get_collation(self):
        self.src_lc_monetary = get_db_collation(self.db, self.dbms, 'lc_monetary')

    
    def get_metadata(self):
        """Método que actualiza la metadata de la tabla de extracción"""
        self.metadata = get_db_metadata(self.db, self.dbms, self.model, self.table, self.condition, self.key)


    def get_data(self):
        """Método que para extraer data"""

        j = []
        for i in self.model:
            j.append("{0} AS {1}".format(self.model[i]["name"], i))

        columns = ", ".join(j)

        query = config.TABLE_QUERY.format(columns=columns, table=self.table,
                                          condition=self.condition, order=self.order)

        connection = self.db.connect()
        self.data = pd.read_sql(sql=query, con=connection)
        pd.set_option('display.max_rows', None)

        connection.close()




