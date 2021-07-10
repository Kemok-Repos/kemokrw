from kemokrw.load import Load
from sqlalchemy import create_engine
from kemokrw.func_db import get_db_metadata, get_db_collation


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

    def __init__(self, db, table, model, condition="", order="", chunksize=10000, key="", src_lc_collation=""):
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
        self.key = key
        self.src_lc_monetary = src_lc_collation
        self.dst_lc_monetary = ''

        # Inicializa metadata
        self.get_metadata()
        self.get_collation()


    @classmethod
    def from_passbolt(cls, passbolt_id, table, model, condition="", order=""):
        """Construye los atributos necesarios para la lectura de la información desde la API de passbolt."""
        pass
        

    def get_metadata(self):
        """Método que actualiza la metadata de la tabla de extracción"""
        self.metadata = get_db_metadata(self.db, self.dbms, self.model, self.table, self.condition, self.key)

    def get_collation(self):
        self.dst_lc_monetary = get_db_collation(self.db,self.dbms,'lc_monetary')

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
        modeyfield = []
        for i in self.model:
            names.append(self.model[i]["name"])
            if self.model[i]["type"] == "money":
                modeyfield.append(self.model[i]["name"])

        data.columns = names

        for i in modeyfield:
            data[i] = data[i].str.replace('Bs.', '', regex=False)
            data[i] = data[i].str.replace('$', '', regex=False)
            if self.dst_lc_monetary == 'es_VE.UTF-8' and self.src_lc_monetary == 'en_US.UTF-8':
                data[i] = data[i].str.replace(',', '', regex=False)
                data[i] = data[i].str.replace('.', ',', regex=False)

        self.chunksize = 10000
        trans = connection.begin()
        try:
            data.to_sql(name=self.table, con=connection, if_exists='append',
                        index=False, chunksize=self.chunksize)

        except Exception as e:
            print(e)
        trans.commit()
        connection.close()
