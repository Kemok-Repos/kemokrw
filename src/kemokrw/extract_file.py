from kemokrw.extract import Extract
from kemokrw.func_api import extract_metadata, match_model, query_model_from_db
import pandas as pd


class ExtractFile(Extract):
    """ Clase ExtractFile implementación de la clase Extract.

     Cumple la función de extraer de un archivo sencillo.

     Atributos
     ---------
     path : str
     sheet : str
     type : str
     model : dict
     metadata : dict

     Métodos
     -------
     get_model():
         Obtiene la configuración del documento de una tabla de modelos.
    get_metadata():
        Obtiene la metadata de la tabla destino.
     get_data():
         Obtiene la data un archivo.
     """
    def __init__(self, path, sheet, file_type, model, separator=',', encoding='utf-8'):
        """Construye los atributos necesarios para cargar la información.

        Parametros
        ----------
            path : str
                PATH del archivo.
            sheet : str
                Nombre de la hoja.
            type : str
                Tipo de documento a crear.
            model : dict
                Un diccionario con la información de columnas a extraer.
            metadata : dict
                Diccionario con el tipo (normalizado) y los chequeos realizados en cada columna para
                determinar diferencias.
        """
        self.path = path
        self.sheet = sheet
        self.file_type = file_type.lower()
        self.separator = separator
        self.encoding = encoding
        self.model = model
        self.metadata = dict()
        self.data = pd.DataFrame()

        # Inicializa metadata
        self.get_metadata()

    @classmethod
    def get_model(cls, db, model_id):
        """Método que construye un objeto de extracción desde un archivo a partir de un modelo en base de datos.

        Parametros
        ----------
            db : str
                Connection string para bases de datos en SQLAlchemy.
            model_id : int
                Id del modelo dentro de la tabla de maestro_de_modelos.
        """
        model_config = query_model_from_db(db, model_id)
        return cls(model_config['path'], model_config['sheet'], model_config['file_type'], model_config['model'])

    def get_metadata(self):
        """ Método que genera la metadata de los datos extraidos. """
        self.get_data()
        self.metadata = extract_metadata(self.model, self.data)

    def get_data(self):
        """Método que genera un Dataframe desde un archivo"""
        self.data = pd.DataFrame()
        if self.file_type == 'excel':
            try:
                self.data = pd.read_excel(self.path, sheet_name=self.sheet)
            except ValueError as err:
                print(err)
        elif self.file_type == 'csv':
            try:
                self.data = pd.read_csv(self.path, sep=self.separator, encoding=self.encoding)
            except pd.errors.EmptyDataError:
                pass
        # Dar formato a la data
        self.data = match_model(self.model, self.data)
