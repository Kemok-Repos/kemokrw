from kemokrw.load import Load
from kemokrw.extract_file import ExtractFile
from kemokrw.func_api import query_model_from_db
from openpyxl import Workbook
import kemokrw.config_api as config
import pandas as pd
import os


class LoadFile(Load):
    """" Clase LoadExcel implementación de la clase Load.

    Cumple la función de cargar información a un documento.

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
    save_data():
        Almacena la data de un pandas.DataFrame Object en una base de datos.
    """
    def __init__(self, path, model, sheet='Hoja1'):
        """Construye los atributos necesarios para almacenar la información.

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
        self.sheet = str(sheet or 'Hoja1')
        extension = path.split('.')
        self.file_type = extension[-1]
        self.model = model
        self.metadata = None

        if self.file_type == 'xlsx' and not os.path.isfile(self.path):
            wb = Workbook()
            wb.save(self.path)

        if self.file_type == 'csv' and not os.path.isfile(self.path):
            empty_file = pd.DataFrame(list())
            empty_file.to_csv(self.path)

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
        return cls(model_config.get('path'), model_config.get('model'), model_config.get('sheet'))

    def get_metadata(self):
        """Método que actualiza la metadata del archivo almacenado"""
        file = ExtractFile(self.path, self.sheet, self.file_type, self.model)
        self.metadata = file.metadata

    def save_data(self, data):
        """Almacenar la información de un DataFrame.

        Parametros
        ----------
            data : pandas.DataFrame Object
                Objeto con la información por almacenar
        """
        column_names = dict()

        for i in self.model:
            model_type = self.model[i]['type'].upper()
            model_type = ''.join(e for e in model_type if e.isalpha() or e.isspace() or e == '[' or e == ']')
            if model_type in config.COLUMN_TYPES['datetime']:
                data = data.astype({i: 'str'})

        for i in self.model:
            column_names[i] = self.model[i]['name']
        data.rename(column_names, axis=1, inplace=True)

        if self.file_type == 'xlsx':
            data.to_excel(self.path, self.sheet, index=False)
        elif self.file_type == 'csv':
            data.to_csv(self.path, index=False)
