from abc import ABC, abstractmethod


class Extract(ABC):
    """Clase Extract

    Extrae la información a traves de los atributos del objeto para generar un panda.DataFrame object y un diccionario
    de metadata como resultado.

    Atributos
    ---------
    metadata : dict
    data : pandas.DataFrame Object

    Métodos
    -------
    get_metadata():
        Obtiene el diccionario de metadata de la fuente.
    get_data():
        Obtiene el panda.DataFrame object de la fuente.
    """

    @abstractmethod
    def get_metadata(self):
        """Obtiene la metadata efectuando las evaluaciiones establecidas."""
        pass

    @abstractmethod
    def get_data(self):
        """Obtiene el panda.DataFrame object de la fuente usando los atributos del objeto."""
        pass
