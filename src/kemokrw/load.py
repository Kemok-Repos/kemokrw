from abc import ABC, abstractmethod


class Load(ABC):
    """Clase Load

    Almacena la información de un panda.DataFrame object y verifíca que la transferencia se haya realizado de manera
    correcta.

    Atributos
    ---------
    metadata : dict

    Métodos
    -------
    get_metadata():
        Obtiene el diccionario de metadata del destino.
    save_data():
        Almacena el panda.DataFrame object en el destino.
    """

    @abstractmethod
    def get_metadata(self):
        """Obtiene la metadata efectuando las evaluaciiones establecidas."""
        pass

    @abstractmethod
    def save_data(self):
        """Almacena el panda.DataFrame object en el destino usando los atributos del objeto.

        Parametros
        ----------
        data : panda.DataFrame object
        """
        pass