from abc import ABC, abstractmethod

class ApiClient(ABC):
    """Clase ApiCLient

    Cliente para manejar la API de diferentes servicios.
    """
    @abstractmethod
    def get(self):
        pass

    def post(self):
        pass

    def put(self):
        pass

    def delete(self):
        pass
