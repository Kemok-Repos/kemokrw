from abc import ABC, abstractmethod

class ApiClient(ABC):
    """Clase Extract

    Encapsula el manejo de una API para simplificar los procesos de autenticación y de llamadas a los endpoints.

    Métodos
    -------
    get():
        Realiza una solicitud a un endpoint utilizando el verbo GET.
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
