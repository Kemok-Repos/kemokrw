from abc import ABC, abstractmethod


class Transfer(ABC):
    """Verificar y ejecuta la transferencia de datos de un lugar a otro.    """

    @abstractmethod
    def verify(self):
        """Verifica que los objetos sean compatibles y que la transferencia haya sido exitosa."""
        pass

    @abstractmethod
    def transfer(self):
        """Ejecuta un algoritmo de transferencia de datos."""
        pass
