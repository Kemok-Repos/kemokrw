from abc import ABC, abstractmethod


class Transfer(ABC):
    """Verificar y ejecuta la transferencia de datos de un lugar a otro.

    Atributos
    ---------
    src : Objeto de una subclase de la clase Extract
    dst : Objeto de una subclase de la clase Load
    """

    @abstractmethod
    def verify(self):
        """Verifica que los objetos sean compatibles y que la transferencia haya sido exitosa."""
        pass

    @abstractmethod
    def transfer(self):
        """Ejecuta un algoritmo de transferencia de datos."""
        pass
