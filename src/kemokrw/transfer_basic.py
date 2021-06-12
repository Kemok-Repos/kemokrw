from transfer import Transfer


class BasicTransfer(Transfer):
    """Clase BasicTransfer implementaciǫn de la clase Transfer.
    
    Cumple la función de verficar la compatibilidad de los objetos a transferir,
    verifica si ambos extremos son "iguales" y transfiere un los atos de una fuente a la otra.

    Atributos
    ---------
    src : pandas.DataFrame Object
    dst : pandas.DataFrame Object
    max_transfer : int

    Métodos
    -------
    verify():
        Verifica si la fuente y el destino son compatibles.
        Verifica si el destino es igual a la fuente.
    transfer(retires=0):
        Tranfiere los datos de la fuente a el destino.
    """

    def __init__(self, src=None, dst=None, max_transfer=0):
        """ Contruye los atributos necesarios para hacer una transferencia y verifica la compatibilidad de las fuentes.

        Parametros
        ----------
            src : pandas.DataFrame Object
                Objeto origen resultante de una implementación de la clase Extract.
            dst : pandas.DataFrame Object
                Objeto destino resultante de una implementación de la clase Load.
            max_transfer : int
                Máxima cantidad de filas a transferir bajo este método.

        Raises
        ------
        Exception
            No compatibility found.
        """
        self.src = src
        self.dst = dst
        self.verification = None
        self.max_transfer = max_transfer

        # Verify for compatibility
        self.verify()
        if self.src.metadata["check_rows"] > max_transfer and self.max_transfer != 0:
            print("*WARNING* Maximum number of rows detected. {0} of {1}.".format(self.src.metadata["check_rows"],
                                                                                  self.max_transfer))

    def verify(self):
        """Verifica la compatibilidad de los objetos a transferir y verifica si el destino es igual a la fuente.

        Este metodo utiliza el atributo metadata y la función get_metada() de los objetos src y dst. (Ver documentación)

        1. Verifica la compatibilidad del modelo de ambos objetos bajo las siguientes reglas:
            - La metadata tiene el mismo número de columnas (metadata[ncols])
            - Las columnas de metadata tienen el mismo tipo respectivamente (metadata[columns][colX][type]
        2. Verifica si la metadata es igual entre los objetos src y dst para aquellos "keys" de chequeo que se
        encuentran en la metadata de ambas objetos.

        Raises
        ------
        Exception
            No compatibility found.
        """
        self.dst.get_metadata()    # Actualiza la metadata del objeto destino

        # Revisa que la cantidad de columnas sea igual
        if self.src.metadata["ncols"] != self.dst.metadata["ncols"]:
            raise Exception('No compatibility found. Different number of columns detected')

        # Revisa que cada tipo de columna sea igual entre pares
        for i in self.src.metadata["columns"]:
            if self.src.metadata["columns"][i]["type"] != self.dst.metadata["columns"][i]["type"]:
                raise Exception('No compatibility found. {} type do not match.'.format(1))

        verification = True
        # Revisa el número de filas en cada extremo
        if self.src.metadata["check_rows"] != self.dst.metadata["check_rows"]:
            verification = False

        # Revisa que cada pareja de columnas para revisar los chequeos
        for i in self.src.metadata["columns"]:
            common_params = set(self.src.metadata["columns"][i].keys()) & set(self.dst.metadata["columns"][i].keys())
            common_params.discard("subtype")
            for j in common_params:
                if self.src.metadata["columns"][i][j] != self.dst.metadata["columns"][i][j]:
                    verification = False

        self.verification = verification
        return verification

    def transfer(self, retries=0):
        """Tranfiere y verifica los datos del origen al destino.

        Este método realiza una verificación de los datos en ambos extremos y si no son iguales intenta transferir
        los datos.

        Parametros
        ----------
        retries : int
            Número de intentos adicionales en caso la verificación falle la primera vez

        Raises
        ------
        Exception
            Verification failed after transfer.
        """
        self.verify()

        total_tries = 1+retries
        n_try = 1

        self.src.get_data()

        while n_try <= total_tries and not self.verification:
            print("Transferring data. Try {0} out of {1}".format(n_try, total_tries))
            self.dst.save_data(self.src.data)
            self.verify()
            n_try += 1

        if self.verification:
            print('Transfer successful.')
        else:
            raise Exception('Verification failed after transfer. Transfer tried {} times'.format(total_tries))
