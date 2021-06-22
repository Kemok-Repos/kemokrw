from kemokrw.transfer import Transfer


class DbKeyTransfer(Transfer):
    """Clase DbKeyTransfer implementación de la clase Transfer.

    La clase se encarga de transferir y asegurar información de base de datos a base de datos
    usando el siguiente algoritmo y una columna(s) de llave primaria (Tablas transaccionales o maestras).

    Fase de actualización

    1. Transferir todos los registros nuevos del origen al destino.

    Fase de verificación

    2. Fragmentar la tabla de origen en paquetes. El tamaño debe ser un parametro.

    3. Realizar verificaciones de cada segmento (ver el método verify() de la clase BasicTransfer)

    Fase de Transferencia

    1. Transferir todos los paquetes identificados.
    2. Almacenar el listado de los segmentos transferidos dentro de la fase de actualizacion y de transferencia
        dentro del atributo updated.

    Atributos
    ---------
    src_dict : dict
        Información necesaria para describir una tabla [db, table, model, condition]
    dst_dict : dict
        Información necesaria para describir una tabla [db, table, model, condition]
    key_column : list
        Columas de la llave primaria
    package_size : int
    updated_ranges : list

    Métodos
    -------
    verify():
        Verifica si la fuente y el destino son compatibles.
        Verifica si el destino es igual a la fuente.
    transfer(retires=0):
        Tranfiere los datos de la fuente a el destino.
    """
    pass
