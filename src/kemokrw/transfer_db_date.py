from kemokrw.transfer import Transfer


class DbDateTransfer(Transfer):
    """Clase DbDateTransfer implementación de la clase Transfer.

    La clase se encarga de transferir y asegurar información de base de datos a base de datos
    usando el siguiente algoritmo y una columna de fecha (Tablas transaccionales o con fecha de modificación).

    Fase de actualización

    1. Transferir todos los registros nuevos del origen al destino. Si la columna esta en formato fecha (sin hora),
        es necesario transferir tambien la información del último día.

    Fase de verificación

    2. Fragmentar la tabla de origen en porciones de tiempo. Estas deben de poder ser parametrizables para mejorar la
        eficiencia.
        Ej. Años anteriores segmentar por año.
            Meses anteriores segmentar por mes.
            Mes actual segmentar por día.

    3. Realizar verificaciones de cada segmento (ver el método verify() de la clase BasicTransfer)
    4. Si se encuentran diferencias en un segmento mayor a un día (Ej. fallo de verificación en 2018),
        framentar y verificar hasta encontrar los días con diferencias.

    Fase de Transferencia

    1. Transferir todos los segmentos identificados.
    2. Almacenar el listado de los segmentos transferidos dentro de la fase de actualizacion y de transferencia
        dentro del atributo updated.

    Las transferencias implementadas en esta clase debén de tomar un máximo de registros por segmento. En caso se
    exceda, el metodo hara "paquetes" para hacer una transferencia controlada.

    Atributos
    ---------
    src_dict : dict
        Información necesaria para describir una tabla [db, table, model, condition]
    dst_dict : dict
        Información necesaria para describir una tabla [db, table, model, condition]
    date_column : string
        Columa de fecha y hora por la cual hacer el la transferencia
    max_transfer : int
    updated_days : list

    Métodos
    -------
    verify():
        Verifica si la fuente y el destino son compatibles.
        Verifica si el destino es igual a la fuente.
    transfer(retires=0, max_tranfer=1000):
        Tranfiere los datos de la fuente a el destino.
    """
    pass
