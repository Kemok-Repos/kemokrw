from re import match
import kemokrw.config_db as config


def model_format_check(model):
    for i in model:
        if not match("^col\d+$", i):
            raise Exception("El formato del modelo es incorrecto: {}".format(i))


def execute_srquery(conn, query):
    """Ejecuta un query con un resultario unitario"""
    result = conn.execute(query)
    for i in result:
        return i[0]


def get_db_metadata(conn, dbms, model, table, condition):
    # Incluir manejo de errores para verificar conexión a base de datos 3 veces
    connection = conn
    connection.execute("SELECT 1;")

    metadata = dict()
    metadata["ncols"] = len(model)

    query = config.TABLE_CHECK["check_rows"].format(table=table, condition=condition)
    metadata["check_rows"] = execute_srquery(connection, query)

    columns = dict()

    for i in model:
        col = dict()
        col["subtype"] = model[i]["type"]
        col_type = col["subtype"].upper()
        col_type = ''.join(e for e in col_type if e.isalpha() or e.isspace() or e == '[' or e == ']')
        for j in config.COLUMN_TYPES:
            if col_type in config.COLUMN_TYPES[j]:
                col["type"] = j
        if "type" not in col.keys():
            print("*WARNING*: {} no es un tipo identificado.".format(col["subtype"]))
            col["type"] = "other"

        for j in config.COLUMN_CHECK[dbms][col["type"]]:
            query = config.COLUMN_CHECK[dbms][col["type"]][j].format(column=model[i]["name"], table=table,
                                                                     condition=condition)
            col[j] = execute_srquery(connection, query)

        columns[i] = col

    metadata["columns"] = columns

    if metadata is None:
        raise Exception("Failed to extract metadata. Check model.")

    return metadata
