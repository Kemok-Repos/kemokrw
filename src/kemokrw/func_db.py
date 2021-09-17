from re import match
from sqlalchemy.exc import OperationalError, DatabaseError
import kemokrw.config_db as config
import pandas as pd


def model_format_check(model):
    """ Revisa que el modelo se encuentre en el formato correcto. """
    for i in model:
        if not match("^col\d+$", i):
            raise Exception("El formato del modelo es incorrecto: {}".format(i))


def execute_srquery(conn, query):
    """ Ejecuta un query con un resultario unitario. """
    attempts = 0
    while attempts < 3:
        try:
            result = conn.execute(query)
            for i in result:
                return i[0]
            break
        except OperationalError as err:
            attempts += 1
            if attempts == 3:
                raise err
        except DatabaseError as err:
            attempts += 1
            if attempts == 3:
                raise err

def get_db_collation(conn,dbms,param):
    connection = conn
    query = config.DB_COLLATION[dbms][param]
    rs = connection.execute(query)
    collation = rs.fetchone()[0]
    return collation


<<<<<<< HEAD
def get_db_metadata(conn, dbms, model, table, condition):
    """ Revisa la conexión y la metadata de un tabla sin extraer los datos. """
=======
def get_db_metadata(conn, dbms, model, table, condition, key):
    # Incluir manejo de errores para verificar conexión a base de datos 3 veces
>>>>>>> @{u}
    connection = conn
    execute_srquery(connection, "SELECT 1;")

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

            orderby = "order by {}".format(key) if col["type"] == "text" and j=="check_hash" else ""
            query = config.COLUMN_CHECK[dbms][col["type"]][j].format(column=model[i]["name"], table=table,
                                                                     condition=condition, order=orderby)
            #print(query)
            col[j] = execute_srquery(connection, query)
            #print(col[j])

        columns[i] = col

    metadata["columns"] = columns

    if metadata is None:
        raise Exception("Failed to extract metadata. Check model.")

    return metadata

def get_db_metadata_pd(df:pd.DataFrame, model, condition, key):
    # Incluir manejo de errores para verificar conexión a base de datos 3 veces

    metadata = dict()
    metadata["ncols"] = df.columns

    #query = config.TABLE_CHECK["check_rows"].format(table=table, condition=condition)
    metadata["check_rows"] = df.shape[0]

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

        for j in config.COLUMN_CHECK['pandas'][col["type"]]:

            orderby = "order by {}".format(key) if col["type"] == "text" and j=="check_hash" else ""
            query = config.COLUMN_CHECK[dbms][col["type"]][j].format(column=model[i]["name"], table=table,
                                                                     condition=condition, order=orderby)
            #print(query)
            col[j] = execute_srquery(connection, query)
<<<<<<< HEAD
            if j == 'check_sum' and col[j]:
                col[j] = float(col[j])
=======
            #print(col[j])

>>>>>>> @{u}
        columns[i] = col

    metadata["columns"] = columns

    if metadata is None:
        raise Exception("Failed to extract metadata. Check model.")

    return metadata
