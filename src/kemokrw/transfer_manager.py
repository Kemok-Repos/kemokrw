from kemokrw.client_google import GoogleClient
from kemokrw.extract_gsheet import ExtractGSheet
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError, DatabaseError

QUERY = """
WITH modelos AS (
SELECT 
	maestro_de_modelos.id,
	maestro_de_modelos.modelo,
	maestro_de_modelos.configuracion||maestro_de_origen.propiedades AS configuracion
FROM maestro_de_modelos
LEFT JOIN maestro_de_origen ON maestro_de_modelos.id_origen = maestro_de_origen.id
)
SELECT 
	maestro_de_transferencias.id,
	'Transferir '||modelo_fuente.modelo||' a '||modelo_destino.modelo AS nombre,
	maestro_de_transferencias.clase_transfer,
	maestro_de_transferencias.clase_fuente,
	modelo_fuente.configuracion AS config_fuente, 
	maestro_de_transferencias.clase_destino,
	modelo_destino.configuracion AS config_destino
FROM maestro_de_transferencias 
LEFT JOIN modelos AS modelo_fuente ON id_fuente = modelo_fuente.id
LEFT JOIN modelos AS modelo_destino ON id_destino = modelo_destino.id
{0};
"""

DB = LoadDB.built_connection_string('bago', '1208af2802aee048ygg589462756050', '45.79.204.111', '5432', 'bago')


def manage_extraction(db, condition=''):
    """ Metodo """
    engine = create_engine(db)
    attempts = 0
    while attempts < 3:
        try:
            connection = engine.connect()
            config_query = connection.execute(QUERY.format(condition))
            connection.close()
            break
        except OperationalError as err:
            attempts += 1
            if attempts == 3:
                raise err
        except DatabaseError as err:
            attempts += 1
            if attempts == 3:
                raise err
    names = ['id', 'nombre', 'clase_transfer', 'clase_fuente', 'config_fuente', 'clase_destino', 'config_destino']
    transfer_list = []
    for i in config_query:
        config = dict()
        for j, k in enumerate(i):
            config[names[j]] = k
        transfer_list.append(config)

    for transfer in transfer_list:
        print(transfer['nombre'])
        try:
            # Creación de objeto fuente
            if transfer['clase_fuente'].lower() == 'extractgsheet':
                src_client = GoogleClient(transfer['config_fuente']['credential_file'],
                                          transfer['config_fuente']['token_file'])
                src = ExtractGSheet(src_client, transfer['config_fuente']['spreadsheetId'],
                                    transfer['config_fuente']['range'], transfer['config_fuente']['model'])
            else:
                print(transfer['clase_fuente'] + ' no es una clase de fuente valida.')

            # Creación de objeto destino
            if transfer['clase_destino'].lower() == 'loaddb':
                condition = ''
                order = ''
                if 'condition' in transfer['config_destino'].keys():
                    condition = transfer['config_destino']['condition']
                if 'order' in transfer['config_destino'].keys():
                    order = transfer['config_destino']['order']

                dst_db = LoadDB.built_connection_string('bago', '1208af2802aee048ygg589462756050', '45.79.204.111',
                                                        '5432', 'bago')
                dst = LoadDB(dst_db, transfer['config_destino']['table'], transfer['config_destino']['model'],
                             condition, order)

            else:
                print(transfer['clase_destino'] + ' no es una clase de destino valida.')

            # Transferencia
            if transfer['clase_transfer'].lower() == 'basictransfer':
                trf = BasicTransfer(src, dst)
                trf.transfer(2)
            else:
                print(transfer['clase_transfer'] + ' no es una clase de transferencia valida.')

        except KeyError as err:
            print('Error: no se encuentra la llave de configuración. ')
            print(err)

    return transfer_list


if __name__ == '__main__':
    a = manage_extraction(DB)
