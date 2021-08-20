from kemokrw.extract_db import ExtractDB
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer


if __name__ == "__main__":
    # Cargar configuración ejemplo de una base de datos de origen

    # Crear objeto de extracción
    #src = ExtractDB(src_config['db'], src_config['table'], src_config['model'])
    src = ExtractDB.query_model(DB1, 'products')

    # Crear objeto de carga
    #dst = LoadDB(dst_config['db'], dst_config['table'], dst_config['model'])
    dst = LoadDB.query_model(DB2, 'erp_pos_products')

    # Crear objeto de transferencia
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
