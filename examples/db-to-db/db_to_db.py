from kemokrw.extract_db import ExtractDB
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
import yaml


if __name__ == "__main__":
    # Cargar configuración ejemplo de una base de datos de origen
    with open('ejemplo_origen.yaml') as file:
        src_config = yaml.load(file, Loader=yaml.FullLoader)

    # Cargar configuración ejemplo de una base de datos de destino
    with open('ejemplo_destino.yaml') as file:
        dst_config = yaml.load(file, Loader=yaml.FullLoader)

    # Crear objeto de extracción
    src = ExtractDB(src_config['db'], src_config['table'], src_config['model'])
    print(src.metadata)

    # Crear objeto de carga
    dst = LoadDB(dst_config['db'], dst_config['table'], dst_config['model'])
    print(dst.metadata)

    src.get_data()

    # Crear objeto de transferencia
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
