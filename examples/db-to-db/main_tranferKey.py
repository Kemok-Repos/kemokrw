from kemokrw.transfer_db_key import DbKeyTransfer
import yaml


if __name__ == "__main__":
    # Cargar configuración ejemplo de una base de datos de origen
    with open('ejemplo_origen.yaml') as file:
        src_config = yaml.load(file, Loader=yaml.FullLoader)

    # Cargar configuración ejemplo de una base de datos de destino
    with open('ejemplo_destino.yaml') as file:
        dst_config = yaml.load(file, Loader=yaml.FullLoader)

    trf = DbKeyTransfer(src_config, dst_config,'idfacturadetalle', 0)
    trf.tranfer()

