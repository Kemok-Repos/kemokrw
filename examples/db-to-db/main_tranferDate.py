from kemokrw.transfer_db_date import DbDateTransfer
import yaml

if __name__ == "__main__":
    # Cargar configuración ejemplo de una base de datos de origen
    with open('ejemplo_origen.yaml') as file:
        src_config = yaml.load(file, Loader=yaml.FullLoader)

    # Cargar configuración ejemplo de una base de datos de destino
    with open('ejemplo_destino.yaml') as file:
        dst_config = yaml.load(file, Loader=yaml.FullLoader)

    trf = DbDateTransfer(src_config, dst_config, 0)
    #trf = DbKeyTransfer(src_config, dst_config, src_config['key_column'], 0)
    trf.tranfer(partitionDate='years_old')

