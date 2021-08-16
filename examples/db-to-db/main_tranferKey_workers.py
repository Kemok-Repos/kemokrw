from kemokrw.transfer_db_key import DbKeyTransfer
import yaml
from multiprocessing import Pool


if __name__ == "__main__":
    # Cargar configuración ejemplo de una base de datos de origen
    with open('ejemplo_origen.yaml') as file:
        src_config = yaml.load(file, Loader=yaml.FullLoader)

    # Cargar configuración ejemplo de una base de datos de destino
    with open('ejemplo_destino.yaml') as file:
        dst_config = yaml.load(file, Loader=yaml.FullLoader)

    # Para utilizar multiprocessing pasar True al constructor.
    trf = DbKeyTransfer(src_config, dst_config,  0, Multiprocessing=True)
    with Pool(2) as p:
        #(verificar varios parametros)
        Params = [True, False]
        p.map(trf.tranfer, Params)

        p.close()
        p.join()
        print('end main')



