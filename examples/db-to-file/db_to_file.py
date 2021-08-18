from kemokrw.extract_db import ExtractDB
from kemokrw.load_file import LoadFile
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer

DB = LoadDB.built_connection_string('user', 'password', 'ip', 'port', 'database')

if __name__ == '__main__':
    src = ExtractDB.query_model(DB, 'maestro_de_modelos')

    model = {'col1': {'name': 'id', 'type': 'integer'},
             'col2': {'name': 'modelo', 'type': 'text'},
             'col3': {'name': 'id_origen', 'type': 'integer'},
             'col4': {'name': 'configuracion', 'type': 'jsonb'}}

    print('Creando archivo de excel')
    dst = LoadFile('maestro_de_modelos.xlsx', 'holi', 'excel', model)
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    print('Creando archivo csv')
    dst = LoadFile('maestro_de_modelos.csv', 'Projects', 'csv', model)
    trf = BasicTransfer(src, dst)
    trf.transfer(2)