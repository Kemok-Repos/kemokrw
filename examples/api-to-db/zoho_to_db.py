from kemokrw.client_zoho import ZohoClient
from kemokrw.extract_zoho import ExtractZoho
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer

DB = LoadDB.built_connection_string('user', 'password', 'ip', 'port', 'database')

if __name__ == '__main__':
    client = ZohoClient(filepath='self_client.json')

    # Transferir modulos
    src = ExtractZoho.get_model(client, DB, 12)
    dst = LoadDB.query_model(DB, 'zh_modules')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transferir contactos
    src = ExtractZoho.get_model(client, DB, 13)
    dst = LoadDB.query_model(DB, 'zh_contacts')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
