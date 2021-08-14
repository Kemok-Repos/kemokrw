from kemokrw.client_zoho import ZohoClient
from kemokrw.extract_zoho import ExtractZoho
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer

DB = LoadDB.built_connection_string('kemok_bi', '9214dbf05b71d25ae3e482c4b56eca3c', '45.56.117.5', '5432', 'kemok_bi')

if __name__ == '__main__':
    client = ZohoClient(filepath='self_client.json')

    # Transferir modulos
    src = ExtractZoho.get_template(client, endpoint='modules')
    dst = LoadDB.query_model(DB, 'zh_modules')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transferir contactos
    src = ExtractZoho.get_template(client, endpoint='contacts')
    dst = LoadDB.query_model(DB, 'zh_contacts')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
