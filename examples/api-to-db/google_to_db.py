from kemokrw.client_google import GoogleClient
from kemokrw.extract_gsheet import ExtractGSheet
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
import pandas as pd

DB = LoadDB.built_connection_string('user', 'pass', 'ip', 'port', 'database')


if __name__ == '__main__':

    client = GoogleClient('credentials.json', 'token.json')

    # Transferir
    src = ExtractGSheet.get_model(client, DB, 1)
    dst = LoadDB.query_model(DB, 'g2s_mapeo_de_dispositivos')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
