from kemokrw.client_google import GoogleClient
from kemokrw.extract_gsheet import ExtractGSheet
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
import pandas as pd

DB = LoadDB.built_connection_string('bago','1208af2802aee048ygg589462756050','45.79.204.111','5432','bago')


if __name__ == '__main__':

    client = GoogleClient('credentials.json', 'token.json')

    # Transferir
    src = ExtractGSheet.query_model(client, DB, 2)
    dst = LoadDB.query_model(DB, 'g2s_facturas_a_incluir')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)