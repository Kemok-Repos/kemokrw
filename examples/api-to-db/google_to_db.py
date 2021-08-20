from kemokrw.client_google import GoogleClient
from kemokrw.extract_gsheet import ExtractGSheet
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
import pandas as pd

DB = LoadDB.built_connection_string('user', 'pass', '45.79.216.118', '5432', 'srtendero')


if __name__ == '__main__':

    client = GoogleClient('credentials.json', 'token.json')

    # Transferir
    src = ExtractGSheet.get_model(client, DB, 4)
    dst = LoadDB.query_model(DB, 'g2s_metas')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
