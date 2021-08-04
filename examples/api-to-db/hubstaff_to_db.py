from kemokrw.client_hubstaff import HubstaffClient
from kemokrw.extract_hubstaff import ExtractHubstaff
from kemokrw.load_db import LoadDB
from sqlalchemy import create_engine
from kemokrw.transfer_basic import BasicTransfer
import datetime

PAT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImRlZmF1bHQifQ.eyJqdGkiOiJVRktKcE5aMSIsImlzcyI6Imh0dHBzOi8vYWNjb3VudC5odWJzdGFmZi5jb20iLCJleHAiOjE2MzM0NTU4MzcsImlhdCI6MTYyNTY3OTgzNywic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBodWJzdGFmZjpyZWFkIGh1YnN0YWZmOndyaXRlIn0.Z3p-GAB09NHnNtRwNNa9TqRnJgl32v4TOGDPqqnsnwF4G72iqpto-aA5Wi0wHdUilvldaDOzq-9fgo14KB-WjH24E35BLlRpimB-AakG1BbILp0I7KaxUMWisQGbwpqE5KyumBv0w-PSyRSVPliPsjSdh3VjTKayqxAPq4JjcwqYh6na6-3c3_IPa2Tioh9EK9eQxgnsby6jEbRSwJ1UjTNqBVVqP9DdqodoLZD_hAIe3KWlDzCtcmNNUlcWyqL6hsah6pAcINznHm6Wo4bro3Phxrzt9VeTdWUC3VqSa-bDuI8TW1jYs-rN1MWq8AZVWpw_PHJFJ-_PYpGohJdX3A"

DB = LoadDB.built_connection_string('kemok_bi','9214dbf05b71d25ae3e482c4b56eca3c','45.56.117.5','5432','kemok_bi')



if __name__ == '__main__':

    client = HubstaffClient(refresh_token=PAT)

    # Transferir proyectos
    src = ExtractHubstaff.get_template(client, endpoint='projects')
    dst = LoadDB.query_model(DB, 'hs_projects')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transferir actividades
    condicion = "WHERE (starts_at AT TIME ZONE 'UTC')::date = '{}'"
    x = datetime.date(2021, 2, 1)
    while x < datetime.date.today():
        print(str(x))
        src = ExtractHubstaff.get_template(client, endpoint='activities', params={"date": str(x)})
        dst = LoadDB.query_model(DB, 'hs_activities', condition=condicion.format(str(x)))
        trf = BasicTransfer(src, dst)
        trf.transfer(2)
        x += datetime.timedelta(days=1)

    # Transferir actividades de applicacion
    condicion = "WHERE (time_slot AT TIME ZONE 'UTC')::date = '{}'"
    x = datetime.date(2021, 7, 1)
    while x < datetime.date.today():
        print(str(x))
        src = ExtractHubstaff.get_template(client, endpoint='application_activities', params={"date": str(x)})
        dst = LoadDB.query_model(DB, 'hs_application_activities', condition=condicion.format(str(x)))
        trf = BasicTransfer(src, dst)
        trf.transfer(2)
        x += datetime.timedelta(days=1)

    model = {'col1': {'name': 'id', 'type': 'integer'}, 'col2': {'name': 'site', 'type': 'text'}, 'col3': {'name': 'date', 'type': 'date'}, 'col4': {'name': 'created_at', 'type': 'timestamp with time zone'}, 'col5': {'name': 'updated_at', 'type': 'timestamp with time zone'}, 'col6': {'name': 'time_slot', 'type': 'timestamp with time zone'}, 'col7': {'name': 'user_id', 'type': 'integer'}, 'col8': {'name': 'project_id', 'type': 'integer'}, 'col9': {'name': 'tracked', 'type': 'integer'}, 'col10': {'name': 'details', 'type': 'text'}}

    # Transferir actividades de url
    condicion = "WHERE (time_slot AT TIME ZONE 'UTC')::date = '{}'"
    x = datetime.date(2021, 7, 1)
    while x < datetime.date.today() + datetime.timedelta(days=1):
        print(str(x))
        src = ExtractHubstaff.get_template(client, endpoint='url_activities', params={"date": str(x)})
        dst = LoadDB(DB, 'hs_url_activities', model, condition=condicion.format(str(x)))
        trf = BasicTransfer(src, dst)
        trf.transfer(2)
        x += datetime.timedelta(days=1)

    # Transferir usuarios
    engine = create_engine(DB)
    connection = engine.connect()
    user_query = connection.execute('SELECT DISTINCT user_id FROM hs_activities;')
    connection.close()
    users = []
    for i in user_query:
        users.append(int(i[0]))

    src = ExtractHubstaff.get_template(client, endpoint='users', id_list=users)
    dst = LoadDB.query_model(DB, 'hs_users')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
