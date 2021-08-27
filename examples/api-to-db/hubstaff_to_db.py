from kemokrw.client_hubstaff import HubstaffClient
from kemokrw.extract_hubstaff import ExtractHubstaff
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer
from sqlalchemy import create_engine
import datetime

PAT = ""
DB = ""

if __name__ == '__main__':

    client = HubstaffClient(refresh_token=PAT)

    # Transferir proyectos
    src = ExtractHubstaff.get_model(client, DB, 2)
    dst = LoadDB.query_model(DB, 'hs_projects')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transferir actividades
    condicion = "WHERE (starts_at AT TIME ZONE 'UTC')::date = '{}'"
    x = datetime.date(2021, 8, 25)
    while x < datetime.date.today():
        print(str(x))
        src = ExtractHubstaff.get_model(client, DB, 3, params={"date": str(x)})
        dst = LoadDB.query_model(DB, 'hs_activities', condition=condicion.format(str(x)))
        trf = BasicTransfer(src, dst)
        trf.transfer(2)
        x += datetime.timedelta(days=1)

    # Transferir actividades de applicacion
    condicion = "WHERE (time_slot AT TIME ZONE 'UTC')::date = '{}'"
    x = datetime.date(2021, 8, 25)
    while x < datetime.date.today():
        print(str(x))
        src = ExtractHubstaff.get_model(client, DB, 4, params={"date": str(x)})
        dst = LoadDB.query_model(DB, 'hs_application_activities', condition=condicion.format(str(x)))
        trf = BasicTransfer(src, dst)
        trf.transfer(2)
        x += datetime.timedelta(days=1)

    model = {'col1': {'name': 'id', 'type': 'integer'}, 'col2': {'name': 'site', 'type': 'text'}, 'col3': {'name': 'date', 'type': 'date'}, 'col4': {'name': 'created_at', 'type': 'timestamp with time zone'}, 'col5': {'name': 'updated_at', 'type': 'timestamp with time zone'}, 'col6': {'name': 'time_slot', 'type': 'timestamp with time zone'}, 'col7': {'name': 'user_id', 'type': 'integer'}, 'col8': {'name': 'project_id', 'type': 'integer'}, 'col9': {'name': 'tracked', 'type': 'integer'}, 'col10': {'name': 'details', 'type': 'text'}}

    # Transferir actividades de url
    condicion = "WHERE (time_slot AT TIME ZONE 'UTC')::date = '{}'"
    x = datetime.date(2021, 8, 25)
    while x < datetime.date.today() + datetime.timedelta(days=1):
        print(str(x))
        src = ExtractHubstaff.get_model(client, DB, 5, params={"date": str(x)})
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

    src = ExtractHubstaff.get_model(client, DB, 1, by_list=users)
    dst = LoadDB.query_model(DB, 'hs_users')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
