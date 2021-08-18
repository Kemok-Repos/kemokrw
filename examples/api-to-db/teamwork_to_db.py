"""
Ejemplo del uso del cliente y extractor de Teamwork

Tomar en cuenta que se debe de  ingresar las credenciales de Teamwork y de la conexion a la base de datos.

"""
from kemokrw.client_teamwork import TeamworkClient
from kemokrw.extract_teamwork import ExtractTeamwork
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer

TOKEN = 'Inserta tu token aquí'

DB = LoadDB.built_connection_string('user', 'password', 'ip', 'puerto', 'base de datos')

if __name__ == '__main__':

    client = TeamworkClient(user_token=TOKEN)

    # Transfer projects
    src = ExtractTeamwork.get_model(client, DB, 6)
    dst = LoadDB.query_model(DB, 'tw_projects')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer project categories
    src = ExtractTeamwork.get_model(client, DB, 7)
    dst = LoadDB.query_model(DB, 'tw_project_categories')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer milestones
    src = ExtractTeamwork.get_model(client, DB, 8)
    dst = LoadDB.query_model(DB, 'tw_milestones')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer tasklists
    src = ExtractTeamwork.get_model(client, DB, 9)
    dst = LoadDB.query_model(DB, 'tw_tasklists')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer tasks
    src = ExtractTeamwork.get_model(client, DB, 10)
    dst = LoadDB.query_model(DB, 'tw_tasks')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer people
    src = ExtractTeamwork.get_model(client, DB, 11)
    dst = LoadDB.query_model(DB, 'tw_people')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
