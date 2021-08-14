from kemokrw.client_teamwork import TeamworkClient
from kemokrw.extract_teamwork import ExtractTeamwork
from kemokrw.load_db import LoadDB
from kemokrw.transfer_basic import BasicTransfer

TOKEN = "twp_3IMa8ceY3g82sfyxrLWf18rUJ8Sv"

DB = LoadDB.built_connection_string('kemok_bi','9214dbf05b71d25ae3e482c4b56eca3c','45.56.117.5','5432','kemok_bi')

if __name__ == '__main__':
    client = TeamworkClient(user_token=TOKEN)

    # Transfer projects
    src = ExtractTeamwork.get_template(client, endpoint='projects')
    dst = LoadDB.query_model(DB, 'tw_projects')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer project categories
    src = ExtractTeamwork.get_template(client, endpoint='projectcategories')
    dst = LoadDB.query_model(DB, 'tw_project_categories')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer milestones
    src = ExtractTeamwork.get_template(client, endpoint='milestones')
    dst = LoadDB.query_model(DB, 'tw_milestones')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer tasklists
    src = ExtractTeamwork.get_template(client, endpoint='tasklists')
    dst = LoadDB.query_model(DB, 'tw_tasklists')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer tasks
    src = ExtractTeamwork.get_template(client, endpoint='tasks')
    dst = LoadDB.query_model(DB, 'tw_tasks')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)

    # Transfer tasklists
    src = ExtractTeamwork.get_template(client, endpoint='people')
    dst = LoadDB.query_model(DB, 'tw_people')
    trf = BasicTransfer(src, dst)
    trf.transfer(2)
