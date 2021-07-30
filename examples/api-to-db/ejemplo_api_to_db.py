from kemokrw.client_hubstaff import HubstaffClient
from kemokrw.extract_hubstaff import ExtractHubstaff
from kemokrw.load_db import LoadDB
from kemokrw.extract_db import ExtractDB
from kemokrw.transfer_basic import BasicTransfer

PAT = "eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsImtpZCI6ImRlZmF1bHQifQ.eyJqdGkiOiJVRktKcE5aMSIsImlzcyI6Imh0dHBzOi8vYWNjb3VudC5odWJzdGFmZi5jb20iLCJleHAiOjE2MzM0NTU4MzcsImlhdCI6MTYyNTY3OTgzNywic2NvcGUiOiJvcGVuaWQgcHJvZmlsZSBlbWFpbCBodWJzdGFmZjpyZWFkIGh1YnN0YWZmOndyaXRlIn0.Z3p-GAB09NHnNtRwNNa9TqRnJgl32v4TOGDPqqnsnwF4G72iqpto-aA5Wi0wHdUilvldaDOzq-9fgo14KB-WjH24E35BLlRpimB-AakG1BbILp0I7KaxUMWisQGbwpqE5KyumBv0w-PSyRSVPliPsjSdh3VjTKayqxAPq4JjcwqYh6na6-3c3_IPa2Tioh9EK9eQxgnsby6jEbRSwJ1UjTNqBVVqP9DdqodoLZD_hAIe3KWlDzCtcmNNUlcWyqL6hsah6pAcINznHm6Wo4bro3Phxrzt9VeTdWUC3VqSa-bDuI8TW1jYs-rN1MWq8AZVWpw_PHJFJ-_PYpGohJdX3A"

DB = LoadDB.built_connection_string('kemok_bi','9214dbf05b71d25ae3e482c4b56eca3c','45.56.117.5','5432','kemok_bi')



if __name__ == '__main__':

    client = HubstaffClient(refresh_token=PAT)
    #users = [933603, 922083, 921958, 933707, 933348, 921323, 933704]
    #model = config.HUBSTAFF['users']['model']
    #ex1 = ExtractHubstaff(client, url='https://api.hubstaff.com/v2/organizations/{organization_id}/projects', endpoint='projects', endpoint_type='by_organization',
    #                      response_key='projects', model=model)
    #print(ex1.metadata)

    # Crear objeto de carga

    #print(dst1.metadata)



    # Transferir proyectos
    # src_project = ExtractHubstaff.get_template(client, endpoint='projects')
    # dst_project = LoadDB.query_model(DB, 'hs_projects')
    # trf = BasicTransfer(src_project, dst_project)
    # trf.transfer(2)

    src_activities = ExtractHubstaff.get_template(client, endpoint='activities', params={"date": "2021-07-29"})
    print(len(src_activities.data))
    src_activities.get_data()
    #src_activities.data.to_csv('ejemplo.csv')
    #dst_activities = LoadDB.query_model(DB, 'hs_activities')
    #trf = BasicTransfer(src_activities, dst_activities)
    #trf.transfer(2)

