import passboltapi
import os

def get_passbolt(passbolt_obj, full=None, resource=None, idresource=None):
    # implemnet passbolt api
    result = list()
    for i in passbolt_obj.get(url="/resources.json?api-version=v2")["body"]:
        result.append({
            "id": i["id"],
            "name": i["name"],
            "username": i["username"],
            "uri": i["uri"],
            "description": i["description"]
        })
    for i in result:
        data = passbolt_obj.get(
            "/secrets/resource/{}.json?api-version=v2".format(i["id"]))
        i["password"] = passbolt_obj.decrypt(data["body"]["data"])

    if full is None:
        for k in result:
            if resource is not None:
                if resource in k['name']:
                    return ({"id": k["id"], "name": k["name"],
                              "uri": k["uri"], "user": k["username"],
                              "password": k["password"],
                              "description": k["description"]})
            if idresource is not None:
                if idresource in k['id']:
                    return ({"id": k["id"], "name": k["name"],
                             "uri": k["uri"], "user": k["username"],
                             "password": k["password"],
                             "description": k["description"]})
    else:
        resources = []
        for k in result:
            resources.append({"id": k["id"], "name": k["name"],
                              "uri": k["uri"], "user": k["username"],
                              "password": k["password"]})
        return resources


def discover_credResource(resource):
    # # return user password credential by name resource in json format
    fileConfig = os.path.dirname(os.path.abspath(__file__)) +'/config.ini'
    with passboltapi.PassboltAPI(config_path=fileConfig) \
            as passbolt:
        return get_passbolt(passbolt_obj=passbolt, resource=resource)


def discover_credId(idresource):
    # return user password credential by idresource in json format
    fileConfig = os.path.dirname(os.path.abspath(__file__)) +'/config.ini'
    with passboltapi.PassboltAPI(config_path=fileConfig) \
            as passbolt:
        return get_passbolt(passbolt_obj=passbolt, idresource=idresource)


def discover_full():
    # return all user passbolt credential in json format
    fileConfig = os.path.dirname(os.path.abspath(__file__)) +'/config.ini'
    with passboltapi.PassboltAPI(config_path=fileConfig) \
            as passbolt:
        return get_passbolt(passbolt_obj=passbolt, full=True)

def json_to_sqlalchemy(cred):
    # convert  json to sqlalchemy string connection
    db = str(cred['description'][str(cred['description']).
             find("DB") + 3:str(cred['description']).find("\n")]).strip()
    data = cred['description'][str(cred['description']).find("port") + 6:]
    port = str(data)[:str(data).find("\n")]
    engine = data[str(data).find(":") + 1:]
    eng = ''
    if engine == 'psql':
        eng = 'postgresql'
    elif engine == 'sqlserver':
        eng = 'mssql+pymssql' + '1533'

    connection = "{}://{}:{}@{}/{}". \
        format(eng, cred['user'], cred['password'], cred['uri'], db)
    return connection
