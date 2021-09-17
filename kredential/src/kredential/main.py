import passboltapi

def get_my_passwords(passbolt_obj,Resource):
    result = list()
    for i in passbolt_obj.get(url="/resources.json?api-version=v2")["body"]:

        result.append({
            "id": i["id"],
            "name": i["name"],
            "username": i["username"],
            "uri": i["uri"]
        })
    for i in result:
        resource = passbolt_obj.get(
            "/secrets/resource/{}.json?api-version=v2".format(i["id"]))
        i["password"] = passbolt_obj.decrypt(resource["body"]["data"])
    for k in result:
        if Resource in k['name']:
            print(k["password"])

def main():
    with passboltapi.PassboltAPI(config_path="config.ini") as passbolt:
        get_my_passwords(passbolt_obj=passbolt, Resource="Teamwork - API")

if __name__ == '__main__':
    main()


