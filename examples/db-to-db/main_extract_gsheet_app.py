from kemokrw.extract_gsheet import *
import yaml

with open('ejemplo_extract_gsheet.yaml') as file:
    extract_config = yaml.load(file, Loader=yaml.FullLoader)

spreadsheet_id =extract_config["spreadsheet_id"]
sheet = extract_config["sheet"]
header = extract_config["header"]
model = {}
model["db"] = extract_config["db"]
model["table"] = extract_config["table"]
model["model"] = extract_config["model"]

gsheet = ExtractGSheet(spreadsheet_id=spreadsheet_id,
                       sheet=sheet,
                       header=header,
                       model=model)
k = gsheet.tranfer()

