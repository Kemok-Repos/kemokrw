from kemokrw.load_gsheet import *
import yaml

with open('ejemplo_load_gsheet.yaml') as file:
    extract_config = yaml.load(file, Loader=yaml.FullLoader)

spreadsheet_id =extract_config["spreadsheet_id"]
sheet = extract_config["sheet"]
header = extract_config["header"]
model = {}
model["db"] = extract_config["db"]
model["table"] = extract_config["table"]
model["model"] = extract_config["model"]

gsheet = LoadGSheet(spreadsheet_id=spreadsheet_id,
                    sheet=sheet,
                    header=header,
                    model=model,
                    condition='',
                    order='')

gsheet.save_data()


