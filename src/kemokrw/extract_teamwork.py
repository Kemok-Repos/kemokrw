from kemokrw.extract import Extract
from kemokrw.func_api import match_model
import kemokrw.config_api as config
import pandas as pd


class ExtractTeamwork(Extract):

    def __init__(self, client, url, endpoint, endpoint_type, response_key, model, params=dict(), id_list=None):
        self.client = client
        self.url = url
        self.endpoint = endpoint
        self.endpoint_type = endpoint_type
        self.response_key = response_key
        self.id_list = id_list
        self.model = model
        self.metadata = None
        self.data = None
        self.params = params
        self.get_metadata()

    @classmethod
    def get_template(cls, client, endpoint, params=dict(), id_list=None):
        endpoints = config.TEAMWORK['by_id'] + config.TEAMWORK['all']
        if endpoint not in endpoints:
            raise Exception(str(endpoint)+' is not a valid endpoint. Check config_api configuration file.')
        model = config.TEAMWORK[endpoint]['model']
        url = config.TEAMWORK[endpoint]['base_url']
        if endpoint in config.TEAMWORK['by_id']:
            endpoint_type = 'by_id'
        elif endpoint in config.TEAMWORK['all']:
            endpoint_type = 'all'
        response_key = config.TEAMWORK[endpoint]['key']
        return cls(client, url, endpoint, endpoint_type, response_key, model, params, id_list)

    def get_metadata(self):
        self.get_data()
        self.metadata = dict()
        self.metadata["ncols"] = len(self.model)
        if not self.data.empty:
            self.metadata["check_rows"] = len(self.data)
        else:
            self.metadata["check_rows"] = 0

        columns = dict()
        for i in self.model:
            col = dict()
            col["subtype"] = self.model[i]["type"]
            col_type = col["subtype"].upper()
            col_type = ''.join(e for e in col_type if e.isalpha() or e.isspace() or e == '[' or e == ']')
            for j in config.COLUMN_TYPES:
                if col_type in config.COLUMN_TYPES[j]:
                    col["type"] = j
            if "type" not in col.keys():
                print("*WARNING*: {} no es un tipo identificado.".format(col["subtype"]))
                col["type"] = "other"

            if col["type"] in ["numeric"] and not self.data.empty:
                col["check_sum"] = self.data[i].sum()
            elif col["type"] in ["boolean"] and not self.data.empty:
                col["check_true"] = self.data[i].sum()
            if not self.data.empty:
                col["check_nn"] = len(self.data[i]) - self.data[i].isna().sum()
            columns[i] = col
        self.metadata["columns"] = columns

    def get_data(self):
        """Genera un Dataframe con la respuesta de la API"""
        self.data = None
        url = self.url
        if self.endpoint_type == 'by_id':
            data = []
            for i in self.id_list:
                response = self.client.get(url.format(id=i), params=self.params)
                data.append(response[self.response_key])
            self.data = pd.DataFrame(data)
        elif self.endpoint_type == 'all':
            pages = True
            page_number = 1
            self.params["page"] = page_number
            while pages:
                response = self.client.get(url, params=self.params)
                page = pd.DataFrame(response[self.response_key])
                self.data = pd.concat([self.data, page])
                if 'x-page' in response.keys():
                    page_number += 1
                    self.params["page"] = page_number
                    pages = response['x-page'] != response['x-pages']
                else:
                    pages = False
        if not self.data.empty and 'x-records' in response.keys():
            if len(self.data) != int(response['x-records']):
                print('* Warning: {0}/{1} records *'.format(len(self.data), response['x-records']))
        elif self.data.empty and 'x-records' in response.keys():
            if int(response['x-records']) != 0:
                print('* Warning: 0/{} records *'.format(response['x-records']))
        if 'page_number' in self.params.keys():
            self.params.pop('page_number')

        self.data = match_model(self.model, self.data)
