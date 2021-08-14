from kemokrw.extract import Extract
from kemokrw.func_api import match_model
import kemokrw.config_api as config
import pandas as pd


class ExtractZoho(Extract):

    def __init__(self, client, url, endpoint, endpoint_type, response_key, model, params=dict(), by_list=None):
        self.client = client
        self.url = url
        self.endpoint = endpoint
        self.endpoint_type = endpoint_type
        self.response_key = response_key
        self.by_list = by_list
        self.model = model
        self.metadata = None
        self.data = None
        self.params = params
        self.get_metadata()

    @classmethod
    def get_template(cls, client, endpoint, params=dict(), by_list=None):
        endpoints = config.ZOHO['by_list'] + config.ZOHO['all']
        if endpoint not in endpoints:
            raise Exception('Endpoint is not a valid. Check config_api configuration file.')
        model = config.ZOHO[endpoint]['model']
        url = config.ZOHO[endpoint]['base_url']
        if endpoint in config.ZOHO['by_list']:
            endpoint_type = 'by_list'
        elif endpoint in config.ZOHO['all']:
            endpoint_type = 'all'
        response_key = config.ZOHO[endpoint]['key']
        return cls(client, url, endpoint, endpoint_type, response_key, model, params, by_list)

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
        if self.endpoint_type == 'by_list':
            data = []
            for i in self.by_list:
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
                if 'info' in response.keys():
                    pages = response['info']['more_records']
                    page_number += 1
                    self.params["page"] = page_number
                else:
                    pages = False
        if 'page' in self.params.keys():
            self.params.pop('page')

        self.data = match_model(self.model, self.data)
