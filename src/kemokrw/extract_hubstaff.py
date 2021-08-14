from kemokrw.extract import Extract
from kemokrw.func_api import match_model
import kemokrw.config_api as config
import pandas as pd
from datetime import datetime, date, timezone, timedelta


class ExtractHubstaff(Extract):

    def __init__(self, client, url, endpoint, endpoint_type, response_key, model, params=dict(), id_list=None):
        self.client = client
        self.url = url
        self.endpoint = endpoint
        self.endpoint_type = endpoint_type
        self.response_key = response_key
        self.id_list = id_list
        self.url_params = {'organization_id': str(self.client.organization_id), 'id': '{id}'}
        self.model = model
        self.metadata = None
        self.data = None
        if 'date' in params.keys():
            start_date = date.fromisoformat(params['date'])
            start_date = datetime(start_date.year, start_date.month, start_date.day, tzinfo=timezone.utc)
            end_date = date.fromisoformat(params['date']) + timedelta(days=1)
            end_date = datetime(end_date.year, end_date.month, end_date.day, tzinfo=timezone.utc)
            params['time_slot[start]'] = start_date.isoformat()
            params['time_slot[stop]'] = end_date.isoformat()
            params.pop('date')
        self.params = params
        self.get_metadata()

    @classmethod
    def get_template(cls, client, endpoint, params=dict(), id_list=None):
        endpoints = config.HUBSTAFF['by_id'] + config.HUBSTAFF['by_organization'] + config.HUBSTAFF['by_project']
        if endpoint not in endpoints:
            raise Exception(str(endpoint)+' is not a valid endpoint. Check config_api configuration file.')
        model = config.HUBSTAFF[endpoint]['model']
        url = config.HUBSTAFF[endpoint]['base_url']
        if endpoint in config.HUBSTAFF['by_id']:
            endpoint_type = 'by_id'
        elif endpoint in config.HUBSTAFF['by_organization']:
            endpoint_type = 'by_organization'
        elif endpoint in config.HUBSTAFF['by_project']:
            endpoint_type = 'by_project'
        response_key = config.HUBSTAFF[endpoint]['key']
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
        url = self.url.format(**self.url_params)
        if self.endpoint_type == 'by_id':
            data = []
            for i in self.id_list:
                response = self.client.get(url.format(id=i), params=self.params)
                data.append(response[self.response_key])
            self.data = pd.DataFrame(data)
        elif self.endpoint_type == 'by_organization':
            while True:
                response = self.client.get(url, params=self.params)
                page = pd.DataFrame(response[self.response_key])
                self.data = pd.concat([self.data, page])
                if 'pagination' not in response.keys():
                    break
                self.params['page_start_id'] = response['pagination']['next_page_start_id']
        if 'page_start_id' in self.params.keys():
            self.params.pop('page_start_id')

        self.data = match_model(self.model, self.data)
