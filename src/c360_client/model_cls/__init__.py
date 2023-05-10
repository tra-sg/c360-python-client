import os
import yaml
import requests
from time import sleep

from c360_client.request_cls import DatalakeClientRequest
from c360_client.utils import get_boto_client

class DatalakeClientModel:
    def __init__(self, defaults={}):
        self.request_inst = DatalakeClientRequest()

    def _request(self, endpoint, **kwargs):
        return self.request_inst.request(
            endpoint=endpoint, **kwargs,
        )

    def get(self, name, groups=[]):
        endpoint = f"models/{name}"
        # should we do pipelines/common/** vs pipelines/users/ghosalya/** ??
        response = self._request(endpoint, method="GET")
        return response

    def download(self, name, groups=[]):
        endpoint = f"models/{name}/download"
        response = self._request(endpoint, method="GET")

        urls = response.json()["urls"]

        for url in urls:
            path = url.split("?")[0].split("models/")[-1]
            path = path.replace("/", "--")

            pkl_res = requests.get(url, stream=True)
            if response.status_code == 200:
                with open(path, 'wb') as f:
                    pkl_res.raw.decode_content = True
                    shutil.copyfileobj(pkl_res.raw, f)
                print("written to:", path)


    def experiment_train(
        self, name, data_source, label, model_type="CLASSIFICATION", description=""
    ):
        """
        data_source:
        """
        endpoint = f"model/exp_train"

        data_source["groups"] = self.request_inst.get_groups(
            groups=data_source.get("groups", [])
        )

        payload = dict(
            model_name=name,
            data_source=data_source,
            label=label,
            model_type=model_type,
            description=description,
        )
        response = self._request(
            endpoint, method="POST", json=payload,
        )
        return response

    def experiment_status(self, name):
        endpoint = f"model/exp_train/{name}"
        response = self._request(endpoint, method="GET")
        return response

    def experiment_wait(self, name):
        while True:
            response = self.experiment_status(name=name)
            response_json = response.json()
            if response_json.get("status", "FAILED") != "IN_PROGRESS":
                return response
            else:
                sleep(3)
