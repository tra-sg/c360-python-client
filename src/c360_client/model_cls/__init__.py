import os
import yaml
from time import sleep

from c360_client.request_cls import DatalakeClientRequest
from c360_client.utils import get_boto_client

class DatalakeClientModel:
    def __init__(self):
        self.request_inst = DatalakeClientRequest()

    def _request(self, endpoint, **kwargs):
        return self.request_inst.request(
            endpoint=endpoint, **kwargs,
        )

    def get(self, name, groups=[]):
        endpoint = f"pipelines/{name}"
        # should we do pipelines/common/** vs pipelines/users/ghosalya/** ??
        response = self._request(endpoint, method="GET")
        return response

    def experiment_train(
        self, name, data_source, label, model_type="CLASSIFICATION", description=""
    ):
        """
        data_source:
        """
        endpoint = f"model/exp_train"
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
