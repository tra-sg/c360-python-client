import os
from .request_cls import DatalakeClientRequest
from .dataset_cls import DatalakeClientDataset
from .model_cls import DatalakeClientModel
from .utils import _get_tenant, _get_stage, _get_default_api_url


def get_project_config():
    return dict(
        tenant=_get_tenant(),
        stage=_get_stage(),
        api_key=os.getenv("C360_API_KEY"),
        api_url=os.getenv("C360_API_URL", _get_default_api_url()),
    )


api = DatalakeClientRequest(**get_project_config())

dataset = DatalakeClientDataset()
model = DatalakeClientModel()
