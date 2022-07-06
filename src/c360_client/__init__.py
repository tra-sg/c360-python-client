import os
from .dataset_cls import DatalakeClientDataset
from .utils import _get_tenant, _get_stage, _get_default_api_url


os.environ["GOOGLE_CLOUD_PROJECT"]=f"{_get_tenant()}-c360"

def get_project_config():
    return dict(
        tenant=_get_tenant(),
        stage=_get_stage(),
        api_key=os.getenv("C360_API_KEY"),
        api_url=os.getenv("C360_API_URL", _get_default_api_url()),
    )


dataset = DatalakeClientDataset(**get_project_config())
