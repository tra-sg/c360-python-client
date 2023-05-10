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


def _configure_gcp_project():
    # If GCP project is not set, take the current tenant and set it.
    if not os.getenv("GOOGLE_CLOUD_PROJECT"):
        tenant = os.getenv("C360_TENANT")
        if tenant:
            os.environ["GOOGLE_CLOUD_PROJECT"] = f"{tenant}-c360"

_DEFAULTS = {}

def set_default_space(default_space):
    global _DEFAULTS
    _DEFAULTS["space"] = default_space

api = DatalakeClientRequest(**get_project_config(), defaults=_DEFAULTS)

dataset = DatalakeClientDataset(defaults=_DEFAULTS)
model = DatalakeClientModel(defaults=_DEFAULTS)
