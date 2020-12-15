import os
from .dataset_cls import DatalakeClientDataset


def get_project_config():
    return dict(
        tenant=os.getenv("C360_TENANT"),
        stage=_get_stage(),
        api_key=os.getenv("C360_API_KEY"),
        api_url=os.getenv("C360_API_URL", _get_default_api_url()),
        boto_endpoint_url=os.getenv("C360_BOTO_ENDPOINT_URL"),
    )


def _get_stage():
    stage = os.getenv("C360_STAGE", "prod")

    if stage not in ("prod", "staging"):
        raise RuntimeError(
            "Error configuring client: `C360_STAGE` can only be 'prod' or"
            f" 'staging' (got {stage})."
        )


def _get_default_api_url():
    tenant = os.getenv("C360_TENANT")
    stage = _get_stage()

    if stage == "prod":
        return f"https://api.{tenant}c360.ai"
    elif stage == "staging":
        return f"https://staging.api.{tenant}c360.ai"


dataset = DatalakeClientDataset(**get_project_config())
