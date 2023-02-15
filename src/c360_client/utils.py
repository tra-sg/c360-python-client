import os
import boto3
import discreetly
import json


def get_boto_client(name, **kwargs):
    """
    Get a boto client. If you are within Colab and you have logged in,
    use the project's stored AWS credentials. Otherwise, create boto
    client as per normal.
    """
    if is_notebook_mode():
        aws_access_key_id, aws_secret_access_key = get_aws_credentials()
        aws_creds = dict(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="ap-southeast-1",
        )
        return boto3.client(name, **aws_creds, **kwargs)
    else:
        return boto3.client(name, **kwargs)


def get_secret_session():
    cfg = {
        "default": {
            "type": "gcp",
            "datastore_project": f"{_get_tenant()}-c360",
            "keyid": (
                f"projects/{_get_tenant()}-c360-kms/locations/"
                "global/keyRings/dev/cryptoKeys/default"
            )
        }
    }
    return discreetly.Session.create(config=cfg)


def get_aws_credentials():
    session = get_secret_session()
    creds = json.loads(session.get(f"/{_get_tenant()}/dev/api/aws_credentials"))

    return creds["aws_access_key_id"], creds["aws_secret_access_key"]


def set_notebook_mode(mode=True):
    if mode:
        os.environ["C360_CLIENT_NOTEBOOK_MODE"] = "TRUE"
    else:
        del os.environ["C360_CLIENT_NOTEBOOK_MODE"]


def is_notebook_mode():
    return (os.getenv("C360_CLIENT_NOTEBOOK_MODE", "").upper() == "TRUE")


#########
#  Library Configuration
#########


__TENANT = None


def _set_tenant(tenant):
    # A hard override of the tenant value
    global __TENANT
    __TENANT = tenant


def _get_tenant():
    if not __TENANT:
        env_tenant = os.getenv("C360_TENANT")

        if env_tenant is None:
            raise RuntimeError("C360_TENANT environment variable not available")

        _set_tenant(env_tenant)

    return __TENANT



__STAGE = None


def _set_stage(stage):
    global __STAGE

    if stage not in ("prod", "staging"):
        raise RuntimeError(
            "Error configuring client: `C360_STAGE` can only be 'prod' or"
            f" 'staging' (got {stage})."
        )

    __STAGE = stage


def _get_stage():
    if not __STAGE:
        stage = os.getenv("C360_STAGE", "prod")
        _set_stage(stage)

    return __STAGE

def _get_api_url(tenant, stage):
    if stage == "prod":
        return f"https://api.{tenant}.c360.ai"
    elif stage == "staging":
        return f"https://api-staging.{tenant}.c360.ai"

def _get_default_api_url():
    tenant = _get_tenant()
    stage = _get_stage()
    return _get_api_url(tenant, stage)
