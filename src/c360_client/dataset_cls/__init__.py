import os
import json
import requests
import boto3
import getpass


class DatalakeClientDataset:
    def __init__(
        self, tenant=None, stage="prod", api_url=None, boto_endpoint_url=None, api_key=None
    ):
        """
        """
        self.api_key = api_key
        self.tenant = tenant
        self.stage = stage
        self.url = api_url

        boto_kwargs = {}
        if boto_endpoint_url:
            boto_kwargs["endpoint_url"] = boto_endpoint_url
        self._s3_client = boto3.client("s3", **boto_kwargs)

        # options
        self._is_user_scoped = True

        self._cached_user_scope = None

    def set_options(self, is_user_scoped=True):
        self._is_user_scoped = is_user_scoped

    def set_api_key(self):
        self.api_key = getpass.getpass("API_KEY:")

    def get_groups(self, groups):
        main_groups = []
        if self._is_user_scoped:
            main_groups.append("users")
            main_groups.append(self._get_user_scope())

        print(main_groups + groups)

        return main_groups + groups

    def _request(self, endpoint, **kwargs):

        # use API key
        if not kwargs.get("headers"):
            kwargs["headers"] = {}

        if not kwargs["headers"].get("Authorization"):
            kwargs["headers"]["Authorization"] = self.api_key

        req = requests.request(url=f"{self.url}/{endpoint}", **kwargs)
        return req

    def _get_user_scope(self, refresh=False):

        if (self._cached_user_scope is not None) and (refresh is not False):
            return self._cached_user_scope

        endpoint = "entity/user/scope"
        response = self._request(endpoint, method="GET")
        self._cached_user_scope = response.json().get("scope")

        return self._cached_user_scope

    def get(self, name, groups=[]):
        endpoint = "dataset/get"
        payload = {
            "name": name,
            "groups": ",".join(self.get_groups(groups)),
            # comma-separated values for get
        }
        response = self._request(endpoint, params=payload, method="GET")
        return response

    def create(self, name, groups=[], dry_run=False):
        endpoint = "dataset/create"
        payload = {
            "name": name,
            "groups": self.get_groups(groups),
            "dry_run": dry_run,
        }
        response = self._request(endpoint, json=payload, method="POST")

        return response

    def upload_table(
        self, dataset, local_path, table=None, zone=None, metadata={}, groups=[], dry_run=False
    ):
        with open(local_path, "rb") as contentfile:
            endpoint = "dataset/table/upload"
            payload = {
                "dataset_name": dataset,
                "table_name": table,
                "zone": zone,
                "table_details": metadata,
                "groups": self.get_groups(groups),
                "dry_run": dry_run,
            }
            # for requests with files, the payload has to be one of the files
            # named 'json'
            # see https://stackoverflow.com/a/35946962

            files_to_upload = {
                "json": (None, json.dumps(payload), 'application/json'),
                "file": (os.path.basename(local_path), contentfile, 'application/octet-stream')
            }

            response = self._request(
                endpoint, method="POST", files=files_to_upload
            )

            return response

    def register_table(self, dataset, table, s3_path, zone=None, metadata={}, groups=[]):
        # assume that the file is already placed in the appropriate s3 file, and
        # register them with metadata.
        endpoint = "dataset/table/register"
        print(metadata)
        payload = {
            "dataset_name": dataset,
            "groups": self.get_groups(groups),
            "table_name": table,
            "zone": zone,
            "s3_path": s3_path,
            "table_details": metadata,
        }
        response = self._request(endpoint, json=payload, method="POST")

        return response

    def initialize(self, name, local_dir, groups=[], table_details={}, permissions={}):
        endpoint = "dataset/initialize"

        payload = {
            "name": name,
            "table_details": table_details,
            "groups": self.get_groups(groups),
        }

        files_to_upload = {}

        # for requests with files, the payload has to be one of the files
        # named 'json'
        # see https://stackoverflow.com/a/35946962

        files_to_upload["json"] = (None, json.dumps(payload), 'application/json')

        local_dir_abspath = os.path.abspath(local_dir)

        for root, dirs, files in os.walk(local_dir_abspath):
            for filename in files:
                abspath = os.path.join(root, filename)
                rel_path = abspath.replace(local_dir_abspath, "").strip("/")
                files_to_upload[rel_path] = (
                    filename, open(abspath, 'rb'), 'application/octet-stream'
                )

        # make the request
        response = self._request(
            endpoint, method="POST", files=files_to_upload
        )

        # close files
        for key, fileobj in files_to_upload.items():
            if key != "json":
                fileobj[1].close()

        return response

    def get_bucket_name(self, sector):
        if self.stage == "prod":
            return f"{self.tenant}-c360-{sector}"
        elif self.stage == "staging":
            return f"{self.tenant}-c360-{sector}-staging"
        else:
            return f"{self.tenant}-c360-{sector}-dev"

    def download_table(self, dataset, table, groups=[], target=None, sector="lake"):
        """
        target - target folder to download. If not given, default to dataset name.
        """
        endpoint = "dataset/table/get"
        payload = {
            "dataset": dataset,
            "table": table,
            "groups": ",".join(self.get_groups(groups)),
            # comma-separated values for get
        }
        paths = (
            self._request(endpoint, params=payload, method="GET")
            .json()
            .get("s3_paths", [])
        )

        target = target or dataset

        os.makedirs(target, exist_ok=True)
        s3_prefix = (
            f"s3://{self.get_bucket_name(sector)}/"
            f"{'/'.join([*self.get_groups(groups), dataset])}"
        )
        print("s3_prefix", s3_prefix)

        for s3_path in paths:
            print(s3_path)
            bucket = s3_path.replace("s3://", "").split("/")[0]
            key = "/".join(s3_path.replace("s3://", "").split("/")[1:])

            local_path = os.path.join(target, s3_path.replace(s3_prefix, "").strip("/"))
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            print(s3_prefix, local_path)

            self._s3_client.download_file(
                Bucket=bucket, Key=key, Filename=local_path,
            )

    def load_to_viztool(self, dataset, table, zone=None, groups=[]):
        # assume that the file is already placed in the appropriate s3 file, and
        # load them to postgres.
        endpoint = "dataset/table/load_to_viztool"
        payload = {
            "dataset": dataset,
            "groups": self.get_groups(groups),
            "table": table,
            "zone": zone,
        }
        response = self._request(endpoint, json=payload, method="POST")

        return response
