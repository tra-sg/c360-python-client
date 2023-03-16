import os
import json
import requests
import getpass
import wget

from c360_client.request_cls import DatalakeClientRequest
from c360_client.utils import get_boto_client


class DatalakeClientDataset:
    def __init__(self, defaults={}):
        """
        A configurable client object for hitting c360 dataset endpoints.
        The object `c360_client.dataset` is an instance of this class.
        """
        self.request_inst = DatalakeClientRequest()
        self._defaults = defaults
        # At this point the request class should be instantiated

    @property
    def tenant(self):
        return self.request_inst.tenant

    @property
    def stage(self):
        return self.request_inst.stage

    def set_api_key(self, api_key=None):
        # TODO: this is deprecated as API authentication is moved to the
        #       request cls.
        self.request_inst.set_api_key(api_key)

    def authenticate(self):
        self.request_inst.authenticate()

    def set_options(self, is_user_scoped=True):
        # TODO: this is deprecated as API options is moved to the
        #       request cls.
        self.request_inst.set_options(is_user_scoped=is_user_scoped)

    def get_groups(self, groups):
        return self.request_inst.get_groups(groups=groups)

    def _request(self, endpoint, **kwargs):
        return self.request_inst.request(
            endpoint=endpoint, **kwargs,
        )

    def get(self, name, groups=[]):
        endpoint = f"dataset/{name}"
        payload = {
            # "name": name,
            "groups": ",".join(self.get_groups(groups)),
            # comma-separated values for get
        }
        response = self._request(endpoint, params=payload, method="GET")
        return response

    def update(self, name, groups=[], **kwargs):
        endpoint = f"dataset/{name}"
        payload = {
            # "name": name,
            "groups": ",".join(self.get_groups(groups)),
            **kwargs
            # comma-separated values for get
        }
        response = self._request(endpoint, params=payload, method="PUT")
        return response


    def create(self, name, groups=[], dry_run=False):
        endpoint = "dataset"
        payload = {
            "name": name,
            "groups": self.get_groups(groups),
            # "dry_run": dry_run,
        }
        response = self._request(endpoint, json=payload, method="POST")

        return response

    def create_table(self, dataset, table, zone, source, metadata={}, groups=[], dry_run=False):
        """
        One method to cover the multiple cases of creating table, that adapts
        depending on the form of `source`,

            { }

                Create an empty table

            { "local_path": local/path/to/csv }

                Creates a table by uploading a local file.

            { "s3_path": path/to/s3 }

                Creates a table by assuming the files under the s3 path. Note that
                the given S3 path must adhere to c360-lake data structure

            { "clone": {...} }

                Clone a dataset from an existing dataset in c360-lake (not yet implemented)
        """

        if "local_path" in source and "s3_path" in source:
            raise ValueError(
                "Argument `source` cannot contain both `local_path` and `s3_path`"
            )

        if "local_path" in source:
            return self.upload_table(
                dataset=dataset,
                table=table,
                zone=zone,
                metadata=metadata,
                groups=groups,
                dry_run=dry_run,
                local_path=source["local_path"],
            )
        elif "s3_path" in source:
            return self.register_table(
                dataset=dataset,
                table=table,
                zone=zone,
                metadata=metadata,
                groups=groups,
                s3_path=source["s3_path"],
            )

    def upload_table(
        self, dataset, local_path, table=None, zone=None, metadata={}, groups=[], dry_run=False
    ):
        # TODO: Accessing this method is deprecated. This method should eventually
        #       be hidden.
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
        # TODO: Accessing this method is deprecated. This method should eventually
        #       be hidden.

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
        endpoint = "dataset/table/get_presigned_url"
        payload = {
            "dataset": dataset,
            "table": table,
            "groups": groups,
            # comma-separated values for get
        }
        response = self._request(endpoint, params=payload, method="GET")
        presigned_urls = response["presigned_urls"]

        target = target or dataset
        os.makedirs(target, exist_ok=True)

        filenames = []

        for i in range(len(presigned_urls)):
            filename = f"{target}/{table}.{i}.parquet"
            wget.download(presigned_urls[i], out=filename)
            filenames.append(filename)

        print("Table downloaded under", target)

        return filenames


    def get_table(self, dataset, table, groups=[], target=None, sector="lake"):
        """
        Downloads a table and load them as pandas DataFrame.
        """
        filenames = self.download_table(dataset, table, groups=groups, target=target, sector=sector)

        try:
            import pandas as pd
            df = pd.concat(
                pd.read_parquet(filename)
                for filename in filenames
            )
            return df.reset_index(drop=True)
        # except ImportError:
        #     raise RuntimeError("Error importing required libraries: pandas")
        except Exception as e:
            raise e


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

    def list_datasets(self, search_filter=""):
        """
        List all datasets.
        """
        endpoint = "dataset/list"
        payload = {
            "filter": search_filter,
        }
        response = self._request(endpoint, json=payload, method="GET")

        return response

    def get_permission(self, dataset):
        endpoint = f"dataset/{dataset}/permissions"
        response = self._request(endpoint, method="GET")
        return response
