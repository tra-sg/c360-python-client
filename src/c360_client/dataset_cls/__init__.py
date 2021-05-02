import os
import json
import requests
import getpass

from c360_client.utils import get_boto_client

import s3fs

from pandas_profiling import ProfileReport
import numpy as np
import matplotlib
import matplotlib.pyplot as plt


class DatalakeClientDataset:
    def __init__(
        self, tenant=None, stage="prod", api_url=None, api_key=None
    ):
        """
        A configurable client object for hitting c360 dataset endpoints.
        The object `c360_client.dataset` is an instance of this class.
        """
        self.api_key = api_key
        self.tenant = tenant
        self.stage = stage
        self.url = api_url

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
        # paths = (
        #     self._request(endpoint, params=payload, method="GET")
        #     .json()
        #     .get("s3_paths", [])
        # )

        target = target or dataset

        os.makedirs(target, exist_ok=True)
        s3_prefix = (
            f"s3://{self.get_bucket_name(sector)}/"
            f"{'/'.join([*self.get_groups(groups), dataset])}"
        )
        print("s3_prefix", s3_prefix)
        s3_client = get_boto_client("s3")

        s3_path = f"{s3_prefix}/{table}"
        # for s3_path in paths:
        print(s3_path)
        bucket = s3_path.replace("s3://", "").split("/")[0]
        key = "/".join(s3_path.replace("s3://", "").split("/")[1:])

        local_path = os.path.join(target, s3_path.replace(s3_prefix, "").strip("/"))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        print(s3_prefix, local_path)

        s3_client.download_file(
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

    def register_to_datalake(df, fileName, bucket, user, dataset, zone, tableName, format="csv"):

        # upload to S3
        s3_client = get_boto_client("s3")

        s3 = s3fs.S3FileSystem(anon=False)

        with s3.open(f'{bucket}/users/{user}/{dataset}/{zone}/{tableName}/{fileName}.{format}','w') as f:
            if format == "csv":
                df.to_csv(f, index=False)
            if format == "excel":
                df.to_excel(f)

        # s3_client.upload_file(
        #     Filename=fileName,
        #     Bucket=bucket,
        #     Key=f"users/{user}/{dataset}/{zone}/{tableName}/{fileName}",
        # )

        # register to Datalake

        response = register_table(
            dataset=dataset,
            table=tableName,
            s3_path=f"s3://{bucket}/users/{user}/{dataset}/{zone}/{tableName}/",
            zone=zone,
            groups=["users", user],
            metadata={
              "format": format,
            },
        )

        response.json()

    def time_series_plot(df):
        # Given dataframe, generate times series plot of numeric data by daily, monthly and yearly frequency
        print("\nTo check time series of numeric data  by daily, monthly and yearly frequency")
        if len(df.select_dtypes(include='datetime64').columns)>0:
            for col in df.select_dtypes(include='datetime64').columns:
                for p in ['D', 'M', 'Y']:
                    if p=='D':
                        print("Plotting daily data")
                    elif p=='M':
                        print("Plotting monthly data")
                    else:
                        print("Plotting yearly data")
                    for col_num in df.select_dtypes(include=np.number).columns:
                        __ = df.copy()
                        __ = __.set_index(col)
                        __T = __.resample(p).sum()
                        ax = __T[[col_num]].plot()
                        ax.set_ylim(bottom=0)
                        ax.get_yaxis().set_major_formatter(
                        matplotlib.ticker.FuncFormatter(lambda x, p: format(int(x), ',')))
                        plt.show()

    def top5_and_bottom5_by_value(df, value_column):
        # Given dataframe, generate top 5 and bottom 5 values for non-numeric data
        columns = df.select_dtypes(include=['object', 'category']).columns
        for col in columns:
            print(f"Top 5 and bottom 5 {value_column} values of " + col)

            cal_df = df[[col, value_column]]
            cal_df = cal_df.groupby([col]).sum().sort_values(by=[value_column], ascending=False)
            print(cal_df)

            print(" ")

    def automate_data_exploration(df, value_column=""):
        profile = ProfileReport(df)
        profile
        time_series_plot(df)

        if value_column != "":
            top5_and_bottom5_by_value(df, value_column)

        return profile
