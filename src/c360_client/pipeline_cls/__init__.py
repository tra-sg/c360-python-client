import os
import yaml

class DatalakeClientPipeline:
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

    def create(self, name, groups=[], path=None):
        """
        Look for the pipelines yaml on the path given, otherwise default to
        `pipelines/{name}.yml`.
        """

        # 1. Read the target file
        path = path or f"pipelines/{name}.yml"

        files = dict()
        files[f"{name}.yml"] = open(path, "rb")

        # 2. Find and include any separate table query file
        with open(path, "r") as yamlstream:
            yaml_dict = yaml.load(yamlstream, Loader=yaml.SafeLoader)

        basepath = os.path.dirname(path)

        for task in yaml_dict.get("tasks", []):
            for step in task["steps"]:
                if "query_file" in step:
                    query_filepath = step["query_file"]
                    query_fullpath = os.path.join(basepath, query_filepath)

                    files[query_filepath] = open(query_fullpath, "rb")

        # now that files are settled, make the multi-form-data request
        return self._request(
            endpoint="pipelines",
            method="post",
            files=files,
        )


    def delete(self, name, groups=[]):
        endpoint = f"pipelines/{name}"
        # should we do pipelines/common/** vs pipelines/users/ghosalya/** ??
        response = self._request(endpoint, method="DELETE")
        return response


    def deploy(self, path="pipelines/" groups=[]):
        """
        Deploy picks up all YAML and SQL files under the path, and attempts to deploy
        everything.
        """
        uploaded_files = {}

        for root, dirs, files in os.walk(path):
            for filename in files:
                if filename.endswith(".yml") or filename.endswith(".sql"):
                    filepath = os.path.join(root, filename)
                    key = filepath.replace(path, "")
                    uploaded_files[key] = open(filepath, "rb")

        return self._request(
            endpoint="pipelines",
            method="PUT",
            files=uploaded_files,
        )
