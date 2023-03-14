import json
from c360_client import get_project_config
import pandas as pd

from c360_client.resource.base import APIResource


def _client():
    from c360_client import dataset
    return dataset


class Dataset(APIResource):
    def __init__(self, name):
        super().__init__()
        self.name = name
        self._data = {}
        self._permissions = None

    def _pull(self):
        return _client().get(self.name)

    def _push(self):
        return _client().update(
            self.name,
            # TODO: support updating all other fields
            description=self.description,
        )

    @property
    def resources(self):
        if not self._data.get("resources"):
            self._data["resources"] = []

        return self.data["resources"]


    def get_table(self, table_name):
        for table in self.resources:
            if table.get("name") == table_name:
                return Table(table_name, dataset=self, data=table)

        raise RuntimeError("Table not found in dataset:", self.name)

    @property
    def tables(self):
        return [
            Table(table['name'], dataset=self, data=table)
            for table in self.data['resources']
        ]

    def add_table(self, table):
        # take a table object and add it to the underlying _data

        # check if table name doesnt clash
        try:
            existing_table = self.get_table(table.name)
            if existing_table:
                raise ValueError("Table already exist:", table.name)
        except RuntimeError:
            pass

        # add data
        self._data["resources"].append(table.data)


    def add_tables(self, *tables):
        for table in tables:
            self.add_table(table)


    @property
    def permissions(self):
        # different endpoint, still lazy-loaded
        if not self._permissions:
            response = _client().get_permission(self.name)
            self._permissions = response.json()

        # TODO: should we pretty print?
        print(json.dumps(self._permissions["permissions"], indent=2))
        return self._permissions["permissions"]

    def set_description(self, new_description=None):
        if new_description:
            self._data["description"] = new_description

    def __repr__(self):
        return f"<Dataset \"{self.name}\"  ||  tables={len(self.data['resources'])}>"

    def local_path(self):
        config = get_project_config()
        return os.path.join(
            self.path.abspath(config.get("local_workdir")),
            self.name
        )


class Table(APIResource):
    def __init__(self, name, dataset, data={}):
        super().__init__()
        self.name = name
        self.dataset = dataset
        self._data = data  # why cant this be inherited
        self._data["name"] = name

    def _push(self):
        pass


    def _pull(self):
        pass

    def load_dataframe(self):
        if self._local_path:
            if self.format == "csv":
                return pd.read_csv(self._local_path)

        return _client().get_table(
            dataset=self.dataset.name,
            table=self.name,
            groups=["common"],
        )

    def __repr__(self):
        return f"<Table \"{self.name}\">"


    @classmethod
    def from_dataframe(cls, name, dataframe, **kwargs):
        # default format to csv
        if "format" not in kwargs:
            kwargs["format"] = "csv"

        table = cls(name, data=kwargs)  # TODO: validate kwargs
        table._write_dataframe(dataframe)

        return table

    def _write_dataframe(self, dataframe):

        if self.format == "csv":
            path = f"{self.dataset.name}/{self.name}/data.csv"
            dataframe.to_csv(path)
            self._write_file(path)
        elif self.format == "parquet":
            path = f"{self.dataset.name}/{self.name}/data.parquet"
            dataframe.to_parquet(path)
            self._write_file(path)
        else:
            raise RuntimeError(f"Unsupported serialization format: {self.format}")


    def _write_file(self, *filepaths):
        # put one or more local files as part of this table
        # if they are not
        dataset_path = self.dataset.local_path()

        for filepath in filepaths:
            if os.path.abspath(filepath).startswith(dataset_path):
                # already in appropriate place
                relpath = os.path.abspath(filepath).replace(dataset_path, "")
                self._data["path"] = [
                    *self.data["path"],
                    rel_path,
                ]


    def _get_path_in_dataset(self):
        zone = self.data.get("zone") or "source_confidential"
        return [
            f"{zone}/{path}" for path in self.data["path"]
        ]
