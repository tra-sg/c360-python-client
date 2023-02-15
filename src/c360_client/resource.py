import json
from requests import Session
from c360_client.request_cls import DatalakeClientRequest


def _client():
    from c360_client import dataset
    return dataset


class APIResource:
    """
    An abstract class for storing various API resources as python object.
    """
    def __init__(self, endpoint):
        self._data = {}
        self.__endpoint = endpoint
        self._etag = None

    def _pull(self):
        # Lazy loading - must use dataset_cls method and returns
        pass

    def pull(self):
        api = DatalakeClientRequest()
        with api.requests_prepare_mode():
            # we have to use the context otherwise we will only get
            # the JSON body
            request_obj = self._pull()
            s = Session()
            response = s.send(request_obj.prepare())
            self._etag = response.headers.get("ETag")
            self._data = response.json()

    def _push(self):
        # Any method from dataset_cls
        pass

    def push(self):
        api = DatalakeClientRequest()
        with api.requests_prepare_mode():
            # we have to use the context otherwise we will only get
            # the JSON body
            request_obj = self._push()
            s = Session()
            prepped = s.prepare_request(request_obj)

            if self._etag:
                prepped.headers["If-Match"] = self._etag

            response = s.send(prepped)

            return response.json()

    @property
    def data(self):
        if not self._data:
            self.pull()

        return self._data

    def __getattr__(self, attr):
        return self.data[attr]

    def __repr__(self):
        # return self._data  # TODO: prettify output
        return json.dumps(
            self.data, indent=2,
        )


class Dataset(APIResource):
    def __init__(self, name):
        endpoint = "dataset/get"
        super().__init__(endpoint)
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

    def table(self, table_name):
        return Table(table_name, dataset=self)

    @property
    def tables(self):
        return [
            Table(table['name'], dataset=self)
            for table in self.data['resources']
        ]

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


class Table(APIResource):
    def __init__(self, name, dataset, data={}):
        endpoint = "dataset/get"
        super().__init__(endpoint)
        self.name = name
        self.dataset = dataset
        self._data = data  # why cant this be inherited

    def _pull(self):
        for table in self.dataset.data["resources"]:
            if table.get("name") == self.name:
                self._data = table

        if not self._data:
            raise RuntimeError("Table not found in dataset:", self.name)

    def load(self):
        return _client().get_table(
            dataset=self.dataset.name,
            table=self.name,
            groups=["common"],
        )

    def __repr__(self):
        return f"<Table \"{self.name}\" dataset=\"{self.dataset.name}\">"
