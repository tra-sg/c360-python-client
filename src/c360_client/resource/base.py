import json
import dictdiffer
from requests import Session
from c360_client.request_cls import DatalakeClientRequest


class APIResource:
    """
    An abstract class for storing various API resources as python object.
    """
    def __init__(self):
        self._data = {}
        self._old_data = {}  # store old data for re-pull comparison
        self._etag = None
        self._metadata = None

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
            self._metadata = response.headers.get("Metadata")
            data = response.json()

            if data != self._data:
                # there is a difference, we store old data
                self._old_data = self._data
                # self.diff()

            self._data = data

    def diff(self):
        # show the difference between the old and the newly pulled data
        if self._metadata:
            print("Last Modified By:", self.get_last_modified_by())
        for diff in list(dictdiffer.diff(self._old_data, self._data)):
            print(diff)

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

            if response.status_code == 200:
                self.pull()

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

    @property
    def metadata(self):
        # Metadata is returned as the string representation of python
        # dictionary (NOT JSON)
        return eval(self._metadata)

    def get_last_modified_by(self):
        arn = self.metadata.get("lastmodifiedby")
        if "c360_user_" in arn:
            user = arn.split("c360_user_")[1]
            user = user.split("/")[0]
            return user

        # if all else fail, return full ARN
        return arn
