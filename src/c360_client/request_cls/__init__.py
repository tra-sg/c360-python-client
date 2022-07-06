import os

class Singleton(type):
    _instances = {}
    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class DatalakeClientRequest(metaclass=Singleton):
    """
    The base class for interacting with API.
    """
    def __init__(
        self, tenant=None, stage="prod", api_url=None, api_key=None,
    )

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

        return main_groups + groups

    def request(self, endpoint, **kwargs):

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


    def get_dataspace(self):
        """
        Returns a list ["users", "<username>"] if it is user scoped,
        otherwise return empty string.
        """

        main_groups = []
        if self._is_user_scoped:
            main_groups.append("users")
            main_groups.append(self._get_user_scope())

        return main_groups
