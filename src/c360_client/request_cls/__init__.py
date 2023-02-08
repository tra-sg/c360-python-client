import os
import requests
import getpass
from c360_client.notebook import deviceauth


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
        self, tenant=None, stage="prod", api_url=None, api_key=None, defaults={}
    ):
        """
        A configurable client object for hitting c360 dataset endpoints.
        The object `c360_client.dataset` is an instance of this class.
        """
        self.api_key = api_key
        self.auth_creds = None
        self.tenant = tenant
        self.stage = stage
        self.url = api_url
        self._defaults = defaults

        # options
        self._cached_user_scope = None

    @property
    def _is_user_scoped(self):
        if self._defaults.get("space"):
            return False
        else:
            return True

    def set_options(self, is_user_scoped=True):
        # deprecated, use c360_client.set_default_space instead
        pass

    def set_api_key(self, api_key=None):
        if api_key:
            self.api_key = api_key
        else:
            self.api_key = getpass.getpass("API_KEY:")

    def get_groups(self, groups=None):
        main_groups = []
        if self._is_user_scoped:
            main_groups.append("users")
            main_groups.append(self._get_user_scope())

        if not groups:
            groups = self._defaults.get("space", [])

        return main_groups + groups

    def request(self, endpoint, **kwargs):

        # use API key
        if not kwargs.get("headers"):
            kwargs["headers"] = {}

        if not kwargs["headers"].get("Authorization"):
            kwargs["headers"]["Authorization"] = self._get_auth_header()

        req = requests.request(url=f"{self.url}/{endpoint}", **kwargs)
        return req

    def _get_auth_header(self):
        if self.auth_creds:
            return f"Bearer {self.auth_creds['id_token']}"
        elif self.api_key:
            return self.api_key
        else:
            raise RuntimeError(
                "Client is not logged in. Please log in with either"
                "`c360_client.api.authenticate()` or `c360_client.api.set_api_key()`"
            )

    def _get_user_scope(self, refresh=False):

        if (self._cached_user_scope is not None) and (refresh is not False):
            return self._cached_user_scope

        endpoint = "entity/user/scope"
        response = self.request(endpoint, method="GET")
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

    def authenticate(self):
        self.auth_creds = deviceauth.authenticate()
        print("Authentication successful; you are now logged in.")
