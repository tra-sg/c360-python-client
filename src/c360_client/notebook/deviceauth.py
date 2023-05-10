import requests
import webbrowser
from time import sleep
from c360_client.utils import (
    _get_tenant, _get_stage
)


MAX_DEVICE_CRED_PULL_ATTEMPTS = 100


def get_device_auth_url():
    if _get_stage().lower() == "prod":
        return f"https://device-auth.{_get_tenant()}.c360.ai"
    elif _get_stage().lower() == "staging":
        return f"https://staging.device-auth.{_get_tenant()}.c360.ai"
    else:
        # for debugging purposes, return to staging
        return f"https://staging.device-auth.{_get_tenant()}.c360.ai"
        # raise RuntimeError("Cannot log in unless `stage` is set to (prod, staging).")


def get_portal_url():
    if _get_stage().lower() == "prod":
        return f"https://{_get_tenant()}.c360.ai"
    elif _get_stage().lower() == "staging":
        return f"https://staging.{_get_tenant()}.c360.ai"
    else:
        return "http://localhost:8000"
        # raise RuntimeError("Cannot log in unless `stage` is set to (prod, staging).")


def request_token(device_code=None):
    auth_url = get_device_auth_url()
    url = f"{auth_url}/token"
    if device_code:
        url = f"{url}?device_code={device_code}&grant_type=urn:ietf:params:oauth:grant-type:device_code"

    response = requests.post(url)
    return response.json()



class URLTab:
    """
    A context manager that opens a URL in a new tab, and does cleanup on exit
    so that the lingering javascript is removed from the final notebook cell.
    """
    def __init__(self, url):
        self.url = url

    def open(self):
        url = self.url
        can_open_wpython = webbrowser.open_new(url)
        if not can_open_wpython:
            print(
                "A new window will open, asking you to log in. If it does not open "
                f"within 5 seconds, you can go there manually: {url}"
            )
            try:
                from IPython.display import Javascript
                display(
                    Javascript('window.open("{url}");'.format(url=url)),
                    display_id="login_url_display"
                )
            except Exception:
                print("Could not open new window with Javascript.")

    def close(self):
        """
        On close, update IPython display to no longer have any Javascript.
        """
        can_open_wpython = webbrowser.open_new(url)
        if not can_open_wpython:
            try:
                from IPython.display import Javascript
                display(display_id="login_url_display", update=True)
            except Exception:
                print("Could not perform javascrip cleanup on URLTab opener.")

    def __enter__(self):
        return self.open()

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.close()


def open_url(url):
    return URLTab(url)


def authenticate():
    initial_token_response = request_token()
    # Initial response would be of the following format:
    # {
    #     "device_code": "APKAEIBAERJR2EXAMPLE",
    #     "user_code": "ANPAJ2UCCR6DPCEXAMPLE",
    #     "verification_uri": "https://<FQDN of the ALB protected Lambda function>/device",
    #     "verification_uri_complete": "https://<FQDN of the ALB protected Lambda function>/device?code=ANPAJ2UCCR6DPCEXAMPLE&authorize=true",
    #     "interval": <Echo of POLLING_INTERVAL environment variable>,
    #     "expires_in": <Echo of CODE_EXPIRATION environment variable>
    # }
    user_code = initial_token_response["user_code"]
    interval = initial_token_response["interval"]
    device_code = initial_token_response["device_code"]

    portal_url = get_portal_url()
    user_facing_url = f"{portal_url}/deviceauth/?code={user_code}"


    with open_url(user_facing_url):
        pull_attempts = 0
        authorized = False
        creds = {}

        while not authorized:
            sleep(interval)
            pull_attempts += 1
            response = request_token(device_code=device_code)
            if response.get("access_token"):
                creds = response
                authorized = True
            elif response.get("error", "") not in ["", "authorization_pending"]:
                raise Exception("Device flow failed:", response)

            if not authorized and (pull_attempts > MAX_DEVICE_CRED_PULL_ATTEMPTS):
                raise TimeoutError("Timed out waiting for user to log in.")

        return creds


if __name__ == "__main__":
    authenticate()
