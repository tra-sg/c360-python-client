import os
import base64
from pyathena import connect as athena_connect
from googleapiclient.discovery import build
from googleapiclient import errors as g_errors
import pandas as pd


from c360_client.utils import (
    get_boto_client,
    get_aws_credentials,
    set_notebook_mode,
    is_notebook_mode,
)


def login():
    """
    Authenticate to your Google account from within a Colab Notebook.
    """
    try:
        from google.colab import auth
        auth.authenticate_user()
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/content/adc.json"
        set_notebook_mode()
    except ImportError:
        raise RuntimeError(
            "Cannot import `google.colab.auth`. Are you running within"
            " a colab notebook?"
        )


def get_athena_connection():

    sts = get_boto_client('sts')
    aws_account_id = sts.get_caller_identity().get('Account')

    aws_creds = {}

    if is_notebook_mode():
        aws_access_key_id, aws_secret_access_key = get_aws_credentials()
        aws_creds = dict(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name="ap-southeast-1",
        )

    pyathena_conn = athena_connect(
        s3_staging_dir=(
            "s3://aws-athena-query-results-"
            "{}-ap-southeast-1/athena".format(aws_account_id)
        ),
        **aws_creds
    )

    return pyathena_conn


def download_athena_table(file_name, data):
    os.makedirs("data", exist_ok=True)
    file_path = "data/" + file_name
    if os.path.exists(file_path):
        print("File exists:", file_path)
        return

    data.to_csv(file_path, index=False)
    print("Downloaded", file_path)


def logout():
    os.remove("/content/adc.json")
    print("Logged out")


def update_description(file_id, description):
    service = build('drive', 'v3')
    if (file_id):
        if not (description):
            print("Description not found. Set description to default - Example Notebook")
            description = "Example Notebook"
        try:
            # First retrieve the file from the API.
            file = service.files().get(fileId=file_id).execute()

            # File's new metadata.
            del file['id']
            file['description'] = description

            # Send the request to the API.
            service.files().update(
                fileId=file_id,
                body=file,
            ).execute()
            print("Updated notebook description to: ", description)

        except g_errors as error:
            print("An error occurred:", error)
            return None
    else:
        print("Please provide file_id")


def publish_notebook(file_id):
    service = build('drive', 'v3')
    # hardcoded `published` folder_id
    folder_id = "1YXUkwJNFxhIpAXV9beu2fsZTeG15E4uP"
    if (file_id):
        try:
            # Retrieve the existing parents to remove
            fileobj = service.files().get(
                fileId=file_id,
                fields='parents'
            ).execute()
            previous_parents = ",".join(fileobj.get('parents'))

            # Move the file to the published folder
            fileobj = service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()

            print("Successfully published. You can now view this notebook in `Labs` section")
        except g_errors as error:
            print("An error occurred:", error)
        return None
    else:
        print("Please provide file_id")


def unpublish_notebook(file_id):
    service = build('drive', 'v3')
    # hardcoded `c360-labs` folder_id
    folder_id = "13cz38UKqBVVPoPPYPxmV73J5Jo4auKnI"
    if (file_id):
        try:
            # Retrieve the existing parents to remove
            fileobj = service.files().get(
                fileId=file_id,
                fields='parents'
            ).execute()

            previous_parents = ",".join(fileobj.get('parents'))
            # Move the file to the published folder
            fileobj = service.files().update(
                fileId=file_id,
                addParents=folder_id,
                removeParents=previous_parents,
                fields='id, parents'
            ).execute()

            print("Successfully unpublished.")
        except g_errors as error:
            print("An error occurred:", error)
            return None
    else:
        print("Please provide file_id")


def upload_thumbnail(file_id):
    try:
        from google.colab import files
    except ImportError:
        raise RuntimeError(
            "Cannot import `google.colab.files`. Are you running within"
            " a colab notebook?"
        )

    service = build('drive', 'v3')

    if (file_id):
        uploaded = files.upload()

        for fn in uploaded.keys():
            with open(fn, "rb") as f:
                # sometimes 1st upload don't take effect
                service.files().update(
                    fileId=file_id,
                    body={
                        "contentHints": {
                            "thumbnail": {
                                "image": base64.urlsafe_b64encode(f.read()).decode('utf8'),
                                "mimeType": "image/png",
                            }
                        }
                    },
                ).execute()

                service.files().update(
                    fileId=file_id,
                    body={
                        "contentHints": {
                            "thumbnail": {
                                "image": base64.urlsafe_b64encode(f.read()).decode('utf8'),
                                "mimeType": "image/png",
                            }
                        }
                    },
                ).execute()
                print("Thumbnail uploaded")
                # remove the uploaded file
                file_name = f"/content/{fn}"
                os.remove(file_name)
    else:
        print("Please provide file_id")


def load_table(table_name, limit=500):
    if (table_name):
        pyathena_conn = get_athena_connection()
        query = (
            # TODO: build query from any dataset
            "SELECT * FROM acme_c360_lake__crm_derived_restricted"
            f".{table_name}_latest LIMIT {limit}"
        )
        file_name = f"{table_name}.csv"

        data = pd.read_sql(
            query,
            pyathena_conn
        )

        download_athena_table(file_name, data)
        print("Loaded table name: ", table_name, " as ", file_name)
    else:
        print("Please provide table name")
