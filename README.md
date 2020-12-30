# c360-python-client

A python client for c360 projects.

## Configuration

This library can be configured by setting environment variables.

The following configuration is mandatory:

- `C360_TENANT` - the name of the tenant

The following configurations are optional:

- `C360_STAGE` - the target stage of the project. Can either be "prod" or "staging". Defaults to "prod".
- `C360_API_KEY` - your API key to authenticate to the API. Can also be
     set by calling `c360_client.dataset.set_api_key()` that will
     prompt you to input the key
- `C360_API_URl` - the URL to make requests to. If not given, the URL
    will be constructed with the given `C360_TENANT` and `C360_STAGE`
    configuration


## Usage

### Dataset Endpoint

* Configure API key interactively (alternatively, you can set
    `C360_API_KEY` environment variable)
    ```
    c360_client.dataset.set_api_key()
    ```

* Get dataset metadata

    ```python
    c360_client.dataset.get("dataset_name")
    ```

* Download a specific table (requires AWS credentials to be set up)

    ```python
    c360_client.dataset.download_table(
        dataset="test_dataset",
        table="test_table",
    )
    ```



## Development

You can install this library in development mode with

```
pip install -r requirements.txt
```

### Testing

Once installed, you can run local tests with

```
pytest
```
