import os
from pytest import fixture
from c360_client.dataset_cls import DatalakeClientDataset


@fixture
def dataset_api(mocker):
    client = DatalakeClientDataset()
    mocker.patch.object(
        client, "_request", return_value={"event": "mock response"}
    )
    mocker.patch.object(
        client, "_get_user_scope", return_value="test_user"
    )
    return client


def test_mocked_endpoint_request(dataset_api):
    """
    All the methods below takes an argument, do some transformation, and
    then use the transformation to call ._request(), which is mocked in
    test. This test simply checks whether they can work up until the
    call of ._request()
    """
    response = dataset_api.get("test_dataset")
    assert response == {'event': 'mock response'}

    response = dataset_api.create("test_dataset")
    assert response == {'event': 'mock response'}

    local_path = os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "test_local.csv"
    )
    response = dataset_api.upload_table("test_dataset", local_path)
    assert response == {'event': 'mock response'}

    response = dataset_api.register_table(
        "test_dataset", "test_table", "test_s3_path"
    )
    assert response == {'event': 'mock response'}

    response = dataset_api.load_to_viztool("test_dataset", "test_table")
    assert response == {'event': 'mock response'}


def test_get_groups(dataset_api):
    group1 = dataset_api.get_groups(["a", "b"])
    assert group1 == ["users", "test_user", "a", "b"]

    dataset_api.set_options(is_user_scoped=False)
    group2 = dataset_api.get_groups(["a", "b"])
    dataset_api.set_options(is_user_scoped=True)

    assert group2 == ["a", "b"]
