import os
from pycaret import classification, regression
from c360_client import get_project_config
from c360_client import dataset
from c360_client.utils import append_stage, _get_tenant, get_aws_credentials
import requests

pycaret_engines = {
    "CLASSIFICATION": classification,
    "REGRESSION": regression,
}


class Model:
    def __init__(self, model_name, model_type="CLASSIFICATION", data=None):
        """
        - model_name: model name to be registered
        """
        self.model_name = model_name
        self.model_type = model_type
        
        # TODO: check if model already exists and load it
        self._model_obj = None
        self._data = data
        self._label = None
        self._experiment = None

        self._engine = pycaret_engines[model_type]

    
    def setup_experiment(self, label=None, data=None, **kwargs):
        if label is None:
            # try to render dropdown (in jupyter notebook)
            # otherwise error saying label is required
            try:
                from ipywidgets import interact, widgets
                from IPython.display import display

                if data is not None:
                    self._data = data

                interact(
                    self._ipython_set_label,
                    label=widgets.Dropdown(
                        options=data.columns,
                        value=data.columns[0],
                        description='Label:',
                        disabled=False,
                    ),
                )

                button = widgets.Button(description='Set Label')
                def on_click(event):
                    self._setup_experiment(
                        label=self._label,
                        data=data,
                        **kwargs
                    )
                button.on_click(on_click)
                display(button)

            except:
                raise ValueError("For execution outside of notebooks, the argument `label` is required.")
        else:
            return self._setup_experiment(label, data=data, **kwargs)

    
    def _setup_experiment(self, label, data=None, **kwargs):
        if data is not None:
            self._data = data

        self._label = label

        self._experiment = self._engine.setup(
            data=data,
            target=label,
            silent=True,
        )

    def _ipython_set_label(self, label):
        try:
            from IPython.display import clear_output
            self._label = label
        except:
            raise RuntimeError("Can only be executed inside a notebook.")

    def create_model(self, algorithm=None):
        if algorithm:
            model = self._engine.create_model(algorithm)
        else:
            model = self._engine.compare_models()

        tuned_model = self._engine.tune_model(model)
        self._model_obj = tuned_model
        return self._model_obj

    def plot_importance(self, all_features=False):
        """
        Plot feature importance, if applicable.

        :param all_features: whether to plot all features. Defautl to
            false, which will only plot top features.
        """

        if all_features:
            return self._engine.plot_model(self._model_obj, "feature_all")
        else:
            return self._engine.plot_model(self._model_obj, "feature")


    def get_model():
        return self._model_obj

    def deploy_model(self):
        if self._model_obj is None:
            raise RuntimeError("Model has not been trained")

        creds = get_aws_credentials()

        os.environ["AWS_ACCESS_KEY_ID"] = creds[0]
        os.environ["AWS_SECRET_ACCESS_KEY"] = creds[1]

        final_model = self._engine.finalize_model(self._model_obj)
        self._engine.deploy_model(
            final_model,
            model_name=self.model_name,
            platform="aws",
            authentication={
                "bucket": append_stage(f"{_get_tenant()}-c360-artifacts")
            },
        )

        os.environ["AWS_ACCESS_KEY_ID"] = ""
        os.environ["AWS_SECRET_ACCESS_KEY"] = ""

        return self._register_model()

    def _register_model(self):
        endpoint = "model"  # POST
        payload = self.get_model_json()

        response = self._request(endpoint, json=payload, method="POST")

        return response


    def _request(self, endpoint, **kwargs):
        project_config = get_project_config()

        # use API key
        if not kwargs.get("headers"):
            kwargs["headers"] = {}

        if not kwargs["headers"].get("Authorization"):
            kwargs["headers"]["Authorization"] = dataset.api_key
            # TODO: api_key shouldnt be a config only to dataset

        api_url = project_config["api_url"]
        req = requests.request(url=f"{api_url}/{endpoint}", **kwargs)
        return req

    def get_model_json(self):
        return {
            "model_name": self.model_name,
            "name": self.model_name,
            "title": f"{self.model_name} (in development)",
            "Description": "A model in development.",
            "exec_metadata": {
                "model_type": self.model_type,
                "columns": [
                    c for c in self._data.columns
                    if c != self._label
                ]
            },
            # below are features that shouldn't be mandatory
            "accuracy_model_comparison": {"data":[]},
            "accuracy_version_comparison": {"data":[]},
            "datasets": [],
            "evaluation": [],
            "feature_importance": {"data":[]},
            "features": [],
            "gsv_model_comparison": {"data":[]},
            "gsv_version_comparison": {"data":[]},
            "proofOfConcept": [],
            "timeline": [],
            "versions": [],
        }