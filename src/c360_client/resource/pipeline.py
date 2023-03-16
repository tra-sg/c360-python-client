from c360_client.resource.base import APIResource
from time import sleep


class Pipeline(APIResource):
    """
    Sample pipeline in yml:

        name: clv
        prefix: CLV
        address:
          dataset: crm
          zone: derived_restricted
        run_type: trigger
        dependencies:
          - table: t200_transaction
        pipeline_group: main

        tasks:
          - source:
              - table: t200_transaction
            steps:
              - code: create
                table: t210_transaction_overview
            type: resource
          - source:
              - table: t200_transaction
            steps:
              - code: create
                table: t210_transaction_overview
            type: resource
    """
    def __init__(self, name):
        super().__init__()
        self.name = name
        self._data = {}

        self._is_ran = False

    def _pull(self):
        pass

    def _push(self):
        pass

    def push(self):
        sleep(0.34)
        return {
          "event": f"Pipeline {self.name} created."
        }

    def run(self):
        sleep(0.34)
        if self._is_ran:
            return {
                "event": f"Pipeline started: {self.name}"
            }
        else:
            self._is_ran = True
            return {
                "error": f"Pipeline not found: {self.name}"
            }

    def get_current_state(self):
        sleep(0.34)
        if not self._is_ran:
            return {
              "error": f"Pipeline not found: {self.name}"
            }
        else:
            return {'status': 'FAILED', 'errorMessage': '[PID: acme-c360::staging::bankanon::20230305::ebbeb25f-69d5-381d-792a-c4b0e3a32dbd] Error: failed with status: {"error": "Anonymization validation failed", "test": "k-anonymity", "reason": "Required at least k-20, found k-13."}', 'errorType': 'Exception', 'stackTrace': ['  File "/var/task/datalake/common/__init__.py", line 94, in wrapped_fn\n    raise get_pipeline_exception(pipeline_id, e)\n', '  File "/var/task/datalake/common/__init__.py", line 92, in wrapped_fn\n    return fn(event, context)\n', '  File "/var/task/datalake/context/__init__.py", line 47, in wrapped_fn\n    return fn(event, lambda_context)\n', '  File "/var/task/datalake/lambda_fns/athena_run.py", line 93, in run\n    raise Exception("failed with status: " + str(result["query_status"]))\n']}

    @property
    def tasks(self):
        return self.data['tasks']

    def add_task(self, **kwargs):
        tasks = self._data.get("tasks", [])
        tasks.append(kwargs)
        self._data["tasks"] = tasks

    def delete(self):
        sleep(0.34)
        return {
            "event": f"Pipeline deleted: {self.name}"
        }
