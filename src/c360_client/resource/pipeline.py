from c360_client.resource.base import APIResource


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

    def _pull(self):
        pass

    def _push(self):
        pass

    @property
    def tasks(self):
        return self.data['tasks']

    def add_task(self, **kwargs):
        tasks = self._data.get("tasks", [])
        tasks.append(kwargs)
        self._data["tasks"] = tasks
