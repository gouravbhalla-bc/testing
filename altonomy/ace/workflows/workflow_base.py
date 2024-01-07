import json


class WorkflowBase(object):

    def __init__(self, json_str: str = "{}"):
        self.json_str = json_str

    def load_json(self):
        return json.loads(self.json_str)
