import logging
import json

# logger = logging.getLogger()
# logger.setLevel(logging.WARNING)

import pickle

from metaflow import FlowSpec, step, Parameter
from dolt_integrations.metaflow.dolt import DoltConfig, DoltDT
import pandas as pd


class CustomQueryDemo(FlowSpec):
    @step
    def start(self):
        master_conf = DoltConfig(database="foo")
        with DoltDT(run=self, config=master_conf) as dolt:
            df = dolt.sql("SELECT * from `bar`", as_key="foo")
        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))
        pass


if __name__ == "__main__":
    CustomQueryDemo()
