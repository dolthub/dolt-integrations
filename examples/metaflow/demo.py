import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import pickle

from metaflow import FlowSpec, step, Parameter
from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT
import pandas as pd


class VersioningDemo(FlowSpec):
    @step
    def start(self):
        master_conf = DoltConfig(database="foo")
        with DoltDT(run=self, config=master_conf) as dolt:
            df = dolt.read("bar")
            dolt.write(df=df, table_name="baz")
        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))
        pass


if __name__ == "__main__":
    VersioningDemo()
