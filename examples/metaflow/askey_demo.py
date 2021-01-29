import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

from metaflow import FlowSpec, step, Parameter, Flow
from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT
import pandas as pd


class AsKeyDemo(FlowSpec):
    @step
    def start(self):
        audit = Flow("VersioningDemo").latest_successful_run.data.dolt
        master_conf = DoltConfig(database="foo")
        with DoltDT(run=self, audit=audit) as dolt:
            self.df1 = dolt.read("bar", as_key="bar1")
        with DoltDT(run=self, config=master_conf) as dolt:
            self.df2 = dolt.read("bar", as_key="bar2")

        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))


if __name__ == "__main__":
    AsKeyDemo()
