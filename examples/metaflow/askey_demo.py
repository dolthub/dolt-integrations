import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

from metaflow import FlowSpec, step, Parameter
from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT
import pandas as pd


class AsKeyDemo(FlowSpec):
    @step
    def start(self):
        snapshot = Flow("VersioningDemo").latest_successful_run.data.dolt
        master_conf = DoltConfig(database="foo")
        with DoltDT(run=self, snapshot=snapshot) as dolt:
            df1 = dolt.read("bar", as_key="bar1")
        with DoltDT(run=self, config=master_conf) as dolt:
            df2 = dolt.read("bar", as_key="bar2")

        self.next(self.end)

    @step
    def end(self):
        print(self.dolt)


if __name__ == "__main__":
    AsKeyDemo()
