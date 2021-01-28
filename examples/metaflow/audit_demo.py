import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import pickle

from metaflow import FlowSpec, step, Parameter, Run
from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT
import pandas as pd


class AuditDemo(FlowSpec):
    read_run_id = Parameter(
        "read-run-id", help="Pass a run_id for a VersionDemo flow", required=True
    )

    @step
    def start(self):
        snapshot = Run(f"VersioningDemo/{self.read_run_id}").data.dolt
        with DoltDT(run=self, snapshot=snapshot) as dolt:
            df = dolt.read("bar")

        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))


if __name__ == "__main__":
    AuditDemo()
