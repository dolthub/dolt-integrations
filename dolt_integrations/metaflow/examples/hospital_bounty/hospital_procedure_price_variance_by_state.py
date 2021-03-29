import logging

import pandas as pd
from metaflow import FlowSpec, Run, step, Parameter
from dolt_integrations.metaflow.dolt import DoltConfig, DoltDT

logger = logging.getLogger()
logger.setLevel(logging.WARNING)


class HospitalProcedurePriceVarianceByState(FlowSpec):

    hospital_price_analysis_db = Parameter(
        "hospital-price-analysis-db", help="Dolt database to write analysis to", required=True
    )

    hospital_price_analysis_db_branch = Parameter(
        "hospital-price-analysis-db-branch", help="Specify branch version", default="master",
    )

    @step
    def start(self):
        analysis_conf = DoltConfig(
            database=self.hospital_price_analysis_db,
            branch=self.hospital_price_analysis_db_branch
        )
        with DoltDT(run=self, config=analysis_conf) as dolt:
            median_price_by_state = dolt.read("state_procedure_medians")
            variance_by_procedure = median_price_by_state.groupby("code").var()
            dolt.write(variance_by_procedure, "variance_by_procedure")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HospitalProcedurePriceVarianceByState()