import logging

from metaflow import FlowSpec, step, Parameter
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

    historical_run_path = Parameter(
        "historical-run-path", help="Read the same data as a path to a previous run"
    )

    @step
    def start(self):
        analysis_conf = DoltConfig(
            database=self.hospital_price_analysis_db,
            branch=self.hospital_price_analysis_db_branch
        )

        with DoltDT(run=self.historical_run_path or self, config=analysis_conf) as dolt:
            median_price_by_state = dolt.read("state_procedure_medians")
            variance_by_procedure = median_price_by_state.groupby("code").var()
            clean = median_price_by_state[median_price_by_state['code'].str.startswith('nan')]
            dolt.write(variance_by_procedure, "variance_by_procedure")

        self.next(self.end)

    @step
    def end(self):
        pass


if __name__ == "__main__":
    HospitalProcedurePriceVarianceByState()
