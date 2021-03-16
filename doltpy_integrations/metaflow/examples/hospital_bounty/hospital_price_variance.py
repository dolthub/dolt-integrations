import logging
import json

from metaflow import FlowSpec, Run, step, Parameter
from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT

logger = logging.getLogger()
logger.setLevel(logging.WARNING)


class HospitalPriceVariance(FlowSpec):

    hospital_price_db = Parameter(
        "hospital-price-db", help="Database of hospital procedure prices", required=True
    )

    hospital_price_db_branch = Parameter(
        "hospital-price-db-branch", help="Specify branch version", default="master",
    )

    historical_run_path = Parameter(
        "historical-run-path", help="Read the same data as a path to a previous run"
    )

    hospital_price_analysis_db = Parameter(
        "hospital-price-analysis-db", help="Dolt database to write analysis to", required=True
    )

    hospital_price_analysis_db_branch = Parameter(
        "hospital-price-analysis-db-branch", help="Specify branch version", default="master",
    )

    @step
    def start(self):

        if not self.run_path:
            read_conf = DoltConfig(database=self.hospital_price_db, branch=self.hospital_branch)
        else:
            read_conf = DoltConfig(run=self, audit=Run(self.historical_run_path).data.dolt)

        with DoltDT(run=self, config=read_conf) as dolt:
            prices_by_state = """
                SELECT
                  h.state,
                  p.code,
                  p.payer,
                  p.price
                FROM
                  prices p
                  LEFT JOIN hospitals h ON p.npi_number = h.npi_number;
            """
            prices = dolt.sql(prices_by_state, as_key="prices")

        median_price_by_state = prices.groupby(['state', 'code']).median()

        write_conf = DoltConfig(database=self.hospital_price_analysis_db)
        with DoltDT(run=self, config=write_conf) as dolt:
            dolt.write(
                median_price_by_state,
                "state_procedure_medians",
                ["state", "code"]
            )

    @step
    def variances(self):
        analysis_conf = DoltConfig(database=self.hospital_price_analysis_db, branch=self.hospital_branch)
        with DoltDT(run=self, config=analysis_conf) as dolt:
            median_price_by_state = dolt.read("state_procedure_medians")
            variance_by_procedure = median_price_by_state.groupby("code").var()
            dolt.write(variance_by_procedure, "variance_by_procedure")

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))
        pass


if __name__ == "__main__":
    HospitalPriceVariance()
