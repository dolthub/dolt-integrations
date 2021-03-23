import logging

import pandas as pd
from metaflow import FlowSpec, Run, step, Parameter
from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT

logger = logging.getLogger()
logger.setLevel(logging.WARNING)


class HospitalPriceStateMedians(FlowSpec):

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
        if not self.historical_run_path:
            read_conf = DoltConfig(
                database=self.hospital_price_db,
                branch=self.hospital_price_db_branch
            )
        else:
            read_conf = DoltConfig(run=self, audit=Run(self.historical_run_path).data.dolt)

        with DoltDT(run=self, config=read_conf) as dolt:
            prices_sql = "SELECT npi_number, code, payer, price FROM prices"
            prices = dolt.sql(prices_sql)
            hospitals_sql = "SELECT state, npi_number FROM hospitals"
            hospitals = dolt.sql(hospitals_sql)

        prices_by_state = prices.join(hospitals, how='left', on='npi_number')
        median_price_by_state = prices_by_state.groupby(['state', 'code']).median()

        print("writing medians to Dolt")
        write_conf = DoltConfig(database=self.hospital_price_analysis_db)
        with DoltDT(run=self, config=write_conf) as dolt:
            dolt.write(
                median_price_by_state,
                "state_procedure_medians",
                ["state", "code"]
            )

        self.next(self.end)


    @step
    def end(self):
        pass


if __name__ == "__main__":
    HospitalPriceStateMedians()
