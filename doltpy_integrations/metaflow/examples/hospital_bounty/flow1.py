import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import altair as alt
from metaflow import FlowSpec, step, Parameter
import pandas as pd

from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT

class Flow1(FlowSpec):

    outfile = Parameter(
        "outfile", help="Pass a name for chart output", required=True, default="flow1.png",
    )

    hospital_branch = Parameter(
        "hospital-branch", help="Specify branch version", required=True, default="master",
    )

    @step
    def start(self):

        with DoltDT(run=self, config=DoltConfig(database="state-age")) as dolt:
            age = dolt.read("age")

        conf = DoltConfig(database="hospital-price-transparency", branch=self.hospital_branch)
        with DoltDT(run=self, config=conf) as dolt:
            get_prices_per_state = """
                SELECT AVG(p.price) as price, state
                FROM `prices` as p
                JOIN `hospitals` as h
                ON p.npi_number = h.npi_number
                WHERE p.code = "27130"
                GROUP BY h.state
            """
            prices = dolt.sql(get_prices_per_state, as_key="prices")

        age.age = age.age.astype(float)
        prices.price = prices.price.astype(float)
        df = age.set_index("state").join(prices.set_index("state")).dropna()

        chart = alt.Chart(df).mark_point().encode(
            x=alt.X('age', scale=alt.Scale(domain=(30, 50))),
            y=alt.Y('price'),
        )

        chart_reg = (
            chart +
            chart.transform_regression('age','price',method="linear").mark_line(color="red")
        )
        chart_reg.properties(width=450).save(self.outfile)

        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))
        pass


if __name__ == "__main__":
    Flow1()
