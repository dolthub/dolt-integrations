import logging
import json

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

import altair as alt
from metaflow import FlowSpec, step, Parameter, Flow
import pandas as pd

from doltpy_integrations.metaflow.dolt import DoltConfig, DoltDT

class Flow2(FlowSpec):
    outfile = Parameter(
        "outfile", help="Pass a name for chart output", required=True, default="flow2.png",
    )

    @step
    def start(self):

        audit = Flow(f"Flow1").latest_successful_run.data.dolt
        with DoltDT(run=self, audit=audit) as dolt:
            age = dolt.read("age")

        with DoltDT(run=self, audit=audit) as dolt:
            prices = dolt.read("prices")

        age.age = age.age.astype(float)
        prices.price = prices.price.astype(float)
        prices = prices[prices.price < 300000]
        df = age.set_index("state").join(prices.set_index("state")).dropna()

        chart = alt.Chart(df).mark_point().encode(
            x=alt.X('age', scale=alt.Scale(domain=(30, 50))),
            y=alt.Y('price'),
        )

        chart_reg = (
            chart +
            chart.transform_regression('age','price',method="linear").mark_line(color="red")
        )
        chart_reg.properties(width=500).save(self.outfile)

        self.next(self.end)

    @step
    def end(self):
        print(json.dumps(self.dolt, indent=4))
        pass


if __name__ == "__main__":
    Flow2()
