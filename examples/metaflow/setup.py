import logging

logger = logging.getLogger()
logger.setLevel(logging.WARNING)

from doltpy.cli import Dolt
from doltpy.cli.write import write_pandas
import pandas as pd

if __name__ == "__main__":
    dolt = Dolt.init("foo")

    df_v1 = pd.DataFrame({"A": [1, 1, 1], "B": [1, 1, 1]})
    df_v2 = pd.DataFrame({"A": [1, 1, 1, 2, 2, 2], "B": [1, 1, 1, 2, 2, 2]})

    write_pandas(dolt=dolt, table="bar", df=df_v1.reset_index(), primary_key=["index"], import_mode="create")
    dolt.add("bar")
    dolt.commit("Initialize bar")

    v1 = list(dolt.log(number="1").keys())[0]

    write_pandas(dolt=dolt, table="bar", df=df_v2.reset_index(), primary_key=["index"], import_mode="update")
    dolt.add("bar")
    dolt.commit("Add rows to bar")

    v2 = list(dolt.log(number="1").keys())[0]
