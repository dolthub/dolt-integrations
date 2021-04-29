import os
import shutil

from doltcli import Dolt
from dolt_integrations.utils import write_pandas
import metaflow
import pandas as pd
import pytest

from dolt_integrations.metaflow import DoltDT, DoltConfig


@pytest.fixture(scope="function")
def dolt_audit1(doltdb):
    db = Dolt(doltdb)
    lg = db.log()
    commit = lg.popitem(last=False)[0]
    yield {
        "actions": {
            "bar": {
                "key": "bar",
                "config_id": "dd9f1f38-6802-4657-b869-602dde993180",
                "pathspec": "VersioningDemo/1611853111934656/start/1",
                "table_name": "bar",
                "kind": "read",
                "query": "SELECT * FROM `bar`",
                "commit": commit,
                "artifact_name": None,
                "timestamp": 1611853112.794624,
            }
        },
        "configs": {
            "dd9f1f38-6802-4657-b869-602dde993180": {
                "id": "dd9f1f38-6802-4657-b869-602dde993180",
                "database": doltdb,
                "branch": "master",
                "commit": commit,
                "dolthub_remote": False,
                "push_on_commit": False,
            }
        },
    }


@pytest.fixture(scope="function")
def dolt_config(doltdb):
    yield DoltConfig(database=doltdb)


@pytest.fixture(scope="function")
def doltdb():
    db_path = os.path.join(os.path.dirname(__file__), "foo")
    try:
        db = Dolt.init(db_path)
        df_v1 = pd.DataFrame({"A": [1, 1, 1], "B": [1, 1, 1]})
        write_pandas(
            dolt=db,
            table="bar",
            df=df_v1.reset_index(),
            primary_key=["index"],
            import_mode="create",
        )
        db.add("bar")
        db.commit("Initialize bar")

        df_v2 = pd.DataFrame({"A": [2, 2, 2], "B": [2, 2, 2]})
        write_pandas(
            dolt=db,
            table="bar",
            df=df_v2.reset_index(),
            primary_key=["index"],
            import_mode="create",
        )
        db.add("bar")
        db.commit("Edit bar")
        yield db_path
    finally:
        if os.path.exists(db_path):
            shutil.rmtree(db_path)


@pytest.fixture(scope="function")
def active_current():
    current = metaflow.current
    current._set_env(is_running=True)
    yield current


class Run:
    pass


@pytest.fixture(scope="function")
def inactive_run():
    current = metaflow.current
    current._set_env(is_running=False)
    yield Run()


@pytest.fixture(scope="function")
def active_run(active_current):
    print(active_current.is_running_flow)
    yield Run()


@pytest.fixture(scope="function")
def progress_run(active_run, dolt_audit1):
    active_run.dolt = dolt_audit1
    yield active_run
