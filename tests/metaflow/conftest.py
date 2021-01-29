import os
import shutil

from doltpy.core import Dolt
from doltpy.core.write import import_df
import metaflow
import pandas as pd
import pytest

from doltpy_integrations.metaflow import DoltDT, DoltConfig

@pytest.fixture(scope="function")
def dolt_audit1():
    yield {
        "actions": {
            "bar": {
                "key": "bar",
                "config_id": "88a0513f-6310-48e3-b403-af909de4b2b8",
                "pathspec": "VersioningDemo/1611853111934656/start/1",
                "table_name": "bar",
                "kind": "read",
                "query": "SELECT * FROM `bar`",
                "commit": "hupehmg3q5hqqb56vjigfn3kuei0s41m",
                "artifact_name": None,
                "timestamp": 1611853112.794624
            }
        },
        "configs": {
            "dd9f1f38-6802-4657-b869-602dde993180": {
                "id": "dd9f1f38-6802-4657-b869-602dde993180",
                "database": "foo",
                "branch": "master",
                "commit": "7o48tp7lhcgh96fc1urjni2h6uhi933g",
                "dolthub_remote": False,
                "push_on_commit": False
            }
        }
    }

@pytest.fixture(scope="function")
def dolt_config(doltdb):
    yield DoltConfig(database=doltdb)

@pytest.fixture(scope="function")
def doltdb():
    db_path = os.path.join(os.path.dirname(__file__), "foo")
    try:
        db = Dolt.init(db_path)
        print(db_path)
        df_v1 = pd.DataFrame({"A": [1, 1, 1], "B": [1, 1, 1]})
        import_df(db, "bar", df_v1.reset_index(), ["index"], "create")
        db.add("bar")
        db.commit("Initialize bar")
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
