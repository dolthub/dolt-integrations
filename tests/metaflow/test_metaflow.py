from doltpy.cli import Dolt
from doltpy.cli.read import read_pandas_sql
from doltpy.cli.write import write_pandas
import metaflow
import numpy as np
import pandas as pd
import pytest

from doltpy_integrations.metaflow import DoltDT, DoltConfig

def test_branchdt_cm_init(active_run, dolt_config):
    with DoltDT(run=active_run, config=dolt_config) as dolt:
        pass

@pytest.mark.xfail
def test_branchdt_cm_init_inactive(inactive_run, dolt_config):
    with DoltDT(run=inactive_run, config=dolt_config) as dolt:
        pass

def test_auditdt_cm_init(active_run, dolt_audit1):
    with DoltDT(run=active_run, audit=dolt_audit1) as dolt:
        pass

@pytest.mark.xfail
def test_auditdt_cm_init_inactive(inactive_run, dolt_audit1):
    with DoltDT(run=inactive_run, audit=dolt_audit1) as dolt:
        pass

def test_branchdt_cm_read(active_run, dolt_config):
    with DoltDT(run=active_run, config=dolt_config) as dolt:
        df = dolt.read("bar")
    np.testing.assert_array_equal(df.A.values, ["1","1","1"])
    audit = active_run.dolt
    assert "bar" in audit["actions"]
    assert audit["actions"]["bar"]["kind"] == "read"
    assert audit["actions"]["bar"]["query"] == "SELECT * FROM `bar`"

def test_branchdt_standalone_init(inactive_run, dolt_config):
    dolt = DoltDT(config=dolt_config)

def test_branchdt_standalone_read(active_run, dolt_config):
    dolt = DoltDT(config=dolt_config)
    df = dolt.read("bar")

def test_auditdt_standalone_init(active_run, dolt_audit1):
    dolt = DoltDT(audit=dolt_audit1)

def test_auditdt_standalone_read(active_run, dolt_audit1):
    dolt = DoltDT(audit=dolt_audit1)
    df = dolt.read("bar")

# branch write success
def test_branchdt_cm_write(active_run, dolt_config, doltdb):
    input_df = pd.DataFrame({"A": [2, 2, 2], "B": [2, 2, 2]})
    with DoltDT(run=active_run, config=dolt_config) as dolt:
        dolt.write(df=input_df, table_name="baz")

    db = Dolt(doltdb)
    output_df = read_pandas_sql(db, "SELECT * from `baz`")
    np.testing.assert_array_equal(output_df.A.values, ["2","2","2"])

    audit = active_run.dolt
    print(audit["actions"]["baz"])
    assert "baz" in audit["actions"]
    assert audit["actions"]["baz"]["kind"] == "write"
    assert audit["actions"]["baz"]["query"] == "SELECT * FROM `baz`"


@pytest.mark.xfail
def test_branchdt_standalone_inactive_write(inactive_run, dolt_config):
    dolt = DoltDT(config=dolt_config)
    input_df = pd.DataFrame({"A": [2, 2, 2], "B": [2, 2, 2]})
    dolt.write(df=input_df, table_name="baz")


@pytest.mark.xfail
def test_auditdt_cm_write(active_run, dolt_audit1):
    input_df = pd.DataFrame({"A": [2, 2, 2], "B": [2, 2, 2]})
    with DoltDT(run=active_run, audit=dolt_audit1) as dolt:
        dolt.write(df=input_df, table_name="baz")

def test_artifact_reference(active_run, dolt_config):
    with DoltDT(run=active_run, config=dolt_config) as dolt:
        active_run.df = dolt.read("bar")
    audit = active_run.dolt
    assert "bar" in audit["actions"]
    assert audit["actions"]["bar"]["artifact_name"] == "df"

def test_custom_query_branch(active_run, dolt_config):
    with DoltDT(run=active_run, config=dolt_config) as dolt:
        df = dolt.sql("SELECT * FROM `bar`", as_key="akey")
    np.testing.assert_array_equal(df.A.values, ["1", "1", "1"])
    audit = active_run.dolt
    assert "akey" in audit["actions"]
    assert audit["actions"]["akey"]["kind"] == "read"
    assert audit["actions"]["akey"]["query"] == "SELECT * FROM `bar`"
