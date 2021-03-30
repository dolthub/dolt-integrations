from doltcli import Dolt
import numpy as np
import pandas as pd
import pytest

from dolt_integrations.metaflow import DoltDT
from dolt_integrations.utils import read_pandas_sql

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
    np.testing.assert_array_equal(df.A.values, [2, 2, 2])
    audit = active_run.dolt
    assert "bar" in audit["actions"]
    assert audit["actions"]["bar"]["kind"] == "read"
    assert audit["actions"]["bar"]["query"] == "SELECT * FROM `bar`"


def test_auditdt_cm_read(active_run, dolt_audit1):
    with DoltDT(run=active_run, audit=dolt_audit1) as dolt:
        df = dolt.read("bar")
    np.testing.assert_array_equal(df.A.values, [2, 2, 2])
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
    np.testing.assert_array_equal(df.A.values, [2, 2, 2])
    assert dolt._dolt["actions"] == {}


def test_auditdt_inactive_standalone_read(inactive_run, dolt_audit1):
    dolt = DoltDT(audit=dolt_audit1)
    df = dolt.read("bar")
    np.testing.assert_array_equal(df.A.values, [2, 2, 2])
    assert dolt._dolt["actions"] == {}


def test_auditdt_inactive_kwarg_standalone_read(inactive_run, dolt_audit1):
    dolt = DoltDT(run=inactive_run, audit=dolt_audit1)
    df = dolt.read("bar")
    np.testing.assert_array_equal(df.A.values, [2, 2, 2])
    assert dolt._dolt["actions"] == {}


# branch write success
def test_branchdt_cm_write(active_run, dolt_config, doltdb):
    input_df = pd.DataFrame({"A": [2, 2, 2], "B": [2, 2, 2]})
    with DoltDT(run=active_run, config=dolt_config) as dolt:
        dolt.write(df=input_df, table_name="baz")

    db = Dolt(doltdb)
    output_df = read_pandas_sql(db, "SELECT * from `baz`")
    np.testing.assert_array_equal(output_df.A.values, [2, 2, 2])

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


def test_custom_query_branch(active_run, dolt_config, doltdb):
    doltdb = Dolt(doltdb)
    logs = list(doltdb.log(2).keys())
    dolt_config.commit = logs[1]

    with DoltDT(run=active_run, config=dolt_config) as dolt:
        df = dolt.sql("SELECT * FROM `bar` LIMIT 2", as_key="akey")
    np.testing.assert_array_equal(df.A.values, [1, 1])
    audit = active_run.dolt
    assert "akey" in audit["actions"]
    assert audit["actions"]["akey"]["kind"] == "read"
    assert audit["actions"]["akey"]["query"] == "SELECT * FROM `bar` LIMIT 2"


@pytest.mark.xfail
def test_auditdt_cm_query(active_run, dolt_audit1):
    with DoltDT(run=active_run, audit=dolt_audit1) as dolt:
        df = dolt.sql("SELECT * FROM `bar`", as_key="akey")


def test_branchdt_diff(inactive_run, dolt_config, doltdb):
    doltdb = Dolt(doltdb)
    logs = list(doltdb.log(2).keys())

    dolt = DoltDT(config=dolt_config)
    diff = dolt.diff(from_commit=logs[1], to_commit=logs[0], table="bar")

    row = diff["bar"].iloc[0]
    assert row.from_A == 1
    assert row.from_B == 1
    assert row.to_A == 2
    assert row.to_B == 2


@pytest.mark.xfail
def test_auditdt_cm_diff(active_run, dolt_audit1, doltdb):
    doltdb = Dolt(doltdb)
    logs = list(doltdb.log(2).keys())

    with DoltDT(run=active_run, audit=dolt_audit1) as dolt:
        df = dolt.sql("SELECT * FROM `bar`", as_key="akey")
        diff = dolt.diff(from_commit=logs[1], to_commit=logs[0], table="bar")
