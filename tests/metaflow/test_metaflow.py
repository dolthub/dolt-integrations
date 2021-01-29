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
    print(df)
    np.testing.assert_array_equal(df.A.values, ["1","1","1"])

