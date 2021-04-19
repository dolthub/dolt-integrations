import os
import tempfile

import doltcli as dolt
import pytest


@pytest.fixture(scope="function")
def doltdb(tmpdir):
    db = dolt.Dolt.init(tmpdir)
    db.sql("create table foo (a bigint primary key, b bigint)")
    db.sql("insert into foo values (0,0), (1,1), (2,2), (3,3), (4,4)")
    db.sql("select dolt_commit('-am', 'Init foo')")
    db.checkout(branch="new", checkout_branch=True)
    db.sql("insert into foo values (5,5), (6,6), (7,7), (8,8)")
    db.sql("select dolt_commit('-am', 'Add rows to new branch')")
    db.checkout(branch="master")
    return db


@pytest.fixture(scope="function")
def tmpfile(tmp_path):
    return os.path.join(tmp_path, "random.csv")
