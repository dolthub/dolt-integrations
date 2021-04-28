import csv

import pytest

from dolt_integrations.core.interface import *


def write_dict_to_csv(data, file):
    csv_columns = list(data[0].keys())
    with open(file, "w") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_columns)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def read_csv_to_dict(file):
    with open(file, "r") as csvfile:
        reader = csv.DictReader(csvfile)
        return list(reader)


def test_export_table_csv(doltdb, tmpfile):
    dolt_export_csv(db=doltdb, tablename="foo", filename=tmpfile)
    res = read_csv_to_dict(tmpfile)
    assert len(res) == 5
    assert set(res[0].keys()) == {"a", "b"}
    assert res[0]["a"] == "0"


def test_export_tablename_csv(doltdb, tmpfile):
    dolt_sql_to_csv(db=doltdb, sql="select * from foo", filename=tmpfile)
    res = read_csv_to_dict(tmpfile)
    assert len(res) == 5
    assert set(res[0].keys()) == {"a", "b"}
    assert res[0]["a"] == "0"


def test_import_csv(doltdb, tmpfile):
    cmp = [
        dict(c=0, d=0),
        dict(c=1, d=1),
        dict(c=2, d=2),
        dict(c=3, d=3),
    ]
    write_dict_to_csv(cmp, tmpfile)
    dolt_import_csv(
        db=doltdb, tablename="bar", filename=tmpfile, save_args=dict(primary_key="c")
    )
    res = doltdb.sql("select * from bar", result_format="csv")
    for r1, r2 in zip(cmp, res):
        assert r1["c"] == int(r2["c"])


def test_action_default():
    action = action_meta(
        tablename="t",
        filename="f",
        from_commit="fc",
        to_commit="tc",
        branch="br",
        kind="save",
        meta_conf=CallbackMeta(fn=lambda x: x)
    )
    assert action["filename"] == "f"


def test_action_callback():
    action = action_meta(
        tablename="t",
        filename="f",
        from_commit="fc",
        to_commit="tc",
        branch="br",
        kind="save",
        meta_conf=CallbackMeta(fn=lambda x: x["filename"]),
    )
    assert action == "f"


def test_action_dolt(doltdb):
    action = action_meta(
        tablename="t",
        filename="f",
        from_commit="fc",
        to_commit="tc",
        branch="br",
        kind="save",
        meta_conf=DoltMeta(db=doltdb, tablename="meta"),
    )
    res = doltdb.sql("select * from meta", result_format="csv")
    assert len(res) > 0


def test_branch_serial(doltdb):
    starting_head = doltdb.head
    assert doltdb.active_branch == "master"

    branch_conf = SerialBranch(branch="new")
    with branch_conf(doltdb) as db:
        assert db.active_branch == "new"

    assert doltdb.active_branch == "master"
    assert doltdb.head == starting_head


@pytest.mark.skip
def test_branch_detach_cm(doltdb):
    branch_conf = MergeBranch(branch_from="new")
    with branch_conf(doltdb) as db:
        assert db.active_branch == "new"


def test_load_table(doltdb, tmpfile):
    res = load(
        db=doltdb,
        tablename="foo",
        filename=tmpfile,
        meta_conf=DoltMeta(db=doltdb, tablename="meta"),
        branch_conf=SerialBranch("new"),
    )
    assert doltdb.active_branch == "master"

    meta_res = doltdb.sql("select * from meta", result_format="csv")
    assert len(meta_res) == 1
    assert meta_res[0]["branch"] == "new"

    res = read_csv_to_dict(tmpfile)
    assert len(res) == 9
    assert set(res[0].keys()) == {"a", "b"}
    assert res[0]["a"] == "0"

def test_load_sql(doltdb, tmpfile):
    res = load(
        db=doltdb,
        sql="select * from foo",
        filename=tmpfile,
        meta_conf=DoltMeta(db=doltdb, tablename="meta"),
        branch_conf=SerialBranch("new"),
    )
    assert doltdb.active_branch == "master"

    meta_res = doltdb.sql("select * from meta", result_format="csv")
    assert len(meta_res) == 1
    assert meta_res[0]["branch"] == "new"

    res = read_csv_to_dict(tmpfile)
    assert len(res) == 9
    assert set(res[0].keys()) == {"a", "b"}
    assert res[0]["a"] == "0"



def test_save(doltdb, tmpfile):
    cmp = [
        dict(c=0, d=0),
        dict(c=1, d=1),
        dict(c=2, d=2),
        dict(c=3, d=3),
    ]
    write_dict_to_csv(cmp, tmpfile)

    doltdb.checkout("new")
    new_head = doltdb.head
    doltdb.checkout("master")
    master_head = doltdb.head

    save(
        db=doltdb,
        tablename="bar",
        filename=tmpfile,
        save_args=dict(primary_key="c"),
        meta_conf=DoltMeta(db=doltdb, tablename="meta"),
        branch_conf=SerialBranch("new"),
    )

    meta_res = doltdb.sql("select * from meta", result_format="csv")
    assert len(meta_res) == 1
    assert meta_res[0]["branch"] == "new"

    assert doltdb.head == master_head

    doltdb.checkout("new")
    assert doltdb.head != new_head

    res = doltdb.sql("select * from bar", result_format="csv")
    for r1, r2 in zip(cmp, res):
        assert r1["c"] == int(r2["c"])
