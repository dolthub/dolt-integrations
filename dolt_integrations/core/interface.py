from contextlib import contextmanager
import datetime
from dataclasses_json import dataclass_json
from dataclasses import asdict, dataclass, field
import json

from typing import Callable, Optional

import doltcli as dolt # typing: ignore


class Branch:
    @contextmanager
    def __call__(self, db: dolt.Dolt):
        starting_head = db.head
        starting_branch = db.active_branch

        try:
            self.checkout(db)
            yield db
        finally:
            self.merge(db, starting_branch)
            db.checkout(starting_branch, error=False)

    def checkout(self, db: dolt.Dolt):
        raise NotImplemented

    def merge(self, db: dolt.Dolt, starting_branch: str):
        raise NotImplemented


@dataclass_json
@dataclass
class MergeBranch(Branch):
    branch_from: str  # existing commit or branch
    merge_to: str
    _type: str = "MergeBranch"

    def checkout(self, db: dolt.Dolt):
        pass

    def merge(self, db: dolt.Dolt, starting_branch: str):
        pass


@dataclass_json
@dataclass
class SerialBranch(Branch):
    branch: str = "master"
    _type: str = "SerialBranch"

    def checkout(self, db: dolt.Dolt):
        # branch must exist
        db.checkout(branch=self.branch, error=False)

    def merge(self, db: dolt.Dolt, starting_branch: str):
        pass


@dataclass_json
@dataclass
class NewBranch(Branch):
    branch: str
    _type: str = "NewBranch"

    def checkout(self, db: dolt.Dolt):
        res = db.sql(f"select * from dolt_branches where name = '{self.branch}'", result_format="csv")
        if len(res) == 0:
            db.sql(f"select dolt_checkout('-b', '{self.branch}')", result_format="csv")
        else:
            db.sql(f"select dolt_checkout('{self.branch}')", result_format="csv")

    def merge(self, db: dolt.Dolt, starting_branch: str):
        pass


@dataclass_json
@dataclass
class Action:
    from_commit: str
    to_commit: str
    branch: str
    filename: str
    timestamp: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now()
    )
    context_id: Optional[str] = None
    tablename: Optional[str] = None
    sql: Optional[str] = None
    kind: Optional[str] = "load"
    meta: Optional[dict] = None


class Meta:
    def create(self, action: Action):
        return action.to_dict()


@dataclass_json
@dataclass
class DoltMeta(Meta):
    db: dolt.Dolt
    tablename: str
    branch_config: Optional[Branch] = None

    def create(self, a: Action):
        branch_config = self.branch_config or SerialBranch(branch=self.db.active_branch)
        with branch_config(self.db):
            tables = self.db.sql(
                f"select * from information_schema.tables where table_name = '{self.tablename}'",
                result_format="csv",
            )

            if len(tables) < 1:
                create_table = f"""
                    create table {self.tablename} (
                        branch text,
                        filename text,
                        kind text,
                        from_commit text,
                        to_commit text,
                        tablename text,
                        timestamp datetime,
                        context_id text,
                        primary key (kind, to_commit, tablename, timestamp, context_id)
                    )
                """
                self.db.sql(create_table, result_format="csv")

            self.db.sql(
                f"""
                    insert into
                    {self.tablename} (kind, filename, branch, from_commit, to_commit, tablename, timestamp, context_id)
                    values ('{a.kind}', '{a.filename}', '{a.branch}', '{a.from_commit}', '{a.to_commit}', '{a.tablename}', '{a.timestamp}', '{a.context_id}')
                    """,
                result_format="csv",
            )
        return a.to_dict()


@dataclass_json
@dataclass
class CallbackMeta:
    fn: Callable

    def create(self, action: Action):
        return self.fn(action.to_dict())


def action_meta(
    filename: str,
    from_commit: str,
    to_commit: str,
    branch: str,
    kind: str,
    tablename: str = None,
    sql: str = None,
    context_id: str = None,
    meta_conf: Meta = None,
):
    action = Action(
        tablename=tablename,
        sql=sql,
        filename=filename,
        from_commit=from_commit,
        to_commit=to_commit,
        branch=branch,
        kind=kind,
        context_id=context_id,
    )

    if meta_conf is None:
        return

    res = meta_conf.create(action)
    return res


@dataclass
class Remote:
    name: Optional[str] = None
    url: Optional[str] = None
    force: Optional[bool] = False

    def pull(self, db: dolt.Dolt):
        pass

    def push(self, db: dolt.Dolt):
        pass


def dolt_export_csv(
    db: dolt.Dolt, tablename: str, filename: str, load_args: dict = None
):
    exp = ["table", "export", "-f", "--file-type", "csv", tablename, filename]
    db.execute(exp)


def dolt_sql_to_csv(
    db: dolt.Dolt, sql: str, filename: str, load_args: dict = None
):
    if load_args is None:
        load_args = {}
    db.sql(query=sql, result_file=filename, result_format="csv", **load_args)
    return


def dolt_import_csv(
    db: dolt.Dolt, tablename: str, filename: str, save_args: dict = None
):
    mode = "-c"
    tables = db.ls()
    for t in tables:
        if t.name == tablename:
            mode = "-r"
            break

    filetype = "--file-type csv"

    imp = ["table", "import", filetype, mode, tablename]
    if save_args and "primary_key" in save_args:
        pks = ",".join(save_args["primary_key"])
        imp.append(f"--pk {pks}")

    imp.append(filename)
    db.execute(imp)


def parse_branch_conf(conf):
    if not isinstance(conf, Branch):
        try:
            if not isinstance(conf, dict) or "_type" not in conf:
                conf = SerialBranch()
            else:
                conf = globals()[conf["_type"]](**conf)
        except Exception as e:
            print("failed to parse `branch_conf`")
            raise e
    return conf

def load(
    db: dolt.Dolt,
    filename: str,
    tablename: Optional[str] = None,
    sql: Optional[str] = None,
    load_args: Optional[dict] = None,
    meta_conf: Optional[Meta]= None,
    remote_conf: Optional[Remote] = None,
    branch_conf: Optional[Branch] = None,
):
    """
    db remote pattern with context
    db checkout pattern with branch context
    load data into csv, return filepath
    metadata context needs current branch
    """
    if tablename is not None and sql is not None:
        raise ValueError("Specify one of: tablename, qury")

    branch_conf = parse_branch_conf(branch_conf)

    if remote_conf is not None:
        remote_conf.pull(db)

    with branch_conf(db) as chk_db:
        if tablename is not None:
            dolt_export_csv(
                db=db, tablename=tablename, filename=filename, load_args=load_args
            )
        elif sql is not None:
            dolt_sql_to_csv(db=db, sql=sql, filename=filename, load_args=load_args)

        commit = chk_db.head
        branch = chk_db.active_branch

    meta = action_meta(
        tablename=tablename,
        sql=sql,
        filename=filename,
        from_commit=commit,
        to_commit=commit,
        branch=branch,
        kind="load",
        meta_conf=meta_conf,
    )

    if remote_conf is not None:
        remote_conf.push(db)

    return meta


def save(
    db: dolt.Dolt,
    tablename,
    filename: str,
    save_args: dict = None,
    meta_conf: Optional[Meta]= None,
    remote_conf: Optional[Remote] = None,
    branch_conf: Optional[Branch] = None,
    commit_message: str = "Automated commit",
):
    """
    pull remote
    checkout branch
    read csv -> use args to save data into Dolt
    commit data
    branch merge
    record action metadata (after merge b/c we care about persisted state)
    remote push
    """
    branch_conf = parse_branch_conf(branch_conf)

    if remote_conf is not None:
        remote_conf.pull(db)

    with branch_conf(db) as chk_db:
        from_commit = chk_db.head

        dolt_import_csv(
            db=db, tablename=tablename, filename=filename, save_args=save_args
        )

        chk_db.sql(f"select dolt_add('.')", result_format="csv")
        status = chk_db.sql("select * from dolt_status", result_format="csv")
        if len(status) > 0:
            chk_db.sql(f"select dolt_commit('-m', '{commit_message}')", result_format="csv")

        to_commit = chk_db.head
        branch = chk_db.active_branch

    meta = action_meta(
        tablename=tablename,
        filename=filename,
        from_commit=from_commit,
        to_commit=to_commit,
        branch=branch,
        kind="save",
        meta_conf=meta_conf,
    )

    if remote_conf is not None:
        remote_conf.push(db)

    return meta
