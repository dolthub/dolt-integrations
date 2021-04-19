from contextlib import contextmanager
import datetime
from dataclasses import asdict, dataclass, field
import json

import typing

import doltcli as dolt


@dataclass
class BranchBase:
    @contextmanager
    def __call__(self, db: dolt.Dolt):
        starting_head = db.head
        starting_active = db.active_branch

        try:
            self.checkout(db)
            yield db
        finally:
            self.merge(db, starting_active)


class ParallelBranch(BranchBase):
    branch_from: str  # existing commit or branch
    merge_to: str  # optional

    def checkout(self, db: dolt.Dolt):
        pass

    def merge(self, db: dolt.Dolt, starting_branch: str):
        pass


@dataclass
class SerialBranch(BranchBase):
    branch: str

    def checkout(self, db: dolt.Dolt):
        # branch must exist
        db.checkout(branch=self.branch, error=False)

    def merge(self, db: dolt.Dolt, starting_branch: str):
        db.checkout(starting_branch, error=False)


class Encoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)


@dataclass
class Action:
    from_commit: str
    to_commit: str
    branch: str
    tablename: str
    filename: str
    timestamp: datetime.datetime = field(
        default_factory=lambda: datetime.datetime.now()
    )
    context_id: str = None
    kind: str = "load"
    meta: dict = None

    def dict(self):
        return asdict(self)

    def json(self):
        return json.dumps(self.dict(), cls=Encoder)


class Meta:
    def create(self, action: Action):
        return action.dict()


@dataclass
class DoltMeta(Meta):
    db: dolt.Dolt
    tablename: str
    branch_config: dict = None

    def create(self, a: Action):
        branch_config = self.branch_config or SerialBranch(branch=self.db.active_branch)
        with branch_config(self.db):
            tables = self.db.sql(
                f"select * from information_schema.tables where table_name = '{self.tablename}'",
                result_format="json",
            )

            if len(tables["rows"]) < 1:
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
                self.db.sql(create_table)

            self.db.sql(
                f"""
                    insert into
                    {self.tablename} (kind, filename, branch, from_commit, to_commit, tablename, timestamp, context_id)
                    values ('{a.kind}', '{a.filename}', '{a.branch}', '{a.from_commit}', '{a.to_commit}', '{a.tablename}', '{a.timestamp}', '{a.context_id}')
                    """,
                result_format="csv",
            )
        return a.dict()


@dataclass
class CallbackMeta:
    fn: typing.Callable

    def create(self, action: Action):
        return self.fn(action.dict())


def action_meta(
    tablename: str,
    filename: str,
    from_commit: str,
    to_commit: str,
    branch: str,
    kind: str,
    context_id: str = None,
    meta_conf: Meta = None,
):
    action = Action(
        tablename=tablename,
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
    name: str = None
    url: str = None
    force: bool = False

    def pull(self, db: dolt.Dolt):
        pass

    def push(self, db: dolt.Dolt):
        pass


def dolt_export_csv(
    db: dolt.Dolt, tablename: str, filename: str, load_args: dict = None
):
    exp = ["table", "export", "-f", "--file-type", "csv", tablename, filename]
    db.execute(exp)


def dolt_import_csv(
    db: dolt.Dolt, tablename: str, filename: str, save_args: dict = None
):
    mode = "-c"
    tables = db.ls()
    for t in tables:
        if t.name == tablename:
            mode = "-u"
            break

    imp = ["table", "import", mode, tablename]
    if "primary_key" in save_args:
        pks = ",".join(save_args["primary_key"])
        imp.append(f"--pk {pks}")

    imp.append(filename)
    db.execute(imp)


def load(
    db: dolt.Dolt,
    tablename: str,
    filename: str,
    load_args: dict = None,
    meta_conf: dict = None,
    remote_conf: dict = None,
    branch_conf: dict = None,
):
    """
    db remote pattern with context
    db checkout pattern with branch context
    load data into csv, return filepath
    metadata context needs current branch
    """
    if remote_conf is not None:
        remote_conf.pull(db)

    with branch_conf(db) as chk_db:
        dolt_export_csv(
            db=db, tablename=tablename, filename=filename, load_args=load_args
        )

        commit = chk_db.head
        branch = chk_db.active_branch

    meta = action_meta(
        tablename=tablename,
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
    meta_conf: dict = None,
    remote_conf: dict = None,
    branch_conf: dict = None,
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
    if remote_conf is not None:
        remote_conf.pull(db)

    with branch_conf(db) as chk_db:
        from_commit = chk_db.head

        dolt_import_csv(
            db=db, tablename=tablename, filename=filename, save_args=save_args
        )

        chk_db.sql(f"select dolt_add('{tablename}')")
        status = chk_db.sql("select * from dolt_status", result_format="csv")
        if len(status) > 0:
            chk_db.sql("select dolt_commit('-m', 'Automated commit')")

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
