from dataclasses import dataclass, field, replace
from functools import wraps
import hashlib
import json
import time
from typing import Dict, List, Optional, Union
import uuid
from dolt_integrations.utils import read_pandas_sql, write_pandas
import pandas as pd

from doltcli import Dolt, DoltException
from metaflow import FlowSpec

DOLT_METAFLOW_ACTIONS = "metaflow_actions"


@dataclass
class DoltAction:
    """
    Describes an interaction with a Dolt database within a
    DoltDT context manager.
    """

    key: str
    config_id: str

    pathspec: str
    table_name: str = None
    commit: str = Optional[None]
    kind: str = "read"
    query: str = None
    artifact_name: str = None
    timestamp: float = field(default_factory=lambda: time.time())

    def dict(self):
        return dict(
            key=self.key,
            config_id=self.config_id,
            pathspec=self.pathspec,
            table_name=self.table_name,
            kind=self.kind,
            query=self.query,
            commit=self.commit,
            artifact_name=self.artifact_name,
            timestamp=self.timestamp,
        )

    def copy(self):
        return replace(self)


@dataclass
class DoltConfig:
    """
    Configuration for connecting to a Dolt database.
    """

    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    database: str = "."
    branch: str = "master"
    commit: str = None
    dolthub_remote: bool = False
    push_on_commit: bool = False
    # dolt_fqn: str

    def dict(self):
        return dict(
            id=self.id,
            database=self.database,
            branch=self.branch,
            commit=self.commit,
            dolthub_remote=self.dolthub_remote,
            push_on_commit=self.push_on_commit,
        )


@dataclass
class DoltAudit(object):
    """
    Dolt lineage metadata used by the DoltDT to track data versions.
    Intended to be used as a metaflow artifact, JSON serializable via .dict().
    """

    actions: Dict[str, DoltAction] = field(default_factory=dict)
    configs: Dict[str, DoltConfig] = field(default_factory=dict)

    def dict(self):
        return dict(
            actions=self.actions,
            configs=self.configs,
        )

    @classmethod
    def from_json(self, data: str):
        return cls(**json.loads(data))


def runtime_only(f):
    @wraps(f)
    def inner(*args, **kwargs):
        from metaflow import current

        if not current.is_running_flow:
            raise ValueError(f"Action only permitted during running flow: {repr(f)}")

        return f(*args, **kwargs)

    return inner


def audit_unsafe(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if isinstance(args[0], DoltAuditDT):
            raise ValueError(f"Action only permitted using branch mode: {repr(f)}")
        return f(*args, **kwargs)

    return inner


class DoltDTBase(object):

    def __init__(self, run: Optional[FlowSpec], config: Optional[DoltConfig] = None):
        """
        Can read or write with Dolt, starting from a single reference commit.
        """

        self._run = run
        if not self._run:
            self._dolt = DoltAudit().dict()
        elif hasattr(self._run, "data") and hasattr(self._run.data, "dolt"):
            self._dolt = self._run.data.dolt
        elif hasattr(self._run, "dolt"):
            self._dolt = self._run.dolt
        else:
            self._run.dolt = DoltAudit().dict()
            self._dolt = self._run.dolt

        if not isinstance(self._dolt, dict):
            raise ValueError(
                f"Dolt artifact should be type: dict; found: {type(self._dolt)}"
            )

        self._config = config
        self._dbcache = {}  # configid -> Dolt instance
        self._new_actions = {}  # keep track of write state to commit at end
        self._pending_writes = []
        self._dolt_marked = {}

    def __enter__(self):
        from metaflow import current

        if not current.is_running_flow:
            raise ValueError("Context manager only usable while running flows")
        self._start_run_attributes = set(vars(self._run).keys())
        return self

    def __exit__(self, *args, allow_empty: bool = True):
        if self._new_actions:
            self._reverse_object_action_marks()
            self._commit_actions()
            self._update_dolt_artifact()
        return

    @runtime_only
    def _reverse_object_action_marks(self):
        new_attributes = set(vars(self._run).keys()) - self._start_run_attributes
        for a in new_attributes:
            obj = getattr(self._run, a, None)
            h = self._hash_object(obj)
            key = self._dolt_marked.get(h, None)
            if key and key in self._new_actions:
                self._new_actions[key].artifact_name = a

    def read(self, table_name: str, as_key: Optional[str] = None):
        action = DoltAction(
            kind="read",
            key=as_key or table_name,
            commit=self._config.commit,
            query=f"SELECT * FROM `{table_name}`",
            config_id=self._config.id,
            pathspec=self._pathspec,
            table_name=table_name,
        )
        return self._execute_read_action(action, self._config)

    @audit_unsafe
    def sql(self, q: str, as_key: str):
        action = DoltAction(
            kind="read",
            key=as_key,
            commit=self._config.commit,
            config_id=self._config.id,
            query=q,
            pathspec=self._pathspec,
            table_name=None,
        )
        return self._execute_read_action(action, self._config)

    @runtime_only
    @audit_unsafe
    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        pks: List[str] = None,
        as_key: str = None,
    ):
        if not pks:
            df = df.reset_index()
            pks = list(df.columns)
        db = self._get_db(self._config)
        write_pandas(dolt=db, table=table_name, df=df, primary_key=pks)

        action = DoltAction(
            kind="write",
            key=as_key or table_name,
            commit=None,
            config_id=self._config.id,
            query=f"SELECT * FROM `{table_name}`",
            pathspec=self._pathspec,
            table_name=table_name,
        )
        self._add_action(action)
        self._mark_object(df, action)
        return

    @audit_unsafe
    def diff(
        self,
        from_commit: str,
        to_commit: str,
        table: Union[str, List[str]]
    ) -> Dict[str, pd.DataFrame]:
        def get_query(table: str) -> str:
            return f"""
                SELECT
                    *
                FROM
                    dolt_diff_{table}
                WHERE
                    from_commit = '{from_commit}'
                    AND to_COMMIT = '{to_commit}'
            """

        db = self._get_db(self._config)
        tables = [table] if isinstance(table, str) else table
        result = {table: read_pandas_sql(db, get_query(table)) for table in tables}
        return result

    def _execute_read_action(self, action: DoltAction, config: DoltConfig):
        db = self._get_db(config)
        starting_commit = self._get_latest_commit_hash(db)
        try:
            db.sql(query=f"set `@@{db.repo_name}_head` = '{action.commit}'", result_format='csv')
            table = read_pandas_sql(db, action.query)
            self._add_action(action)
            self._mark_object(table, action)
            return table
        except Exception as e:
            raise e
        finally:
            db.sql(query=f"set `@@{db.repo_name}_head` = '{starting_commit}'")


    @runtime_only
    def _add_action(self, action: DoltAction):
        if action.key in self._new_actions:
            raise ValueError("Duplicate key attempted to override dolt state")

        if action.kind == "write":
            self._pending_writes.append(action)

        self._new_actions[action.key] = action
        return

    def _hash_object(self, obj):
        if isinstance(obj, pd.DataFrame):
            h = hashlib.sha256(pd.util.hash_pandas_object(obj, index=True).values).hexdigest()
        else:
            h = hash(obj)
        return h

    @runtime_only
    def _mark_object(self, obj, action: DoltAction):
        self._dolt_marked[self._hash_object(obj)] = action.key

    @runtime_only
    @audit_unsafe
    def _commit_actions(self, allow_empty: bool = True):
        if not self._pending_writes:
            return

        db = self._get_db(self._config)
        for a in self._pending_writes:
            db.add(a.table_name)

        db.commit(f"Run: {self._pathspec}", allow_empty=allow_empty)
        commit = self._get_latest_commit_hash(db)
        for a in self._pending_writes:
            self._new_actions[a.key].commit = commit

        return

    def _update_dolt_artifact(self):
        self._dolt["actions"].update(
            {k: v.dict() for k, v in self._new_actions.items()}
        )
        self._dolt["configs"][self._config.id] = self._config.dict()
        return

    def _get_db(self, config: DoltConfig):
        if config.id in self._dbcache:
            return self._dbcache[config.id]

        # TODO: clone remote
        try:
            Dolt.init(repo_dir=config.database)
        except DoltException as e:
            pass

        doltdb = Dolt(repo_dir=config.database)
        try:
            doltdb.checkout(config.branch, checkout_branch=False)
        except DoltException as e:
            pass

        if not doltdb.status().is_clean:
            raise Exception(
                "DoltDT as context manager requires clean working set for transaction semantics"
            )

        if not config.commit:
            config.commit = self._get_latest_commit_hash(doltdb)

        self._dbcache[config.id] = doltdb
        return doltdb

    @staticmethod
    def _get_latest_commit_hash(dolt: Dolt) -> str:
        return dolt.head

    @property
    def _pathspec(self):
        from metaflow import current

        return f"{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}"

class DoltBranchDT(DoltDTBase):

    def __init__(self, run: FlowSpec, config: DoltConfig):
        super().__init__(run=run, config=config)
        self._get_db(self._config)

class DoltAuditDT(DoltDTBase):
    def __init__(self, audit: dict, run: Optional[FlowSpec] = None):
        """
        Can only read from a AuditDT, and reading is isolated to the audit.
        """
        super().__init__(run=run)
        self._read_audit = audit
        self._sactions = {k: DoltAction(**v) for k, v in audit["actions"].items()}
        self._sconfigs = {k: DoltConfig(**v) for k, v in audit["configs"].items()}

    def read(self, key, as_key: Optional[str] = None):
        audit_action = self._sactions.get(key, None)
        if not audit_action:
            raise ValueError("Key not found in audit")

        action = audit_action.copy()
        action.key = as_key or key
        if action.kind != "read":
            action.kind = "read"
            action.query = action.query or f"SELECT * FROM `{action.table_name}`",

        config = self._sconfigs[action.config_id]
        return self._execute_read_action(action, config)

    def __exit__(self, *args, allow_empty: bool = True):
        if self._new_actions:
            self._reverse_object_action_marks()
            self._update_dolt_artifact()
        return

    def _update_dolt_artifact(self):
        for k, v in self._new_actions.items():
            self._dolt["actions"][k] = v.dict()
            self._dolt["configs"][v.config_id] = self._sconfigs[v.config_id].dict()
        return

def DoltDT(
    run: Optional[FlowSpec] = None,
    audit: Optional[dict] = None,
    config: Optional[DoltConfig] = None,
):
    if config and audit:
        raise ValueError("Specify audit or config mode, not both.")
    elif audit:
        return DoltAuditDT(audit=audit, run=run)
    elif config:
        return DoltBranchDT(run, config)
    elif run and hasattr(run, "data") and hasattr(run.data, "dolt"):
        return DoltAuditDT(audit=run.data.dolt, run=run)
    else:
        raise ValueError("Specify one of: audit, config")