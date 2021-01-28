from collections import defaultdict
from dataclasses import dataclass, field, replace
from functools import wraps
import json
import os
import time
from typing import Dict, List, Optional, Union
import uuid

import pandas as pd

from doltpy.core import Dolt
from doltpy.core.write import import_df
from doltpy.core.dolt import DoltException
from doltpy.core.read import read_table
from doltpy.core.read import read_table_sql
from metaflow import FlowSpec, Run, current

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
    commit: str = None
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
    # fully_qualified_name: str

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
class DoltSnapshot(object):
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


# TODO: expose other dolt functions?
#   - dolt config
#   - dolt log
#   - dolt creds


def runtime_only(f):
    @wraps(f)
    def inner(*args, **kwargs):
        from metaflow import current

        if not current.is_running_flow:
            return
        return f(*args, **kwargs)

    return inner


def snapshot_unsafe(f):
    @wraps(f)
    def inner(*args, **kwargs):
        if isinstance(args[0], DoltSnapshotDT):
            return
        return f(*args, **kwargs)

    return inner


class DoltDTBase(object):
    def __init__(self, run: FlowSpec, config: DoltConfig):

        self._run = run
        if hasattr(self._run, "data") and hasattr(self._run.data, "dolt"):
            self._dolt = self._run.data.dolt
        elif hasattr(self._run, "dolt"):
            self._dolt = self._run.dolt
        else:
            self._run.dolt = DoltSnapshot().dict()
            self._dolt = self._run.dolt

        if not isinstance(self._dolt, dict):
            raise ValueError(
                f"Dolt artifact should be type: dict; found: {type(self._dolt)}"
            )

        self._config = config
        self._dbcache = {}  # configid -> Dolt instance
        self._new_actions = {}  # keep track of write state to commit at end
        self._pending_writes = []

        self._get_db(self._config)

    def __enter__(self):
        from metaflow import current

        if not current.is_running_flow:
            Exception("Context manager only usable while running flows")
        self._start_run_attributes = set(vars(self._run).keys())
        return self

    def __exit__(self, *args, allow_empty: bool = True):
        # TODO: how to associate new variables with dolt actions?
        new_attributes = set(vars(self._run).keys()) - self._start_run_attributes

        if self._new_actions:
            self._commit_actions()
            self._update_dolt_artifact()

        return

    def read(self, tablename: str, as_key: Optional[str] = None):
        action = DoltAction(
            kind="read",
            key=as_key or key,
            commit=self._config.commit,
            config_id=self._config.id,
            pathspec=self._pathspec,
            table_name=key,
        )
        table = self._execute_read_action(action, self._config)
        return table

    @snapshot_unsafe
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
        self._add_action(action)
        db = self._get_db(self._config)
        return read_table_sql(db, f'{q} AS OF "{action.commit}"')

    @runtime_only
    @snapshot_unsafe
    def write(
        self,
        df: pd.DataFrame,
        table_name: str,
        pks: List[str] = None,
        as_key: str = None,
    ):
        db = self._get_db(self._config)
        if not pks:
            df = df.reset_index()
            pks = df.columns
        import_df(repo=db, table_name=table_name, data=df, primary_keys=pks)
        action = DoltAction(
            kind="write",
            key=table_name or key,
            commit=None,
            config_id=self._config.id,
            pathspec=self._pathspec,
            table_name=table_name,
        )
        self._add_action(action)
        return

    def _execute_read_action(self, action: DoltAction, config: DoltConfig):
        db = self._get_db(config)
        table = self._get_table_asof(db, action.table_name, action.commit)
        self._add_action(action)
        return table

    @runtime_only
    def _add_action(self, action: DoltAction):
        if action.key in self._new_actions:
            raise ValueError("Duplicate key attempted to override dolt state")

        if action.kind == "write":
            self._pending_writes.append(action)

        self._new_actions[action.key] = action
        return

    @runtime_only
    @snapshot_unsafe
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
        lg = dolt.log()
        return lg.popitem(last=False)[0]

    @property
    def _pathspec(self):
        from metaflow import current

        return f"{current.flow_name}/{current.run_id}/{current.step_name}/{current.task_id}"

    def _get_table_asof(
        self, dolt: Dolt, table_name: str, commit: str = None
    ) -> pd.DataFrame:
        base_query = f"SELECT * FROM `{table_name}`"
        if commit:
            return read_table_sql(dolt, f'{base_query} AS OF "{commit}"')
        else:
            return read_table_sql(dolt, base_query)


class DoltSnapshotDT(DoltDTBase):
    def __init__(self, snapshot: DoltSnapshot, run: Optional[FlowSpec]):
        """
        Can only read from a SnapshotDT, and reading is isolated to the snapshot.
        """
        super().__init__(run=run, config=DoltConfig())
        self._read_snapshot = snapshot
        self._sactions = {k: DoltAction(**v) for k, v in snapshot["actions"].items()}
        self._sconfigs = {k: DoltConfig(**v) for k, v in snapshot["configs"].items()}

    def read(self, key, as_key: Optional[str] = None):
        snapshot_action = self._sactions.get(key, None)
        if not snapshot_action:
            raise ValueError("Key not found in snapshot")

        action = snapshot_action.copy()
        action.key = as_key or key
        action.kind = "read"

        config = self._sconfigs[action.config_id]
        table = self._execute_read_action(action, config)
        return table


class DoltBranchDT(DoltDTBase):
    def __init__(self, run: FlowSpec, config: DoltConfig):
        """
        Can read or write with Dolt, starting from a single reference commit.
        """
        super().__init__(run=run, config=config)

    def read(self, key: str, as_key: Optional[str] = None):
        action = DoltAction(
            kind="read",
            key=as_key or key,
            commit=self._config.commit,
            config_id=self._config.id,
            pathspec=self._pathspec,
            table_name=key,
        )
        table = self._execute_read_action(action, self._config)
        return table


def DoltDT(
    run: Optional[FlowSpec] = None,
    snapshot: Optional[dict] = None,
    config: Optional[DoltConfig] = None,
):
    if config and snapshot:
        raise ValueError("Specify snapshot or config mode, not both.")
    elif snapshot:
        return DoltSnapshotDT(snapshot=snapshot, run=run)
    elif config:
        return DoltBranchDT(run, config)
    elif run and hasattr(run, "data") and hasattr(run.data, "dolt"):
        return DoltSnapshotDT(snapshot=run.data.dolt, run=run)
    else:
        raise ValueError("Specify one of: snapshot, config")
