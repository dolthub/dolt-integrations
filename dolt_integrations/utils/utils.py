import pandas as pd
import datetime
from typing import Optional, List
from doltcli import Dolt
from doltcli.utils import (  # type: ignore
    _import_helper,
    get_read_table_asof_query,
    read_table_sql,
)


def parse_to_pandas(sql_output: str) -> pd.DataFrame:
    return pd.read_csv(sql_output)


def read_pandas_sql(dolt: Dolt, sql: str) -> pd.DataFrame:
    return read_table_sql(dolt, sql, result_parser=parse_to_pandas)


def read_pandas(dolt: Dolt, table: str, as_of: str = None) -> pd.DataFrame:
    return read_pandas_sql(dolt, get_read_table_asof_query(table, as_of))


def write_pandas(
    dolt: Dolt,
    table: str,
    df: pd.DataFrame,
    import_mode: Optional[str] = None,
    primary_key: Optional[List[str]] = None,
    commit: Optional[bool] = False,
    commit_message: Optional[str] = None,
    commit_date: Optional[datetime.datetime] = None,
):
    """

    :param dolt:
    :param table:
    :param df:
    :param import_mode:
    :param primary_key:
    :param commit:
    :param commit_message:
    :param commit_date:
    :return:
    """

    def writer(filepath: str):
        clean = df.dropna(subset=primary_key)
        clean.to_csv(filepath, index=False)

    _import_helper(
        dolt=dolt,
        table=table,
        write_import_file=writer,
        primary_key=primary_key,
        import_mode=import_mode,
        commit=commit,
        commit_message=commit_message,
        commit_date=commit_date,
    )
