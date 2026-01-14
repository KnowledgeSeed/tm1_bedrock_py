import re
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Optional, Union, Sequence, Literal, Any, List, Dict

import pandas as pd
import yaml

from TM1_bedrock_py import utility


@dataclass(frozen=True)
class ColumnSpec:
    aliases: Optional[Dict[Union[str, int], str]] = None
    has_header: bool = True


def verify_format_one(
        query: Optional[str] = None,
        column_list: Optional[List[str]] = None
) -> bool:
    mandatory_columns = ["Parent", "Child"]
    optional_columns = ["ElementType", "Dimension", "Hierarchy", "Weight"]

    def normalize_column_name(column: str) -> str:
        cleaned = column.strip().strip('`"[]')
        cleaned = cleaned.split(".")[-1]
        cleaned = re.sub(r"\s+as\s+.+$", "", cleaned, flags=re.IGNORECASE).strip()
        return cleaned

    def has_required_columns(columns: Sequence[str]) -> bool:
        normalized = [normalize_column_name(column) for column in columns]
        has_mandatory = all(column in normalized for column in mandatory_columns)
        extras = set(normalized) - set(mandatory_columns)
        return has_mandatory and extras.issubset(optional_columns)

    if column_list:
        return has_required_columns(column_list)
    if query:
        match = re.search(r"select\s+(.*?)\s+from\s", query, flags=re.IGNORECASE | re.DOTALL)
        if not match:
            return False
        select_list = match.group(1)
        columns = [part.strip() for part in select_list.split(",") if part.strip()]
        return has_required_columns(columns)
    raise ValueError("No columns or query provided for validation.")


def read_csv_parent_child_to_df(
        source: Union[str, Path, PathLike[str]],
        column_spec: Optional[ColumnSpec] = None,
        header: Optional[int] = None,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:

    if decimal is None:
        decimal = utility.get_local_decimal_separator()
    if sep is None:
        sep = utility.get_local_regex_separator()
    if column_spec is not None:
        header = 0 if column_spec.has_header else None
        if column_spec.aliases:
            kwargs["usecols"] = list(column_spec.aliases.keys())
    if column_spec is not None or header is not None:
        kwargs["header"] = header

    df = pd.read_csv(
            filepath_or_buffer=source,
            sep=sep,
            decimal=decimal,
            **kwargs
        )

    if column_spec and column_spec.aliases:
        df = df.rename(columns=column_spec.aliases)

    if not verify_format_one(column_list=list(df.columns)):
        raise ValueError(
            "CSV columns must include Parent, Child and only allow optional ElementType, Dimension, Hierarchy, Weight."
        )

    return df


def read_xlsx_parent_child_to_df(
        source: Union[str, Path, PathLike[str]],
        sheet_name: str,
        column_spec: Optional[ColumnSpec] = None,
        **kwargs
) -> pd.DataFrame:
    if column_spec is not None:
        kwargs["header"] = 0 if column_spec.has_header else None
        if column_spec.aliases:
            kwargs["usecols"] = list(column_spec.aliases.keys())
    df = pd.read_excel(
        io=source,
        sheet_name=sheet_name,
        **kwargs
    )

    if column_spec and column_spec.aliases:
        df = df.rename(columns=column_spec.aliases)

    if not verify_format_one(column_list=list(df.columns)):
        raise ValueError(
            "XLSX columns must include Parent, Child and only allow optional ElementType, Dimension, Hierarchy, Weight."
        )

    return df


def read_sql_parent_child_to_df(
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        table_columns: Optional[list] = None,
        schema: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:
    if not verify_format_one(query=sql_query, column_list=table_columns):
        raise ValueError(
            "SQL columns must include Parent, Child and only allow optional ElementType, Dimension, Hierarchy, Weight."
        )

    if not engine:
        engine = utility.create_sql_engine(**kwargs)

    if table_name:
        return pd.read_sql_table(
                     con=engine,
                     table_name=table_name,
                     columns=table_columns,
                     schema=schema,
        )
    if sql_query:
        return pd.read_sql_query(
            sql=sql_query,
            con=engine,
        )
    else:
        raise ValueError


def read_yaml_parent_child_to_df(
        source: Union[str, Path, PathLike[str]],
        template_key: Optional[str] = None,
        column_spec: Optional[ColumnSpec] = None,
        **_kwargs
) -> pd.DataFrame:
    with open(source, "r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle)

    if template_key:
        if not isinstance(payload, dict):
            raise ValueError("YAML input must be a mapping to use template_key.")
        if template_key not in payload:
            raise ValueError(f"YAML template_key not found: {template_key}")
        payload = payload[template_key]

    if isinstance(payload, list):
        if len(payload) != 1 or not isinstance(payload[0], dict):
            raise ValueError("YAML template must be a single mapping for Format 1.")
        payload = payload[0]

    if not isinstance(payload, dict):
        raise ValueError("YAML input must be a mapping with keys like format and rows.")

    rows = payload.get("rows", [])
    df = pd.DataFrame(rows)

    if column_spec and column_spec.aliases:
        df = df.rename(columns=column_spec.aliases)

    if payload.get("format") == "parent_child":
        if not verify_format_one(column_list=list(df.columns)):
            raise ValueError(
                "YAML columns must include Parent, Child and only allow optional ElementType, Dimension, Hierarchy, Weight."
            )

    return df


def read_source_to_df(
        source: Union[str, Path, PathLike[str]],
        source_type: Literal["csv", "sql", "xlsx", "yaml"],
        *,
        column_aliases: Optional[Dict] = None,
        has_header: Optional[bool] = True,
        column_spec: Optional[ColumnSpec] = None,
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:
    """ either has mandatory or I get their names/pos and rename them"""
    if column_spec is None:
        column_spec = ColumnSpec(aliases=column_aliases, has_header=has_header)

    if source_type is "csv":
        return read_csv_parent_child_to_df(source=source, column_spec=column_spec, **kwargs)
    if source_type is "xlsx":
        return read_xlsx_parent_child_to_df(source=source, column_spec=column_spec, **kwargs)
    if source_type is "sql":
        return read_sql_parent_child_to_df(engine=engine, sql_query=sql_query, **kwargs)
    if source_type is "yaml":
        return read_yaml_parent_child_to_df(source=source, column_spec=column_spec, **kwargs)
    else:
        raise ValueError
