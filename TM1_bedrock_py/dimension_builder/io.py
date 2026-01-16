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


def normalize_dataframe_header(column_spec: Union[ColumnSpec, None], df: pd.DataFrame) -> pd.DataFrame:
    if column_spec and column_spec.aliases:
        df = df.rename(columns=column_spec.aliases)
    return df


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


def verify_format_two(
        column_list: Optional[List[str]] = None
) -> bool:
    optional_columns = ["Hierarchy", "Weight", "Dimension", "ElementType"]

    if not column_list:
        return False

    normalized = [col.strip() for col in column_list]
    level_columns = [col for col in normalized if re.match(r"^level\d+$", col, flags=re.IGNORECASE)]
    if not level_columns:
        return False

    extras = set(normalized) - set(level_columns)
    return extras.issubset(optional_columns)


def extract_select_columns(sql_query: str) -> Optional[List[str]]:
    match = re.search(r"select\s+(.*?)\s+from\s", sql_query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    select_list = match.group(1)
    return [part.strip() for part in select_list.split(",") if part.strip()]


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

    df = normalize_dataframe_header(column_spec, df)

    if verify_format_one(column_list=list(df.columns)):
        return df
    if verify_format_two(column_list=list(df.columns)):
        return df.fillna("")
    raise ValueError(
        "CSV columns must include Parent and Child (Format 1) or Level# columns (Format 2) "
        "and only allow optional ElementType, Dimension, Hierarchy, Weight."
    )


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

    df = normalize_dataframe_header(column_spec, df)

    if verify_format_one(column_list=list(df.columns)):
        return df
    if verify_format_two(column_list=list(df.columns)):
        return df.fillna("")
    raise ValueError(
        "XLSX columns must include Parent and Child (Format 1) or Level# columns (Format 2) "
        "and only allow optional ElementType, Dimension, Hierarchy, Weight."
    )


def read_sql_parent_child_to_df(
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        table_columns: Optional[list] = None,
        schema: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:
    columns = None
    if table_columns:
        columns = table_columns
    elif sql_query:
        columns = extract_select_columns(sql_query)
    if not columns or not (verify_format_one(column_list=columns) or verify_format_two(column_list=columns)):
        raise ValueError(
            "SQL columns must include Parent and Child (Format 1) or Level# columns (Format 2) "
            "and only allow optional ElementType, Dimension, Hierarchy, Weight."
        )

    if not engine:
        engine = utility.create_sql_engine(**kwargs)

    if table_name:
        df = pd.read_sql_table(
                     con=engine,
                     table_name=table_name,
                     columns=table_columns,
                     schema=schema,
        )
    elif sql_query:
        df = pd.read_sql_query(
            sql=sql_query,
            con=engine,
        )
    else:
        raise ValueError
    if verify_format_two(column_list=list(df.columns)):
        return df.fillna("")
    return df


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

    if payload.get("format") == "parent_child":
        df = pd.DataFrame(rows)
        df = normalize_dataframe_header(column_spec, df)
        if not verify_format_one(column_list=list(df.columns)):
            raise ValueError(
                "YAML columns must include Parent, Child and only allow optional ElementType, Dimension, Hierarchy, Weight."
            )
        return df.fillna("")

    if payload.get("format") == "level_columns":
        level_columns = payload.get("level_columns")
        if column_spec.aliases:
            df_columns = list(column_spec.aliases.keys())
        else:
            df_columns = level_columns
            for row in rows:
                col_keys = list(row.keys())
                df_columns += [key for key in col_keys if key not in level_columns]

        df = pd.DataFrame(data=rows, columns=df_columns)
        df = normalize_dataframe_header(column_spec, df)

        if not verify_format_two(column_list=list(df.columns)):
            raise ValueError(
                "YAML columns must include Level# columns and only allow optional ElementType, Dimension, Hierarchy, Weight."
            )
        return df.fillna("")
    else:
        raise ValueError


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


def read_existing_edges_df(tm1_service: Any, dimension_name: str, hierarchy_name: Optional[str] = None):
    if hierarchy_name is None:
        hierarchy_name = dimension_name
    hierarchy = tm1_service.hierarchies.get(dimension_name, hierarchy_name)
    edge_list = [
        {
            "Parent": parent,
            "Child": child,
            "Weight": weight,
            "Dimension": hierarchy.dimension_name,
            "Hierarchy": hierarchy.name
        }
        for (parent, child), weight in hierarchy.edges.items()
    ]
    existing_edges_df = pd.DataFrame(edge_list)
    return existing_edges_df
