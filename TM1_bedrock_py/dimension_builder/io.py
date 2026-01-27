import os
from os import PathLike
from pathlib import Path
from typing import Optional, Union, Any, List, Tuple

import pandas as pd
import yaml

from TM1_bedrock_py import utility



def read_csv_source_to_df(
        source: Union[str, Path, PathLike[str]],
        column_names: Optional[List[str]] = None,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:

    if decimal is None:
        decimal = utility.get_local_decimal_separator()
    if sep is None:
        sep = utility.get_local_regex_separator()
    if column_names:
        kwargs["usecols"] = column_names

    df = pd.read_csv(
            filepath_or_buffer=source,
            sep=sep,
            decimal=decimal,
            **kwargs
        )

    return df.fillna("")


def read_xlsx_source_to_df(
        source: Union[str, Path, PathLike[str]],
        sheet_name: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        **kwargs
) -> pd.DataFrame:
    if column_names:
        kwargs["usecols"] = column_names

    df = pd.read_excel(
        io=source,
        sheet_name=sheet_name,
        **kwargs
    )

    return df.fillna("")


def read_sql_source_to_df(
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        **kwargs
) -> pd.DataFrame:
    if not engine:
        engine = utility.create_sql_engine(**kwargs)

    if table_name:
        df = pd.read_sql_table(
                     con=engine,
                     table_name=table_name,
                     columns=column_names,
                     schema=schema,
        )
    elif sql_query:
        df = pd.read_sql_query(
            sql=sql_query,
            con=engine,
        )
    else:
        raise ValueError
    return df.fillna("")


def read_yaml_source_to_df(
        source: Union[str, Path, PathLike[str]],
        template_key: Optional[str] = None,
        column_names: Optional[List[str]] = None,
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

    if not isinstance(payload, dict):
        if isinstance(payload, list):
            payload = payload[0]
        else:
            raise ValueError("YAML input must be a mapping with keys like format and rows.")

    rows = payload.get("rows", [])

    if rows:
        if column_names:
            return  pd.DataFrame(data=rows, columns=column_names).fillna("")
        return pd.DataFrame(data=rows).fillna("")
    else:
        raise ValueError(f"YAML template must contain mapping of input data under the key 'rows'.")


def read_source_to_df(
        source: Optional[Union[str, Path, PathLike[str]]] = None,
        *,
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        **kwargs
) -> Optional[pd.DataFrame]:

    if source is None and engine is None:
        return None

    if source:
        filename, file_extension = os.path.splitext(source)
        source_type = file_extension
    else:
        source_type = "sql"

    if source_type == ".csv":
        return read_csv_source_to_df(source=source, column_names=column_names, **kwargs)
    if source_type == ".xlsx":
        return read_xlsx_source_to_df(source=source, column_names=column_names, **kwargs)
    if source_type == "sql":
        return read_sql_source_to_df(engine=engine, sql_query=sql_query, table_name=table_name, column_names=column_names, **kwargs)
    if source_type == ".yaml" or source_type == ".yml":
        return read_yaml_source_to_df(source=source, column_names=column_names, **kwargs)
    else:
        raise ValueError("Type of the input file is invalid. Please use the following: 'csv', 'xslx', 'yaml', 'sql'")


def read_existing_edges_df(tm1_service: Any, dimension_name: str) -> Optional[pd.DataFrame]:
    dimension = tm1_service.dimensions.get(dimension_name)
    edge_list = [
        {
            "Parent": parent,
            "Child": child,
            "Weight": weight,
            "Dimension": dimension_name,
            "Hierarchy": hierarchy.name
        }
        for hierarchy in dimension.hierarchies
        if hierarchy.name != "Leaves"
        for (parent, child), weight in hierarchy.edges.items()
    ]
    if len(edge_list) == 0:
        return None
    return pd.DataFrame(edge_list)


def read_existing_elements_df_for_hierarchy(
        tm1_service: Any, dimension_name: str, hierarchy_name: Optional[str] = None
) -> pd.DataFrame:
    if hierarchy_name is None:
        hierarchy_name = dimension_name
    existing_elements_df = tm1_service.elements.get_elements_dataframe(
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
        skip_consolidations=False,
        attribute_suffix=True,
        skip_parents=True,
        skip_weights=True,
        element_type_column="ElementType"
    )
    existing_elements_df.rename(columns={dimension_name: "ElementName"}, inplace=True)

    existing_elements_df.insert(2, "Dimension", dimension_name)
    existing_elements_df.insert(3, "Hierarchy", hierarchy_name)
    return existing_elements_df


def read_existing_elements_df(
        tm1_service: Any, dimension_name: str
) -> pd.DataFrame:
    leaves = ["Leaves"]
    hierarchy_names = tm1_service.hierarchies.get_all_names(dimension_name)
    hierarchy_names = list(set(hierarchy_names) - set(leaves))

    dfs_to_concat = []
    for hierarchy_name in hierarchy_names:
        current_elements_df = read_existing_elements_df_for_hierarchy(tm1_service, dimension_name, hierarchy_name)
        dfs_to_concat.append(current_elements_df)

    return pd.concat(dfs_to_concat, ignore_index=True)


def retrieve_existing_schema(tm1_service: Any, dimension_name: str) -> Tuple[Optional[pd.DataFrame], pd.DataFrame]:
    existing_edges_df = read_existing_edges_df(tm1_service, dimension_name)
    existing_elements_df = read_existing_elements_df(tm1_service, dimension_name)
    return existing_edges_df, existing_elements_df
