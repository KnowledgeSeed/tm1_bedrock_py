import os
from os import PathLike
from pathlib import Path
from typing import Optional, Union, Any, List, Tuple, Literal, Callable, Dict
from TM1_bedrock_py.loader import dataframe_to_sql
import pandas as pd
import yaml, json

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
    keyword_arguments = kwargs | {"usecols": column_names} if column_names else kwargs

    df = pd.read_csv(
        filepath_or_buffer=source,
        sep=sep,
        decimal=decimal,
        **keyword_arguments
    )

    return df.fillna("")


def read_xlsx_source_to_df(
        source: Union[str, Path, PathLike[str]],
        sheet_name: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        **kwargs
) -> pd.DataFrame:
    sheet_identifier = sheet_name if sheet_name is not None else 0
    keyword_arguments = kwargs | {"usecols": column_names} if column_names else kwargs

    excel_dataframe = pd.read_excel(
        io=source,
        sheet_name=sheet_identifier,
        **keyword_arguments
    )

    return excel_dataframe.fillna("")


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
        raise ValueError("No sql query or table name was specified for an sql source, aborting.")
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
            return pd.DataFrame(data=rows, columns=column_names).fillna("")
        return pd.DataFrame(data=rows).fillna("")
    else:
        raise ValueError(f"YAML template must contain mapping of input data under the key 'rows'.")


def read_json_source_to_df(
        source: Union[str, Path, os.PathLike[str]],
        template_key: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        **_kwargs
) -> pd.DataFrame:
    with open(file=source, mode="r", encoding="utf-8") as file_handle:
        json_payload = json.load(fp=file_handle)

    if template_key and not isinstance(json_payload, dict):
        raise ValueError("JSON input must be a mapping to use template_key.")

    if template_key and template_key not in json_payload:
        raise ValueError(f"JSON template_key not found: {template_key}")

    targeted_payload = json_payload[template_key] if template_key else json_payload

    normalized_payload = targeted_payload[0] if isinstance(targeted_payload, list) else targeted_payload

    if not isinstance(normalized_payload, dict):
        raise ValueError("JSON input must be a mapping with keys like format and rows.")

    row_data = normalized_payload.get("rows", [])

    if not row_data:
        raise ValueError("JSON template must contain mapping of input data under the key 'rows'.")

    dataframe_construction_arguments = {"data": row_data} | ({"columns": column_names} if column_names else {})

    return pd.DataFrame(**dataframe_construction_arguments).fillna("")


def read_source_to_dataframe(
        source: Optional[Union[str, Path, os.PathLike[str]]] = None,
        *,
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        column_names: Optional[List[str]] = None,
        **kwargs
) -> Optional[pd.DataFrame]:

    if source is None and sql_query is None and table_name is None:
        raise ValueError("Must provide at least one type of source.")

    source_type = Path(str(source)).suffix.lower() if source else "sql"

    reader_function_mapping = {
        ".csv": read_csv_source_to_df,
        ".xlsx": read_xlsx_source_to_df,
        ".yaml": read_yaml_source_to_df,
        ".yml": read_yaml_source_to_df,
        ".json": read_json_source_to_df,
        "sql": read_sql_source_to_df
    }

    if source_type not in reader_function_mapping:
        raise ValueError(f"Type of the input file is invalid. Permitted types: {list(reader_function_mapping.keys())}")

    execution_arguments = kwargs | {"column_names": column_names}

    if source_type == "sql":
        execution_arguments |= {"engine": engine, "sql_query": sql_query, "table_name": table_name}
    else:
        execution_arguments |= {"source": source}

    return reader_function_mapping[source_type](**execution_arguments)


@utility.log_exec_metrics
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


@utility.log_exec_metrics
def read_existing_edges_df_filtered(
        tm1_service: Any, dimension_name: str, hierarchy_names: list[str]
) -> Optional[pd.DataFrame]:
    hierarchies = [
        tm1_service.hierarchies.get(dimension_name=dimension_name, hierarchy_name=hier)
        for hier in hierarchy_names
    ]
    edge_list = [
        {
            "Parent": parent,
            "Child": child,
            "Weight": weight,
            "Dimension": dimension_name,
            "Hierarchy": hierarchy.name
        }
        for hierarchy in hierarchies
        for (parent, child), weight in hierarchy.edges.items()
    ]
    if len(edge_list) == 0:
        return None
    return pd.DataFrame(edge_list)


@utility.log_exec_metrics
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


@utility.log_exec_metrics
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


@utility.log_exec_metrics
def read_existing_elements_df_filtered(
        tm1_service: Any, dimension_name: str, hierarchy_names: list[str]
) -> pd.DataFrame:
    dfs_to_concat = []
    for hierarchy_name in hierarchy_names:
        current_elements_df = read_existing_elements_df_for_hierarchy(tm1_service, dimension_name, hierarchy_name)
        dfs_to_concat.append(current_elements_df)

    return pd.concat(dfs_to_concat, ignore_index=True)


@utility.log_exec_metrics
def retrieve_existing_schema(tm1_service: Any, dimension_name: str) -> Tuple[Optional[pd.DataFrame], pd.DataFrame]:
    existing_edges_df = read_existing_edges_df(tm1_service, dimension_name)
    existing_elements_df = read_existing_elements_df(tm1_service, dimension_name)
    return existing_edges_df, existing_elements_df


def write_dataframe_to_csv(
        dataframe: pd.DataFrame,
        file_path_destination: Optional[str] = None,
        separator: str = None,
        decimal_separator: str = None,
        include_index: bool = False,
        encoding_type: str = "utf-8",
        **kwargs: Any
) -> None:
    if decimal_separator is None:
        decimal_separator = utility.get_local_decimal_separator()
    if separator is None:
        separator = utility.get_local_regex_separator()

    dataframe.to_csv(
        path_or_buf=file_path_destination,
        sep=separator,
        decimal=decimal_separator,
        index=include_index,
        encoding=encoding_type,
        **kwargs
    )


def write_dataframe_to_xlsx(
        dataframe: pd.DataFrame,
        file_path_destination: str,
        sheet_name: str = "Sheet1",
        include_index: bool = False,
        **kwargs: Any
) -> None:
    dataframe.to_excel(
        excel_writer=file_path_destination,
        sheet_name=sheet_name,
        engine="openpyxl",
        index=include_index,
        **kwargs
    )


def write_dataframe_to_yaml(
        dataframe: pd.DataFrame,
        file_path_destination: str,
        default_flow_style: bool = False,
        encoding_type: str = "utf-8",
        **kwargs: Any
) -> None:
    dictionary_representation = dataframe.to_dict(orient="records")

    with open(file_path_destination, mode="w", encoding=encoding_type) as output_file:
        yaml.dump(
            data=dictionary_representation,
            stream=output_file,
            default_flow_style=default_flow_style,
            **kwargs
        )


def write_dataframe_to_json(
        dataframe: pd.DataFrame,
        file_path_destination: str,
        orientation: Literal["split", "records", "index", "columns", "values", "table"] = "records",
        write_lines: bool = True,
        **kwargs: Any
) -> None:
    dataframe.to_json(
        path_or_buf=file_path_destination,
        orient=orientation,
        lines=write_lines,
        **kwargs
    )


def execute_dataframe_writers(
        dataframe: pd.DataFrame,
        target_destinations: List[Literal["sql", "csv", "xlsx", "yaml", "json"]],
        *,
        file_path_destination: Optional[str] = None,
        table_name: Optional[str] = None,
        sql_engine: Optional[Any] = None,
        if_exists_strategy: Literal["fail", "replace", "append"] = "append",
        database_schema: Optional[str] = None,
        csv_separator: str = None,
        decimal_separator: str = None,
        excel_sheet_name: str = "Sheet1",
        include_index: bool = False,
        yaml_default_flow_style: bool = False,
        json_orientation: Literal["split", "records", "index", "columns", "values", "table"] = "records",
        **kwargs: Any
) -> None:
    configuration_mapping: Dict[str, Dict[str, Any]] = {
        "sql": {
            "table_name": table_name,
            "engine": sql_engine,
            "if_exists": if_exists_strategy,
            "schema": database_schema,
            "index": include_index
        },
        "csv": {
            "file_path_destination": file_path_destination,
            "separator": csv_separator,
            "decimal_separator": decimal_separator,
            "include_index": include_index
        },
        "xlsx": {
            "file_path_destination": file_path_destination,
            "sheet_name": excel_sheet_name,
            "include_index": include_index
        },
        "yaml": {
            "file_path_destination": file_path_destination,
            "default_flow_style": yaml_default_flow_style
        },
        "json": {
            "file_path_destination": file_path_destination,
            "orientation": json_orientation
        }
    }

    writer_function_mapping: Dict[str, Callable[..., None]] = {
        "sql": dataframe_to_sql,
        "csv": write_dataframe_to_csv,
        "xlsx": write_dataframe_to_xlsx,
        "yaml": write_dataframe_to_yaml,
        "json": write_dataframe_to_json
    }

    for target_format in target_destinations:
        execution_parameters = {**configuration_mapping[target_format], **kwargs}
        writer_function_mapping[target_format](
            dataframe=dataframe,
            **execution_parameters
        )
