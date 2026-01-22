import re
from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Optional, Union, Sequence, Literal, Any, List, Dict, Tuple

import pandas as pd
import yaml

from TM1_bedrock_py import utility
from TM1_bedrock_py.dimension_builder.exceptions import InvalidInputFormatError, SchemaValidationError


@dataclass(frozen=False)
class ColumnSpec:
    parent_name: Optional[str] = None
    child_name: Optional[str] = None
    type_name: Optional[str] = None
    weight_name: Optional[str] = None
    dim_name: Optional[str] = None
    hier_name: Optional[str] = None
    level_names: Optional[List[str]] = None
    attribute_names: Optional[List[str]] = None
    aliases: Optional[Dict[Union[str, int], str]] = None
    source_format: Optional[Literal["parent_child", "indented_levels", "attributes"]] = None

    def relabel_columns_on_aliases(self):
        if self.aliases:
            self.parent_name = self.aliases.get("Parent")
            self.child_name = self.aliases.get("Child")
            self.type_name = self.aliases.get("ElementType")
            self.weight_name = self.aliases.get("Weight")
            self.dim_name = self.aliases.get("Dimension")
            self.hier_name = self.aliases.get("Hierarchy")
            self.level_names = self.aliases.get("Levels")
            self.attribute_names = self.aliases.get("Attributes")

    def as_list(self):
        col = [self.parent_name, self.child_name, self.type_name, self.weight_name, self.dim_name, self.hier_name]
        if self.level_names:
            col.extend(self.level_names)
        if self.attribute_names:
            col.extend(self.attribute_names)
        return [x for x in col if x is not None]

    def as_list_parent_child(self):
        col = [self.parent_name, self.child_name, self.type_name, self.weight_name, self.dim_name, self.hier_name]
        if self.attribute_names:
             col.extend(self.attribute_names)
        return [x for x in col if x is not None]

    def as_list_indented_levels(self):
        col = self.level_names + [self.type_name, self.weight_name, self.dim_name, self.hier_name]
        if self.attribute_names:
            col.extend(self.attribute_names)
        return [x for x in col if x is not None]

    def add_optional_columns(self, extras):
        if "ElementType" in extras: self.type_name = "ElementType"
        if "Weight" in extras: self.weight_name = "Weight"
        if "Dimension" in extras: self.dim_name = "Dimension"
        if "Hierarchy" in extras: self.hier_name = "Hierarchy"


def normalize_dataframe_header(column_spec: Union[ColumnSpec, None], df: pd.DataFrame) -> pd.DataFrame:
    if column_spec and column_spec.aliases:
        df = df.rename(columns=column_spec.aliases)
    return df


def verify_parent_child(
        column_list: Optional[List[str]] = None,
        col_spec: Optional[ColumnSpec] = None
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
        if col_spec:
            optional_columns.extend(col_spec.attribute_names or [])
            col_spec.add_optional_columns(extras & set(optional_columns))
        return has_mandatory and extras.issubset(optional_columns)

    if column_list:
        return has_required_columns(column_list)

    raise InvalidInputFormatError


def verify_indented_levels(
        column_list: Optional[List[str]] = None,
        col_spec: Optional[ColumnSpec] = None
) -> bool:
    optional_columns = ["Hierarchy", "Weight", "Dimension", "ElementType", "Attributes"]

    if not column_list:
        return False

    normalized = [col.strip() for col in column_list]
    level_columns = [col for col in normalized if re.match(r"^level\d+$", col, flags=re.IGNORECASE)]

    extras = set(normalized) - set(level_columns)
    if col_spec:
        col_spec.level_names = level_columns
        if not level_columns:
            return False
        optional_columns.extend(col_spec.attribute_names or [])
        col_spec.add_optional_columns(extras & set(optional_columns))
    return extras.issubset(optional_columns)


def normalize_format(column_spec: Union[ColumnSpec, None], df: pd.DataFrame) -> Any:
    if column_spec.source_format == "attributes":
        return df.fillna("")
    if (verify_parent_child(column_list=list(df.columns), col_spec=column_spec)
            or verify_indented_levels(column_list=list(df.columns), col_spec=column_spec)):
        df = df[column_spec.as_list()].fillna("")
        return df
    else:
        raise SchemaValidationError("Source columns must include Parent and Child or Level# columns")


def extract_select_columns(sql_query: str) -> Optional[List[str]]:
    match = re.search(r"select\s+(.*?)\s+from\s", sql_query, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    select_list = match.group(1)
    return [part.strip() for part in select_list.split(",") if part.strip()]


def read_csv_source_to_df(
        source: Union[str, Path, PathLike[str]],
        column_spec: Optional[ColumnSpec] = None,
        sep: Optional[str] = None,
        decimal: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:

    if decimal is None:
        decimal = utility.get_local_decimal_separator()
    if sep is None:
        sep = utility.get_local_regex_separator()
    if column_spec is not None:
        if column_spec.aliases:
            kwargs["usecols"] = list(column_spec.aliases.keys())
        elif column_spec.source_format == "attributes":
            kwargs["usecols"] = column_spec.attribute_names

    df = pd.read_csv(
            filepath_or_buffer=source,
            sep=sep,
            decimal=decimal,
            **kwargs
        )

    df = normalize_dataframe_header(column_spec, df)
    df = normalize_format(column_spec, df)
    return df


def read_xlsx_source_to_df(
        source: Union[str, Path, PathLike[str]],
        sheet_name: Optional[str] = None,
        column_spec: Optional[ColumnSpec] = None,
        **kwargs
) -> pd.DataFrame:
    if column_spec is not None:
        if column_spec.aliases:
            kwargs["usecols"] = list(column_spec.aliases.keys())
        elif column_spec.source_format == "attributes":
            kwargs["usecols"] = column_spec.attribute_names
    df = pd.read_excel(
        io=source,
        sheet_name=sheet_name,
        **kwargs
    )

    df = normalize_dataframe_header(column_spec, df)
    df = normalize_format(column_spec, df)
    return df


def read_sql_source_to_df(
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        schema: Optional[str] = None,
        column_spec: Optional[ColumnSpec] = None,
        **kwargs
) -> pd.DataFrame:
    columns = column_spec.as_list()
    if column_spec.source_format == "attributes":
        columns = column_spec.attribute_names
    if sql_query:
        columns = extract_select_columns(sql_query)
    elif column_spec.source_format != "attributes":
        if column_spec.aliases:
            columns = column_spec.as_list()
        if not columns or not (verify_parent_child(column_list=columns, col_spec=column_spec)
                               or verify_indented_levels(column_list=columns, col_spec=column_spec)):
            raise ValueError(
                "SQL columns must include Parent and Child or Level# columns "
                "and only allow optional ElementType, Dimension, Hierarchy, Weight, Attributes list."
            )

    if not engine:
        engine = utility.create_sql_engine(**kwargs)

    if table_name:
        df = pd.read_sql_table(
                     con=engine,
                     table_name=table_name,
                     columns=columns,
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
        if column_spec.aliases:
            df_columns = list(column_spec.aliases.keys())
        else:
            df_columns = column_spec.as_list_parent_child()
            for row in rows:
                col_keys = list(row.keys())
                df_columns += [key for key in col_keys if key not in df_columns]
            if not verify_parent_child(column_list=df_columns, col_spec=column_spec):
                df_columns = column_spec.as_list_parent_child()

        df = pd.DataFrame(data=rows, columns=df_columns)
        df = normalize_dataframe_header(column_spec, df)
        return df.fillna("")

    elif payload.get("format") == "level_columns":
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

        if not verify_indented_levels(column_list=list(df.columns), col_spec=column_spec):
            raise ValueError(
                "YAML columns must include Level# columns and only allow optional ElementType, Dimension, Hierarchy, Weight."
            )
        return df.fillna("")
    elif payload.get("format") == "attributes":
        columns = column_spec.attribute_names
        return pd.DataFrame(data=rows, columns=columns).fillna("")
    else:
        raise ValueError


def read_source_to_df(
        source_type: Literal["csv", "sql", "xlsx", "yaml"],
        source: Optional[Union[str, Path, PathLike[str]]] = None,
        source_format: Optional[Literal["parent_child", "indented_levels", "attributes"]] = None,
        *,
        parent_name: Optional[str] = None,
        child_name: Optional[str] = None,
        type_name: Optional[str] = None,
        weight_name: Optional[str] = None,
        dim_name: Optional[str] = None,
        hier_name: Optional[str] = None,
        level_names: Optional[List[str]] = None,
        attribute_names: Optional[List[str]] = None,
        column_aliases: Optional[Dict] = None,
        engine: Optional[Any] = None,
        sql_query: Optional[str] = None,
        table_name: Optional[str] = None,
        **kwargs
) -> pd.DataFrame:
    column_spec = ColumnSpec(source_format=source_format)
    if source_format == "parent_child":
        column_spec.parent_name = parent_name or "Parent"
        column_spec.child_name = child_name or "Child"
    if source_format == "indented_levels":
        column_spec.level_names=level_names or "Level1"
    column_spec.type_name = type_name
    column_spec.weight_name = weight_name
    column_spec.dim_name = dim_name
    column_spec.hier_name = hier_name

    if attribute_names:
        column_spec.attribute_names = attribute_names

    if column_aliases:
        column_spec.aliases=column_aliases
        column_spec.relabel_columns_on_aliases()

    if source_type == "csv":
        return read_csv_source_to_df(source=source, column_spec=column_spec, **kwargs)
    if source_type == "xlsx":
        return read_xlsx_source_to_df(source=source, column_spec=column_spec, **kwargs)
    if source_type == "sql":
        return read_sql_source_to_df(engine=engine, sql_query=sql_query, table_name=table_name, column_spec=column_spec, **kwargs)
    if source_type == "yaml":
        return read_yaml_source_to_df(source=source, column_spec=column_spec, **kwargs)
    else:
        raise ValueError


def read_existing_edges_df(tm1_service: Any, dimension_name: str) -> pd.DataFrame:
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


def retrieve_existing_schema(tm1_service: Any, dimension_name: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    existing_edges_df = read_existing_edges_df(tm1_service, dimension_name)
    existing_elements_df = read_existing_elements_df(tm1_service, dimension_name)
    return existing_edges_df, existing_elements_df
