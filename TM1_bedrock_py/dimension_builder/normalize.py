from typing import Optional, Tuple, Callable, Literal, Union, Dict, List, Any, Set

import numpy as np
import pandas as pd

from TM1_bedrock_py import utility as baseutils
from TM1_bedrock_py.dimension_builder import utility
from TM1_bedrock_py.dimension_builder.exceptions import InvalidAttributeColumnNameError
from TM1_bedrock_py.dimension_builder.validate import (
    validate_schema_for_type_mapping,
    validate_schema_for_numeric_values
)

pd.set_option('future.no_silent_downcasting', True)

_TYPE_MAPPING = {
    "s": "String", "S": "String", "String": "String",  "string": "String",
    "numeric": "Numeric", "n": "Numeric", "N": "Numeric", "Numeric": "Numeric",
    "c": "Consolidated", "C": "Consolidated", "Consolidated": "Consolidated", "consolidated": "Consolidated"
}

_ATTR_TYPE_MAPPING = {
    "s": "String", "S": "String", "String": "String",  "string": "String",
    "numeric": "Numeric", "n": "Numeric", "N": "Numeric", "Numeric": "Numeric",
    "a": "Alias",  "A": "Alias", "Alias": "Alias", "alias": "Alias"
}


# input dimension dataframe normalization functions to ensure uniform format.

def normalize_base_column_names(input_df: pd.DataFrame, dim_column: Optional[str] = None,
                                hier_column: Optional[str] = None, parent_column: Optional[str] = None,
                                child_column: Optional[str] = None, element_column: Optional[str] = None,
                                type_column: Optional[str] = None, weight_column: Optional[str] = None, **kwargs
                                ) -> pd.DataFrame:
    mapping_rules = {
        "Dimension": ["dimension", dim_column],
        "Hierarchy": ["hierarchy", hier_column],
        "Parent": ["parent", parent_column],
        "Child": ["child", child_column, element_column, "elementname", "ElementName", "name", "Name"],
        "ElementType": ["type", "Type", "elementtype", type_column],
        "Weight": ["weight", weight_column]
    }
    existing_columns = set(input_df.columns)

    rename_dict = {
        source: target
        for target, sources in mapping_rules.items()
        for source in sources
        if source and source in existing_columns
    }

    return input_df.rename(columns=rename_dict)


def add_attribute_type_suffixes(input_df: pd.DataFrame, attr_type_map: Optional[dict]) -> pd.DataFrame:
    if attr_type_map is None:
        return input_df

    missing_cols = set(attr_type_map.keys()) - set(input_df.columns)
    if missing_cols:
        raise InvalidAttributeColumnNameError(
            f"The following attributes from the map are missing in the DataFrame: {missing_cols}")

    rename_dict = {
        attr_name: f"{attr_name}:{attr_type}"
        for attr_name, attr_type in attr_type_map.items()
    }
    return input_df.rename(columns=rename_dict)


@baseutils.log_exec_metrics
def normalize_attr_column_names(
        input_df: pd.DataFrame,
        attribute_columns: list[str] = None,
        attribute_parser: Union[Literal["colon", "square_brackets"], Callable] = "colon"
) -> Tuple[pd.DataFrame, list[str]]:
    if attribute_columns is None:
        attribute_columns = utility.get_attribute_columns_list(input_df=input_df)

    rename_map = {}
    renamed_columns = []

    for col in attribute_columns:
        name_part, type_part = utility.parse_attribute_string(col, attribute_parser)
        normalized_type = _ATTR_TYPE_MAPPING.get(type_part)

        if name_part.strip() == "":
            raise InvalidAttributeColumnNameError(f"Missing attribute name in column '{col}'. ")

        if normalized_type is None:
            raise InvalidAttributeColumnNameError(f"Unknown attribute type '{type_part}' in column '{col}'. ")

        new_name = f"{name_part}:{normalized_type}"

        if new_name.count(":") != 1:
            raise InvalidAttributeColumnNameError(f"Naming Validation Error: Resulting name '{new_name}' ")

        rename_map[col] = new_name
        renamed_columns.append(new_name)

    return input_df.rename(columns=rename_map), renamed_columns


def assign_missing_base_columns(
        input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None
) -> pd.DataFrame:
    if "Dimension" not in input_df.columns:
        input_df["Dimension"] = dimension_name
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = hierarchy_name if hierarchy_name is not None else dimension_name

    return input_df


def assign_missing_weight_column(input_df: pd.DataFrame) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0

    return input_df


@baseutils.log_exec_metrics
def assign_missing_base_values(
        input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None
) -> pd.DataFrame:
    input_df["Dimension"] = input_df["Dimension"].replace(r'^\s*$', np.nan, regex=True).fillna(dimension_name)

    if hierarchy_name is None:
        hierarchy_name = dimension_name
    input_df["Hierarchy"] = input_df["Hierarchy"].replace(r'^\s*$', np.nan, regex=True).fillna(hierarchy_name)
    return input_df


def assign_missing_weight_values(input_df: pd.DataFrame) -> pd.DataFrame:
    input_df["Weight"] = input_df["Weight"].replace(r'^\s*$', np.nan, regex=True).fillna(1.0)
    return input_df


def validate_and_normalize_numeric_values(input_df: pd.DataFrame, column_name: str) -> None:
    converted_series = pd.to_numeric(input_df[column_name], errors='coerce')
    validate_schema_for_numeric_values(input_df, converted_series, column_name)

    input_df[column_name] = converted_series.astype(float)


def normalize_string_values(input_df: pd.DataFrame, column_name: str) -> None:
    input_df[column_name] = input_df[column_name].fillna("")
    input_df[column_name] = input_df[column_name].astype(str)
    input_df[column_name] = input_df[column_name].str.strip()


@baseutils.log_exec_metrics
def validate_and_normalize_base_column_types(input_df: pd.DataFrame) -> None:
    base_string_columns = ["Parent", "Child", "ElementType", "Dimension", "Hierarchy"]
    for column_name in base_string_columns:
        normalize_string_values(input_df=input_df, column_name=column_name)
    validate_and_normalize_numeric_values(input_df=input_df, column_name="Weight")


@baseutils.log_exec_metrics
def validate_and_normalize_attr_column_types(elements_df: pd.DataFrame, attr_columns: list[str]) -> None:
    for attr_column in attr_columns:
        _, attr_type = utility.parse_attribute_string(attr_column)
        if attr_type in ("Alias", "String"):
            normalize_string_values(input_df=elements_df, column_name=attr_column)
        else:
            validate_and_normalize_numeric_values(input_df=elements_df, column_name=attr_column)


def assign_missing_type_column(input_df: pd.DataFrame):
    if "ElementType" not in input_df.columns:
        input_df["ElementType"] = ""


@baseutils.log_exec_metrics
def assign_missing_type_values(input_df: pd.DataFrame) -> None:
    parent_list = input_df['Parent'].unique()
    is_empty = input_df['ElementType'].isin([np.nan, None, ""])

    input_df.loc[is_empty & input_df['Child'].isin(parent_list), 'ElementType'] = 'Numeric'
    input_df.loc[is_empty & ~input_df['Child'].isin(parent_list), 'ElementType'] = 'Consolidated'


@baseutils.log_exec_metrics
def validate_and_normalize_type_values(input_df: pd.DataFrame) -> pd.DataFrame:
    validate_schema_for_type_mapping(input_df=input_df, type_mapping=_TYPE_MAPPING)
    input_df['ElementType'] = input_df['ElementType'].map(_TYPE_MAPPING)
    return input_df


@baseutils.log_exec_metrics
def assign_missing_attribute_values(
        elements_df: pd.DataFrame, attribute_columns: list[str]
) -> None:
    element_name_column = 'ElementName'

    for attribute_info in attribute_columns:
        _, attr_type = utility.parse_attribute_string(attribute_info)

        if attr_type == "String":
            elements_df[attribute_info] = elements_df[attribute_info].fillna("")

        elif attr_type == "Numeric":
            elements_df[attribute_info] = elements_df[attribute_info].fillna(0.0)

        elif attr_type == "Alias":
            condition = elements_df[attribute_info].isna() | (elements_df[attribute_info] == "")

            elements_df[attribute_info] = np.where(
                condition,
                elements_df[element_name_column],
                elements_df[attribute_info]
            )


@baseutils.log_exec_metrics
def separate_edge_df_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    column_list = ["Parent", "Child", "Weight", "Dimension", "Hierarchy"]
    edges_df = input_df[column_list].copy()
    return edges_df


@baseutils.log_exec_metrics
def separate_elements_df_columns(
        input_df: pd.DataFrame,
        attribute_columns: list[str]
) -> pd.DataFrame:
    base_columns = ["Child", "ElementType", "Dimension", "Hierarchy"]
    elements_df = input_df[base_columns + attribute_columns].copy()
    elements_df = elements_df.rename(columns={"Child": "ElementName"})
    return elements_df


@baseutils.log_exec_metrics
def convert_levels_to_edges(input_df: pd.DataFrame, level_columns: list[str]) -> pd.DataFrame:
    input_df[level_columns] = input_df[level_columns].replace(r'^\s*$', np.nan, regex=True)

    mask = input_df[level_columns].notna().to_numpy()
    cols_count = len(level_columns)
    last_valid_idx = cols_count - 1 - np.fliplr(mask).argmax(axis=1)
    has_data = mask.any(axis=1)
    temp_filled = input_df.groupby("Hierarchy")[level_columns].ffill()

    vals = temp_filled.to_numpy(dtype=object)
    col_indices = np.arange(cols_count)
    keep_mask = (col_indices[None, :] <= last_valid_idx[:, None]) & has_data[:, None]
    vals = np.where(keep_mask, vals, np.nan)
    counts = np.sum(~pd.isna(vals), axis=1)
    rows = np.arange(len(input_df))

    child_idx = counts - 1
    parent_idx = child_idx - 1

    child_values = vals[rows, child_idx]
    parent_values = vals[rows, parent_idx]
    parent_values = np.where(child_idx == 0, np.nan, parent_values)

    input_df['Parent'] = parent_values
    input_df['Child'] = child_values
    input_df.drop(columns=level_columns, inplace=True)

    return input_df


@baseutils.log_exec_metrics
def drop_invalid_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    edges_df['Parent'] = edges_df['Parent'].replace("", np.nan)
    edges_df = edges_df.dropna(subset=['Parent'])
    return edges_df


@baseutils.log_exec_metrics
def deduplicate_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    edges_df = edges_df.drop_duplicates(subset=["Parent", "Child", "Hierarchy"])
    return edges_df


@baseutils.log_exec_metrics
def deduplicate_elements(elements_df: pd.DataFrame) -> pd.DataFrame:
    elements_df = elements_df.drop_duplicates(subset=["ElementName", "Dimension", "Hierarchy"]).reset_index(drop=True)
    return elements_df


@baseutils.log_exec_metrics
def normalize_input_schema(
        input_df: pd.DataFrame,
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        level_columns: Optional[list[str]] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        attr_type_map: Optional[dict] = None,
        input_elements_df: pd.DataFrame = None,
        input_elements_df_element_column: Optional[str] = None,
        attribute_parser: Union[Literal["colon", "square_brackets"], Callable] = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    # raw edges/combined input structure base normalization
    input_df = normalize_base_column_names(input_df=input_df, dim_column=dim_column, hier_column=hier_column,
                                           parent_column=parent_column, child_column=child_column,
                                           type_column=type_column, weight_column=weight_column)
    input_df = assign_missing_base_columns(input_df=input_df, dimension_name=dimension_name,
                                           hierarchy_name=hierarchy_name)
    input_df = assign_missing_base_values(input_df=input_df,
                                          dimension_name=dimension_name, hierarchy_name=hierarchy_name)

    # level format handling
    if level_columns:
        input_df = convert_levels_to_edges(input_df=input_df, level_columns=level_columns)

    # raw separate elements input base normalization and merge
    if input_elements_df is not None:
        input_elements_df = normalize_base_column_names(input_df=input_elements_df, dim_column=dim_column,
                                                        hier_column=hier_column,
                                                        element_column=input_elements_df_element_column,
                                                        type_column=type_column)
        input_elements_df = assign_missing_base_columns(input_df=input_elements_df,
                                                        dimension_name=dimension_name, hierarchy_name=hierarchy_name)
        input_elements_df = assign_missing_base_values(input_df=input_elements_df,
                                                       dimension_name=dimension_name, hierarchy_name=hierarchy_name)
        input_df = pd.merge(input_df, input_elements_df, on=['Child', 'Dimension', 'Hierarchy'], how='left')

    # combined input structure base normalization final steps
    input_df = assign_missing_weight_column(input_df)
    input_df = assign_missing_weight_values(input_df)
    validate_and_normalize_base_column_types(input_df)
    assign_missing_type_column(input_df=input_df)
    assign_missing_type_values(input_df=input_df)
    validate_and_normalize_type_values(input_df=input_df)
    input_df = add_attribute_type_suffixes(input_df, attr_type_map)

    # attribute normalization steps
    attribute_columns = utility.get_attribute_columns_list(input_df=input_df)
    input_df, attribute_columns = normalize_attr_column_names(
        input_df=input_df, attribute_columns=attribute_columns, attribute_parser=attribute_parser)

    # schema separation steps
    edges_df = separate_edge_df_columns(input_df=input_df)
    elements_df = separate_elements_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    # separated schema clearing steps
    edges_df = drop_invalid_edges(edges_df)
    edges_df = deduplicate_edges(edges_df)
    elements_df = deduplicate_elements(elements_df)

    return edges_df, elements_df


def clear_orphan_parent_edges(
        edges_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    edges_df.drop(edges_df[edges_df["Parent"] == orphan_consolidation_name].index, inplace=True)
    return edges_df.reset_index(drop=True)


def clear_orphan_parent_elements(
        elements_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    elements_df.drop(elements_df[elements_df["ElementName"] == orphan_consolidation_name].index, inplace=True)
    return elements_df.reset_index(drop=True)


def normalize_existing_schema_for_builder(
        existing_edges_df: Optional[pd.DataFrame], existing_elements_df: pd.DataFrame,
        old_orphan_parent_name: str = "OrphanParent", clear_orphan_parents: bool = True
) -> Tuple[Optional[pd.DataFrame], pd.DataFrame]:
    # further enhance if necessary, currently this seems enough
    if clear_orphan_parents:
        existing_elements_df = clear_orphan_parent_elements(existing_elements_df, old_orphan_parent_name)
    existing_elements_df, attribute_columns = normalize_attr_column_names(existing_elements_df)

    if existing_edges_df is None:
        return None, existing_elements_df

    if clear_orphan_parents:
        existing_edges_df = clear_orphan_parent_edges(existing_edges_df, old_orphan_parent_name)

    existing_elements_df.reset_index(drop=True, inplace=True)
    existing_edges_df.reset_index(drop=True, inplace=True)

    return existing_edges_df, existing_elements_df


def normalize_existing_schema_full(
        existing_edges_df: Optional[pd.DataFrame], existing_elements_df: pd.DataFrame,
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    if existing_elements_df is None:
        return None, None

    existing_elements_df, attribute_columns = normalize_attr_column_names(existing_elements_df)
    assign_missing_attribute_values(elements_df=existing_elements_df, attribute_columns=attribute_columns)
    validate_and_normalize_attr_column_types(elements_df=existing_elements_df, attr_columns=attribute_columns)
    existing_elements_df.reset_index(drop=True, inplace=True)

    if existing_edges_df is None:
        return None, existing_elements_df
    existing_edges_df.reset_index(drop=True, inplace=True)
    return existing_edges_df, existing_elements_df


def normalize_updated_schema_for_builder(
        updated_edges_df: pd.DataFrame, updated_elements_df: pd.DataFrame
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    attribute_columns = utility.get_attribute_columns_list(input_df=updated_elements_df)

    # further enhance if necessary, currently this seems enough
    assign_missing_attribute_values(elements_df=updated_elements_df, attribute_columns=attribute_columns)
    validate_and_normalize_attr_column_types(elements_df=updated_elements_df, attr_columns=attribute_columns)
    return updated_edges_df, updated_elements_df


def process_parent_child_format(
        elements_df: pd.DataFrame,
        edges_df: pd.DataFrame,
        **_kwargs: Any
) -> pd.DataFrame:
    merged_dataframe: pd.DataFrame = pd.merge(
        edges_df,
        elements_df,
        left_on=["Dimension", "Hierarchy", "Child"],
        right_on=["Dimension", "Hierarchy", "ElementName"],
        how="outer"
    )

    missing_parent_mask: pd.Series = merged_dataframe["Child"].isna()
    merged_dataframe.loc[missing_parent_mask, "Child"] = merged_dataframe.loc[missing_parent_mask, "ElementName"]

    columns_to_drop: List[str] = ["ElementName"]
    merged_dataframe = merged_dataframe.drop(columns=columns_to_drop)

    roots_mask: pd.Series = merged_dataframe["Parent"].isna() & merged_dataframe["Child"].isin(edges_df["Parent"])
    isolated_mask: pd.Series = merged_dataframe["Parent"].isna() & ~merged_dataframe["Child"].isin(edges_df["Parent"])
    edges_mask: pd.Series = merged_dataframe["Parent"].notna()

    # Applied explicit sorting to guarantee deterministic outputs for testing and downstream systems
    roots_dataframe: pd.DataFrame = merged_dataframe[roots_mask].sort_values(
        by=["Dimension", "Hierarchy", "Child"]
    )
    edges_dataframe_part: pd.DataFrame = merged_dataframe[edges_mask].sort_values(
        by=["Dimension", "Hierarchy", "Parent", "Child"]
    )
    isolated_dataframe: pd.DataFrame = merged_dataframe[isolated_mask].sort_values(
        by=["Dimension", "Hierarchy", "Child"]
    )

    ordered_merged_dataframe: pd.DataFrame = pd.concat(
        [roots_dataframe, edges_dataframe_part, isolated_dataframe],
        ignore_index=True
    )

    # Converts all pandas NaN (float64) values to Python None (object) to align dtypes
    ordered_merged_dataframe = ordered_merged_dataframe.where(pd.notnull(ordered_merged_dataframe), None)

    base_columns: List[str] = ["Dimension", "Hierarchy", "Parent", "Child", "Weight"]
    attribute_columns: List[str] = [
        column for column in ordered_merged_dataframe.columns
        if column not in base_columns
    ]

    ordered_columns: List[str] = base_columns + attribute_columns

    return ordered_merged_dataframe[ordered_columns]


def process_hierarchical_levels_format(
        elements_df: pd.DataFrame,
        edges_df: pd.DataFrame,
        format_selector: str,
        maximum_levels_depth: Optional[int] = None,
        **_kwargs: Any
) -> pd.DataFrame:
    elements_records: List[Dict[str, Any]] = elements_df.to_dict(orient="records")
    edges_records: List[Dict[str, Any]] = edges_df.to_dict(orient="records")

    hierarchy_elements_mapping: Dict[Tuple[str, str], Dict[str, Dict[str, Any]]] = {}

    for element_record in elements_records:
        grouping_key: Tuple[str, str] = (element_record["Dimension"], element_record["Hierarchy"])
        element_name: str = element_record["ElementName"]

        if grouping_key not in hierarchy_elements_mapping:
            hierarchy_elements_mapping[grouping_key] = {}

        attribute_dictionary: Dict[str, Any] = {
            key: value for key, value in element_record.items()
            if key not in ["Dimension", "Hierarchy", "ElementName"]
        }
        hierarchy_elements_mapping[grouping_key][element_name] = attribute_dictionary

    hierarchy_edges_mapping: Dict[Tuple[str, str], Dict[str, List[Dict[str, Any]]]] = {}
    hierarchy_children_sets: Dict[Tuple[str, str], Set[str]] = {}

    for edge_record in edges_records:
        grouping_key = (edge_record["Dimension"], edge_record["Hierarchy"])
        parent_name: str = edge_record["Parent"]
        child_name: str = edge_record["Child"]

        if grouping_key not in hierarchy_edges_mapping:
            hierarchy_edges_mapping[grouping_key] = {}
        if grouping_key not in hierarchy_children_sets:
            hierarchy_children_sets[grouping_key] = set()

        if parent_name not in hierarchy_edges_mapping[grouping_key]:
            hierarchy_edges_mapping[grouping_key][parent_name] = []

        hierarchy_edges_mapping[grouping_key][parent_name].append({
            "child_name": child_name,
            "edge_weight": edge_record["Weight"]
        })
        hierarchy_children_sets[grouping_key].add(child_name)

    generated_hierarchical_rows: List[Dict[str, Any]] = []
    absolute_maximum_depth: int = 0

    for grouping_key in sorted(hierarchy_elements_mapping.keys()):
        elements_mapping = hierarchy_elements_mapping[grouping_key]
        dimension_name: str = grouping_key[0]
        hierarchy_name: str = grouping_key[1]

        all_element_names: Set[str] = set(elements_mapping.keys())
        child_element_names: Set[str] = hierarchy_children_sets.get(grouping_key, set())
        root_element_names: Set[str] = all_element_names - child_element_names

        # Alphabetical sorting guarantees deterministic tree traversal entry points
        actual_roots: List[str] = sorted([
            root for root in root_element_names
            if root in hierarchy_edges_mapping.get(grouping_key, {})
        ])
        isolated_elements: List[str] = sorted([
            root for root in root_element_names
            if root not in hierarchy_edges_mapping.get(grouping_key, {})
        ])

        ordered_traversal_roots: List[str] = actual_roots + isolated_elements
        expanded_nodes_tracker: Set[str] = set()

        for root_element in ordered_traversal_roots:
            traversal_stack: List[Tuple[str, Optional[float], int, List[str]]] = [
                (root_element, None, 1, [root_element])
            ]

            while traversal_stack:
                current_node, current_weight, current_level, current_path = traversal_stack.pop()

                absolute_maximum_depth = max(absolute_maximum_depth, current_level)

                row_dictionary: Dict[str, Any] = {
                    "Dimension": dimension_name,
                    "Hierarchy": hierarchy_name,
                    "Weight": current_weight
                }

                if format_selector == "indented-levels":
                    row_dictionary[f"Level{current_level}"] = current_node
                elif format_selector == "filled-levels":
                    for level_index, path_node in enumerate(current_path):
                        row_dictionary[f"Level{level_index + 1}"] = path_node

                element_attributes: Dict[str, Any] = elements_mapping.get(current_node, {})
                row_dictionary.update(element_attributes)

                generated_hierarchical_rows.append(row_dictionary)

                if current_node not in expanded_nodes_tracker:
                    expanded_nodes_tracker.add(current_node)

                    children_list: List[Dict[str, Any]] = hierarchy_edges_mapping.get(grouping_key, {}).get(
                        current_node, [])
                    for child_data in reversed(children_list):
                        child_node_name: str = child_data["child_name"]
                        child_edge_weight: float = child_data["edge_weight"]
                        new_traversal_path: List[str] = current_path + [child_node_name]
                        traversal_stack.append(
                            (child_node_name, child_edge_weight, current_level + 1, new_traversal_path)
                        )

    hierarchical_dataframe: pd.DataFrame = pd.DataFrame(generated_hierarchical_rows)

    target_maximum_depth: int = absolute_maximum_depth
    if maximum_levels_depth is not None and maximum_levels_depth > absolute_maximum_depth:
        target_maximum_depth = maximum_levels_depth

    level_column_names: List[str] = [f"Level{level_index}" for level_index in range(1, target_maximum_depth + 1)]

    for level_column in level_column_names:
        if level_column not in hierarchical_dataframe.columns:
            hierarchical_dataframe[level_column] = None

    base_columns = ["Dimension", "Hierarchy"]
    metric_columns: List[str] = ["Weight"]
    attribute_columns = [
        column for column in hierarchical_dataframe.columns
        if column not in base_columns and column not in level_column_names and column not in metric_columns
    ]

    ordered_columns = base_columns + level_column_names + metric_columns + attribute_columns

    return hierarchical_dataframe[ordered_columns]




