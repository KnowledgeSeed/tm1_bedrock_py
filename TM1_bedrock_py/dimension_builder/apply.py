from pathlib import Path
from typing import Any, Literal, Tuple, Optional, Callable, Union

import pandas as pd
import numpy as np
from TM1py.Objects import Dimension, Hierarchy

from TM1_bedrock_py import utility as baseutils
from TM1_bedrock_py.dimension_builder import normalize, utility, io
from TM1_bedrock_py.dimension_builder.validate import (
    post_validate_schema,
    pre_validate_input_schema,
    validate_element_type_consistency,
    validate_sort_order_config, validate_hierarchy_sort_order_config
)


@baseutils.log_exec_metrics
def apply_update_on_edges(
        legacy_df: Optional[pd.DataFrame],
        input_df: pd.DataFrame,
        retained_hierarchies: list[str],
        orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    if legacy_df is None:
        return input_df

    for hierarchy in retained_hierarchies:
        curr_input = input_df[input_df["Hierarchy"] == hierarchy]
        input_parents = curr_input["Parent"].unique()
        input_children = curr_input["Child"].unique()

        cond_hierarchy = legacy_df['Hierarchy'].eq(hierarchy)
        if not cond_hierarchy.any():
            continue

        cond_parent = legacy_df['Parent'].isin(input_parents)
        cond_child = ~legacy_df['Child'].isin(input_children)
        legacy_df.loc[cond_hierarchy & cond_parent & cond_child, 'Parent'] = orphan_consolidation_name

    return pd.concat([input_df, legacy_df], ignore_index=True).drop_duplicates().reset_index(drop=True)


@baseutils.log_exec_metrics
def apply_update_with_unwind_on_edges(
        legacy_df: Optional[pd.DataFrame],
        input_df: pd.DataFrame,
        retained_hierarchies: list[str],
        orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    if legacy_df is None:
        return input_df

    keep_mask = pd.Series(True, index=legacy_df.index)

    for hierarchy in retained_hierarchies:
        curr_input = input_df[input_df["Hierarchy"] == hierarchy]
        input_children = set(curr_input["Child"])

        in_hierarchy = legacy_df['Hierarchy'].eq(hierarchy)
        if not in_hierarchy.any():
            continue

        legacy_df.loc[in_hierarchy, "Parent"] = orphan_consolidation_name

        rows_to_drop = in_hierarchy & legacy_df['Child'].isin(input_children)
        keep_mask = keep_mask & (~rows_to_drop)

    legacy_final = legacy_df[keep_mask]
    return pd.concat([input_df, legacy_final], ignore_index=True).drop_duplicates()


_UPDATE_STRATEGIES = {
    "update": apply_update_on_edges,
    "update_with_unwind": apply_update_with_unwind_on_edges
}


@baseutils.log_exec_metrics
def add_orphan_consolidation_elements(
        elements_df: pd.DataFrame,
        orphan_consolidation_name: str,
        dimension_name: str,
        retained_hierarchies: list[str]
) -> pd.DataFrame:
    attribute_columns = utility.get_attribute_columns_list(input_df=elements_df)
    orphan_parent_df = pd.DataFrame({
        "ElementName": orphan_consolidation_name,
        "ElementType": "Consolidated",
        "Dimension": dimension_name,
        "Hierarchy": retained_hierarchies
    })
    orphan_parent_df[attribute_columns] = None
    return pd.concat([elements_df, orphan_parent_df], ignore_index=True).drop_duplicates()


@baseutils.log_exec_metrics
def assign_root_orphan_edges(
        legacy_edges_df: Optional[pd.DataFrame],
        legacy_elements_df: pd.DataFrame,
        edges_df: pd.DataFrame,
        orphan_consolidation_name: str,
        dimension_name: str,
        retained_hierarchies: list[str]
) -> pd.DataFrame:

    new_rows_collection = []

    for hierarchy in retained_hierarchies:
        if legacy_edges_df is not None:
            current_edges_subset = legacy_edges_df[legacy_edges_df['Hierarchy'] == hierarchy]
            current_elements_subset = legacy_elements_df[legacy_elements_df['Hierarchy'] == hierarchy]
            legacy_children_set = set(current_edges_subset['Child'])
            legacy_elements_set = set(current_elements_subset['ElementName'])
            root_orphans = list(legacy_elements_set - legacy_children_set)
        else:
            root_orphans = legacy_elements_df[legacy_elements_df['Hierarchy'] == hierarchy]['ElementName'].tolist()

        if root_orphans:
            hierarchy_orphan_rows = pd.DataFrame({
                'Parent': orphan_consolidation_name,
                'Child': root_orphans,
                'Weight': 1.0,
                'Dimension': dimension_name,
                'Hierarchy': hierarchy
            })
            new_rows_collection.append(hierarchy_orphan_rows)

    if new_rows_collection:
        updated_df = pd.concat([edges_df] + new_rows_collection, ignore_index=True)
        return updated_df

    return edges_df


@baseutils.log_exec_metrics
def apply_update_on_elements(
        legacy_df: pd.DataFrame, input_df: pd.DataFrame
) -> pd.DataFrame:
    return pd.concat([input_df, legacy_df], ignore_index=True).drop_duplicates()


@baseutils.log_exec_metrics
def apply_updates(
        mode: Literal["rebuild", "update", "update_with_unwind"],
        existing_edges_df: Optional[pd.DataFrame], input_edges_df: pd.DataFrame,
        existing_elements_df: pd.DataFrame, input_elements_df: pd.DataFrame,
        dimension_name: str, orphan_consolidation_name: str = "OrphanParent"
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if mode == "rebuild":
        return input_edges_df, input_elements_df

    legacy_elements_df = utility.get_legacy_elements(existing_elements_df, input_elements_df)
    if len(legacy_elements_df) == 0:
        return input_edges_df, input_elements_df
    legacy_edges_df = utility.get_legacy_edges(existing_edges_df, input_edges_df)

    input_element_hierarchies = utility.get_hierarchy_list(input_elements_df)
    legacy_element_hierarchies = utility.get_hierarchy_list(legacy_elements_df)
    retained_element_hierarchies = list(set(input_element_hierarchies) & set(legacy_element_hierarchies))

    updated_elements_df = apply_update_on_elements(
        legacy_df=legacy_elements_df, input_df=input_elements_df
    )
    updated_elements_df = add_orphan_consolidation_elements(
        elements_df=updated_elements_df, orphan_consolidation_name=orphan_consolidation_name,
        dimension_name=dimension_name, retained_hierarchies=retained_element_hierarchies
    )

    updated_edges_df = _UPDATE_STRATEGIES[mode](
        legacy_df=legacy_edges_df, input_df=input_edges_df, retained_hierarchies=retained_element_hierarchies,
        orphan_consolidation_name=orphan_consolidation_name
    )
    updated_edges_df = assign_root_orphan_edges(
        legacy_edges_df=legacy_edges_df, legacy_elements_df=legacy_elements_df, edges_df=updated_edges_df,
        orphan_consolidation_name=orphan_consolidation_name,
        dimension_name=dimension_name, retained_hierarchies=retained_element_hierarchies
    )

    return updated_edges_df, updated_elements_df


# input to be completed with io
@baseutils.log_exec_metrics
def init_input_schema(
        dimension_name: str,
        input_format: Literal["parent_child", "indented_levels", "filled_levels"],

        input_datasource: Optional[Union[str, Path]] = None,
        sql_engine: Optional[Any] = None,
        sql_table_name: Optional[str] = None,
        sql_query: Optional[str] = None,
        filter_input_columns: Optional[list[str]] = None,
        raw_input_df: pd.DataFrame = None,

        hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        level_columns: Optional[list[str]] = None, type_column: Optional[str] = None,
        weight_column: Optional[str] = None,

        input_elements_datasource: Optional[Union[str, Path]] = None,
        input_elements_df_element_column: Optional[str] = None,
        sql_elements_engine: Optional[Any] = None,
        sql_table_elements_name: Optional[str] = None,
        sql_elements_query: Optional[str] = None,
        filter_input_elements_columns: Optional[list[str]] = None,
        raw_input_elements_df: pd.DataFrame = None,
        attribute_parser: Union[Literal["colon", "square_brackets"], Callable] = "colon",
        **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    if raw_input_df is None:
        raw_input_df = io.read_source_to_df(
            source=input_datasource, column_names=filter_input_columns,
            engine=sql_engine, sql_query=sql_query, table_name=sql_table_name, **kwargs
        )

    if raw_input_elements_df is None:
        if not sql_elements_engine and (sql_elements_query or sql_table_elements_name):
            sql_elements_engine = sql_engine
        raw_input_elements_df = io.read_source_to_df(
            source=input_elements_datasource, column_names=filter_input_elements_columns,
            engine=sql_elements_engine, sql_query=sql_elements_query, table_name=sql_table_elements_name, **kwargs
        )

    pre_validate_input_schema(input_format=input_format, input_df=raw_input_df, level_columns=level_columns)

    input_edges_df, input_elements_df = normalize.normalize_input_schema(
        input_df=raw_input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name,
        level_columns=level_columns, parent_column=parent_column, child_column=child_column,
        dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column,
        input_elements_df=raw_input_elements_df,
        input_elements_df_element_column=input_elements_df_element_column,
        attribute_parser=attribute_parser)

    post_validate_schema(input_edges_df, input_elements_df)
    return input_edges_df, input_elements_df


@baseutils.log_exec_metrics
def init_existing_schema_for_builder(
        tm1_service: Any, dimension_name: str,
        old_orphan_parent_name: str = "OrphanParent",
        clear_orphan_parents: bool = True
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    if not tm1_service.dimensions.exists(dimension_name):
        return None, None

    existing_edges_df, existing_elements_df = io.retrieve_existing_schema(tm1_service, dimension_name)
    existing_edges_df, existing_elements_df = normalize.normalize_existing_schema_for_builder(existing_edges_df,
                                                                                              existing_elements_df,
                                                                                              old_orphan_parent_name,
                                                                                              clear_orphan_parents)
    return existing_edges_df, existing_elements_df


@baseutils.log_exec_metrics
def init_existing_schema_full(
        tm1_service: Any, dimension_name: str
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    existing_edges_df, existing_elements_df = io.retrieve_existing_schema(tm1_service, dimension_name)
    existing_edges_df, existing_elements_df = normalize.normalize_existing_schema_full(existing_edges_df,
                                                                                       existing_elements_df)

    return existing_edges_df, existing_elements_df


@baseutils.log_exec_metrics
def init_existing_schema_filtered(
        tm1_service: Any, dimension_name: str, hierarchy_names: list[str]
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    if not hierarchy_names:
        return None, None

    existing_edges_df_filtered = io.read_existing_edges_df_filtered(tm1_service, dimension_name, hierarchy_names)
    existing_elements_df_filtered = io.read_existing_elements_df_filtered(tm1_service, dimension_name, hierarchy_names)

    normalized_edges_df, normalized_elements_df = normalize.normalize_existing_schema_full(
        existing_edges_df_filtered, existing_elements_df_filtered)

    return normalized_edges_df, normalized_elements_df


@baseutils.log_exec_metrics
def delete_conflicting_elements(tm1_service: Any, conflicts: pd.DataFrame, dimension_name: str):
    delete_records = utility.get_delete_records_for_conflicting_elements(conflicts)
    for hierarchy_name, element_name in delete_records:
        tm1_service.elements.delete(
            dimension_name=dimension_name, hierarchy_name=hierarchy_name, element_name=element_name)


@baseutils.log_exec_metrics
def resolve_schema(
        dimension_name: str, tm1_service: Any,
        input_edges_df: pd.DataFrame, input_elements_df: pd.DataFrame,
        existing_edges_df: Optional[pd.DataFrame], existing_elements_df: Optional[pd.DataFrame],
        mode: Literal["rebuild", "update", "update_with_unwind"] = "rebuild",
        allow_type_changes: bool = False,
        orphan_parent_name: str = "OrphanParent",
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if existing_elements_df is None:
        return input_edges_df, input_elements_df

    conflicts = validate_element_type_consistency(existing_elements_df, input_elements_df, allow_type_changes)

    if allow_type_changes and (conflicts is not None):
        delete_conflicting_elements(tm1_service=tm1_service, conflicts=conflicts, dimension_name=dimension_name)

    updated_edges_df, updated_elements_df = apply_updates(
        mode=mode,
        existing_edges_df=existing_edges_df, input_edges_df=input_edges_df,
        existing_elements_df=existing_elements_df, input_elements_df=input_elements_df,
        dimension_name=dimension_name, orphan_consolidation_name=orphan_parent_name)

    normalized_updated_edges_df, normalized_updated_elements_df = normalize.normalize_updated_schema_for_builder(
        updated_edges_df, updated_elements_df)

    post_validate_schema(normalized_updated_edges_df, normalized_updated_elements_df)

    return updated_edges_df, updated_elements_df


@baseutils.log_exec_metrics
def build_dimension_object(
        dimension_name: str, edges_df: Optional[pd.DataFrame], elements_df: pd.DataFrame
) -> Dimension:
    hierarchy_names = utility.get_hierarchy_list(input_df=elements_df)

    dimension = Dimension(name=dimension_name)
    hierarchies = {
        hierarchy_name: Hierarchy(name=hierarchy_name, dimension_name=dimension_name)
        for hierarchy_name in hierarchy_names
    }

    attribute_strings = utility.get_attribute_columns_list(input_df=elements_df)

    for hierarchy_name in hierarchies.keys():
        for attr_string in attribute_strings:
            attr_name, attr_type = utility.parse_attribute_string(attr_string)
            hierarchies[hierarchy_name].add_element_attribute(attr_name, attr_type)

    for _, elements_df_row in elements_df.iterrows():
        hierarchy_name = elements_df_row['Hierarchy']
        element_name = elements_df_row['ElementName']
        element_type = elements_df_row['ElementType']

        hierarchies[hierarchy_name].add_element(element_name=element_name, element_type=element_type)

    if edges_df is not None:
        for _, edges_df_row in edges_df.iterrows():
            hierarchy_name = edges_df_row['Hierarchy']
            parent_name = edges_df_row['Parent']
            child_name = edges_df_row['Child']
            weight = edges_df_row['Weight']
            hierarchies[hierarchy_name].add_edge(parent=parent_name, component=child_name, weight=weight)

    for hierarchy in hierarchies.values():
        dimension.add_hierarchy(hierarchy)

    return dimension


@baseutils.log_exec_metrics
def build_hierarchy_object(
        dimension_name: str, hierarchy_name: str, edges_df: Optional[pd.DataFrame], elements_df: pd.DataFrame
) -> Hierarchy:

    hierarchy = Hierarchy(name=hierarchy_name, dimension_name=dimension_name)

    attribute_strings = utility.get_attribute_columns_list(input_df=elements_df)

    for attr_string in attribute_strings:
        attr_name, attr_type = utility.parse_attribute_string(attr_string)
        hierarchy.add_element_attribute(attr_name, attr_type)

    for _, elements_df_row in elements_df.iterrows():
        element_name = elements_df_row['ElementName']
        element_type = elements_df_row['ElementType']

        hierarchy.add_element(element_name=element_name, element_type=element_type)

    if edges_df is not None:
        for _, edges_df_row in edges_df.iterrows():
            parent_name = edges_df_row['Parent']
            child_name = edges_df_row['Child']
            weight = edges_df_row['Weight']
            hierarchy.add_edge(parent=parent_name, component=child_name, weight=weight)

    return hierarchy


@baseutils.log_exec_metrics
def prepare_attributes_for_load(elements_df: pd.DataFrame, dimension_name: str) -> Tuple[pd.DataFrame, str, list[str]]:
    writable_attr_df = utility.unpivot_attributes_to_cube_format(
        elements_df=elements_df, dimension_name=dimension_name
    )
    element_attributes_cube_name = "}ElementAttributes_" + dimension_name
    element_attributes_cube_dims = [dimension_name, element_attributes_cube_name]

    return writable_attr_df, element_attributes_cube_name, element_attributes_cube_dims


def generate_schema_from_attributes(
        elements_df: pd.DataFrame, attribute_columns: list[str]
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    mask_invalid = elements_df[attribute_columns].isna().any(axis=1)
    unassigned_edges = elements_df.loc[mask_invalid, ["ElementName"]].copy()
    unassigned_edges['Parent'] = 'Unassigned'
    unassigned_edges = unassigned_edges.rename(columns={"ElementName": 'Child'})

    df_valid = elements_df[~mask_invalid].copy()
    edge_frames = []

    df_working = df_valid.copy()
    current_parent_col = attribute_columns[0]

    for i in range(len(attribute_columns) - 1):
        next_attr_col = attribute_columns[i + 1]
        unique_edges = df_working[[current_parent_col, next_attr_col]].drop_duplicates()
        collision_mask = unique_edges.duplicated(subset=[next_attr_col], keep='first')

        unique_edges['Child_ID'] = np.where(
            collision_mask,
            unique_edges[current_parent_col].astype(str) + '_' + unique_edges[next_attr_col].astype(str),
            unique_edges[next_attr_col].astype(str)
        )

        level_edges = unique_edges[[current_parent_col, 'Child_ID']].rename(
            columns={current_parent_col: 'Parent', 'Child_ID': 'Child'}
        )
        edge_frames.append(level_edges)

        df_working = df_working.merge(
            unique_edges[[current_parent_col, next_attr_col, 'Child_ID']],
            on=[current_parent_col, next_attr_col],
            how='left'
        )

        # Update the column name pointer for the next iteration
        current_parent_col = 'Child_ID'

    element_edges = df_working[[current_parent_col, "ElementName"]].drop_duplicates()
    element_edges.columns = ['Parent', 'Child']
    edge_frames.append(element_edges)
    attr_hier_edges_df = pd.concat(edge_frames + [unassigned_edges], ignore_index=True)
    attr_hier_edges_df["Weight"] = 1.0

    new_elements_list = attr_hier_edges_df['Parent'].drop_duplicates().tolist()
    new_elements_df = pd.DataFrame
    new_elements_df["ElementName"] = new_elements_list
    new_elements_df["ElementType"] = "Consolidated"
    attr_hier_elements_df = pd.concat([elements_df, new_elements_df])

    return attr_hier_edges_df, attr_hier_elements_df


@baseutils.log_exec_metrics
def remove_empty_subtrees(edges_df, elements_df):
    leaf_types = ['Numeric', 'String']
    leaf_elements = set(elements_df.loc[elements_df['ElementType'].isin(leaf_types), 'ElementName'])
    valid_nodes = set(edges_df.loc[edges_df['Child'].isin(leaf_elements), 'Child'])

    while True:
        valid_parents = set(edges_df.loc[edges_df['Child'].isin(valid_nodes), 'Parent'])
        new_nodes = valid_parents - valid_nodes

        if not new_nodes:
            break

        valid_nodes.update(new_nodes)

    clean_edges_df = edges_df[edges_df['Child'].isin(valid_nodes)].copy()
    active_elements = set(clean_edges_df['Parent']).union(set(clean_edges_df['Child']))
    clean_elements_df = elements_df[elements_df['ElementName'].isin(active_elements)].copy()

    return clean_edges_df, clean_elements_df


def apply_hierarchy_sort_order_attributes(
        tm1_service: Any, dimension_name: str,
        dimension_sort_order_config: dict = None,
        hierarchy_sort_order_config: dict[str, dict] = None
) -> None:
    all_hierarchies = tm1_service.hierarchies.get_all_names(dimension_name)

    if dimension_sort_order_config:
        validate_sort_order_config(dimension_sort_order_config)
    if hierarchy_sort_order_config:
        validate_hierarchy_sort_order_config(all_hierarchies, hierarchy_sort_order_config)

    def to_sort_tuple(cfg: dict) -> tuple:
        return cfg["CompSortType"], cfg["CompSortSense"], cfg["ElSortType"], cfg["ElSortSense"]

    for h_name in all_hierarchies:
        active_config = hierarchy_sort_order_config.get(h_name) if hierarchy_sort_order_config else None
        if not active_config:
            active_config = dimension_sort_order_config

        if active_config:
            tm1_service.hierarchies._implement_hierarchy_sort_order(
                dimension_name=dimension_name,
                hierarchy_name=h_name,
                hierarchy_sort_order=to_sort_tuple(active_config)
            )
