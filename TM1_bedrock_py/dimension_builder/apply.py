from pathlib import Path
from typing import Any, Literal, Tuple, Optional, Callable, Union
import pandas as pd
from TM1py.Objects import Dimension, Hierarchy
from TM1_bedrock_py.dimension_builder import normalize, utility, io
from TM1_bedrock_py.dimension_builder.validate import (
    post_validate_schema,
    pre_validate_input_schema,
    validate_element_type_consistency
)
from TM1_bedrock_py.loader import dataframe_to_cube
from TM1_bedrock_py import utility as baseutils


@baseutils.log_exec_metrics
def apply_update_on_edges(
        legacy_df: Optional[pd.DataFrame],
        input_df: pd.DataFrame,
        retained_hierarchies: list[str],
        orphan_consolidation_name: str = "OrphanParent",
        **kwargs
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

    return pd.concat([input_df, legacy_df], ignore_index=True).drop_duplicates()


@baseutils.log_exec_metrics
def apply_update_with_unwind_on_edges(
        legacy_df: Optional[pd.DataFrame],
        input_df: pd.DataFrame,
        retained_hierarchies: list[str],
        orphan_consolidation_name: str = "OrphanParent",
        **kwargs
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
        retained_hierarchies: list[str],
        **kwargs
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
        retained_hierarchies: list[str],
        **kwargs
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
        legacy_df: pd.DataFrame, input_df: pd.DataFrame,
        **kwargs
) -> pd.DataFrame:
    return pd.concat([input_df, legacy_df], ignore_index=True).drop_duplicates()


@baseutils.log_exec_metrics
def apply_updates(
        mode: Literal["rebuild", "update", "update_with_unwind"],
        existing_edges_df: Optional[pd.DataFrame], input_edges_df: pd.DataFrame,
        existing_elements_df: pd.DataFrame, input_elements_df: pd.DataFrame,
        dimension_name: str, orphan_consolidation_name: str = "OrphanParent",
        **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:
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
        input_datasource: Union[str, Path],
        input_format: Literal["parent_child", "indented_levels", "filled_levels", "attributes"],
        dimension_name: str, hierarchy_name: str = None, tm1_service: Any = None,
        other_service: Any = None, dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        level_columns: Optional[list[str]] = None, type_column: Optional[str] = None,
        weight_column: Optional[str] = None,
        input_elements_datasource: Optional[Union[str, Path]] = None,
        input_elements_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon",
        sql_engine: Optional[Any] = None,
        sql_table_name: Optional[str] = None,
        sql_query: Optional[str] = None,
        sql_elements_engine: Optional[Any] = None,
        sql_table_elements_name: Optional[str] = None,
        sql_elements_query: Optional[str] = None,
        filter_input_columns: Optional[list[str]] = None,
        filter_input_elements_columns: Optional[list[str]] = None,
        **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    # to be used in io with other datasources
    _ = tm1_service
    _ = other_service
    # to be completed with io for edges/complete data retrieval
    raw_input_df = io.read_source_to_df(
        source=input_datasource, column_names=filter_input_columns,
        engine=sql_engine, sql_query=sql_query, table_name=sql_table_name, **kwargs
    )
    # to be completed with io for extension element/attribute
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


def init_existing_schema(
        tm1_service: Any, dimension_name: str,
        old_orphan_parent_name: str = "OrphanParent"
) -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    if not tm1_service.dimensions.exists(dimension_name):
        return None, None

    existing_edges_df, existing_elements_df = io.retrieve_existing_schema(tm1_service, dimension_name)
    existing_edges_df, existing_elements_df = normalize.normalize_existing_schema(
        existing_edges_df, existing_elements_df, old_orphan_parent_name)
    return existing_edges_df, existing_elements_df


@baseutils.log_exec_metrics
def resolve_schema(
        dimension_name: str, tm1_service: Any,
        input_edges_df: pd.DataFrame, input_elements_df: pd.DataFrame,
        existing_edges_df: Optional[pd.DataFrame], existing_elements_df: Optional[pd.DataFrame],
        mode: Literal["rebuild", "update", "update_with_unwind"] = "rebuild",
        allow_type_changes: bool = False,
        orphant_parent_name: str = "OrphanParent",
        **kwargs
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    if existing_elements_df is None:
        return input_edges_df, input_elements_df

    conflicts = validate_element_type_consistency(existing_elements_df, input_elements_df, allow_type_changes)

    if allow_type_changes and not conflicts.empty:
        non_conflicting_existing_edges_df = utility.get_non_conflicting_edges(existing_edges_df, conflicts)
        non_conflicting_existing_elements_df = utility.get_non_conflicting_elements(existing_elements_df, conflicts)
        rebuild_dimension_structure(
            tm1_service, dimension_name, non_conflicting_existing_edges_df, non_conflicting_existing_elements_df)

    updated_edges_df, updated_elements_df = apply_updates(
        mode=mode,
        existing_edges_df=existing_edges_df, input_edges_df=input_edges_df,
        existing_elements_df=existing_elements_df, input_elements_df=input_elements_df,
        dimension_name=dimension_name, orphan_consolidation_name=orphant_parent_name)

    normalized_updated_edges_df, normalized_updated_elements_df = normalize.normalize_updated_schema(
        updated_edges_df, updated_elements_df)

    post_validate_schema(normalized_updated_edges_df, normalized_updated_elements_df)

    return updated_edges_df, updated_elements_df


@baseutils.log_exec_metrics
def rebuild_dimension_structure(
        tm1_service: Any, dimension_name: str, edges_df: pd.DataFrame, elements_df: pd.DataFrame, **kwargs
) -> None:
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

    for _, edges_df_row in edges_df.iterrows():
        hierarchy_name = edges_df_row['Hierarchy']
        parent_name = edges_df_row['Parent']
        child_name = edges_df_row['Child']
        weight = edges_df_row['Weight']
        hierarchies[hierarchy_name].add_edge(parent=parent_name, component=child_name, weight=weight)

    for hierarchy in hierarchies.values():
        dimension.add_hierarchy(hierarchy)

    tm1_service.dimensions.update_or_create(dimension)


@baseutils.log_exec_metrics
def update_element_attributes(tm1_service: Any, elements_df: pd.DataFrame, dimension_name: str, **kwargs) -> None:
    writable_elements_df = utility.unpivot_attributes_to_cube_format(
        elements_df=elements_df, dimension_name=dimension_name
    )
    element_attributes_cube_name = "}ElementAttributes_" + dimension_name
    element_attributes_cube_dims = [dimension_name, element_attributes_cube_name]
    dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=writable_elements_df,
        cube_name=element_attributes_cube_name,
        cube_dims=element_attributes_cube_dims,
        use_blob=True,
    )
