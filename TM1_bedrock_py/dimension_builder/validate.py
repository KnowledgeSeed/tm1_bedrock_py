import pandas as pd
import numpy as np
from typing import Hashable
from TM1_bedrock_py.dimension_builder.exceptions import (SchemaValidationError,
                                                         GraphValidationError,
                                                         LevelColumnInvalidRowError)


# level column invalid row errors for normalize functions


def validate_row_for_element_count_indented_levels(
        elements_in_row: int, row_index: Hashable
) -> None:
    if elements_in_row == 0:
        raise LevelColumnInvalidRowError(
            row_index=row_index, error_type="Empty row, no element found. Exactly one is expected."
        )
    if elements_in_row > 1:
        raise LevelColumnInvalidRowError(
            row_index=row_index, error_type="Multiple elements found. Exactly one is expected."
        )


def validate_row_for_element_count_filled_levels(
        element_level: int, row_index: Hashable
) -> None:
    if element_level == -1:
        raise LevelColumnInvalidRowError(
            row_index=row_index, error_type="Empty row, no element found. Exactly one is expected."
        )


def validate_row_for_complete_fill_filled_levels(found_empty: bool, row_index: Hashable) -> None:
    if found_empty:
        raise LevelColumnInvalidRowError(
            row_index, f"Row has a gap: level is filled but a previous level was empty."
        )


def validate_row_for_parent_child_in_indented_level_columns(
        row_index: Hashable, element_level: int, hierarchy: str, stack: dict
):
    if element_level != 0 and stack[hierarchy].get(element_level - 1) is None:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Missing parent of child element")


# schema validation for normalize functions


def validate_schema_for_parent_child_columns(input_df: pd.DataFrame) -> None:
    if "Parent" not in input_df.columns:
        raise SchemaValidationError("Parent column is missing.")
    if "Child" not in input_df.columns:
        raise SchemaValidationError("Child column is missing.")


def validate_schema_for_level_columns(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    for level_column in level_columns:
        if level_column not in input_df.columns:
            raise SchemaValidationError("Level column "+level_column+" is missing.")


# schema validations for post-validation


def validate_attr_df_schema_for_inconsistent_element_type(input_df: pd.DataFrame) -> None:
    inconsistent_counts = input_df.groupby("ElementName")["ElementType"].nunique()

    if (inconsistent_counts > 1).any():
        bad_elements = inconsistent_counts[inconsistent_counts > 1].index.tolist()
        raise SchemaValidationError(f"Inconsistency found! These ElementNames have multiple types: {bad_elements}")


def validate_attr_df_schema_for_inconsistent_leaf_attributes(input_df: pd.DataFrame) -> None:
    n_df = input_df[input_df["ElementType"].isin(["N", "S"])]
    exclude_cols = ["Hierarchy", "Dimension"]
    check_cols = [col for col in input_df.columns if col not in exclude_cols]

    inconsistencies = n_df.groupby("ElementName")[check_cols].nunique()
    bad_mask = (inconsistencies > 1).any(axis=1)
    bad_elements = inconsistencies[bad_mask].index.tolist()

    if bad_elements:
        offending_data = n_df[n_df["ElementName"].isin(bad_elements)].sort_values("ElementName")

        raise SchemaValidationError(
            f"Inconsistency Error: The following ElementNames (type 'N' and 'S') have conflicting "
            f"values in required columns: {bad_elements}\n"
            f"Conflicting data:\n{offending_data}"
        )

# graph validations for post-validation


def validate_graph_for_leaves_as_parents(edges_df: pd.DataFrame, attr_df: pd.DataFrame) -> None:
    unique_parents = set(edges_df["Parent"].unique())

    mask = attr_df["ElementType"].isin(["N", "S"])
    target_elements = attr_df.loc[mask, "ElementName"]

    is_in_parents = target_elements.isin(unique_parents)

    if is_in_parents.any():
        found_elements = target_elements[is_in_parents].unique().tolist()
        GraphValidationError(f"The following N/S elements were found as Parents: {found_elements}")


def validate_graph_for_self_loop(input_df: pd.DataFrame) -> None:
    if input_df["Parent"].eq(input_df["Child"]).any():
        raise GraphValidationError("A child is the parent of itself, self loop detected.")


def validate_graph_for_cycles_with_dfs(input_df: pd.DataFrame) -> None:
    adj = input_df.groupby("Parent")["Child"].apply(list).to_dict()
    visited = set()
    rec_stack = set()

    def has_cycle(graph_node):
        visited.add(graph_node)
        rec_stack.add(graph_node)

        for child in adj.get(graph_node, []):
            if child not in visited:
                if has_cycle(child):
                    return True
            elif child in rec_stack:
                return True

        rec_stack.remove(graph_node)
        return False

    all_nodes = set(input_df["Parent"]).union(set(input_df["Child"]))
    for node in all_nodes:
        if node not in visited:
            if has_cycle(node):
                raise GraphValidationError(f"Graph contains a cycle starting from node: {node}")


def post_validation_steps(edges_df: pd.DataFrame, attr_df: pd.DataFrame) -> None:
    validate_attr_df_schema_for_inconsistent_element_type(input_df=attr_df)
    validate_attr_df_schema_for_inconsistent_leaf_attributes(input_df=attr_df)

    validate_graph_for_self_loop(input_df=edges_df)
    validate_graph_for_leaves_as_parents(edges_df=edges_df, attr_df=attr_df)
    validate_graph_for_cycles_with_dfs(input_df=edges_df)
