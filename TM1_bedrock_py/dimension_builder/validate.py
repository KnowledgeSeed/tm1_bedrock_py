import pandas as pd
from collections import defaultdict
from typing import Hashable, Literal, Optional
from TM1_bedrock_py.dimension_builder.exceptions import (
    SchemaValidationError,
    GraphValidationError,
    LevelColumnInvalidRowError,
    ElementTypeConflictError,
    InvalidInputParameterError
)


def validate_parameters_for_input_format(
        input_format: Literal["parent_child", "indented_levels", "filled_levels"],
        level_columns: Optional[list[str]],
):
    if input_format != "parent_child" and level_columns is None:
        raise InvalidInputParameterError(
            "Missing required parameter 'level_columns'."
            "Parameter is mandatory for level column type inputs."
        )


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


def validate_schema_for_type_mapping(input_df: pd.DataFrame, type_mapping: dict) -> None:
    current_values = set(input_df['ElementType'].unique())
    valid_keys = set(type_mapping.keys())
    unknown_values = current_values - valid_keys
    if unknown_values:
        bad_vals_list = sorted(list(unknown_values))
        raise SchemaValidationError(f"Type normalization failed: Found unknown 'ElementType' values: {bad_vals_list}")


def validate_schema_for_numeric_values(input_df: pd.DataFrame, converted_series: pd.Series, col_name: str) -> None:
    failed_mask = converted_series.isna() & input_df[col_name].notna()
    if failed_mask.any():
        bad_values = input_df.loc[failed_mask, col_name].unique()

        raise SchemaValidationError(
            f"Conversion Failed: The weight column contains non-numeric values that cannot be converted to float.\n"
            f"Invalid values found: {list(bad_values)}"
        )

# schema validations for post-validation


def validate_schema_for_node_integrity(edges_df: pd.DataFrame, elements_df: pd.DataFrame):
    edge_nodes = set(edges_df['Parent'].unique()) | set(edges_df['Child'].unique())
    attr_nodes = set(elements_df['ElementName'].unique())

    missing_nodes = edge_nodes - attr_nodes
    if missing_nodes:
        missing_list = sorted(list(missing_nodes))
        error_msg = f"Validation Failed: Found {len(missing_list)} node(s) in 'edges_df' not defined in 'elements_df'."
        raise SchemaValidationError(error_msg)


def validate_elements_df_schema_for_inconsistent_element_type(input_df: pd.DataFrame) -> None:
    inconsistent_counts = input_df.groupby("ElementName")["ElementType"].nunique()

    if (inconsistent_counts > 1).any():
        bad_elements = inconsistent_counts[inconsistent_counts > 1].index.tolist()
        raise SchemaValidationError(f"Inconsistency found! These ElementNames have multiple types: {bad_elements}")


def validate_elements_df_schema_for_inconsistent_leaf_attributes(input_df: pd.DataFrame) -> None:
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


def validate_graph_for_leaves_as_parents(edges_df: pd.DataFrame, elements_df: pd.DataFrame) -> None:
    unique_parents = set(edges_df["Parent"].unique())

    mask = elements_df["ElementType"].isin(["N", "S"])
    target_elements = elements_df.loc[mask, "ElementName"]

    is_in_parents = target_elements.isin(unique_parents)

    if is_in_parents.any():
        found_elements = target_elements[is_in_parents].unique().tolist()
        raise GraphValidationError(f"The following N/S elements were found as Parents: {found_elements}")


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


def validate_graph_for_cycles_with_kahn(edges_df: pd.DataFrame) -> None:
    adj = defaultdict(list)
    in_degree = defaultdict(int)
    all_nodes = set()

    for parent, child in zip(edges_df['Parent'], edges_df['Child']):
        adj[parent].append(child)
        in_degree[child] += 1
        all_nodes.add(parent)
        all_nodes.add(child)

    queue = [node for node in all_nodes if in_degree[node] == 0]
    processed_count = 0

    while queue:
        node = queue.pop()
        processed_count += 1

        if node in adj:
            for neighbor in adj[node]:
                in_degree[neighbor] -= 1
                if in_degree[neighbor] == 0:
                    queue.append(neighbor)

    total_nodes = len(all_nodes)

    if processed_count != total_nodes:
        problem_node = next(n for n, deg in in_degree.items() if deg > 0)
        path_visited = set()
        current = problem_node
        path = []

        while current not in path_visited:
            path_visited.add(current)
            path.append(current)

            found_next = False
            for child in adj[current]:
                if in_degree[child] > 0:
                    current = child
                    found_next = True
                    break

            if not found_next:
                break

        try:
            cycle_start_index = path.index(current)
            actual_cycle = path[cycle_start_index:] + [current]
            cycle_str = " -> ".join(map(str, actual_cycle))
        except ValueError:
            cycle_str = str(current)

        raise GraphValidationError(
            f"Cycle detected! The graph contains a circular dependency.\n"
            f"Cycle path: {cycle_str}"
        )


def post_validation_steps(edges_df: pd.DataFrame, elements_df: pd.DataFrame) -> None:
    validate_schema_for_node_integrity(edges_df=edges_df, elements_df=elements_df)
    validate_elements_df_schema_for_inconsistent_element_type(input_df=elements_df)
    validate_elements_df_schema_for_inconsistent_leaf_attributes(input_df=elements_df)

    validate_graph_for_self_loop(input_df=edges_df)
    validate_graph_for_leaves_as_parents(edges_df=edges_df, elements_df=elements_df)
    validate_graph_for_cycles_with_kahn(edges_df=edges_df)


def validate_element_type_consistency(existing_elements_df: pd.DataFrame, input_elements_df: pd.DataFrame):
    cols_needed = ['ElementName', 'Hierarchy', 'ElementType']
    df_existing_sub = existing_elements_df[cols_needed]
    df_input_sub = input_elements_df[cols_needed]

    merged_df = pd.merge(
        df_existing_sub,
        df_input_sub,
        on=['ElementName', 'Hierarchy'],
        how='inner',
        suffixes=('_existing', '_input')
    )

    conflicts = merged_df[merged_df['ElementType_existing'] != merged_df['ElementType_input']]
    print(conflicts)

    if not conflicts.empty:
        num_conflicts = len(conflicts)
        examples = conflicts.head(10).to_dict(orient='records')

        error_msg = f"Validation Failed: Found {num_conflicts} conflict(s) where 'ElementType' changed."
        error_msg += f"\nConflicts (First 10): {examples}"
        raise ElementTypeConflictError(error_msg)
