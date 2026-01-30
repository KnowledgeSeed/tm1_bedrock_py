import pandas as pd
import numpy as np
from collections import defaultdict
from typing import Literal, Optional, Any
from TM1_bedrock_py.dimension_builder.exceptions import (
    SchemaValidationError,
    GraphValidationError,
    ElementTypeConflictError,
    InvalidInputParameterError,
    InvalidLevelColumnRecordError,
    MissingDimensionError,
    MissingHierarchyError,
    DimensionAlreadyExistsError,
    HierarchyAlreadyExistsError
)
from TM1_bedrock_py import utility as baseutils

# level column invalid row errors for normalize functions


@baseutils.log_exec_metrics
def validate_filled_structure(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    mask = input_df[level_columns].notna().to_numpy()
    rows_count = len(input_df)
    row_indices = np.arange(rows_count)

    row_sums = mask.sum(axis=1)
    has_data = row_sums > 0

    first_valid_idx = mask.argmax(axis=1)
    last_valid_idx = mask.shape[1] - 1 - np.fliplr(mask).argmax(axis=1)

    # 1. LEFT ALIGNMENT CHECK
    # -----------------------------------------------------
    # If a row has data, the first value MUST be at index 0.
    not_left_aligned = (first_valid_idx != 0) & has_data

    if not_left_aligned.any():
        bad_rows = row_indices[not_left_aligned]
        raise InvalidLevelColumnRecordError(
            f"Filled Validation Failed: Rows must start at Level 1: {list(bad_rows)}"
        )

    # 2. GAP CHECK
    # -----------------------------------------------------
    # The span (distance from start to end) must equal the count of items.
    span = last_valid_idx - first_valid_idx + 1
    has_gaps = (span != row_sums) & has_data

    if has_gaps.any():
        bad_rows = row_indices[has_gaps]
        raise InvalidLevelColumnRecordError(
            f"Filled Validation Failed: Rows cannot contain gaps: {list(bad_rows)}"
        )


@baseutils.log_exec_metrics
def validate_indented_structure(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    mask = input_df[level_columns].notna().to_numpy()
    rows_count = len(input_df)
    row_indices = np.arange(rows_count)

    # 1. SINGLE VALUE CHECK
    # -----------------------------------------------------
    row_sums = mask.sum(axis=1)
    has_data = row_sums > 0
    invalid_counts = (row_sums != 1) & has_data

    if invalid_counts.any():
        bad_rows = row_indices[invalid_counts]
        raise InvalidLevelColumnRecordError(
            f"Indented Validation Failed: Rows must contain exactly ONE value: {list(bad_rows)}"
        )

    # 2. ORPHAN / CONNECTIVITY CHECK
    # -----------------------------------------------------
    # Logic: If I am at Column 2 (Level 3), the contextual hierarchy at Row-1
    # must have a value at Column 1 (Level 2).

    context_mask = input_df[level_columns].ffill(axis=0).notna().to_numpy()
    prev_context = np.vstack([
        np.zeros(len(level_columns), dtype=bool),
        context_mask[:-1]
    ])

    current_col_idx = mask.argmax(axis=1)
    required_parent_col = current_col_idx - 1

    is_not_root = current_col_idx > 0
    parent_missing = ~prev_context[row_indices, required_parent_col]

    is_orphan = has_data & is_not_root & parent_missing

    if is_orphan.any():
        bad_rows = row_indices[is_orphan]
        raise InvalidLevelColumnRecordError(
            f"Indented Validation Failed: Rows are indented too far: {list(bad_rows)}"
        )


# schema validation for normalize functions


@baseutils.log_exec_metrics
def validate_schema_for_parent_child_columns(input_df: pd.DataFrame) -> None:
    if "Parent" not in input_df.columns:
        raise SchemaValidationError("Parent column is missing.")
    if "Child" not in input_df.columns:
        raise SchemaValidationError("Child column is missing.")


@baseutils.log_exec_metrics
def validate_schema_for_level_columns(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    if level_columns is None:
        raise InvalidInputParameterError(
            "Missing required parameter 'level_columns'."
            "Parameter is mandatory for level column type inputs.")

    for level_column in level_columns:
        if level_column not in input_df.columns:
            raise SchemaValidationError("Level column "+level_column+" is missing.")


@baseutils.log_exec_metrics
def validate_schema_for_type_mapping(input_df: pd.DataFrame, type_mapping: dict) -> None:
    current_values = set(input_df['ElementType'].unique())
    valid_keys = set(type_mapping.keys())
    unknown_values = current_values - valid_keys
    if unknown_values:
        bad_vals_list = sorted(list(unknown_values))
        raise SchemaValidationError(f"Type normalization failed: Found unknown 'ElementType' values: {bad_vals_list}")


@baseutils.log_exec_metrics
def validate_schema_for_numeric_values(input_df: pd.DataFrame, converted_series: pd.Series, col_name: str) -> None:
    failed_mask = converted_series.isna() & input_df[col_name].notna()
    if failed_mask.any():
        bad_values = input_df.loc[failed_mask, col_name].unique()

        raise SchemaValidationError(
            f"Conversion Failed: The weight column contains non-numeric values that cannot be converted to float.\n"
            f"Invalid values found: {list(bad_values)}"
        )

# schema validations for post-validation


@baseutils.log_exec_metrics
def validate_schema_for_node_integrity(edges_df: pd.DataFrame, elements_df: pd.DataFrame):
    edge_nodes = set(edges_df['Parent'].unique()) | set(edges_df['Child'].unique())
    attr_nodes = set(elements_df['ElementName'].unique())

    missing_nodes = edge_nodes - attr_nodes
    if missing_nodes:
        missing_list = sorted(list(missing_nodes))
        error_msg = f"Validation Failed: Found {len(missing_list)} node(s) in 'edges_df' not defined in 'elements_df'."
        raise SchemaValidationError(error_msg)


@baseutils.log_exec_metrics
def validate_elements_df_schema_for_inconsistent_element_type(input_df: pd.DataFrame) -> None:
    inconsistent_counts = input_df.groupby("ElementName")["ElementType"].nunique()

    if (inconsistent_counts > 1).any():
        bad_elements = inconsistent_counts[inconsistent_counts > 1].index.tolist()
        raise SchemaValidationError(f"Inconsistency found! These ElementNames have multiple types: {bad_elements}")


@baseutils.log_exec_metrics
def validate_elements_df_schema_for_inconsistent_leaf_attributes(input_df: pd.DataFrame) -> None:
    n_df = input_df[input_df["ElementType"].isin(["Numeric", "String"])]
    exclude_cols = ["Hierarchy", "Dimension"]
    check_cols = [col for col in input_df.columns if col not in exclude_cols]

    inconsistencies = n_df.groupby("ElementName")[check_cols].nunique()
    bad_mask = (inconsistencies > 1).any(axis=1)
    bad_elements = inconsistencies[bad_mask].index.tolist()

    if bad_elements:
        offending_data = n_df[n_df["ElementName"].isin(bad_elements)].sort_values("ElementName")

        raise SchemaValidationError(
            f"Inconsistency Error: The following ElementNames (type 'Numeric' and 'String') have conflicting "
            f"values in required columns: {bad_elements}\n"
            f"Conflicting data:\n{offending_data}"
        )

# graph validations for post-validation


@baseutils.log_exec_metrics
def validate_graph_for_leaves_as_parents(edges_df: pd.DataFrame, elements_df: pd.DataFrame) -> None:
    unique_parents = set(edges_df["Parent"].unique())

    mask = elements_df["ElementType"].isin(["N", "S"])
    target_elements = elements_df.loc[mask, "ElementName"]

    is_in_parents = target_elements.isin(unique_parents)

    if is_in_parents.any():
        found_elements = target_elements[is_in_parents].unique().tolist()
        raise GraphValidationError(f"The following N/S elements were found as Parents: {found_elements}")


@baseutils.log_exec_metrics
def validate_graph_for_self_loop(input_df: pd.DataFrame) -> None:
    if input_df["Parent"].eq(input_df["Child"]).any():
        raise GraphValidationError("A child is the parent of itself, self loop detected.")


@baseutils.log_exec_metrics
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


@baseutils.log_exec_metrics
def post_validate_schema(edges_df: pd.DataFrame, elements_df: pd.DataFrame) -> None:
    validate_schema_for_node_integrity(edges_df=edges_df, elements_df=elements_df)
    validate_elements_df_schema_for_inconsistent_element_type(input_df=elements_df)
    validate_elements_df_schema_for_inconsistent_leaf_attributes(input_df=elements_df)

    validate_graph_for_self_loop(input_df=edges_df)
    validate_graph_for_leaves_as_parents(edges_df=edges_df, elements_df=elements_df)
    validate_graph_for_cycles_with_kahn(edges_df=edges_df)


@baseutils.log_exec_metrics
def pre_validate_input_schema(
        input_format: Literal["parent_child", "indented_levels", "filled_levels"],
        input_df: pd.DataFrame, level_columns: Optional[list[str]] = None
) -> None:
    if input_format == "parent_child":
        validate_schema_for_parent_child_columns(input_df)
        return

    validate_schema_for_level_columns(input_df, level_columns)
    if input_format == "indented_levels":
        validate_indented_structure(input_df, level_columns)
    else:
        validate_filled_structure(input_df, level_columns)


@baseutils.log_exec_metrics
def validate_element_type_consistency(
        existing_elements_df: pd.DataFrame, input_elements_df: pd.DataFrame, allow_type_changes: bool
) -> Optional[pd.DataFrame]:
    if existing_elements_df is None:
        return None

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
    if not conflicts.empty:
        num_conflicts = len(conflicts)
        examples = conflicts.head(10).to_dict(orient='records')

        if not allow_type_changes:
            error_msg = f"Validation Failed: Found {num_conflicts} conflict(s) where 'ElementType' changed."
            error_msg += f"\nConflicts (First 10): {examples}"
            raise ElementTypeConflictError(error_msg)

        else:
            return conflicts

    return None


@baseutils.log_exec_metrics
def validate_dimension_clonability(
        tm1_service: Any,
        source_dimension_name: str, source_hierarchies_actual: list[str],
        hierarchy_rename_map: dict, source_hierarchy_filter: Optional[list[str]] = None
) -> None:
    # 1. Validate Source Dimension Existence
    if not tm1_service.dimensions.exists(source_dimension_name):
        raise MissingDimensionError(f"Specified source dimension '{source_dimension_name}' does not exist.")

    actual_hierarchies = set(source_hierarchies_actual)

    # 3. Validation: Ensure all requested hierarchies actually exist in TM1
    if source_hierarchy_filter:
        scope = set(source_hierarchy_filter)
        missing_hierarchies = scope - actual_hierarchies
        if missing_hierarchies:
            formatted_missing = ", ".join(missing_hierarchies)
            raise MissingHierarchyError(f"The following source hierarchies do not exist: {formatted_missing}")
    else:
        scope = actual_hierarchies

    # 5. Validate Rename Map for scope
    renames_not_in_scope = set(hierarchy_rename_map.keys()) - scope

    if renames_not_in_scope:
        formatted_invalid = ", ".join(renames_not_in_scope)
        raise InvalidInputParameterError(
            f"The following hierarchies in the rename map are not part of the clone scope: {formatted_invalid}"
        )


@baseutils.log_exec_metrics
def validate_hierarchy_clonability(
        tm1_service: Any,
        dimension_name: str,
        source_hierarchy_name: str
) -> None:
    if not tm1_service.dimensions.exists(dimension_name):
        raise MissingDimensionError(f"Specified source dimension '{dimension_name}' does not exist.")

    if not tm1_service.hierarchies.exists(dimension_name, source_hierarchy_name):
        raise MissingHierarchyError(f"Specified source hierarchy '{source_hierarchy_name}' does not exist.")
