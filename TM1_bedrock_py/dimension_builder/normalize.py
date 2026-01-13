import pandas as pd
import numpy as np
from typing import Optional, Any
from TM1_bedrock_py.dimension_builder.exceptions import LevelColumnInvalidRowError

# input dimension dataframe normalization functions to ensure uniform format.


def normalize_all_column_names(input_df: pd.DataFrame,
                               dim_column: str | int = None, hier_column: str | int = None,
                               level_columns: list[str] | list[int] = None,
                               parent_column: str | int = None, child_column: str | int = None,
                               type_column: str | int = None, weight_column: str | int = None) -> None:

    def normalize_column_name(column_id: Optional[str | int], column_name: str) -> None:
        if column_id is None:
            return
        if isinstance(column_id, int):
            column_id = input_df.columns[column_id]
        input_df.rename(columns={column_id: column_name}, inplace=True)

    normalize_column_name(dim_column, "Dimension")
    normalize_column_name(hier_column, "Hierarchy")
    for index, level in enumerate(level_columns):
        normalize_column_name(level, "Level"+str(index))
    normalize_column_name(parent_column, "Parent")
    normalize_column_name(child_column, "Child")
    normalize_column_name(type_column, "ElementType")
    normalize_column_name(weight_column, "Weight")


def assign_missing_columns(input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0
    if "Dimension" not in input_df.columns:
        input_df["Dimension"] = dimension_name
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = hierarchy_name if hierarchy_name is not None else dimension_name

    return input_df


def fill_column_empty_values_with_defaults(input_df: pd.DataFrame, column_name: str, default_value: Any) -> None:
    input_df[column_name] = input_df[column_name].replace(r'^\s*$', np.nan, regex=True).fillna(default_value)


def fill_empty_types_with_inferred(input_df: pd.DataFrame) -> None:
    parent_list = input_df["Parent"].unique()
    input_df.loc[input_df['ElementType'].isna() & (input_df["Child"] in parent_list), 'ElementType'] = 'C'
    input_df.loc[input_df['ElementType'].isna() & (input_df["Child"] not in parent_list), 'ElementType'] = 'N'


def reorder_edge_df_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    column_order = ["Parent", "Child", "Weight", "Dimension", "Hierarchy"]
    return input_df[column_order]


def normalize_parent_child(input_df: pd.DataFrame, dimension_name: str) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = dimension_name

    column_order = ["Parent", "Child", "ElementType", "Weight", "Hierarchy"]
    edges_df = input_df[column_order]

    edges_df['Weight'] = edges_df['Weight'].replace(r'^\s*$', np.nan, regex=True).fillna(1.0)
    edges_df['Hierarchy'] = edges_df['Hierarchy'].replace(r'^\s*$', np.nan, regex=True).fillna(dimension_name)

    return edges_df


def create_stack(input_df: pd.DataFrame) -> dict:
    hierarchies = input_df["Hierarchy"].unique()
    stack = {hier: {} for hier in hierarchies}
    return stack


def update_stack(stack: dict, hierarchy: str, element_level: int, element_name: str) -> dict:
    stack[hierarchy][element_level] = element_name
    for stack_level in list(stack[hierarchy].keys()):
        if stack_level > element_level:
            del stack[hierarchy][stack_level]
    return stack


def validate_row(elements_in_row: int, row_index: int, element_level: int, hierarchy: str, stack: dict) -> None:
    if elements_in_row == 0:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Empty row, no element found")
    if elements_in_row > 1:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Multiple elements found")
    if element_level != 0 and stack[hierarchy].get(element_level - 1) is None:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Missing parent of child element")

def normalize_indented_level_columns(
        input_df: pd.DataFrame, dimension_name: str, level_columns: list[str]
) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = dimension_name

    input_df['Weight'] = input_df['Weight'].replace(r'^\s*$', np.nan, regex=True).fillna(1.0)
    input_df['Hierarchy'] = input_df['Hierarchy'].replace(r'^\s*$', np.nan, regex=True).fillna(dimension_name)

    edges_df = pd.DataFrame(columns=pd.Index(["Parent", "Child", "ElementType", "Weight", "Hierarchy"]))

    hierarchies = input_df["Hierarchy"].unique()
    stack = {hier: {} for hier in hierarchies}

    for row_index, df_row in input_df.iterrows():
        current_hierarchy = df_row["Hierarchy"]
        elements_in_row = 0
        element_level = 0
        element_name = ""
        for level_index, level_column in enumerate(level_columns):
            current_level_value = df_row[level_column]
            if current_level_value is not None and current_level_value != "":
                element_name = current_level_value
                element_level = level_index
                elements_in_row += 1

        if elements_in_row == 0:
            raise LevelColumnInvalidRowError(row_index=row_index, error_type="Empty row, no element found")
        if elements_in_row > 1:
            raise LevelColumnInvalidRowError(row_index=row_index, error_type="Multiple elements found")
        if element_level != 0 and stack[current_hierarchy].get(element_level-1) is None:
            raise LevelColumnInvalidRowError(row_index=row_index, error_type="Missing parent of child element")

        parent_element_name = None if element_level == 0 else stack[current_hierarchy][element_level - 1]

        edges_df.loc[len(edges_df)] = {"Parent": parent_element_name, "Child": element_name,
                                       "ElementType": df_row["ElementType"], "Weight": df_row["Weight"],
                                       "Hierarchy": df_row["Hierarchy"]}

        stack[current_hierarchy][element_level] = element_name
        for stack_level in list(stack[current_hierarchy].keys()):
            if stack_level > element_level:
                del stack[current_hierarchy][stack_level]

    return edges_df
