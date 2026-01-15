import pandas as pd
import numpy as np
from typing import Optional, Any, Hashable, Tuple
from TM1_bedrock_py.dimension_builder.validate import (validate_row_for_element_count,
                                                       validate_row_for_parent_child_in_filled_level_columns,
                                                       validate_row_for_parent_child_in_indented_level_columns,
                                                       validate_schema_for_parent_child,
                                                       validate_schema_for_level_columns)


# input dimension dataframe normalization functions to ensure uniform format.


def normalize_all_column_names(
        input_df: pd.DataFrame,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        element_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None
) -> pd.DataFrame:

    def normalize_column_name(column_id: Optional[str], column_name: str) -> None:
        if column_id is None:
            return
        input_df.rename(columns={column_id: column_name}, inplace=True)

    normalize_column_name(dim_column, "Dimension")
    normalize_column_name(hier_column, "Hierarchy")
    normalize_column_name(parent_column, "Parent")
    normalize_column_name(child_column, "Child")
    normalize_column_name(element_column, "ElementName")
    normalize_column_name(type_column, "ElementType")
    normalize_column_name(weight_column, "Weight")

    return input_df


def assign_missing_edge_columns(input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0
    if "Dimension" not in input_df.columns:
        input_df["Dimension"] = dimension_name
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = hierarchy_name if hierarchy_name is not None else dimension_name

    return input_df


def assign_parent_child_to_level_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    if "Parent" not in input_df.columns:
        input_df["Parent"] = ""
    if "Child" not in input_df.columns:
        input_df["Child"] = ""

    return input_df


def fill_column_empty_values_with_defaults(input_df: pd.DataFrame, column_name: str, default_value: Any) -> None:
    input_df[column_name] = input_df[column_name].replace(r'^\s*$', np.nan, regex=True).fillna(default_value)


def assign_missing_edge_values(input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None):
    fill_column_empty_values_with_defaults(input_df=input_df, column_name="Weight", default_value=1.0)
    fill_column_empty_values_with_defaults(input_df=input_df, column_name="Dimension", default_value=dimension_name)
    fill_column_empty_values_with_defaults(
        input_df=input_df, column_name="Hierarchy",
        default_value=hierarchy_name if hierarchy_name is not None else dimension_name
    )


def assign_missing_type_column(input_df: pd.DataFrame):
    if "ElementType" not in input_df.columns:
        input_df["ElementType"] = ""


def assign_missing_type_values(edges_df: pd.DataFrame, attr_df: pd.DataFrame) -> None:
    parent_list = edges_df['Parent'].unique()
    is_empty = attr_df['ElementType'].isin([np.nan, None, ""])

    attr_df.loc[is_empty & attr_df['ElementName'].isin(parent_list), 'ElementType'] = 'N'
    attr_df.loc[is_empty & ~attr_df['ElementName'].isin(parent_list), 'ElementType'] = 'C'


def separate_edge_df_columns(edges_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
    validate_schema_for_parent_child(input_df)

    edges_df["Parent"] = input_df["Parent"]
    edges_df["Child"] = input_df["Child"]
    edges_df["Weight"] = input_df["Weight"]
    edges_df["Dimension"] = input_df["Dimension"]
    edges_df["Hierarchy"] = input_df["Hierarchy"]
    return edges_df


def separate_attr_df_columns(
        input_df: pd.DataFrame,
        attr_columns: Optional[list[str]] = None
):
    attr_columns = attr_columns if attr_columns is not None else []
    type_column = ["ElementType"] if "ElementType" in input_df.columns else []
    attr_df = input_df[["Child"] + type_column + attr_columns].copy()
    attr_df = attr_df.rename(columns={"Child": "ElementName"})
    attr_df = attr_df.drop_duplicates(subset=["ElementName"]).reset_index(drop=True)
    return attr_df


def reorder_edge_df_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    column_order = ["Parent", "Child", "Weight", "Dimension", "Hierarchy"]
    return input_df[column_order]


def reorder_attr_df_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    cols_to_move = ["ElementName", "ElementType"]
    new_columns = cols_to_move + [col for col in input_df.columns if col not in cols_to_move]
    output_df = input_df[new_columns]
    return output_df


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


def parse_indented_level_columns(df_row: pd.Series, row_index: Hashable, level_columns: list):
    elements_in_row = 0
    element_level = 0
    element_name = ""
    for level_index, level_column in enumerate(level_columns):
        current_level_value = df_row[level_column]
        if current_level_value is not None and current_level_value != "":
            element_name = current_level_value
            element_level = level_index
            elements_in_row += 1

    validate_row_for_element_count(elements_in_row=elements_in_row, row_index=row_index)
    return element_name, element_level


def parse_filled_level_columns(df_row: pd.Series, level_columns: list):
    element_level = 0
    element_name = ""
    for level_index, level_column in enumerate(level_columns):
        current_level_value = df_row[level_column]
        if current_level_value is not None and current_level_value != "":
            element_name = current_level_value
            element_level = level_index

    return element_name, element_level


def create_empty_edges_df():
    edges_df = pd.DataFrame(columns=pd.Index(["Parent", "Child", "Weight", "Dimension", "Hierarchy"]))
    return edges_df


def create_empty_attr_df(attr_columns: Optional[list] = None):
    if attr_columns is None: attr_columns = []
    attr_df_columns = ["ElementName", "ElementType"] + attr_columns
    attr_df = pd.DataFrame(columns=pd.Index(attr_df_columns))
    return attr_df


def parse_indented_levels_into_parent_child(input_df: pd.DataFrame, level_columns: list[str],):
    validate_schema_for_level_columns(input_df, level_columns)

    stack = create_stack(input_df)
    for row_index, df_row in input_df.iterrows():
        current_hierarchy = df_row["Hierarchy"]

        element_name, element_level = parse_indented_level_columns(df_row=df_row, row_index=row_index,
                                                                   level_columns=level_columns)
        input_df.at[(row_index, "Child")] = element_name
        validate_row_for_parent_child_in_indented_level_columns(
            row_index=row_index, element_level=element_level,
            hierarchy=current_hierarchy, stack=stack
        )

        parent_element_name = None if element_level == 0 else stack[current_hierarchy][element_level - 1]
        input_df.at[(row_index, "Parent")] = parent_element_name

        stack = update_stack(
            stack=stack, hierarchy=current_hierarchy,
            element_level=element_level, element_name=element_name
        )
    return input_df


def parse_filled_levels_into_parent_child(input_df: pd.DataFrame, level_columns: list[str],):
    validate_schema_for_level_columns(input_df, level_columns)

    for row_index, df_row in input_df.iterrows():
        element_name, element_level = parse_filled_level_columns(df_row=df_row, level_columns=level_columns)

        validate_row_for_parent_child_in_filled_level_columns(
            df_row=df_row, level_columns=level_columns, element_level=element_level, row_index=row_index
        )
        parent_element_name = df_row[level_columns[element_level-1]] if element_level > 0 else ""
        input_df.at[(row_index, "Child")] = element_name
        input_df.at[(row_index, "Parent")] = parent_element_name

    return input_df


def drop_invalid_edges_df_rows(edges_df: pd.DataFrame) -> pd.DataFrame:
    edges_df['Parent'] = edges_df['Parent'].replace("", np.nan)
    edges_df = edges_df.dropna(subset=['Name'])
    return edges_df


def normalize_parent_child(
        input_df: pd.DataFrame,
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        attr_columns: Optional[list[str]] = None,
        input_attr_df: pd.DataFrame = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    edges_df = create_empty_edges_df()
    attr_df = create_empty_attr_df(attr_columns) if input_attr_df is None else input_attr_df

    input_df = normalize_all_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        parent_column=parent_column, child_column=child_column,
        type_column=type_column, weight_column=weight_column
    )
    attr_df = normalize_all_column_names(
        input_df=attr_df,
        element_column=attr_df.columns[0],
        type_column=type_column,
    )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    edges_df = separate_edge_df_columns(edges_df=edges_df, input_df=input_df)

    if input_attr_df is None:
        attr_df = separate_attr_df_columns(input_df=input_df, attr_columns=attr_columns)
    assign_missing_type_column(input_df=attr_df)

    assign_missing_edge_values(input_df=edges_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(edges_df=edges_df, attr_df=attr_df)

    reorder_edge_df_columns(input_df=edges_df)
    reorder_attr_df_columns(input_df=attr_df)

    edges_df = drop_invalid_edges_df_rows(edges_df)

    return edges_df, attr_df


def normalize_indented_level_columns(
        input_df: pd.DataFrame,
        level_columns: list[str],
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        attr_columns: Optional[list[str]] = None,
        input_attr_df: pd.DataFrame = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    edges_df = create_empty_edges_df()
    attr_df = create_empty_attr_df(attr_columns) if input_attr_df is None else input_attr_df

    input_df = normalize_all_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column
    )
    attr_df = normalize_all_column_names(
        input_df=attr_df,
        element_column=attr_df.columns[0],
        type_column=type_column,
    )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)

    # indented levels into parent child specific processes
    input_df = assign_parent_child_to_level_columns(input_df=input_df)
    input_df = parse_indented_levels_into_parent_child(input_df=input_df, level_columns=level_columns)

    edges_df = separate_edge_df_columns(edges_df=edges_df, input_df=input_df)

    if input_attr_df is None:
        attr_df = separate_attr_df_columns(input_df=input_df, attr_columns=attr_columns)
    assign_missing_type_column(input_df=attr_df)

    assign_missing_edge_values(input_df=edges_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(edges_df=edges_df, attr_df=attr_df)

    reorder_edge_df_columns(input_df=edges_df)
    reorder_attr_df_columns(input_df=attr_df)

    edges_df = drop_invalid_edges_df_rows(edges_df)

    return edges_df, attr_df


def normalize_filled_level_columns(
        input_df: pd.DataFrame,
        level_columns: list[str],
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        attr_columns: Optional[list[str]] = None,
        input_attr_df: pd.DataFrame = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    edges_df = create_empty_edges_df()
    attr_df = create_empty_attr_df(attr_columns) if input_attr_df is None else input_attr_df

    input_df = normalize_all_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column
    )
    attr_df = normalize_all_column_names(
        input_df=attr_df,
        element_column=attr_df.columns[0],
        type_column=type_column,
    )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)

    # filled levels into parent child specific processes
    input_df = assign_parent_child_to_level_columns(input_df=input_df)
    input_df = parse_filled_levels_into_parent_child(input_df=input_df, level_columns=level_columns)

    edges_df = separate_edge_df_columns(edges_df=edges_df, input_df=input_df)

    if input_attr_df is None:
        attr_df = separate_attr_df_columns(input_df=input_df, attr_columns=attr_columns)
    assign_missing_type_column(input_df=attr_df)

    assign_missing_edge_values(input_df=edges_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(edges_df=edges_df, attr_df=attr_df)

    reorder_edge_df_columns(input_df=edges_df)
    reorder_attr_df_columns(input_df=attr_df)

    edges_df = drop_invalid_edges_df_rows(edges_df)

    return edges_df, attr_df
