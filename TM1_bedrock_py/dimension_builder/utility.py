import pandas as pd
from typing import Hashable, Tuple, Callable, Literal
import re

from TM1_bedrock_py.dimension_builder.validate import (
    validate_row_for_element_count_indented_levels,
    validate_row_for_element_count_filled_levels,
    validate_row_for_complete_fill_filled_levels
)


def get_hierarchy_list(input_df: pd.DataFrame) -> list[str]:
    return input_df["Hierarchy"].unique().tolist()


def get_attribute_columns_list(input_df: pd.DataFrame, level_columns: list[str]) -> list[str]:
    non_attribute_columns = [
        "Parent", "Child", "ElementName", "ElementType", "Weight", "Dimension", "Hierarchy"
    ] + level_columns
    attr_columns = [c for c in input_df.columns if c not in non_attribute_columns]
    return attr_columns


def create_stack(input_df: pd.DataFrame) -> dict:
    hierarchies = get_hierarchy_list(input_df=input_df)
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

    validate_row_for_element_count_indented_levels(elements_in_row=elements_in_row, row_index=row_index)
    return element_name, element_level


def parse_filled_level_columns(df_row: pd.Series, row_index: Hashable, level_columns: list):
    element_name = ""
    element_level = -1
    found_empty = False

    for level_index, level_column in enumerate(level_columns):
        val = df_row[level_column]
        is_filled = val is not None and val != ""
        if is_filled:
            validate_row_for_complete_fill_filled_levels(found_empty, row_index)
            element_name = val
            element_level = level_index
        else:
            found_empty = True

    validate_row_for_element_count_filled_levels(element_level, row_index)

    return element_name, element_level


def _parse_attribute_string_colon(attr_name_and_type: str) -> Tuple[str, str]:
    name_part, type_part = attr_name_and_type.split(sep=":", maxsplit=1)
    return name_part, type_part


def _parse_attribute_string_square_brackets(attr_name_and_type: str) -> Tuple[str, str]:
    pattern = r"^(.+)\[(.+)\]$"
    match = re.match(pattern, attr_name_and_type)
    if not match:
        raise ValueError(f"Invalid format: '{attr_name_and_type}'. Expected 'Name[Type]'.")

    name_part, type_part = match.groups()
    return name_part, type_part


def parse_attribute_string(
        attr_name_and_type: str, parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[str, str]:
    strategies = {
        "square_brackets": _parse_attribute_string_square_brackets,
        "colon": _parse_attribute_string_colon
    }
    func = parser

    if isinstance(func, str):
        func = strategies[func]
    return func(attr_name_and_type)


def get_legacy_edges(existing_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
    keys = ["Parent", "Child", "Dimension", "Hierarchy"]
    merged = existing_df.merge(
        input_df[keys].drop_duplicates(),
        on=keys,
        how='left',
        indicator=True
    )
    result = merged[merged['_merge'] == 'left_only']
    return result.drop(columns=['_merge'])


def get_legacy_elements(existing_df: pd.DataFrame, input_df: pd.DataFrame) -> pd.DataFrame:
    keys = ["ElementName", "Dimension", "Hierarchy"]
    merged = existing_df.merge(
        input_df[keys].drop_duplicates(),
        on=keys,
        how='left',
        indicator=True
    )
    result = merged[merged['_merge'] == 'left_only']
    return result.drop(columns=['_merge'])


def unpivot_attributes_to_cube_format(elements_df: pd.DataFrame, dimension_name: str) -> pd.DataFrame:
    attribute_dimension_name = "}ElementAttributes_" + dimension_name
    attribute_columns = get_attribute_columns_list(input_df=elements_df, level_columns=[])

    elements_df_copy = elements_df.copy()
    elements_df_copy[dimension_name] = elements_df_copy['Hierarchy'] + ':' + elements_df_copy['ElementName']
    df_to_melt = elements_df_copy.drop(columns=['ElementName', 'ElementType', 'Dimension', 'Hierarchy'])
    df_to_melt = df_to_melt.rename(columns={
        attr_string: parse_attribute_string(attr_string)[0]
        for attr_string in attribute_columns
    })

    return df_to_melt.melt(
        id_vars=[dimension_name],
        var_name=attribute_dimension_name,
        value_name='Value'
    )
