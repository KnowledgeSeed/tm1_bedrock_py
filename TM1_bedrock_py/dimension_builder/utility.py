import pandas as pd
from typing import Tuple, Callable, Literal
import re


def get_hierarchy_list(input_df: pd.DataFrame) -> list[str]:
    return input_df["Hierarchy"].unique().tolist()


def get_attribute_columns_list(input_df: pd.DataFrame) -> list[str]:
    non_attribute_columns = [
        "Parent", "Child", "ElementName", "ElementType", "Weight", "Dimension", "Hierarchy"
    ]
    attr_columns = [c for c in input_df.columns if c not in non_attribute_columns]
    return attr_columns


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
    pd.options.mode.chained_assignment = None

    attribute_dimension_name = "}ElementAttributes_" + dimension_name
    attribute_columns = get_attribute_columns_list(input_df=elements_df)

    elements_df_copy = elements_df.copy()
    elements_df_copy[dimension_name] = elements_df_copy['Hierarchy'] + ':' + elements_df_copy['ElementName']
    df_to_melt = elements_df_copy.drop(columns=['ElementName', 'ElementType', 'Dimension', 'Hierarchy'])
    df_to_melt = df_to_melt.rename(columns={
        attr_string: parse_attribute_string(attr_string)[0]
        for attr_string in attribute_columns
    })
    unpivoted_df = df_to_melt.melt(
        id_vars=[dimension_name],
        var_name=attribute_dimension_name,
        value_name='Value'
    )
    return unpivoted_df
