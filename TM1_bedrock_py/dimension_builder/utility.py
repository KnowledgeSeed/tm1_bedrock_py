import pandas as pd
from typing import Tuple, Callable, Literal, Optional, Union
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
        attr_name_and_type: str, parser: Union[Literal["colon", "square_brackets"], Callable] = "colon"
) -> Tuple[str, str]:
    strategies = {
        "square_brackets": _parse_attribute_string_square_brackets,
        "colon": _parse_attribute_string_colon
    }
    func = parser

    if isinstance(func, str):
        func = strategies[func]
    return func(attr_name_and_type)


def get_legacy_edges(existing_df: Optional[pd.DataFrame], input_df: pd.DataFrame) -> Optional[pd.DataFrame]:
    if existing_df is None:
        return None

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

    elements_df[dimension_name] = elements_df['Hierarchy'] + ':' + elements_df['ElementName']
    df_to_melt = elements_df.drop(columns=['ElementName', 'ElementType', 'Dimension', 'Hierarchy'])
    df_to_melt = df_to_melt.rename(columns={
        attr_string: parse_attribute_string(attr_string)[0]
        for attr_string in attribute_columns
    })
    melted_df = df_to_melt.melt(
        id_vars=[dimension_name],
        var_name=attribute_dimension_name,
        value_name='Value'
    )
    return melted_df


def get_delete_records_for_conflicting_elements(conflicts: pd.DataFrame) -> list[tuple]:
    conflicting_hierarchies = conflicts["Hierarchy"].unique().tolist() + ["Leaves"]
    delete_dict = {
        hier: []
        for hier in conflicting_hierarchies
    }
    for _, conflicts_row in conflicts.iterrows():
        element_name = conflicts_row["ElementName"]
        element_type = conflicts_row["ElementType_existing"]
        hierarchy_name = conflicts_row["Hierarchy"]

        if element_type in ("String", "Numeric") and element_name not in delete_dict["Leaves"]:
            delete_dict["Leaves"].append(element_name)
        delete_dict[hierarchy_name].append(element_name)

    targets = [
        (hier, element)
        for hier, elements in delete_dict.items()
        for element in elements
    ]

    return targets
