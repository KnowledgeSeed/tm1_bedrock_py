import pandas as pd
import numpy as np
from typing import Optional, Any, Hashable, Tuple, Callable, Literal
import re

from TM1_bedrock_py.dimension_builder.exceptions import InvalidAttributeColumnNameError
from TM1_bedrock_py.dimension_builder.validate import (validate_row_for_element_count_indented_levels,
                                                       validate_row_for_element_count_filled_levels,
                                                       validate_row_for_complete_fill_filled_levels,
                                                       validate_row_for_parent_child_in_indented_level_columns,
                                                       validate_schema_for_parent_child_columns,
                                                       validate_schema_for_level_columns,
                                                       validate_schema_for_type_mapping,
                                                       validate_schema_for_numeric_values)


_TYPE_MAPPING = {
    "s": "String", "S": "String", "String": "String",  "string": "String", "numeric": "Numeric",
    "n": "Numeric", "N": "Numeric", "Numeric": "Numeric",
    "c": "Consolidated", "C": "Consolidated", "Consolidated": "Consolidated", "consolidated": "Consolidated"
}

_ATTR_TYPE_MAPPING = {
    "s": "String", "S": "String", "String": "String",  "string": "String", "numeric": "Numeric",
    "n": "Numeric", "N": "Numeric", "Numeric": "Numeric",
    "a": "Alias",  "A": "Alias", "Alias": "Alias", "alias": "Alias"
}


# input dimension dataframe normalization functions to ensure uniform format.


def normalize_all_column_names(
        input_df: pd.DataFrame,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        element_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None
) -> pd.DataFrame:

    def normalize_column_name(column_name: Optional[str], new_column_name: str) -> None:
        if column_name is not None and column_name in input_df.columns:
            input_df.rename(columns={column_name: new_column_name}, inplace=True)

    normalize_column_name(dim_column, "Dimension")
    normalize_column_name(hier_column, "Hierarchy")
    normalize_column_name(parent_column, "Parent")
    normalize_column_name(child_column, "Child")
    normalize_column_name(element_column, "Child")
    normalize_column_name(type_column, "ElementType")
    normalize_column_name(weight_column, "Weight")

    return input_df


def assign_missing_edge_columns(
        input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None
) -> pd.DataFrame:
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


def normalize_numeric_values(input_df: pd.DataFrame, column_name: str) -> None:
    converted_series = pd.to_numeric(input_df[column_name], errors='coerce')
    validate_schema_for_numeric_values(input_df, converted_series, column_name)

    input_df[column_name] = converted_series.astype(float)


def normalize_string_values(input_df: pd.DataFrame, column_name: str) -> None:
    input_df[column_name] = input_df[column_name].fillna("")
    input_df[column_name] = input_df[column_name].astype(str)
    input_df[column_name] = input_df[column_name].str.strip()


def normalize_base_column_types(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    base_string_columns = ["Parent", "Child", "ElementType", "Dimension", "Hierarchy"] + level_columns
    for column_name in base_string_columns:
        normalize_string_values(input_df=input_df, column_name=column_name)
    normalize_numeric_values(input_df=input_df, column_name="Weight")


def normalize_attr_column_types(input_df: pd.DataFrame, attr_columns: list[str],
                                attribute_parser: Literal["colon", "square_brackets"] | Callable) -> None:
    for attr_column in attr_columns:
        _, attr_type = parse_attribute_string(attr_column, attribute_parser)
        if attr_type in ("Alias", "String"):
            normalize_string_values(input_df=input_df, column_name=attr_column)
        else:
            normalize_numeric_values(input_df=input_df, column_name=attr_column)


def assign_missing_type_column(input_df: pd.DataFrame):
    if "ElementType" not in input_df.columns:
        input_df["ElementType"] = ""


def assign_missing_type_values(input_df: pd.DataFrame) -> None:
    parent_list = input_df['Parent'].unique()
    is_empty = input_df['ElementType'].isin([np.nan, None, ""])

    input_df.loc[is_empty & input_df['Child'].isin(parent_list), 'ElementType'] = 'Numeric'
    input_df.loc[is_empty & ~input_df['Child'].isin(parent_list), 'ElementType'] = 'Consolidated'


def normalize_type_values(input_df: pd.DataFrame) -> pd.DataFrame:
    validate_schema_for_type_mapping(input_df=input_df, type_mapping=_TYPE_MAPPING)
    input_df['ElementType'] = input_df['ElementType'].map(_TYPE_MAPPING)
    return input_df


def assign_missing_attribute_values(
        input_df: pd.DataFrame, attribute_columns: list[str],
        parser: Literal["colon", "square_brackets"] | Callable) -> None:
    for attribute_info in attribute_columns:
        _, attr_type = parse_attribute_string(attribute_info, parser)

        if attr_type == "String":
            input_df[attribute_info] = input_df[attribute_info].fillna("")

        elif attr_type == "Numeric":
            input_df[attribute_info] = input_df[attribute_info].fillna(0.0)

        elif attr_type == "Alias":
            condition = input_df[attribute_info].isna() | (input_df[attribute_info] == "")

            input_df[attribute_info] = np.where(
                condition,
                input_df['Child'],
                input_df[attribute_info]
            )


def separate_edge_df_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    validate_schema_for_parent_child_columns(input_df)
    column_list = ["Parent", "Child", "Weight", "Dimension", "Hierarchy"]
    edges_df = input_df[column_list].copy()
    return edges_df


def separate_attr_df_columns(
        input_df: pd.DataFrame,
        attribute_columns: list[str]
) -> pd.DataFrame:
    base_columns = ["Child", "ElementType", "Dimension", "Hierarchy"]
    attr_df = input_df[base_columns + attribute_columns].copy()
    attr_df = attr_df.rename(columns={"Child": "ElementName"})
    return attr_df


def get_hierarchy_list(input_df: pd.DataFrame) -> list[str]:
    return input_df["Hierarchy"].unique()


def get_attribute_columns_list(input_df: pd.DataFrame, level_columns: list[str]) -> list[str]:
    non_attribute_columns = ["Parent", "Child", "ElementType", "Weight", "Dimension", "Hierarchy"] + level_columns
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
        element_name, element_level = parse_filled_level_columns(
            df_row=df_row, level_columns=level_columns, row_index=row_index
        )

        parent_element_name = df_row[level_columns[element_level-1]] if element_level > 0 else ""
        input_df.at[(row_index, "Child")] = element_name
        input_df.at[(row_index, "Parent")] = parent_element_name

    return input_df


def drop_invalid_edges_df_rows(edges_df: pd.DataFrame) -> pd.DataFrame:
    edges_df['Parent'] = edges_df['Parent'].replace("", np.nan)
    edges_df = edges_df.dropna(subset=['Parent'])
    edges_df = edges_df.drop_duplicates(subset=["Parent", "Child", "Hierarchy"])
    return edges_df


def drop_invalid_attr_df_rows(attr_df: pd.DataFrame) -> pd.DataFrame:
    attr_df = attr_df.drop_duplicates(subset=["ElementName", "Dimension", "Hierarchy"]).reset_index(drop=True)
    return attr_df


def normalize_parent_child(
        input_df: pd.DataFrame,
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        input_attr_df: pd.DataFrame = None,
        input_attr_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    input_df = normalize_all_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        parent_column=parent_column, child_column=child_column,
        type_column=type_column, weight_column=weight_column
    )
    if input_attr_df is not None:
        input_attr_df = normalize_all_column_names(
            input_df=input_attr_df,
            element_column=input_attr_df_element_column,
            type_column=type_column,
            dim_column=dim_column, hier_column=hier_column
        )
        input_df = pd.merge(
            input_df, input_attr_df,
            on='Child', how='left'
        )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_column(input_df=input_df)
    assign_missing_edge_values(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(input_df=input_df)

    attribute_columns = get_attribute_columns_list(input_df=input_df, level_columns=[])
    assign_missing_attribute_values(input_df=input_df, attribute_columns=attribute_columns, parser=attribute_parser)

    normalize_base_column_types(input_df=input_df, level_columns=[])
    normalize_attr_column_types(input_df=input_df, attr_columns=attribute_columns, attribute_parser=attribute_parser)
    normalize_type_values(input_df=input_df)

    edges_df = separate_edge_df_columns(input_df=input_df)
    attr_df = separate_attr_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    edges_df = drop_invalid_edges_df_rows(edges_df)
    attr_df = drop_invalid_attr_df_rows(attr_df)

    return edges_df, attr_df


def normalize_indented_level_columns(
        input_df: pd.DataFrame,
        level_columns: list[str],
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        input_attr_df: pd.DataFrame = None,
        input_attr_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    input_df = normalize_all_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column
    )

    input_df = assign_parent_child_to_level_columns(input_df=input_df)
    input_df = parse_indented_levels_into_parent_child(input_df=input_df, level_columns=level_columns)

    if input_attr_df is not None:
        input_attr_df = normalize_all_column_names(
            input_df=input_attr_df,
            element_column=input_attr_df_element_column,
            type_column=type_column,
            dim_column=dim_column, hier_column=hier_column
        )
        input_df = pd.merge(
            input_df, input_attr_df,
            on='Child', how='left'
        )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_column(input_df=input_df)
    assign_missing_edge_values(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(input_df=input_df)

    attribute_columns = get_attribute_columns_list(input_df=input_df, level_columns=level_columns)
    assign_missing_attribute_values(input_df=input_df, attribute_columns=attribute_columns, parser=attribute_parser)

    normalize_base_column_types(input_df=input_df, level_columns=level_columns)
    normalize_attr_column_types(input_df=input_df, attr_columns=attribute_columns, attribute_parser=attribute_parser)
    normalize_type_values(input_df=input_df)

    edges_df = separate_edge_df_columns(input_df=input_df)
    attr_df = separate_attr_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    edges_df = drop_invalid_edges_df_rows(edges_df)
    attr_df = drop_invalid_attr_df_rows(attr_df)

    return edges_df, attr_df


def normalize_filled_level_columns(
        input_df: pd.DataFrame,
        level_columns: list[str],
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        input_attr_df: pd.DataFrame = None,
        input_attr_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    input_df = normalize_all_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column
    )

    input_df = assign_parent_child_to_level_columns(input_df=input_df)
    input_df = parse_filled_levels_into_parent_child(input_df=input_df, level_columns=level_columns)

    if input_attr_df is not None:
        input_attr_df = normalize_all_column_names(
            input_df=input_attr_df,
            element_column=input_attr_df_element_column,
            type_column=type_column,
            dim_column=dim_column, hier_column=hier_column
        )
        input_df = pd.merge(
            input_df, input_attr_df,
            on='Child', how='left'
        )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_column(input_df=input_df)
    assign_missing_edge_values(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(input_df=input_df)

    attribute_columns = get_attribute_columns_list(input_df=input_df, level_columns=level_columns)
    assign_missing_attribute_values(input_df=input_df, attribute_columns=attribute_columns, parser=attribute_parser)

    normalize_base_column_types(input_df=input_df, level_columns=level_columns)
    normalize_attr_column_types(input_df=input_df, attr_columns=attribute_columns, attribute_parser=attribute_parser)
    normalize_type_values(input_df=input_df)

    edges_df = separate_edge_df_columns(input_df=input_df)
    attr_df = separate_attr_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    edges_df = drop_invalid_edges_df_rows(edges_df)
    attr_df = drop_invalid_attr_df_rows(attr_df)

    return edges_df, attr_df


def delete_leaves_hierarchy_from_df(input_df: pd.DataFrame) -> None:
    input_df.drop(input_df[input_df['Hierarchy'] == 'Leaves'].index, inplace=True)
    input_df.reset_index(drop=True, inplace=True)


def get_element_attribute_names_as_list(attr_df: pd.DataFrame) -> list[str]:
    exclude = ["ElementName", "ElementType", "Dimension", "Hierarchy"]
    return attr_df.columns.difference(exclude, sort=False).tolist()


def _validate_and_parse_attribute_string_default(attr_name_and_type: str) -> Tuple[str, str]:
    if attr_name_and_type.count(":") != 1:
        raise InvalidAttributeColumnNameError(
            f"Invalid attribute format: '{attr_name_and_type}'. "
            "Expected format: <name>:<type>"
        )

    name, type_code = attr_name_and_type.split(sep=":", maxsplit=1)

    if not name.strip():
        raise InvalidAttributeColumnNameError("Attribute name must not be empty")

    if not type_code.strip():
        raise InvalidAttributeColumnNameError("Attribute type must not be empty")

    if type_code not in _ATTR_TYPE_MAPPING:
        raise InvalidAttributeColumnNameError(
            f"Invalid attribute type '{type_code}'. "
            f"Allowed types: {sorted(set(_ATTR_TYPE_MAPPING))}"
        )

    return name, _ATTR_TYPE_MAPPING[type_code]


def _validate_and_parse_attribute_string_square_brackets(attr_name_and_type: str) -> Tuple[str, str]:
    match = re.fullmatch(r"([^\[\]]+)\[([^\[\]]+)\]", attr_name_and_type)

    if not match:
        raise InvalidAttributeColumnNameError(
            f"Invalid attribute format: '{attr_name_and_type}'. "
            "Expected format: <name>[<type>]"
        )

    name, type_code = match.groups()

    if not name.strip():
        raise InvalidAttributeColumnNameError("Attribute name must not be empty")

    if type_code not in _ATTR_TYPE_MAPPING:
        raise InvalidAttributeColumnNameError(
            f"Invalid attribute type '{type_code}'. "
            f"Allowed types: {sorted(set(_ATTR_TYPE_MAPPING))}"
        )

    return name, _ATTR_TYPE_MAPPING[type_code]


def parse_attribute_string(
        attr_name_and_type: str, parse_function: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[str, str]:
    if parse_function == "colon":
        parse_function = _validate_and_parse_attribute_string_default
    elif parse_function == "square_brackets":
        parse_function = _validate_and_parse_attribute_string_square_brackets
    return parse_function(attr_name_and_type)


def get_writable_attr_df(attr_df: pd.DataFrame, dimension_name: str) -> pd.DataFrame:
    attribute_dimension_name = "}ElementAttributes_" + dimension_name
    attribute_strings = get_element_attribute_names_as_list(attr_df)

    attr_df_copy = attr_df.copy()
    attr_df_copy[dimension_name] = attr_df_copy['Hierarchy'] + ':' + attr_df_copy['ElementName']
    df_to_melt = attr_df_copy.drop(columns=['ElementName', 'ElementType', 'Dimension', 'Hierarchy'])
    df_to_melt = df_to_melt.rename(columns={
        attr_string: parse_attribute_string(attr_string)[0]
        for attr_string in attribute_strings
    })

    return df_to_melt.melt(
        id_vars=[dimension_name],
        var_name=attribute_dimension_name,
        value_name='Value'
    )
