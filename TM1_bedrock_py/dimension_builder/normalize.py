import pandas as pd
import numpy as np
from typing import Optional, Tuple, Callable, Literal
from TM1_bedrock_py.dimension_builder import utility

from TM1_bedrock_py.dimension_builder.validate import (
    validate_row_for_parent_child_in_indented_level_columns,
    validate_schema_for_parent_child_columns,
    validate_schema_for_level_columns,
    validate_schema_for_type_mapping,
    validate_schema_for_numeric_values
)
from TM1_bedrock_py.dimension_builder.exceptions import InvalidAttributeColumnNameError


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


def normalize_all_base_column_names(
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


def normalize_attr_column_names(
        input_df: pd.DataFrame,
        attribute_columns: list[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, list[str]]:
    if attribute_columns is None:
        attribute_columns = utility.get_attribute_columns_list(input_df=input_df, level_columns=[])

    rename_map = {}
    renamed_columns = []

    for col in attribute_columns:
        name_part, type_part = utility.parse_attribute_string(col, attribute_parser)
        normalized_type = _ATTR_TYPE_MAPPING.get(type_part)

        if name_part.strip() is "":
            raise InvalidAttributeColumnNameError(
                f"Missing attribute name in column '{col}'. "
            )

        if normalized_type is None:
            raise InvalidAttributeColumnNameError(
                f"Unknown attribute type '{type_part}' in column '{col}'. "
                f"Must be one of: {list(_ATTR_TYPE_MAPPING.keys())}"
            )

        new_name = f"{name_part}:{normalized_type}"

        if new_name.count(":") != 1:
            raise InvalidAttributeColumnNameError(
                f"Naming Validation Error: Resulting name '{new_name}' "
            )

        rename_map[col] = new_name
        renamed_columns.append(new_name)

    return input_df.rename(columns=rename_map), renamed_columns


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


def assign_missing_edge_values(input_df: pd.DataFrame, dimension_name: str, hierarchy_name: str = None):
    input_df["Weight"] = input_df["Weight"].replace(r'^\s*$', np.nan, regex=True).fillna(1.0)
    input_df["Dimension"] = input_df["Dimension"].replace(r'^\s*$', np.nan, regex=True).fillna(dimension_name)

    if hierarchy_name is None:
        hierarchy_name = dimension_name
    input_df["Hierarchy"] = input_df["Hierarchy"].replace(r'^\s*$', np.nan, regex=True).fillna(hierarchy_name)


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


def normalize_attr_column_types(
        input_df: pd.DataFrame, attr_columns: list[str],
) -> None:
    for attr_column in attr_columns:
        _, attr_type = utility.parse_attribute_string(attr_column)
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
        input_df: pd.DataFrame, attribute_columns: list[str] = None,
) -> None:
    if attribute_columns is None:
        attribute_columns = utility.get_attribute_columns_list(input_df=input_df, level_columns=[])

    element_name_column = 'Child' if 'Child' in input_df.columns else 'ElementName'

    for attribute_info in attribute_columns:
        _, attr_type = utility.parse_attribute_string(attribute_info)

        if attr_type == "String":
            input_df[attribute_info] = input_df[attribute_info].fillna("")

        elif attr_type == "Numeric":
            input_df[attribute_info] = input_df[attribute_info].fillna(0.0)

        elif attr_type == "Alias":
            condition = input_df[attribute_info].isna() | (input_df[attribute_info] == "")

            input_df[attribute_info] = np.where(
                condition,
                input_df[element_name_column],
                input_df[attribute_info]
            )


def separate_edge_df_columns(input_df: pd.DataFrame) -> pd.DataFrame:
    validate_schema_for_parent_child_columns(input_df)
    column_list = ["Parent", "Child", "Weight", "Dimension", "Hierarchy"]
    edges_df = input_df[column_list].copy()
    return edges_df


def separate_elements_df_columns(
        input_df: pd.DataFrame,
        attribute_columns: list[str]
) -> pd.DataFrame:
    base_columns = ["Child", "ElementType", "Dimension", "Hierarchy"]
    elements_df = input_df[base_columns + attribute_columns].copy()
    elements_df = elements_df.rename(columns={"Child": "ElementName"})
    return elements_df


def assign_parent_child_to_indented_levels(input_df: pd.DataFrame, level_columns: list[str], ):
    validate_schema_for_level_columns(input_df, level_columns)

    stack = utility.create_stack(input_df)
    for row_index, df_row in input_df.iterrows():
        current_hierarchy = df_row["Hierarchy"]

        element_name, element_level = utility.parse_indented_level_columns(
            df_row=df_row, row_index=row_index,
            level_columns=level_columns
        )
        input_df.at[(row_index, "Child")] = element_name
        validate_row_for_parent_child_in_indented_level_columns(
            row_index=row_index, element_level=element_level,
            hierarchy=current_hierarchy, stack=stack
        )

        parent_element_name = None if element_level == 0 else stack[current_hierarchy][element_level - 1]
        input_df.at[(row_index, "Parent")] = parent_element_name

        stack = utility.update_stack(
            stack=stack, hierarchy=current_hierarchy,
            element_level=element_level, element_name=element_name
        )
    return input_df


def assign_parent_child_to_filled_levels(input_df: pd.DataFrame, level_columns: list[str], ):
    validate_schema_for_level_columns(input_df, level_columns)

    for row_index, df_row in input_df.iterrows():
        element_name, element_level = utility.parse_filled_level_columns(
            df_row=df_row, level_columns=level_columns, row_index=row_index
        )

        parent_element_name = df_row[level_columns[element_level-1]] if element_level > 0 else ""
        input_df.at[(row_index, "Child")] = element_name
        input_df.at[(row_index, "Parent")] = parent_element_name

    return input_df


def drop_invalid_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    edges_df['Parent'] = edges_df['Parent'].replace("", np.nan)
    edges_df = edges_df.dropna(subset=['Parent'])
    return edges_df


def deduplicate_edges(edges_df: pd.DataFrame) -> pd.DataFrame:
    edges_df = edges_df.drop_duplicates(subset=["Parent", "Child", "Hierarchy"])
    return edges_df


def deduplicate_elements(elements_df: pd.DataFrame) -> pd.DataFrame:
    elements_df = elements_df.drop_duplicates(subset=["ElementName", "Dimension", "Hierarchy"]).reset_index(drop=True)
    return elements_df


def normalize_parent_child(
        input_df: pd.DataFrame,
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        parent_column: Optional[str] = None, child_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        input_elements_df: pd.DataFrame = None,
        input_elements_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    input_df = normalize_all_base_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        parent_column=parent_column, child_column=child_column,
        type_column=type_column, weight_column=weight_column
    )

    if input_elements_df is not None:
        input_elements_df = normalize_all_base_column_names(
            input_df=input_elements_df, dim_column=dim_column, hier_column=hier_column,
            element_column=input_elements_df_element_column, type_column=type_column
        )
        input_df = pd.merge(input_df, input_elements_df, on='Child', how='left')

    attribute_columns = utility.get_attribute_columns_list(input_df=input_df, level_columns=[])
    input_df, attribute_columns = normalize_attr_column_names(
        input_df=input_df, attribute_columns=attribute_columns, attribute_parser=attribute_parser
    )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_column(input_df=input_df)

    assign_missing_edge_values(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(input_df=input_df)
    assign_missing_attribute_values(input_df=input_df, attribute_columns=attribute_columns)

    normalize_base_column_types(input_df=input_df, level_columns=[])
    normalize_type_values(input_df=input_df)
    normalize_attr_column_types(input_df=input_df, attr_columns=attribute_columns)

    edges_df = separate_edge_df_columns(input_df=input_df)
    elements_df = separate_elements_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    edges_df = drop_invalid_edges(edges_df)
    edges_df = deduplicate_edges(edges_df)
    elements_df = deduplicate_elements(elements_df)

    return edges_df, elements_df


def normalize_indented_level_columns(
        input_df: pd.DataFrame,
        level_columns: list[str],
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        input_elements_df: pd.DataFrame = None,
        input_elements_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    input_df = normalize_all_base_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column
    )

    input_df = assign_parent_child_to_level_columns(input_df=input_df)
    input_df = assign_parent_child_to_indented_levels(input_df=input_df, level_columns=level_columns)

    if input_elements_df is not None:
        input_elements_df = normalize_all_base_column_names(
            input_df=input_elements_df, dim_column=dim_column, hier_column=hier_column,
            element_column=input_elements_df_element_column, type_column=type_column
        )
        input_df = pd.merge(input_df, input_elements_df, on='Child', how='left')

    attribute_columns = utility.get_attribute_columns_list(input_df=input_df, level_columns=level_columns)
    input_df, attribute_columns = normalize_attr_column_names(
        input_df=input_df, attribute_columns=attribute_columns, attribute_parser=attribute_parser
    )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_column(input_df=input_df)

    assign_missing_edge_values(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(input_df=input_df)
    assign_missing_attribute_values(input_df=input_df, attribute_columns=attribute_columns)

    normalize_base_column_types(input_df=input_df, level_columns=[])
    normalize_type_values(input_df=input_df)
    normalize_attr_column_types(input_df=input_df, attr_columns=attribute_columns)

    edges_df = separate_edge_df_columns(input_df=input_df)
    elements_df = separate_elements_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    edges_df = drop_invalid_edges(edges_df)
    edges_df = deduplicate_edges(edges_df)
    elements_df = deduplicate_elements(elements_df)

    return edges_df, elements_df


def normalize_filled_level_columns(
        input_df: pd.DataFrame,
        level_columns: list[str],
        dimension_name: str, hierarchy_name: str = None,
        dim_column: Optional[str] = None, hier_column: Optional[str] = None,
        type_column: Optional[str] = None, weight_column: Optional[str] = None,
        input_elements_df: pd.DataFrame = None,
        input_elements_df_element_column: Optional[str] = None,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> Tuple[pd.DataFrame, pd.DataFrame]:

    input_df = normalize_all_base_column_names(
        input_df=input_df, dim_column=dim_column, hier_column=hier_column,
        type_column=type_column, weight_column=weight_column
    )

    input_df = assign_parent_child_to_level_columns(input_df=input_df)
    input_df = assign_parent_child_to_filled_levels(input_df=input_df, level_columns=level_columns)

    if input_elements_df is not None:
        input_elements_df = normalize_all_base_column_names(
            input_df=input_elements_df, dim_column=dim_column, hier_column=hier_column,
            element_column=input_elements_df_element_column, type_column=type_column
        )
        input_df = pd.merge(input_df, input_elements_df, on='Child', how='left')

    attribute_columns = utility.get_attribute_columns_list(input_df=input_df, level_columns=level_columns)
    input_df, attribute_columns = normalize_attr_column_names(
        input_df=input_df, attribute_columns=attribute_columns, attribute_parser=attribute_parser
    )

    assign_missing_edge_columns(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_column(input_df=input_df)

    assign_missing_edge_values(input_df=input_df, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
    assign_missing_type_values(input_df=input_df)
    assign_missing_attribute_values(input_df=input_df, attribute_columns=attribute_columns)

    normalize_base_column_types(input_df=input_df, level_columns=[])
    normalize_type_values(input_df=input_df)
    normalize_attr_column_types(input_df=input_df, attr_columns=attribute_columns)

    edges_df = separate_edge_df_columns(input_df=input_df)
    elements_df = separate_elements_df_columns(input_df=input_df, attribute_columns=attribute_columns)

    edges_df = drop_invalid_edges(edges_df)
    edges_df = deduplicate_edges(edges_df)
    elements_df = deduplicate_elements(elements_df)

    return edges_df, elements_df


def clear_orphan_parent_edges(
        edges_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    edges_df.drop(edges_df[edges_df["Parent"] == orphan_consolidation_name].index, inplace=True)
    return edges_df


def clear_orphan_parent_element_attributes(
        elements_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    elements_df.drop(elements_df[elements_df["ElementName"] == orphan_consolidation_name].index, inplace=True)
    return elements_df
