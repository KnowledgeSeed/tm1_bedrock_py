import pandas as pd
import numpy as np
from typing import Hashable
from TM1_bedrock_py.dimension_builder.exceptions import (SchemaValidationError,
                                                         GraphValidationError,
                                                         LevelColumnInvalidRowError)


def validate_row_for_element_count(
        elements_in_row: int, row_index: Hashable
) -> None:
    if elements_in_row == 0:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Empty row, no element found")
    if elements_in_row > 1:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Multiple elements found")


def validate_row_for_parent_child_in_indented_level_columns(
        row_index: Hashable, element_level: int, hierarchy: str, stack: dict
):
    if element_level != 0 and stack[hierarchy].get(element_level - 1) is None:
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Missing parent of child element")


def validate_row_for_parent_child_in_filled_level_columns(
        df_row: pd.Series, level_columns: list[str | int], element_level: int, row_index: Hashable
):
    if element_level > 0 and df_row[level_columns[element_level - 1]] in (np.nan, None, ""):
        raise LevelColumnInvalidRowError(row_index=row_index, error_type="Missing parent of child element")


def validate_schema_for_parent_child(input_df: pd.DataFrame) -> None:
    if "Parent" not in input_df.columns:
        raise SchemaValidationError("Parent column is missing.")
    if "Child" not in input_df.columns:
        raise SchemaValidationError("Child column is missing.")


def validate_schema_for_level_columns(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    for level_column in level_columns:
        if level_column not in input_df.columns:
            raise SchemaValidationError("Level column "+level_column+" is missing.")


def validate_schema_data(edges_df: pd.DataFrame) -> None:
    is_null = edges_df["Child"].isna()
    is_empty_string = edges_df["Child"].astype(str).str.strip() == ""
    if (is_null | is_empty_string).any():
        raise SchemaValidationError("Column 'Child' contains null, empty, or whitespace-only strings.")

    valid_elements = {"N", "C", "S"}
    actual_elements = set(edges_df["ElementType"].unique())
    invalid_elements = actual_elements - valid_elements

    if invalid_elements:
        raise SchemaValidationError(
            f"Column 'ElementType' contains invalid values: {invalid_elements}. "
            f"Allowed values are: {valid_elements}"
        )


