import pandas as pd
import numpy as np
from typing import Hashable
from TM1_bedrock_py.dimension_builder.exceptions import (SchemaValidationError,
                                                         GraphValidationError,
                                                         LevelColumnInvalidRowError)


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


def validate_schema_for_parent_child(input_df: pd.DataFrame) -> None:
    if "Parent" not in input_df.columns:
        raise SchemaValidationError("Parent column is missing.")
    if "Child" not in input_df.columns:
        raise SchemaValidationError("Child column is missing.")


def validate_schema_for_level_columns(input_df: pd.DataFrame, level_columns: list[str]) -> None:
    for level_column in level_columns:
        if level_column not in input_df.columns:
            raise SchemaValidationError("Level column "+level_column+" is missing.")


