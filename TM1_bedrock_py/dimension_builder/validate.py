import pandas as pd
from TM1_bedrock_py.dimension_builder.exceptions import SchemaValidationError, GraphValidationError


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


