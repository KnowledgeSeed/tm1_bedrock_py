import pandas as pd
from TM1_bedrock_py.dimension_builder.exceptions import LevelColumnInvalidRowError

# input dimension dataframe normalization functions to ensure uniform format.


def normalize_parent_child(input_df: pd.DataFrame, dimension_name: str) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = dimension_name

    return input_df


def normalize_level_columns(input_df: pd.DataFrame, dimension_name: str, level_columns: list[str]) -> pd.DataFrame:
    if "Weight" not in input_df.columns:
        input_df["Weight"] = 1.0
    if "Hierarchy" not in input_df.columns:
        input_df["Hierarchy"] = dimension_name
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

        stack[current_hierarchy][element_level] = element_name
        for stack_level in list(stack[current_hierarchy].keys()):
            if stack_level > element_level:
                del stack[current_hierarchy][stack_level]

        parent_element_name = "" if element_level == 0 else stack[current_hierarchy][element_level-1]

        edges_df.loc[len(edges_df)] = {"Parent": parent_element_name, "Child": element_name,
                                       "ElementType": df_row["ElementType"], "Weight": df_row["Weight"],
                                       "Hierarchy": df_row["Hierarchy"]}

    return edges_df
