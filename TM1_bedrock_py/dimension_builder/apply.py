from typing import Any, Literal, Callable
import pandas as pd
from TM1py.Objects import Dimension, Hierarchy
from TM1_bedrock_py.dimension_builder import normalize
from TM1_bedrock_py.loader import dataframe_to_cube


def apply_update_without_unwind_on_edges_df(
        existing_df: pd.DataFrame, input_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    # get difference, set all parents to OP if the parent is in input and the child isn't

    existing_only = normalize.get_edges_difference(existing_df, input_df)

    input_children = set(input_df["Child"].unique().to_list())
    input_parents = set(input_df["Parent"].unique().to_list())
    cond_parent = existing_only['Parent'].isin(input_parents)
    cond_child = ~existing_only['Child'].isin(input_children)
    existing_only.loc[cond_parent & cond_child, 'Parent'] = orphan_consolidation_name

    return pd.concat([input_df, existing_only], ignore_index=True).drop_duplicates()


def apply_update_with_unwind_on_edges_df(
        existing_df: pd.DataFrame, input_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    # get difference, set all parent names to OP, delete rows that have a child in input

    existing_only = normalize.get_edges_difference(existing_df, input_df)
    existing_only["Parent"] = orphan_consolidation_name

    input_children = set(input_df["Child"].unique().to_list())
    existing_only_filtered = existing_only[~existing_only['Child'].isin(input_children)]

    return pd.concat([input_df, existing_only_filtered], ignore_index=True).drop_duplicates()


def assign_orphan_consolidation_rows_to_attr_df(
        input_hierarchies: list[str],
        attribute_columns: list[str],
        dimension_name: str,
        orphan_consolidation_name: str,
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> pd.DataFrame:
    orphan_parent_elements_df = pd.DataFrame({
        "Hierarchy": input_hierarchies,
        "ElementName": orphan_consolidation_name,
        "ElementType": "Consolidated",
        "Dimension": dimension_name
    })
    orphan_parent_elements_df[attribute_columns] = None
    normalize.assign_missing_attribute_values(
        input_df=orphan_parent_elements_df, attribute_columns=attribute_columns, parser=attribute_parser
    )
    return orphan_parent_elements_df


def apply_update_on_attr_df(
        existing_df: pd.DataFrame, input_df: pd.DataFrame,
        dimension_name: str, orphan_consolidation_name: str = "OrphanParent",
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
) -> pd.DataFrame:
    existing_only = normalize.get_attr_difference(existing_df, input_df)
    output_df = pd.concat([input_df, existing_only], ignore_index=True).drop_duplicates()

    input_hierarchies = input_df["Hierarchy"].unique().to_list()
    attribute_columns = normalize.get_attribute_columns_list(input_df=input_df, level_columns=[])

    orphan_parent_elements_df = assign_orphan_consolidation_rows_to_attr_df(
        input_hierarchies=input_hierarchies, attribute_columns=attribute_columns,
        dimension_name=dimension_name, orphan_consolidation_name=orphan_consolidation_name,
        attribute_parser=attribute_parser
    )

    return pd.concat([output_df, orphan_parent_elements_df], ignore_index=True).drop_duplicates()


def apply_update(
        mode: Literal["update", "update_with_unwind"],
        existing_edges_df: pd.DataFrame, input_edges_df: pd.DataFrame,
        existing_attr_df: pd.DataFrame, input_attr_df: pd.DataFrame,
        dimension_name: str, orphan_consolidation_name: str = "OrphanParent",
        attribute_parser: Literal["colon", "square_brackets"] | Callable = "colon"
):
    if mode == "update":
        updated_edges_df = apply_update_without_unwind_on_edges_df(
            existing_df=existing_edges_df, input_df=input_edges_df, orphan_consolidation_name=orphan_consolidation_name
        )
    else:
        updated_edges_df = apply_update_with_unwind_on_edges_df(
            existing_df=existing_edges_df, input_df=input_edges_df, orphan_consolidation_name=orphan_consolidation_name
        )

    updated_attr_df = apply_update_on_attr_df(
        existing_df=existing_attr_df, input_df=input_attr_df, dimension_name=dimension_name,
        orphan_consolidation_name=orphan_consolidation_name, attribute_parser=attribute_parser
    )

    return updated_edges_df, updated_attr_df


def rebuild_dimension_structure(
        tm1_service: Any, dimension_name: str, edges_df: pd.DataFrame, attr_df: pd.DataFrame
) -> None:
    hierarchy_names = normalize.get_hierarchy_list(input_df=attr_df)

    dimension = Dimension(name=dimension_name)
    hierarchies = {
        hierarchy_name: Hierarchy(name=hierarchy_name, dimension_name=dimension_name)
        for hierarchy_name in hierarchy_names
    }

    attribute_strings = normalize.get_attribute_columns_as_list(attr_df)

    for hierarchy_name in hierarchies.keys():
        for attr_string in attribute_strings:
            attr_name, attr_type = normalize.parse_attribute_string(attr_string)
            hierarchies[hierarchy_name].add_element_attribute(attr_name, attr_type)

    for _, attr_df_row in attr_df.iterrows():
        hierarchy_name = attr_df_row['Hierarchy']
        element_name = attr_df_row['ElementName']
        element_type = attr_df_row['ElementType']

        hierarchies[hierarchy_name].add_element(element_name=element_name, element_type=element_type)

    for _, edges_df_row in edges_df.iterrows():
        hierarchy_name = edges_df_row['Hierarchy']
        parent_name = edges_df_row['Parent']
        child_name = edges_df_row['Child']
        weight = edges_df_row['Weight']
        hierarchies[hierarchy_name].add_edge(parent=parent_name, component=child_name, weight=weight)

    for hierarchy in hierarchies.values():
        dimension.add_hierarchy(hierarchy)

    tm1_service.dimensions.update_or_create(dimension)


def fill_dimension_element_attributes(tm1_service: Any, attr_df: pd.DataFrame, dimension_name: str) -> None:
    writable_attr_df = normalize.get_writable_attr_df(attr_df=attr_df, dimension_name=dimension_name)
    element_attributes_cube_name = "}ElementAttributes_" + dimension_name
    element_attributes_cube_dims = [dimension_name, element_attributes_cube_name]
    dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=writable_attr_df,
        cube_name=element_attributes_cube_name,
        cube_dims=element_attributes_cube_dims,
        use_blob=True,
    )
