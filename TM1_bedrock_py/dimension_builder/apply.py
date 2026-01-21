from typing import Any, Literal
import pandas as pd
from TM1py.Objects import Dimension, Hierarchy
from TM1_bedrock_py.dimension_builder import normalize, utility
from TM1_bedrock_py.loader import dataframe_to_cube


def apply_update_on_edges(
        legacy_df: pd.DataFrame, input_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    # get difference, set all parents to OP if the parent is in input and the child isn't

    if len(legacy_df) == 0:
        return input_df

    input_children = set(input_df["Child"].unique().tolist())
    input_parents = set(input_df["Parent"].unique().tolist())
    cond_parent = legacy_df['Parent'].isin(input_parents)
    cond_child = ~legacy_df['Child'].isin(input_children)
    legacy_df.loc[cond_parent & cond_child, 'Parent'] = orphan_consolidation_name

    return pd.concat([input_df, legacy_df], ignore_index=True).drop_duplicates()


def apply_update_with_unwind_on_edges(
        legacy_df: pd.DataFrame, input_df: pd.DataFrame, orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:
    # get difference, set all parent names to OP, delete rows that have a child in input

    if len(legacy_df) == 0:
        return input_df

    legacy_df["Parent"] = orphan_consolidation_name

    input_children = set(input_df["Child"].unique().tolist())
    legacy_filtered = legacy_df[~legacy_df['Child'].isin(input_children)]

    return pd.concat([input_df, legacy_filtered], ignore_index=True).drop_duplicates()


def add_orphan_consolidation_elements(
        orphan_consolidation_name: str,
        dimension_name: str,
        input_hierarchies: list[str],
        attribute_columns: list[str],
) -> pd.DataFrame:
    orphan_parent_elements_df = pd.DataFrame({
        "ElementName": orphan_consolidation_name,
        "ElementType": "Consolidated",
        "Dimension": dimension_name,
        "Hierarchy": input_hierarchies
    })
    orphan_parent_elements_df[attribute_columns] = None
    normalize.assign_missing_attribute_values(
        input_df=orphan_parent_elements_df, attribute_columns=attribute_columns
    )
    return orphan_parent_elements_df


def assign_root_orphan_edges(
        legacy_edges: pd.DataFrame,
        legacy_elements: pd.DataFrame,
        edges_df: pd.DataFrame,
        orphan_consolidation_name: str,
        dimension_name: str,
        input_hierarchies: list[str],
) -> pd.DataFrame:

    new_rows_collection = []
    legacy_edges_exist = len(legacy_edges) > 0

    for hierarchy in input_hierarchies:
        if legacy_edges_exist:
            current_edges_subset = legacy_edges[legacy_edges['Hierarchy'] == hierarchy]
            current_elements_subset = legacy_elements[legacy_elements['Hierarchy'] == hierarchy]
            legacy_children_set = set(current_edges_subset['Child'])
            legacy_elements_set = set(current_elements_subset['ElementName'])
            root_orphans = list(legacy_elements_set - legacy_children_set)
        else:
            root_orphans = legacy_elements[legacy_elements['Hierarchy'] == hierarchy]['ElementName'].tolist()

        if root_orphans:
            hierarchy_orphan_rows = pd.DataFrame({
                'Parent': orphan_consolidation_name,
                'Child': root_orphans,
                'Weight': 1.0,
                'Dimension': dimension_name,
                'Hierarchy': hierarchy
            })
            new_rows_collection.append(hierarchy_orphan_rows)

    if new_rows_collection:
        updated_edges_df = pd.concat([edges_df] + new_rows_collection, ignore_index=True)
        return updated_edges_df

    return edges_df


def apply_update_on_elements(
        legacy_df: pd.DataFrame, input_df: pd.DataFrame,
        dimension_name: str, input_hierarchies: list[str], orphan_consolidation_name: str = "OrphanParent"
) -> pd.DataFrame:

    updated_df = pd.concat([input_df, legacy_df], ignore_index=True).drop_duplicates()

    attribute_columns = utility.get_attribute_columns_list(input_df=input_df, level_columns=[])
    orphan_parent_elements_df = add_orphan_consolidation_elements(
        orphan_consolidation_name=orphan_consolidation_name,
        dimension_name=dimension_name, input_hierarchies=input_hierarchies,
        attribute_columns=attribute_columns
    )

    return pd.concat([updated_df, orphan_parent_elements_df], ignore_index=True).drop_duplicates()


def apply_update(
        mode: Literal["update", "update_with_unwind"],
        existing_edges_df: pd.DataFrame, input_edges_df: pd.DataFrame,
        existing_elements_df: pd.DataFrame, input_elements_df: pd.DataFrame,
        dimension_name: str, orphan_consolidation_name: str = "OrphanParent"
):
    legacy_elements = utility.get_legacy_elements(existing_elements_df, input_elements_df)
    if len(legacy_elements) == 0:
        return input_edges_df, input_elements_df

    legacy_edges = utility.get_legacy_edges(existing_edges_df, input_edges_df)

    if mode == "update":
        updated_edges_df = apply_update_on_edges(
            legacy_df=legacy_edges, input_df=input_edges_df,
            orphan_consolidation_name=orphan_consolidation_name
        )
    else:
        updated_edges_df = apply_update_with_unwind_on_edges(
            legacy_df=legacy_edges, input_df=input_edges_df,
            orphan_consolidation_name=orphan_consolidation_name
        )

    input_hierarchies = input_elements_df["Hierarchy"].unique().tolist()
    updated_edges_df = assign_root_orphan_edges(
        legacy_edges=legacy_edges, legacy_elements=legacy_elements, edges_df=updated_edges_df,
        orphan_consolidation_name=orphan_consolidation_name,
        dimension_name=dimension_name, input_hierarchies=input_hierarchies
    )
    updated_elements_df = apply_update_on_elements(
        legacy_df=legacy_elements, input_df=input_elements_df,
        dimension_name=dimension_name, input_hierarchies=input_hierarchies,
        orphan_consolidation_name=orphan_consolidation_name
    )

    return updated_edges_df, updated_elements_df


def rebuild_dimension_structure(
        tm1_service: Any, dimension_name: str, edges_df: pd.DataFrame, elements_df: pd.DataFrame
) -> None:
    hierarchy_names = utility.get_hierarchy_list(input_df=elements_df)

    dimension = Dimension(name=dimension_name)
    hierarchies = {
        hierarchy_name: Hierarchy(name=hierarchy_name, dimension_name=dimension_name)
        for hierarchy_name in hierarchy_names
    }

    attribute_strings = utility.get_attribute_columns_list(input_df=elements_df, level_columns=[])

    for hierarchy_name in hierarchies.keys():
        for attr_string in attribute_strings:
            attr_name, attr_type = utility.parse_attribute_string(attr_string)
            hierarchies[hierarchy_name].add_element_attribute(attr_name, attr_type)

    for _, elements_df_row in elements_df.iterrows():
        hierarchy_name = elements_df_row['Hierarchy']
        element_name = elements_df_row['ElementName']
        element_type = elements_df_row['ElementType']

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


def update_element_attributes(tm1_service: Any, elements_df: pd.DataFrame, dimension_name: str) -> None:
    writable_elements_df = utility.unpivot_attributes_to_cube_format(elements_df=elements_df, dimension_name=dimension_name)
    element_attributes_cube_name = "}ElementAttributes_" + dimension_name
    element_attributes_cube_dims = [dimension_name, element_attributes_cube_name]
    dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=writable_elements_df,
        cube_name=element_attributes_cube_name,
        cube_dims=element_attributes_cube_dims,
        use_blob=True,
    )
