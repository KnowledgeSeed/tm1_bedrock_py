from typing import Any
import pandas as pd
from TM1py.Objects import Dimension, Hierarchy, Element, ElementAttribute
from TM1_bedrock_py.dimension_builder import normalize


def rebuild_dimension(
        tm1_service: Any, dimension_name: str, edges_df: pd.DataFrame, attr_df: pd.DataFrame,
        recreate_leaves: bool = True
) -> None:
    hierarchy_names = normalize.get_hierarchy_list(input_df=attr_df)

    dimension = Dimension(name=dimension_name)
    hierarchies = {
        hierarchy_name: Hierarchy(name=hierarchy_name, dimension_name=dimension_name)
        for hierarchy_name in hierarchy_names
    }

    attribute_strings = normalize.get_element_attribute_names_as_list(attr_df)

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

    if recreate_leaves:
        leaves_hierarchy = Hierarchy(name="Leaves", dimension_name=dimension_name)
        leaves_df = normalize.get_leaves_df(attr_df)
        for _, leaves_df_row in leaves_df.iterrows():
            element_name = leaves_df_row['ElementName']
            element_type = leaves_df_row['ElementType']
            leaves_hierarchy.add_element(element_name=element_name, element_type=element_type)
        dimension.add_hierarchy(leaves_hierarchy)

    tm1_service.dimensions.update_or_create(dimension)
