import pandas as pd
import time, random
from TM1_bedrock_py.dimension_builder.validate import (
    validate_graph_for_cycles_with_kahn, validate_graph_for_cycles_with_dfs
)
from TM1_bedrock_py.dimension_builder.exceptions import GraphValidationError

EXPECTED_DF_PARENT_CHILD = {
    "Parent": ["Total Products", "Total Products", "All Regions", "EMEA", "EMEA"],
    "Child": ["Product A", "Product B", "EMEA", "Hungary", "Germany"],
    "ElementType": ["N", "N", "C", "N", "N"],
    "Weight": [1, 1, 1, 1, 1],
    "Hierarchy": ["Default", "Default", "Default", "Default", "Default"]
}

EXPECTED_DF_PC_MO = {
    "Parent": ["Total Products", "Total Products", "All Regions", "EMEA", "EMEA"],
    "Child": ["Product A", "Product B", "EMEA", "Hungary", "Germany"]
}

EXPECTED_DF_PARENT_CHILD_ATTR = {
    "Parent": ["Total Products", "Total Products", "All Regions", "EMEA", "EMEA"],
    "Child": ["Product A", "Product B", "EMEA", "Hungary", "Germany"],
    "ElementType": ["N", "N", "C", "N", "N"],
    "Weight": [1, 1, 1, 1, 1],
    "Hierarchy": ["Default", "Default", "Default", "Default", "Default"],
    "Element": ["Hungary", "Germany", "USA", "", ""],
    "Color": ["Red", "Black", "Blue", "", ""],
    "CountryCode": ["HU", "DE", "US", "", ""],
    "IsActive": [1, 1, 1, 1, 1]
}

sql_query_parent_child = """
     SELECT
       Parent,
       Child,
       ElementType,
       Weight,
       Hierarchy
     FROM users;
"""

sql_query_pc_mo = """
     SELECT
       Parent,
       Child
    FROM users;
"""

sql_query_level_columns = """
     SELECT
       Hierarchy,
       Level1,
       Level2,
       Level3,
       Level4,
       Weight
     FROM users;
"""

dtype_mapping_parent_child = {
    "Parent": "object",
    "Child": "object",
    "ElementType": "object",
    "Weight": "float64",
    "Hierarchy": "object",
}

dtype_mapping_pc_mo = {
    "Parent": "object",
    "Child": "object",
}

dtype_mapping_level_columns = {
    "Hierarchy": "object",
    "Level1": "object",
    "Level2": "object",
    "Level3": "object",
    "Level4": "object",
    "Weight": "float64",
}

dtype_mapping_attr_only = {
    "Element": "object",
    "Color": "object",
    "CountryCode": "object",
    "IsActive": "float64"
}

col_names = ["Parent", "Child", "ElementType", "Hierarchy", "Weight"]
col_names_m_o = ["Parent", "Child"]
level_columns = ["Hierarchy", "Level1", "Level2", "Level3", "Level4", "Weight"]

EXPECTED_DF_LEVEL_COLUMNS = {
    "Hierarchy": [
        "Default", "Default", "Default", "Default", "Default", "Default", "Default", "Default",
        "Alt", "Alt", "Alt", "Alt"
    ],
    "Level1": [
        "All Regions", "", "", "", "", "", "", "",
        "All Regions", "", "", ""
    ],
    "Level2": [
        "", "EMEA", "", "", "AMER", "", "", "",
        "", "EU", "", ""
    ],
    "Level3": [
        "", "", "Hungary", "Germany", "", "USA", "", "",
        "", "", "Hungary", "Germany"
    ],
    "Level4": [
        "", "", "", "", "", "", "California", "New York",
        "", "", "", ""
    ],
    "Weight": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
}

EXPECTED_DF_LEVEL_COLUMNS_ATTR= {
    "Hierarchy": [
        "Default", "Default", "Default", "Default", "Default", "Default", "Default", "Default",
        "Alt", "Alt", "Alt", "Alt"
    ],
    "Level1": [
        "All Regions", "", "", "", "", "", "", "",
        "All Regions", "", "", ""
    ],
    "Level2": [
        "", "EMEA", "", "", "AMER", "", "", "",
        "", "EU", "", ""
    ],
    "Level3": [
        "", "", "Hungary", "Germany", "", "USA", "", "",
        "", "", "Hungary", "Germany"
    ],
    "Level4": [
        "", "", "", "", "", "", "California", "New York",
        "", "", "", ""
    ],
    "Weight": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
    "Element": ["Hungary", "Germany", "USA", "", "", "", "", "", "", "", "", ""],
    "Color": ["Red", "Black", "Blue", "", "", "", "", "", "", "", "", ""],
    "CountryCode": ["HU", "DE", "US", "", "", "", "", "", "", "", "", ""],
    "IsActive": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]
}

EXPECTED_DF_LEVEL_COLUMNS_FILLED = {
    "Hierarchy": [
        "Default", "Default", "Default", "Default", "Default", "Default", "Default", "Default",
        "Alt", "Alt", "Alt", "Alt"
    ],
    "Level1": [
        "All Regions", "All Regions", "All Regions", "All Regions", "All Regions", "All Regions", "All Regions", "All Regions",
        "All Regions", "All Regions", "All Regions", "All Regions"
    ],
    "Level2": [
        "", "EMEA", "EMEA", "EMEA", "AMER", "AMER", "AMER", "AMER",
        "", "EU", "EU", "EU"
    ],
    "Level3": [
        "", "", "Hungary", "Germany", "", "USA", "USA", "USA",
        "", "", "Hungary", "Germany"
    ],
    "Level4": [
        "", "", "", "", "", "", "California", "New York",
        "", "", "", ""
    ],
    "Weight": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1],
}

EXPECTED_DF_ATTR_ONLY = {
    "Element": ["Hungary", "Germany", "USA"],
    "Color": ["Red", "Black", "Blue"],
    "CountryCode": ["HU", "DE", "US"],
    "IsActive": [1, 1, 1]
}

columns_parent_child = {
    "parent_name": "Parent",
    "child_name": "Child",
    "type_name": "ElementType",
    "weight_name": "Weight",
    "hier_name": "Hierarchy",
    "attribute_names": ["Element", "Color", "CountryCode", "IsActive"]
}

columns_attr_only = {
    "attribute_names": ["Element", "Color", "CountryCode", "IsActive"]
}

columns_level_columns = {
    "level_names": ["Level1", "Level2", "Level3", "Level4"],
    "weight_name": "Weight",
    "hier_name": "Hierarchy",
    "attribute_names": ["Element", "Color", "CountryCode", "IsActive"]
}


def generate_diamond_dag(num_nodes=1000000, layers=20):
    print(f"Generating DAG with ~{num_nodes} nodes and diamonds...")

    parents = []
    children = []

    # 1. Distribute nodes into layers
    nodes_per_layer = num_nodes // layers
    layer_nodes = {}
    current_id = 0
    for i in range(layers):
        layer_nodes[i] = list(range(current_id, current_id + nodes_per_layer))
        current_id += nodes_per_layer

    # 2. Create Forward Edges (Valid DAG parts)
    # Connect Layer i -> Layer i+1
    for i in range(layers - 1):
        current_layer = layer_nodes[i]
        next_layer = layer_nodes[i + 1]

        for p_node in current_layer:
            # Connect to 2 random nodes in next layer (Creates Diamonds)
            # Using random.sample ensures we don't pick the same child twice for one parent
            targets = random.sample(next_layer, min(2, len(next_layer)))
            for c_node in targets:
                parents.append(p_node)
                children.append(c_node)

    df = pd.DataFrame({'Parent': parents, 'Child': children})
    return df


def test_kahn_algorithm():
    # 1. Generate Valid DAG (Diamond structure)
    df = generate_diamond_dag(num_nodes=250000, layers=20)
    print(f"Generated {len(df)} edges.")

    new_df = pd.DataFrame({
        "Parent": ["1", "2", "3", "4", "5", "6"],
        "Child": ["2", "3", "4", "5", "6", "1"],
    })
    df = pd.concat([df, new_df], ignore_index=True)

    print("validation with khan started")
    start = time.time()
    try:
        validate_graph_for_cycles_with_kahn(df)
        print("FAILURE: Algorithm failed to detect the cycle.")
    except GraphValidationError as e:
        print(f"SUCCESS: Cycle detected in {time.time() - start:.4f} seconds.")
        print(f"Error: {e}")
    print("validation with khan finished")

    print("validation with dfs started")
    start = time.time()
    try:
        validate_graph_for_cycles_with_dfs(df)
        print("FAILURE: Algorithm failed to detect the cycle.")
    except GraphValidationError as e:
        print(f"SUCCESS: Cycle detected in {time.time() - start:.4f} seconds.")
        print(f"Error: {e}")
    print("validation with dfs finished")


