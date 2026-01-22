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


def generate_random_dimension_data(
        hierarchy_count: int = 1,
        node_count_per_hierarchy: int = 100,
        root_node_count: int = 1,
        max_depth: int = 5,
        attribute_count: int = 3
):
    """
    Generates a dictionary representing dimension data in an indented level format
    with combined edges, elements, and attributes.
    """

    # --- 1. Setup Column Headers and Containers ---

    # Level columns
    level_cols = [f"Level{i}" for i in range(max_depth)]

    # Attribute columns
    attr_defs = []
    for i in range(1, attribute_count + 1):
        # Randomly assign type 's' (string) or 'n' (numeric)
        a_type = random.choice(['s', 'n'])
        attr_defs.append({'name': f"TestAttribute{i}:{a_type}", 'type': a_type})

    # Initialize the output dictionary
    data = {}
    for col in level_cols:
        data[col] = []

    fixed_cols = ["Dimension", "Hierarchy", "Weight", "ElementType"]
    for col in fixed_cols:
        data[col] = []

    for attr in attr_defs:
        data[attr['name']] = []

    # --- 2. Generate the Tree Structure (Graph) ---
    # We generate one generic tree structure (or forest of trees) that will be
    # repeated for each hierarchy.

    nodes = []  # List of dicts representing nodes

    # Create Roots
    for r in range(root_node_count):
        nodes.append({
            'id': f"Root_{r + 1}",
            'depth': 0,
            'parent_index': None,
            'children_indices': []
        })

    # Create remaining nodes attached to random valid parents
    # We use indices to reference nodes in the 'nodes' list
    current_count = root_node_count

    while current_count < node_count_per_hierarchy:
        # Find potential parents: nodes that are not at max_depth - 1 (leaves of the max depth)
        potential_parents_indices = [
            i for i, n in enumerate(nodes)
            if n['depth'] < max_depth - 1
        ]

        if not potential_parents_indices:
            # Fallback if max depth is too restrictive for the node count
            break

        parent_idx = random.choice(potential_parents_indices)
        parent_node = nodes[parent_idx]

        new_node_idx = len(nodes)
        new_node = {
            'id': f"Element_{current_count + 1}",
            'depth': parent_node['depth'] + 1,
            'parent_index': parent_idx,
            'children_indices': []
        }

        nodes.append(new_node)
        nodes[parent_idx]['children_indices'].append(new_node_idx)
        current_count += 1

    # --- 3. Helper Functions for Values ---

    def get_attr_value(attr_type):
        # 10% chance of empty (None)
        if random.random() < 0.10:
            return None

        if attr_type == 's':
            # Random string length 10
            return ''.join(random.choices(string.ascii_letters + string.digits, k=10))
        elif attr_type == 'n':
            # Random float 0-100
            return round(random.uniform(0, 100), 2)
        return None

    # --- 4. Populate Data (Flatten Tree per Hierarchy) ---

    def recursive_add_rows(node_idx, hierarchy_name, dim_name="DimGen"):
        node = nodes[node_idx]

        # Determine Element Type
        # C if it has children, N if it does not
        el_type = "C" if len(node['children_indices']) > 0 else "N"

        # 1. Fill Level Columns
        # All levels are None except the one matching the node's depth
        for i, col in enumerate(level_cols):
            if i == node['depth']:
                data[col].append(node['id'])
            else:
                data[col].append(None)

        # 2. Fill Fixed Columns
        data["Dimension"].append(dim_name)
        data["Hierarchy"].append(hierarchy_name)
        data["Weight"].append(1.0)
        data["ElementType"].append(el_type)

        # 3. Fill Attribute Columns
        for attr in attr_defs:
            data[attr['name']].append(get_attr_value(attr['type']))

        # 4. Recursion: Process Children
        for child_idx in node['children_indices']:
            recursive_add_rows(child_idx, hierarchy_name, dim_name)

    # Main Loop over Hierarchies
    dim_name = "DimGenerated"

    for h in range(hierarchy_count):
        hier_name = f"Hierarchy_{h + 1}"
        if h == 0:
            hier_name = dim_name  # Usually main hierarchy has same name as dim

        # Identify roots (depth 0) and start traversal
        root_indices = [i for i, n in enumerate(nodes) if n['depth'] == 0]

        for root_idx in root_indices:
            recursive_add_rows(root_idx, hier_name, dim_name)

    return data

