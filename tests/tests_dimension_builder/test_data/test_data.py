import pandas as pd
import time, random, string
from TM1_bedrock_py.dimension_builder.validate import validate_graph_for_cycles_with_kahn
from TM1_bedrock_py.dimension_builder.exceptions import GraphValidationError
from typing import Tuple
from TM1_bedrock_py import utility as baseutils

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


@baseutils.log_exec_metrics
def generate_hierarchy_data(
        dimension_name: str,
        hierarchy_names: list[str],
        nodes_per_hierarchy: int,
        max_depth: int,
        number_of_attributes: int,
        consistent_leaf_attributes: bool,
        **kwargs
) -> Tuple[dict, list[str]]:
    """
    Generates a dictionary representing a dimension structure in 'indented levels' format.

    Args:
        dimension_name (str): The value for the 'Dimension' column.
        hierarchy_names (list[str]): A list of hierarchy names to iterate over.
        nodes_per_hierarchy (int): The number of elements to generate per hierarchy.
        max_depth (int): The depth of the hierarchy.
        number_of_attributes (int): Number of attribute columns to generate.
        consistent_leaf_attributes (bool):
            If True, 'N' type elements will have the same attribute values across hierarchies (valid TM1 data).
            If False, every row gets a unique random value (invalid TM1 data).

    Returns:
        dict: The data in the specified dictionary format.
    """

    # --- 1. PRE-CALCULATE STRUCTURE (Single Hierarchy Base) ---
    indices = range(nodes_per_hierarchy)

    # Element Names (e.g., Element1, Element2...)
    base_element_names = [f"Element{i + 1}" for i in indices]

    # Levels (Indented diagonal pattern)
    base_levels = {}
    data_level_columns = []
    for level in range(max_depth):
        base_levels[f"Level{level}"] = [
            base_element_names[i] if (i % max_depth) == level else None
            for i in indices
        ]
        data_level_columns.append(f"Level{level}")

    # ElementType ("N" if at max depth, else "C")
    # We use (max_depth - 1) because indices are 0-based
    base_element_types = [
        "N" if (i % max_depth) == (max_depth - 1) else "C"
        for i in indices
    ]

    # Create a boolean mask for Leaf nodes to speed up attribute generation
    # True if N, False if C
    is_leaf_mask_base = [(t == "N") for t in base_element_types]

    # --- 2. EXPAND STRUCTURE (Multiply by Number of Hierarchies) ---
    num_hierarchies = len(hierarchy_names)
    total_rows = nodes_per_hierarchy * num_hierarchies

    data = {}

    # Expand Levels
    for level_name, col_data in base_levels.items():
        data[level_name] = col_data * num_hierarchies

    # Expand Metadata
    data["Dimension"] = [dimension_name] * total_rows
    data["Hierarchy"] = [h for h in hierarchy_names for _ in range(nodes_per_hierarchy)]
    data["Weight"] = [1.0] * total_rows
    data["ElementType"] = base_element_types * num_hierarchies

    # --- 3. GENERATE ATTRIBUTES ---

    # Expand the leaf mask for the full dataset
    full_leaf_mask = is_leaf_mask_base * num_hierarchies

    for i in range(number_of_attributes):
        is_string_attr = (i % 2 == 0)
        attr_key = f"TestAttribute{i}:{'s' if is_string_attr else 'n'}"

        # A. Generate purely random noise for the ENTIRE dataset (used for C levels or everything if flag is False)
        if is_string_attr:
            # Generate total_rows * random 8-char strings
            # k=8,
            random_noise = [
                ''.join(random.choices(string.ascii_letters, k=8))
                for _ in range(total_rows)
            ]
        else:
            # Generate total_rows * random floats
            random_noise = [random.uniform(0, 100) for _ in range(total_rows)]

        # B. Apply Logic
        if not consistent_leaf_attributes:
            # Case 1: All Random (Invalid Data)
            data[attr_key] = random_noise
        else:
            # Case 2: Consistent Leaves, Random Parents

            # Generate consistent values for the Base Hierarchy (size = nodes_per_hierarchy)
            if is_string_attr:
                base_consistent_values = [
                    ''.join(random.choices(string.ascii_letters, k=8))
                    for _ in indices
                ]
            else:
                base_consistent_values = [random.uniform(0, 100) for _ in indices]

            # Repeat these consistent values for all hierarchies
            full_consistent_values = base_consistent_values * num_hierarchies

            # Merge: Use consistent value if Leaf, otherwise use random noise
            # Zip is very fast here, avoiding index lookups
            data[attr_key] = [
                cons if is_leaf else rand
                for cons, rand, is_leaf in zip(full_consistent_values, random_noise, full_leaf_mask)
            ]

    return data, data_level_columns


