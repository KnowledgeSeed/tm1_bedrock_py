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
