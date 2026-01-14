EXPECTED_DF = {
    "Parent": ["Total Products", "Total Products", "All Regions", "EMEA", "EMEA"],
    "Child": ["Product A", "Product B", "EMEA", "Hungary", "Germany"],
    "ElementType": ["N", "N", "C", "N", "N"],
    "Weight": [1, 1, 1, 1, 1],
    "Hierarchy": ["Default", "Default", "Default", "Default", "Default"]
}

EXPECTED_DF_MANDATORY_ONLY = {
    "Parent": ["Total Products", "Total Products", "All Regions", "EMEA", "EMEA"],
    "Child": ["Product A", "Product B", "EMEA", "Hungary", "Germany"]
}

sql_query = """
     SELECT
       Parent,
       Child,
       ElementType,
       Weight,
       Hierarchy
     FROM dbo.Dim_Product_Edges;
"""

sql_query_m_o = """
     SELECT
       Parent,
       Child
    FROM dbo.Dim_Product_Edges;
"""

sql_query_format_2 = """
     SELECT
       Hierarchy,
       Level1,
       Level2,
       Level3,
       Level4,
       Weight
     FROM dbo.Dim_Product_Edges_Level;
"""

dtype_mapping = {
    "Parent": "object",
    "Child": "object",
    "ElementType": "object",
    "Weight": "float64",
    "Hierarchy": "object",
}

dtype_mapping_m_o = {
    "Parent": "object",
    "Child": "object",
}

dtype_mapping_format_2 = {
    "Hierarchy": "object",
    "Level1": "object",
    "Level2": "object",
    "Level3": "object",
    "Level4": "object",
    "Weight": "float64",
}

col_names = ["Parent", "Child", "ElementType", "Hierarchy", "Weight"]
col_names_m_o = ["Parent", "Child"]
level_columns = ["Hierarchy", "Level1", "Level2", "Level3", "Level4", "Weight"]

EXPECTED_DF_FORMAT_2 = {
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

EXPECTED_DF_FORMAT_2_ALT = {
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
    "Hierarchy": [
        "Default", "Default", "Default", "Default", "Default", "Default", "Default", "Default",
        "Alt", "Alt", "Alt", "Alt"
    ],
}
