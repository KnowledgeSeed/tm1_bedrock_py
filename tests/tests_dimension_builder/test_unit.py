import sqlite3

import pandas as pd
import pytest

from TM1_bedrock_py.dimension_builder.io import read_source_to_df, ColumnSpec
from tests.tests_dimension_builder.test_data import *

test_data_csv_format_1 = [
    (
        EXPECTED_DF,
        ColumnSpec(aliases={name: name for name in col_names}),
    ),
    (
        EXPECTED_DF_MANDATORY_ONLY,
        ColumnSpec(aliases={name: name for name in col_names_m_o}),
    ),
    (
        EXPECTED_DF_FORMAT_2,
        ColumnSpec(aliases={name: name for name in level_columns}),
    ),
]


@pytest.mark.parametrize("expected_df, column_spec", test_data_csv_format_1)
def test_read_csv_parent_child_to_df(tmp_path, expected_df, column_spec):
    source = tmp_path / "input.csv"
    pd.DataFrame(expected_df).to_csv(source, index=False)
    expected_dataframe = pd.DataFrame(expected_df)
    dataframe = read_source_to_df(source=str(source), source_type="csv", column_spec=column_spec, sep=",", decimal=".")
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_xlsx_format_1 = [
    (EXPECTED_DF, "Hierarchy"),
    (EXPECTED_DF_MANDATORY_ONLY, "Hierarchy"),
    (EXPECTED_DF_FORMAT_2, "Hierarchy"),
]


@pytest.mark.parametrize("expected_df, sheet_name", test_data_xlsx_format_1)
def test_read_xlsx_parent_child_to_df(tmp_path, expected_df, sheet_name):
    source = tmp_path / "input.xlsx"
    pd.DataFrame(expected_df).to_excel(source, sheet_name=sheet_name, index=False)
    expected_dataframe = pd.DataFrame(expected_df)
    column_spec = ColumnSpec(aliases={name: name for name in expected_dataframe.columns})
    dataframe = read_source_to_df(
        source=str(source),
        source_type="xlsx",
        sheet_name=sheet_name,
        column_spec=column_spec,
    )
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_sql_format_1 = [
    (EXPECTED_DF, sql_query, dtype_mapping, "format_one"),
    (EXPECTED_DF_MANDATORY_ONLY, sql_query_m_o, dtype_mapping_m_o, "format_one"),
    (EXPECTED_DF_FORMAT_2, sql_query_format_2, dtype_mapping_format_2, "format_two"),
]


@pytest.mark.parametrize("expected_df, query, dtype, format_type", test_data_sql_format_1)
def test_read_sql_parent_child_to_df(expected_df, query, dtype, format_type):
    connection = sqlite3.connect(":memory:")
    try:
        cursor = connection.cursor()
        cursor.execute("ATTACH DATABASE ':memory:' AS dbo;")
        if format_type == "format_one":
            cursor.execute(
                """
                CREATE TABLE dbo.Dim_Product_Edges (
                    Parent TEXT,
                    Child TEXT,
                    ElementType TEXT,
                    Weight REAL,
                    Hierarchy TEXT
                );
                """
            )
            rows = list(zip(
                EXPECTED_DF["Parent"],
                EXPECTED_DF["Child"],
                EXPECTED_DF["ElementType"],
                EXPECTED_DF["Weight"],
                EXPECTED_DF["Hierarchy"],
            ))
            cursor.executemany(
                "INSERT INTO dbo.Dim_Product_Edges (Parent, Child, ElementType, Weight, Hierarchy) "
                "VALUES (?, ?, ?, ?, ?);",
                rows,
            )
        else:
            cursor.execute(
                """
                CREATE TABLE dbo.Dim_Product_Edges_Level (
                    Hierarchy TEXT,
                    Level1 TEXT,
                    Level2 TEXT,
                    Level3 TEXT,
                    Level4 TEXT,
                    Weight REAL
                );
                """
            )
            rows = list(zip(
                expected_df["Hierarchy"],
                expected_df["Level1"],
                expected_df["Level2"],
                expected_df["Level3"],
                expected_df["Level4"],
                expected_df["Weight"],
            ))
            cursor.executemany(
                "INSERT INTO dbo.Dim_Product_Edges_Level (Hierarchy, Level1, Level2, Level3, Level4, Weight) "
                "VALUES (?, ?, ?, ?, ?, ?);",
                rows,
            )
        connection.commit()

        expected_dataframe = pd.DataFrame(expected_df).astype(dtype)
        dataframe = read_source_to_df(
            source="unused",
            source_type="sql",
            sql_query=query,
            engine=connection,
        )
        pd.testing.assert_frame_equal(dataframe, expected_dataframe)
    finally:
        connection.close()


yaml_aliases = {"Forbearer": "Parent", "Offspring": "Child", "Type": "ElementType", "W": "Weight", "Hier": "Hierarchy"}
test_data_yaml_format_1 = [
    (EXPECTED_DF, "test_read_yaml_parent_child_to_df", None),
    (EXPECTED_DF_MANDATORY_ONLY, "test_read_yaml_parent_child_to_df_mandatory_only", None),
    (EXPECTED_DF, "test_read_yaml_parent_child_to_df_aliases", ColumnSpec(aliases=yaml_aliases)),
    (EXPECTED_DF_FORMAT_2_ALT, "test_read_yaml_parent_child_to_df_format_two", None),
]


@pytest.mark.parametrize("expected_df, template_key, col_spec", test_data_yaml_format_1)
def test_read_yaml_parent_child_to_df(expected_df, template_key, col_spec):
    source = "tests/tests_dimension_builder/test_unit.yaml"
    expected_dataframe = pd.DataFrame(expected_df)
    expected_dataframe.fillna("")
    dataframe = read_source_to_df(source=source, source_type="yaml", template_key=template_key, column_spec=col_spec)
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


# test normalize functions

