from pathlib import Path

import pandas as pd
import pytest
from sqlalchemy import create_engine

from TM1_bedrock_py.dimension_builder.io import read_source_to_df
from tests.tests_dimension_builder.test_data.test_data import *

DATA_DIR = Path(__file__).resolve().parent / "test_data"


test_data_csv = [
    ("parent_child.csv", EXPECTED_DF_PARENT_CHILD, "parent_child"),
    ("source_m_o.csv", EXPECTED_DF_PC_MO, "parent_child"),
    ("level_columns.csv", EXPECTED_DF_LEVEL_COLUMNS, "indented_levels"),
    ("level_columns_filled_levels.csv", EXPECTED_DF_LEVEL_COLUMNS_FILLED, "indented_levels")
]
@pytest.mark.parametrize("file_path, expected_df, source_format", test_data_csv)
def test_read_csv_source_to_df(file_path, expected_df, source_format):
    source = DATA_DIR / file_path
    pd.DataFrame(expected_df).to_csv(source, index=False)
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    dataframe = read_source_to_df(
        source=str(source), source_type="csv", source_format=source_format, sep=",", decimal="."
    ).sort_index(axis=1)

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_csv_attr_list = [
    ("source_attribute_list.csv", EXPECTED_DF_PARENT_CHILD_ATTR, "parent_child"),
    ("level_columns_attribute_list.csv", EXPECTED_DF_LEVEL_COLUMNS_ATTR, "indented_levels"),
    ("attributes_only.csv", EXPECTED_DF_ATTR_ONLY, "attributes")
]
@pytest.mark.parametrize("file_path, expected_df, source_format", test_data_csv_attr_list)
def test_read_csv_source_to_df_with_attribute_list(file_path, expected_df, source_format):
    source = DATA_DIR / file_path
    pd.DataFrame(expected_df).to_csv(source, index=False)
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    attributes = ["Element", "Color", "CountryCode", "IsActive"]
    dataframe = read_source_to_df(
        source=str(source), source_type="csv", attribute_names=attributes, source_format=source_format, sep=",", decimal="."
    ).sort_index(axis=1)

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_xlsx = [
    (EXPECTED_DF_PARENT_CHILD, "Hierarchy", "parent_child"),
    (EXPECTED_DF_PC_MO, "Hierarchy", "parent_child"),
    (EXPECTED_DF_LEVEL_COLUMNS, "Hierarchy", "indented_levels"),
    (EXPECTED_DF_LEVEL_COLUMNS_FILLED, "Hierarchy", "indented_levels")
]
@pytest.mark.parametrize("expected_df, sheet_name, source_format", test_data_xlsx)
def test_read_xlsx_source_to_df(tmp_path, expected_df, sheet_name, source_format):
    source = tmp_path / "input.xlsx"
    pd.DataFrame(expected_df).to_excel(source, sheet_name=sheet_name, index=False)
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    dataframe = read_source_to_df(
        source=str(source),
        source_type="xlsx",
        sheet_name=sheet_name,
        source_format=source_format
    ).sort_index(axis=1)
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_xlsx_attr_list = [
    (EXPECTED_DF_PARENT_CHILD_ATTR, "Hierarchy", "parent_child"),
    (EXPECTED_DF_LEVEL_COLUMNS_ATTR, "Hierarchy", "indented_levels"),
    (EXPECTED_DF_ATTR_ONLY, "Attributes", "attributes")
]
@pytest.mark.parametrize("expected_df, sheet_name, source_format", test_data_xlsx_attr_list)
def test_read_xlsx_source_to_df_with_attribute_list(tmp_path, expected_df, sheet_name, source_format):
    source = tmp_path / "input.xlsx"
    pd.DataFrame(expected_df).to_excel(source, sheet_name=sheet_name, index=False)
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    attributes = ["Element", "Color", "CountryCode", "IsActive"]
    dataframe = read_source_to_df(
        source=str(source),
        source_type="xlsx",
        sheet_name=sheet_name,
        attribute_names=attributes,
        source_format=source_format,
    ).sort_index(axis=1)
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_sql = [
    (EXPECTED_DF_PARENT_CHILD, sql_query_parent_child, dtype_mapping_parent_child, "parent_child"),
    (EXPECTED_DF_PC_MO, sql_query_pc_mo, dtype_mapping_pc_mo, "parent_child"),
    (EXPECTED_DF_LEVEL_COLUMNS, sql_query_level_columns, dtype_mapping_level_columns, "indented_levels"),
]
@pytest.mark.parametrize("expected_df, query, dtype, format_type", test_data_sql)
def test_read_sql_source_to_df(expected_df, query,  dtype, format_type):
    engine = create_engine('sqlite://', echo=False)
    if format_type == "parent_child":
        df = pd.DataFrame(EXPECTED_DF_PARENT_CHILD)
    else:
        df = pd.DataFrame(EXPECTED_DF_LEVEL_COLUMNS)
    try:
        df.to_sql(name='users', con=engine)

        expected_dataframe = pd.DataFrame(expected_df).astype(dtype).sort_index(axis=1)
        with engine.connect() as connection:
            dataframe = read_source_to_df(
                source_type="sql",
                sql_query=query,
                engine=connection,
                source_format=format_type
            )
            dataframe = dataframe.astype(dtype).sort_index(axis=1)
            pd.testing.assert_frame_equal(dataframe, expected_dataframe)
    finally:
        connection.close()


test_data_sql = [
    (EXPECTED_DF_PARENT_CHILD_ATTR, columns_parent_child, dtype_mapping_parent_child, "parent_child"),
    (EXPECTED_DF_ATTR_ONLY, columns_attr_only, dtype_mapping_attr_only, "attributes"),
    (EXPECTED_DF_LEVEL_COLUMNS_ATTR, columns_level_columns, dtype_mapping_level_columns, "indented_levels"),
]
@pytest.mark.parametrize("expected_df, columns, dtype, format_type", test_data_sql)
def test_read_sql_source_to_df_attribute_list(expected_df, columns,  dtype, format_type):
    engine = create_engine('sqlite://', echo=False)
    df = pd.DataFrame(expected_df)
    try:
        df.to_sql(name='users', con=engine)

        expected_dataframe = pd.DataFrame(expected_df).astype(dtype).sort_index(axis=1)
        with engine.connect() as connection:
            dataframe = read_source_to_df(
                source_type="sql",
                source_format=format_type,
                engine=connection,
                table_name="users",
                **columns
            )
            dataframe = dataframe.astype(dtype).sort_index(axis=1)
            pd.testing.assert_frame_equal(dataframe, expected_dataframe)
    finally:
        connection.close()


yaml_aliases = {"Forbearer": "Parent", "Offspring": "Child", "Type": "ElementType", "W": "Weight", "Hier": "Hierarchy"}
test_data_yaml = [
    (EXPECTED_DF_PARENT_CHILD, "parent_child", "test_read_yaml_source_to_df", None),
    (EXPECTED_DF_PC_MO, "parent_child", "test_read_yaml_source_to_df_mandatory_only", None),
    (EXPECTED_DF_PARENT_CHILD, "parent_child", "test_read_yaml_source_to_df_aliases", yaml_aliases),
    (EXPECTED_DF_PARENT_CHILD, "parent_child", "test_read_yaml_source_to_df_extra_cols", None),
    (EXPECTED_DF_LEVEL_COLUMNS, "indented_levels", "test_read_yaml_source_to_df_indented_levels", None),
]
@pytest.mark.parametrize("expected_df, source_format, template_key, yaml_aliases", test_data_yaml)
def test_read_yaml_source_to_df(expected_df, source_format, template_key, yaml_aliases):
    source = Path(__file__).resolve().parent / "test_unit.yaml"
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    expected_dataframe.fillna("")
    dataframe = read_source_to_df(
        source=source,
        source_type="yaml",
        template_key=template_key,
        column_aliases=yaml_aliases,
        source_format=source_format
    ).sort_index(axis=1)
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


test_data_yaml = [
    (EXPECTED_DF_PARENT_CHILD_ATTR, "parent_child", "test_read_yaml_source_to_df_attr_list"),
    (EXPECTED_DF_LEVEL_COLUMNS_ATTR, "indented_levels", "test_read_yaml_source_to_df_indented_levels_attr_list"),
    (EXPECTED_DF_ATTR_ONLY, "attributes", "test_read_yaml_source_to_df_attr_only")
]
@pytest.mark.parametrize("expected_df, source_format, template_key", test_data_yaml)
def test_read_yaml_source_to_df_attribute_list(expected_df, source_format, template_key):
    source = Path(__file__).resolve().parent / "test_unit.yaml"
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    expected_dataframe.fillna("")

    attributes = ["Element", "Color", "CountryCode", "IsActive"]
    dataframe = read_source_to_df(
        source=source,
        source_type="yaml",
        template_key=template_key,
        attribute_names=attributes,
        source_format=source_format
    ).sort_index(axis=1)
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


# test normalize functions
