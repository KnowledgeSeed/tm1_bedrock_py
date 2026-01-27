from pathlib import Path
import pandas as pd
import numpy as np
import parametrize_from_file
import pytest
from sqlalchemy import create_engine

from TM1_bedrock_py.dimension_builder import validate, normalize, utility
from TM1_bedrock_py.dimension_builder.io import read_source_to_df
from TM1_bedrock_py.dimension_builder.exceptions import (
    SchemaValidationError,
    GraphValidationError
)
from tests.tests_dimension_builder.test_data.test_data import *

DATA_DIR = Path(__file__).resolve().parent / "test_data"


def test_read_source_to_df_empty_parameters():
    input_datasource = None
    filter_input_columns = None
    sql_engine = None
    sql_query = None
    sql_table_name = None
    dataframe = read_source_to_df(source=input_datasource, column_names=filter_input_columns,
        engine=sql_engine, sql_query=sql_query, table_name=sql_table_name)
    assert dataframe is None


test_data_csv = [
    ("parent_child.csv", EXPECTED_DF_PARENT_CHILD, None),
    ("parent_child.csv", EXPECTED_DF_PC_MO, ["Parent", "Child"]),
    ("level_columns.csv", EXPECTED_DF_LEVEL_COLUMNS, None),
    ("level_columns_filled_levels.csv", EXPECTED_DF_LEVEL_COLUMNS_FILLED, None)
]
@pytest.mark.parametrize("file_path, expected_df, columns", test_data_csv)
def test_read_csv_source_to_df(file_path, expected_df, columns):
    source = DATA_DIR / file_path
    expected_dataframe = pd.DataFrame(expected_df).fillna("").sort_index(axis=1)
    dataframe = read_source_to_df(
        source=str(source), sep=",", decimal=".", column_names=columns
    ).sort_index(axis=1)

    pd.testing.assert_frame_equal(dataframe, expected_dataframe)



test_data_xlsx = [
    (EXPECTED_DF_PARENT_CHILD, "Hierarchy", None),
    (EXPECTED_DF_LEVEL_COLUMNS, "Hierarchy", None),
    (EXPECTED_DF_LEVEL_COLUMNS_FILLED, "Hierarchy", None)
]
@pytest.mark.parametrize("expected_df, sheet_name, columns", test_data_xlsx)
def test_read_xlsx_source_to_df(tmp_path, expected_df, sheet_name, columns):
    source = tmp_path / "input.xlsx"
    pd.DataFrame(expected_df).to_excel(source, sheet_name=sheet_name, index=False)
    expected_dataframe = pd.DataFrame(expected_df).fillna("").sort_index(axis=1)
    dataframe = read_source_to_df(
        source=str(source),
        sheet_name=sheet_name,
        column_names=columns
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
                sql_query=query,
                engine=connection,
            )
            dataframe = dataframe.astype(dtype).sort_index(axis=1)
            pd.testing.assert_frame_equal(dataframe, expected_dataframe)
    finally:
        connection.close()


test_data_yaml = [
    (EXPECTED_DF_PARENT_CHILD, "test_read_yaml_source_to_df", None),
    (EXPECTED_DF_PARENT_CHILD_ATTR, "test_read_yaml_source_to_df_attr_list", None),
    (EXPECTED_DF_LEVEL_COLUMNS, "test_read_yaml_source_to_df_indented_levels", None)
]
@pytest.mark.parametrize("expected_df, template_key, columns", test_data_yaml)
def test_read_yaml_source_to_df(expected_df, template_key, columns):
    source = Path(__file__).resolve().parent / "test_unit.yaml"
    expected_dataframe = pd.DataFrame(expected_df).sort_index(axis=1)
    expected_dataframe.fillna("")
    dataframe = read_source_to_df(
        source=source,
        template_key=template_key,
        column_names=columns
    ).sort_index(axis=1)
    pd.testing.assert_frame_equal(dataframe, expected_dataframe)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder normalize module
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_normalize_all_column_names(
    input_df,
    dim_column,
    hier_column,
    parent_column,
    child_column,
    element_column,
    type_column,
    weight_column,
    expected_df
):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.normalize_base_column_names(input_df=input_df, dim_column=dim_column, hier_column=hier_column,
                                                      parent_column=parent_column, child_column=child_column,
                                                      element_column=element_column, type_column=type_column,
                                                      weight_column=weight_column)

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_assign_missing_edge_columns(input_df, dimension_name, hierarchy_name, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.assign_missing_base_columns(input_df=input_df, dimension_name=dimension_name,
                                                      hierarchy_name=hierarchy_name)

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_assign_missing_edge_values(input_df, dimension_name, hierarchy_name, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    normalize.assign_missing_base_values(input_df=input_df, dimension_name=dimension_name,
                                         hierarchy_name=hierarchy_name)

    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_assign_missing_type_column(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    normalize.assign_missing_type_column(input_df=input_df)

    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_assign_missing_type_values(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    normalize.assign_missing_type_values(input_df=input_df)

    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_separate_edge_df_columns(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.separate_edge_df_columns(input_df=input_df)

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_separate_elements_df_columns(input_df, attribute_columns, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.separate_elements_df_columns(
        input_df=input_df,
        attribute_columns=attribute_columns
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_convert_levels_to_edges(input_df, level_columns, expected_df):
    input_df = pd.DataFrame(input_df)
    print("")
    print("input_df")
    print(input_df)
    expected_df = pd.DataFrame(expected_df)
    expected_df.fillna(value=np.nan, inplace=True)

    output_df = normalize.convert_levels_to_edges(input_df=input_df, level_columns=level_columns)
    print("")
    print("output_df")
    print(output_df)

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_drop_invalid_edges(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.drop_invalid_edges(edges_df=input_df)
    output_df = normalize.deduplicate_edges(edges_df=output_df)

    pd.testing.assert_frame_equal(
        output_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False
    )


@parametrize_from_file
def test_deduplicate_elements(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.deduplicate_elements(elements_df=input_df)

    pd.testing.assert_frame_equal(
        output_df,
        expected_df,
        check_dtype=False
    )


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder validate module
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_validate_elements_df_schema_for_inconsistent_element_type_success(df_data):
    """
    Tests cases where the schema is consistent and no exception should be raised.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_elements_df_schema_for_inconsistent_element_type(input_df)


@parametrize_from_file
def test_validate_elements_df_schema_for_inconsistent_element_type_failure(
        df_data, expected_exception, expected_message_part
):
    """
    Tests cases where inconsistent element types are detected.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_elements_df_schema_for_inconsistent_element_type(input_df)

    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_elements_df_schema_for_inconsistent_leaf_attributes_success(df_data):
    """
    Tests that N/S elements can have different Hierarchy/Dimension values
    without triggering an exception.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_elements_df_schema_for_inconsistent_leaf_attributes(input_df)


@parametrize_from_file
def test_validate_elements_df_schema_for_inconsistent_leaf_attributes_failure(
        df_data, expected_exception, expected_message_part
):
    """
    Tests that conflicting attributes for N/S elements raise SchemaValidationError.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_elements_df_schema_for_inconsistent_leaf_attributes(input_df)

    # We check for the main error description and the specific bad elements
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_graph_for_leaves_as_parents_success(edges_data, attr_data):
    """
    Tests cases where no N or S elements act as parents in the hierarchy.
    """
    edges_df = pd.DataFrame(edges_data)
    elements_df = pd.DataFrame(attr_data)
    validate.validate_graph_for_leaves_as_parents(edges_df, elements_df)


@parametrize_from_file
def test_validate_graph_for_leaves_as_parents_failure(edges_data, attr_data, expected_exception, expected_message_part):
    """
    Tests that a GraphValidationError is raised if an N or S element is a parent.
    """
    edges_df = pd.DataFrame(edges_data)
    elements_df = pd.DataFrame(attr_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_graph_for_leaves_as_parents(edges_df, elements_df)

    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_graph_for_self_loop_success(df_data):
    """
    Tests that no exception is raised when all Parent-Child pairs are different.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_graph_for_self_loop(input_df)


@parametrize_from_file
def test_validate_graph_for_self_loop_failure(df_data, expected_exception, expected_message_part):
    """
    Tests that GraphValidationError is raised when a Parent is equal to its Child.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_graph_for_self_loop(input_df)

    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_graph_for_cycles_with_kahn_success(df_data):
    """
    Tests acyclic graphs (DAGs), including complex shapes like diamonds.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_graph_for_cycles_with_kahn(input_df)


@parametrize_from_file
def test_validate_graph_for_cycles_with_kahn_failure(df_data, expected_exception, expected_message_part):
    """
    Tests that cycles (direct and indirect) raise a GraphValidationError.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_graph_for_cycles_with_kahn(input_df)
    print(excinfo.value)
    assert expected_message_part in str(excinfo.value)
