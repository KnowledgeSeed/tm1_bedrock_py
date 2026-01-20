from pathlib import Path

import pandas as pd
import parametrize_from_file
import pytest
from sqlalchemy import create_engine

from TM1_bedrock_py.dimension_builder import validate, normalize
from TM1_bedrock_py.dimension_builder.io import read_source_to_df
from TM1_bedrock_py.dimension_builder.exceptions import (
    SchemaValidationError,
    GraphValidationError,
    LevelColumnInvalidRowError
)
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

    output_df = normalize.normalize_all_column_names(
        input_df=input_df,
        dim_column=dim_column,
        hier_column=hier_column,
        parent_column=parent_column,
        child_column=child_column,
        element_column=element_column,
        type_column=type_column,
        weight_column=weight_column
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_assign_missing_edge_columns(input_df, dimension_name, hierarchy_name, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.assign_missing_edge_columns(
        input_df=input_df,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_assign_parent_child_to_level_columns(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.assign_parent_child_to_level_columns(input_df=input_df)

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_fill_column_empty_values_with_defaults(input_df, column_name, default_value, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    normalize.fill_column_empty_values_with_defaults(
        input_df=input_df,
        column_name=column_name,
        default_value=default_value
    )

    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_assign_missing_edge_values(input_df, dimension_name, hierarchy_name, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    normalize.assign_missing_edge_values(
        input_df=input_df,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name
    )

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
def test_separate_attr_df_columns(input_df, input_level_columns, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.separate_attr_df_columns(
        input_df=input_df,
        level_columns=input_level_columns
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_create_stack(input_df, expected_stack):
    input_df = pd.DataFrame(input_df)

    output_stack = normalize.create_stack(input_df=input_df)

    assert output_stack == expected_stack


@parametrize_from_file
def test_update_stack(stack, hierarchy, element_level, element_name, expected_stack):
    output_stack = normalize.update_stack(
        stack=stack,
        hierarchy=hierarchy,
        element_level=element_level,
        element_name=element_name
    )
    assert output_stack == expected_stack


@parametrize_from_file
def test_parse_indented_level_columns(input_row, row_index, level_columns, expected_name, expected_level):
    df_row = pd.Series(input_row)

    element_name, element_level = normalize.parse_indented_level_columns(
        df_row=df_row,
        row_index=row_index,
        level_columns=level_columns
    )

    assert element_name == expected_name
    assert element_level == expected_level


@parametrize_from_file
def test_parse_indented_level_columns_failure(input_row, row_index, level_columns, expected_exception,
                                              expected_message):
    df_row = pd.Series(input_row)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        normalize.parse_indented_level_columns(
            df_row=df_row,
            row_index=row_index,
            level_columns=level_columns
        )

    assert expected_message in str(excinfo.value)


@parametrize_from_file
def test_parse_filled_level_columns(input_row, row_index, level_columns, expected_name, expected_level):
    df_row = pd.Series(input_row)
    element_name, element_level = normalize.parse_filled_level_columns(
        df_row=df_row,
        row_index=row_index,
        level_columns=level_columns
    )
    assert element_name == expected_name
    assert element_level == expected_level


@parametrize_from_file
def test_parse_filled_level_columns_failure(
        input_row,
        row_index,
        level_columns,
        expected_exception,
        expected_message
):
    df_row = pd.Series(input_row)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        normalize.parse_filled_level_columns(
            df_row=df_row,
            row_index=row_index,
            level_columns=level_columns
        )

    assert expected_message in str(excinfo.value)


@parametrize_from_file
def test_parse_indented_levels_into_parent_child(input_df, level_columns, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.parse_indented_levels_into_parent_child(
        input_df=input_df,
        level_columns=level_columns
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_parse_indented_levels_into_parent_child_failure(
        input_df,
        level_columns,
        expected_exception,
        expected_message
):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        normalize.parse_indented_levels_into_parent_child(
            input_df=input_df,
            level_columns=level_columns
        )

    assert expected_message in str(excinfo.value)


@parametrize_from_file
def test_parse_filled_levels_into_parent_child(input_df, level_columns, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.parse_filled_levels_into_parent_child(
        input_df=input_df,
        level_columns=level_columns
    )

    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_parse_filled_levels_into_parent_child_failure(
        input_df,
        level_columns,
        expected_exception,
        expected_message
):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        normalize.parse_indented_levels_into_parent_child(
            input_df=input_df,
            level_columns=level_columns
        )

    assert expected_message in str(excinfo.value)


@parametrize_from_file
def test_drop_invalid_edges_df_rows(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.drop_invalid_edges_df_rows(edges_df=input_df)

    pd.testing.assert_frame_equal(
        output_df.reset_index(drop=True),
        expected_df.reset_index(drop=True),
        check_dtype=False
    )


@parametrize_from_file
def test_drop_invalid_attr_df_rows(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.drop_invalid_attr_df_rows(attr_df=input_df)

    pd.testing.assert_frame_equal(
        output_df,
        expected_df,
        check_dtype=False
    )


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder validate module
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_validate_attr_df_schema_for_inconsistent_element_type_success(df_data):
    """
    Tests cases where the schema is consistent and no exception should be raised.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_attr_df_schema_for_inconsistent_element_type(input_df)


@parametrize_from_file
def test_validate_attr_df_schema_for_inconsistent_element_type_failure(df_data, expected_exception,
                                                                       expected_message_part):
    """
    Tests cases where inconsistent element types are detected.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_attr_df_schema_for_inconsistent_element_type(input_df)

    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_attr_df_schema_for_inconsistent_leaf_attributes_success(df_data):
    """
    Tests that N/S elements can have different Hierarchy/Dimension values
    without triggering an exception.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_attr_df_schema_for_inconsistent_leaf_attributes(input_df)


@parametrize_from_file
def test_validate_attr_df_schema_for_inconsistent_leaf_attributes_failure(df_data, expected_exception,
                                                                          expected_message_part):
    """
    Tests that conflicting attributes for N/S elements raise SchemaValidationError.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_attr_df_schema_for_inconsistent_leaf_attributes(input_df)

    # We check for the main error description and the specific bad elements
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_graph_for_leaves_as_parents_success(edges_data, attr_data):
    """
    Tests cases where no N or S elements act as parents in the hierarchy.
    """
    edges_df = pd.DataFrame(edges_data)
    attr_df = pd.DataFrame(attr_data)
    validate.validate_graph_for_leaves_as_parents(edges_df, attr_df)


@parametrize_from_file
def test_validate_graph_for_leaves_as_parents_failure(edges_data, attr_data, expected_exception, expected_message_part):
    """
    Tests that a GraphValidationError is raised if an N or S element is a parent.
    """
    edges_df = pd.DataFrame(edges_data)
    attr_df = pd.DataFrame(attr_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_graph_for_leaves_as_parents(edges_df, attr_df)

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
def test_validate_graph_for_cycles_with_dfs_success(df_data):
    """
    Tests acyclic graphs (DAGs), including complex shapes like diamonds.
    """
    input_df = pd.DataFrame(df_data)
    validate.validate_graph_for_cycles_with_dfs(input_df)


@parametrize_from_file
def test_validate_graph_for_cycles_with_dfs_failure(df_data, expected_exception, expected_message_part):
    """
    Tests that cycles (direct and indirect) raise a GraphValidationError.
    """
    input_df = pd.DataFrame(df_data)
    exception_type = eval(expected_exception)

    with pytest.raises(exception_type) as excinfo:
        validate.validate_graph_for_cycles_with_dfs(input_df)

    assert expected_message_part in str(excinfo.value)
