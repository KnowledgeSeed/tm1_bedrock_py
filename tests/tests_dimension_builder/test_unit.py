from pathlib import Path

import numpy as np
import parametrize_from_file
import pytest
from sqlalchemy import create_engine

from TM1_bedrock_py.dimension_builder import validate, normalize, utility, apply
from TM1_bedrock_py.dimension_builder.io import read_source_to_df
from TM1_bedrock_py.dimension_builder.exceptions import (
    SchemaValidationError,
    InvalidLevelColumnRecordError,
    InvalidInputParameterError,
    ElementTypeConflictError,
    InvalidAttributeColumnNameError
)
from tests.tests_dimension_builder.test_data.test_data import *

DATA_DIR = Path(__file__).resolve().parent / "test_data"


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder utility module
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_parse_attribute_string_success(attr_name_and_type, parser, expected_name, expected_type):
    name, attr_type = utility.parse_attribute_string(attr_name_and_type, parser)
    assert name == expected_name
    assert attr_type == expected_type


@parametrize_from_file
def test_parse_attribute_string_failure(attr_name_and_type, parser, expected_exception, expected_message_part):
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.parse_attribute_string(attr_name_and_type, parser)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_get_legacy_edges_success(existing_df, input_df, expected_df):
    existing_df = pd.DataFrame(existing_df)
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = utility.get_legacy_edges(existing_df, input_df)
    pd.testing.assert_frame_equal(
        output_df.sort_index(axis=1).reset_index(drop=True),
        expected_df.sort_index(axis=1).reset_index(drop=True)
    )


@parametrize_from_file
def test_get_legacy_elements_success(existing_df, input_df, expected_df):
    existing_df = pd.DataFrame(existing_df)
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = utility.get_legacy_elements(existing_df, input_df)
    pd.testing.assert_frame_equal(
        output_df.sort_index(axis=1).reset_index(drop=True),
        expected_df.sort_index(axis=1).reset_index(drop=True)
    )


@parametrize_from_file
def test_unpivot_attributes_to_cube_format_success(elements_df, dimension_name, expected_df):
    elements_df = pd.DataFrame(elements_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = utility.unpivot_attributes_to_cube_format(elements_df, dimension_name)
    pd.testing.assert_frame_equal(
        output_df.sort_values(by=expected_df.columns.tolist()).reset_index(drop=True),
        expected_df.sort_values(by=expected_df.columns.tolist()).reset_index(drop=True)
    )


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder io module
# ------------------------------------------------------------------------------------------------------------

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
    expected_df = pd.DataFrame(expected_df)
    expected_df.fillna(value=np.nan, inplace=True)

    output_df = normalize.convert_levels_to_edges(input_df=input_df, level_columns=level_columns)
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



@parametrize_from_file
def test_add_attribute_type_suffixes_success(input_df, attr_type_map, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = normalize.add_attribute_type_suffixes(input_df=input_df, attr_type_map=attr_type_map)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_add_attribute_type_suffixes_failure(input_df, attr_type_map, expected_exception, expected_message_part):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        normalize.add_attribute_type_suffixes(input_df=input_df, attr_type_map=attr_type_map)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_normalize_attr_column_names_success(
        input_df, attribute_columns, attribute_parser, expected_df, expected_columns
):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df, attr_cols = normalize.normalize_attr_column_names(
        input_df=input_df,
        attribute_columns=attribute_columns,
        attribute_parser=attribute_parser
    )
    pd.testing.assert_frame_equal(output_df, expected_df)
    assert attr_cols == expected_columns


@parametrize_from_file
def test_normalize_attr_column_names_failure(
        input_df, attribute_columns, attribute_parser, expected_exception, expected_message_part
):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        normalize.normalize_attr_column_names(
            input_df=input_df,
            attribute_columns=attribute_columns,
            attribute_parser=attribute_parser
        )
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_assign_missing_weight_column_success(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = normalize.assign_missing_weight_column(input_df=input_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_assign_missing_weight_values_success(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = normalize.assign_missing_weight_values(input_df=input_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_validate_and_normalize_numeric_values_success(input_df, column_name, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    normalize.validate_and_normalize_numeric_values(input_df=input_df, column_name=column_name)
    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_validate_and_normalize_numeric_values_failure(
        input_df, column_name, expected_exception, expected_message_part
):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        normalize.validate_and_normalize_numeric_values(input_df=input_df, column_name=column_name)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_normalize_string_values_success(input_df, column_name, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    normalize.normalize_string_values(input_df=input_df, column_name=column_name)
    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_validate_and_normalize_base_column_types_success(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    normalize.validate_and_normalize_base_column_types(input_df=input_df)
    pd.testing.assert_frame_equal(input_df, expected_df)


@parametrize_from_file
def test_validate_and_normalize_base_column_types_failure(input_df, expected_exception, expected_message_part):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        normalize.validate_and_normalize_base_column_types(input_df=input_df)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_and_normalize_attr_column_types_success(elements_df, attr_columns, expected_df):
    elements_df = pd.DataFrame(elements_df)
    expected_df = pd.DataFrame(expected_df)
    normalize.validate_and_normalize_attr_column_types(elements_df=elements_df, attr_columns=attr_columns)
    pd.testing.assert_frame_equal(elements_df, expected_df)


@parametrize_from_file
def test_validate_and_normalize_attr_column_types_failure(
        elements_df, attr_columns, expected_exception, expected_message_part
):
    elements_df = pd.DataFrame(elements_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        normalize.validate_and_normalize_attr_column_types(elements_df=elements_df, attr_columns=attr_columns)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_and_normalize_type_values_success(input_df, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = normalize.validate_and_normalize_type_values(input_df=input_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_validate_and_normalize_type_values_failure(input_df, expected_exception, expected_message_part):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        normalize.validate_and_normalize_type_values(input_df=input_df)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_assign_missing_attribute_values_success(elements_df, attribute_columns, expected_df):
    elements_df = pd.DataFrame(elements_df)
    expected_df = pd.DataFrame(expected_df)
    normalize.assign_missing_attribute_values(elements_df=elements_df, attribute_columns=attribute_columns)
    pd.testing.assert_frame_equal(elements_df, expected_df)


@parametrize_from_file
def test_normalize_input_schema_success(
        input_df, dimension_name, hierarchy_name, dim_column, hier_column, level_columns,
        parent_column, child_column, type_column, weight_column, attr_type_map,
        input_elements_df, input_elements_df_element_column, attribute_parser,
        expected_edges_df, expected_elements_df
):
    input_df = pd.DataFrame(input_df)
    expected_edges_df = pd.DataFrame(expected_edges_df)
    expected_elements_df = pd.DataFrame(expected_elements_df)
    if input_elements_df is not None:
        input_elements_df = pd.DataFrame(input_elements_df)

    edges_df, elements_df = normalize.normalize_input_schema(
        input_df=input_df,
        dimension_name=dimension_name,
        hierarchy_name=hierarchy_name,
        dim_column=dim_column,
        hier_column=hier_column,
        level_columns=level_columns,
        parent_column=parent_column,
        child_column=child_column,
        type_column=type_column,
        weight_column=weight_column,
        attr_type_map=attr_type_map,
        input_elements_df=input_elements_df,
        input_elements_df_element_column=input_elements_df_element_column,
        attribute_parser=attribute_parser
    )
    pd.testing.assert_frame_equal(edges_df, expected_edges_df)
    pd.testing.assert_frame_equal(elements_df, expected_elements_df)


@parametrize_from_file
def test_clear_orphan_parent_edges_success(edges_df, orphan_consolidation_name, expected_df):
    edges_df = pd.DataFrame(edges_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = normalize.clear_orphan_parent_edges(edges_df, orphan_consolidation_name)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_clear_orphan_parent_elements_success(elements_df, orphan_consolidation_name, expected_df):
    elements_df = pd.DataFrame(elements_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = normalize.clear_orphan_parent_elements(elements_df, orphan_consolidation_name)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_normalize_existing_schema_success(
        existing_edges_df, existing_elements_df, old_orphan_parent_name, expected_edges_df, expected_elements_df
):
    if existing_edges_df is not None:
        existing_edges_df = pd.DataFrame(existing_edges_df)
    existing_elements_df = pd.DataFrame(existing_elements_df)
    expected_edges_df = None if expected_edges_df is None else pd.DataFrame(expected_edges_df)
    expected_elements_df = pd.DataFrame(expected_elements_df)

    output_edges_df, output_elements_df = normalize.normalize_existing_schema(
        existing_edges_df, existing_elements_df, old_orphan_parent_name
    )
    if expected_edges_df is None:
        assert output_edges_df is None
    else:
        pd.testing.assert_frame_equal(output_edges_df, expected_edges_df)
    pd.testing.assert_frame_equal(output_elements_df, expected_elements_df)


@parametrize_from_file
def test_normalize_updated_schema_success(updated_edges_df, updated_elements_df, expected_edges_df, expected_elements_df):
    updated_edges_df = pd.DataFrame(updated_edges_df)
    updated_elements_df = pd.DataFrame(updated_elements_df)
    expected_edges_df = pd.DataFrame(expected_edges_df)
    expected_elements_df = pd.DataFrame(expected_elements_df)
    output_edges_df, output_elements_df = normalize.normalize_updated_schema(
        updated_edges_df, updated_elements_df
    )
    pd.testing.assert_frame_equal(output_edges_df, expected_edges_df)
    pd.testing.assert_frame_equal(output_elements_df, expected_elements_df)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder validate module
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_validate_filled_structure_success(input_df, level_columns):
    input_df = pd.DataFrame(input_df)
    validate.validate_filled_structure(input_df, level_columns)


@parametrize_from_file
def test_validate_filled_structure_failure(input_df, level_columns, expected_message_part):
    input_df = pd.DataFrame(input_df)
    with pytest.raises(InvalidLevelColumnRecordError) as excinfo:
        validate.validate_filled_structure(input_df, level_columns)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_indented_structure_success(input_df, level_columns):
    input_df = pd.DataFrame(input_df)
    validate.validate_indented_structure(input_df, level_columns)


@parametrize_from_file
def test_validate_indented_structure_failure(input_df, level_columns, expected_message_part):
    input_df = pd.DataFrame(input_df)
    with pytest.raises(InvalidLevelColumnRecordError) as excinfo:
        validate.validate_indented_structure(input_df, level_columns)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_schema_for_parent_child_columns_success(input_df):
    input_df = pd.DataFrame(input_df)
    validate.validate_schema_for_parent_child_columns(input_df)


@parametrize_from_file
def test_validate_schema_for_parent_child_columns_failure(input_df, expected_message_part):
    input_df = pd.DataFrame(input_df)
    with pytest.raises(SchemaValidationError) as excinfo:
        validate.validate_schema_for_parent_child_columns(input_df)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_schema_for_level_columns_success(input_df, level_columns):
    input_df = pd.DataFrame(input_df)
    validate.validate_schema_for_level_columns(input_df, level_columns)


@parametrize_from_file
def test_validate_schema_for_level_columns_failure(input_df, level_columns, expected_exception, expected_message_part):
    input_df = pd.DataFrame(input_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        validate.validate_schema_for_level_columns(input_df, level_columns)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_schema_for_type_mapping_success(input_df, type_mapping):
    input_df = pd.DataFrame(input_df)
    validate.validate_schema_for_type_mapping(input_df, type_mapping)


@parametrize_from_file
def test_validate_schema_for_type_mapping_failure(input_df, type_mapping, expected_message_part):
    input_df = pd.DataFrame(input_df)
    with pytest.raises(SchemaValidationError) as excinfo:
        validate.validate_schema_for_type_mapping(input_df, type_mapping)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_schema_for_numeric_values_success(input_df, column_name):
    input_df = pd.DataFrame(input_df)
    converted_series = pd.to_numeric(input_df[column_name], errors='coerce')
    validate.validate_schema_for_numeric_values(input_df, converted_series, column_name)


@parametrize_from_file
def test_validate_schema_for_numeric_values_failure(input_df, column_name, expected_message_part):
    input_df = pd.DataFrame(input_df)
    converted_series = pd.to_numeric(input_df[column_name], errors='coerce')
    with pytest.raises(SchemaValidationError) as excinfo:
        validate.validate_schema_for_numeric_values(input_df, converted_series, column_name)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_schema_for_node_integrity_success(edges_data, elements_data):
    edges_df = pd.DataFrame(edges_data)
    elements_df = pd.DataFrame(elements_data)
    validate.validate_schema_for_node_integrity(edges_df, elements_df)


@parametrize_from_file
def test_validate_schema_for_node_integrity_failure(edges_data, elements_data, expected_message_part):
    edges_df = pd.DataFrame(edges_data)
    elements_df = pd.DataFrame(elements_data)
    with pytest.raises(SchemaValidationError) as excinfo:
        validate.validate_schema_for_node_integrity(edges_df, elements_df)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_post_validate_schema_success(edges_data, elements_data):
    edges_df = pd.DataFrame(edges_data)
    elements_df = pd.DataFrame(elements_data)
    validate.post_validate_schema(edges_df, elements_df)


@parametrize_from_file
def test_pre_validate_input_schema_success(input_format, input_df, level_columns):
    input_df = pd.DataFrame(input_df)
    validate.pre_validate_input_schema(
        input_format=input_format,
        input_df=input_df,
        level_columns=level_columns
    )


@parametrize_from_file
def test_validate_element_type_consistency_success(existing_elements_df, input_elements_df, allow_type_changes):
    existing_elements_df = pd.DataFrame(existing_elements_df)
    input_elements_df = pd.DataFrame(input_elements_df)
    result = validate.validate_element_type_consistency(
        existing_elements_df, input_elements_df, allow_type_changes
    )
    assert result is None


@parametrize_from_file
def test_validate_element_type_consistency_failure(
        existing_elements_df, input_elements_df, allow_type_changes, expected_exception, expected_message_part
):
    existing_elements_df = pd.DataFrame(existing_elements_df)
    input_elements_df = pd.DataFrame(input_elements_df)
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        validate.validate_element_type_consistency(
            existing_elements_df, input_elements_df, allow_type_changes
        )
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_validate_element_type_consistency_allow_changes(
        existing_elements_df, input_elements_df, allow_type_changes, expected_conflicts
):
    existing_elements_df = pd.DataFrame(existing_elements_df)
    input_elements_df = pd.DataFrame(input_elements_df)
    conflicts = validate.validate_element_type_consistency(
        existing_elements_df, input_elements_df, allow_type_changes
    )
    expected_df = pd.DataFrame(expected_conflicts)
    pd.testing.assert_frame_equal(
        conflicts.reset_index(drop=True),
        expected_df.reset_index(drop=True)
    )


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
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_get_delete_records_for_conflicting_elements(conflicts_input, expected_tuples):
    """
    Tests acyclic graphs (DAGs), including complex shapes like diamonds.
    """
    conflicts = pd.DataFrame(conflicts_input)
    output_tuples = utility.get_delete_records_for_conflicting_elements(conflicts)
    expected_output = [tuple(x) for x in expected_tuples]
    assert output_tuples == expected_output


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dimension builder apply module
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_apply_update_on_edges_success(legacy_df, input_df, retained_hierarchies, orphan_consolidation_name, expected_df):
    legacy_df = None if legacy_df is None else pd.DataFrame(legacy_df)
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = apply.apply_update_on_edges(
        legacy_df=legacy_df,
        input_df=input_df,
        retained_hierarchies=retained_hierarchies,
        orphan_consolidation_name=orphan_consolidation_name
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_apply_update_with_unwind_on_edges_success(
        legacy_df, input_df, retained_hierarchies, orphan_consolidation_name, expected_df
):
    legacy_df = None if legacy_df is None else pd.DataFrame(legacy_df)
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = apply.apply_update_with_unwind_on_edges(
        legacy_df=legacy_df,
        input_df=input_df,
        retained_hierarchies=retained_hierarchies,
        orphan_consolidation_name=orphan_consolidation_name
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_add_orphan_consolidation_elements_success(
        elements_df, orphan_consolidation_name, dimension_name, retained_hierarchies, expected_df
):
    elements_df = pd.DataFrame(elements_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = apply.add_orphan_consolidation_elements(
        elements_df=elements_df,
        orphan_consolidation_name=orphan_consolidation_name,
        dimension_name=dimension_name,
        retained_hierarchies=retained_hierarchies
    )
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_assign_root_orphan_edges_success(
        legacy_edges_df, legacy_elements_df, edges_df, orphan_consolidation_name,
        dimension_name, retained_hierarchies, expected_df
):
    legacy_edges_df = None if legacy_edges_df is None else pd.DataFrame(legacy_edges_df)
    legacy_elements_df = pd.DataFrame(legacy_elements_df)
    edges_df = pd.DataFrame(edges_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = apply.assign_root_orphan_edges(
        legacy_edges_df=legacy_edges_df,
        legacy_elements_df=legacy_elements_df,
        edges_df=edges_df,
        orphan_consolidation_name=orphan_consolidation_name,
        dimension_name=dimension_name,
        retained_hierarchies=retained_hierarchies
    )
    output_df = output_df.sort_values(by=["Parent", "Child"]).reset_index(drop=True)
    expected_df = expected_df.sort_values(by=["Parent", "Child"]).reset_index(drop=True)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_apply_update_on_elements_success(legacy_df, input_df, expected_df):
    legacy_df = pd.DataFrame(legacy_df)
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = apply.apply_update_on_elements(legacy_df=legacy_df, input_df=input_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_apply_updates_success(
        mode, existing_edges_df, input_edges_df, existing_elements_df,
        input_elements_df, dimension_name, orphan_consolidation_name,
        expected_edges_df, expected_elements_df
):
    existing_edges_df = None if existing_edges_df is None else pd.DataFrame(existing_edges_df)
    input_edges_df = pd.DataFrame(input_edges_df)
    existing_elements_df = pd.DataFrame(existing_elements_df)
    input_elements_df = pd.DataFrame(input_elements_df)
    expected_edges_df = pd.DataFrame(expected_edges_df)
    expected_elements_df = pd.DataFrame(expected_elements_df)
    output_edges_df, output_elements_df = apply.apply_updates(
        mode=mode,
        existing_edges_df=existing_edges_df,
        input_edges_df=input_edges_df,
        existing_elements_df=existing_elements_df,
        input_elements_df=input_elements_df,
        dimension_name=dimension_name,
        orphan_consolidation_name=orphan_consolidation_name
    )
    pd.testing.assert_frame_equal(output_edges_df, expected_edges_df)
    pd.testing.assert_frame_equal(output_elements_df, expected_elements_df)


@parametrize_from_file
def test_init_input_schema_success(
        monkeypatch, input_datasource, sql_engine, sql_table_name, sql_query, filter_input_columns,
        raw_input_df, input_elements_datasource, input_elements_df_element_column, sql_elements_engine,
        sql_table_elements_name, sql_elements_query, filter_input_elements_columns, raw_input_elements_df,
        dimension_name, input_format, hierarchy_name, dim_column, hier_column, parent_column, child_column,
        level_columns, type_column, weight_column, attribute_parser, expected_edges_df, expected_elements_df
):
    raw_input_df = pd.DataFrame(raw_input_df)
    raw_input_elements_df = pd.DataFrame(raw_input_elements_df)
    expected_edges_df = pd.DataFrame(expected_edges_df)
    expected_elements_df = pd.DataFrame(expected_elements_df)

    call_count = {"n": 0}

    def fake_read_source_to_df(**_kwargs):
        call_count["n"] += 1
        return raw_input_df if call_count["n"] == 1 else raw_input_elements_df

    def fake_normalize_input_schema(**_kwargs):
        return expected_edges_df, expected_elements_df

    monkeypatch.setattr(apply.io, "read_source_to_df", fake_read_source_to_df)
    monkeypatch.setattr(apply.normalize, "normalize_input_schema", fake_normalize_input_schema)
    monkeypatch.setattr(apply, "pre_validate_input_schema", lambda **_kwargs: None)
    monkeypatch.setattr(apply, "post_validate_schema", lambda *_args, **_kwargs: None)

    edges_df, elements_df = apply.init_input_schema(
        dimension_name=dimension_name,
        input_format=input_format,
        input_datasource=input_datasource,
        sql_engine=sql_engine,
        sql_table_name=sql_table_name,
        sql_query=sql_query,
        filter_input_columns=filter_input_columns,
        raw_input_df=raw_input_df,
        hierarchy_name=hierarchy_name,
        dim_column=dim_column,
        hier_column=hier_column,
        parent_column=parent_column,
        child_column=child_column,
        level_columns=level_columns,
        type_column=type_column,
        weight_column=weight_column,
        input_elements_datasource=input_elements_datasource,
        input_elements_df_element_column=input_elements_df_element_column,
        sql_elements_engine=sql_elements_engine,
        sql_table_elements_name=sql_table_elements_name,
        sql_elements_query=sql_elements_query,
        filter_input_elements_columns=filter_input_elements_columns,
        raw_input_elements_df=raw_input_elements_df,
        attribute_parser=attribute_parser
    )
    pd.testing.assert_frame_equal(edges_df, expected_edges_df)
    pd.testing.assert_frame_equal(elements_df, expected_elements_df)


@parametrize_from_file
def test_init_existing_schema_success(
        monkeypatch, tm1_exists, existing_edges_df, existing_elements_df,
        normalized_edges_df, normalized_elements_df, dimension_name, old_orphan_parent_name
):
    existing_edges_df = None if existing_edges_df is None else pd.DataFrame(existing_edges_df)
    existing_elements_df = pd.DataFrame(existing_elements_df)
    normalized_edges_df = None if normalized_edges_df is None else pd.DataFrame(normalized_edges_df)
    normalized_elements_df = pd.DataFrame(normalized_elements_df)

    class DummyDimensions:
        def __init__(self, exists_value):
            self._exists = exists_value

        def exists(self, _name):
            return self._exists

    class DummyTM1:
        def __init__(self, exists_value):
            self.dimensions = DummyDimensions(exists_value)

    tm1_service = DummyTM1(tm1_exists)

    def fake_retrieve_existing_schema(_tm1, _dim):
        return existing_edges_df, existing_elements_df

    def fake_normalize_existing_schema(_edges, _elements, _old_name):
        return normalized_edges_df, normalized_elements_df

    monkeypatch.setattr(apply.io, "retrieve_existing_schema", fake_retrieve_existing_schema)
    monkeypatch.setattr(apply.normalize, "normalize_existing_schema", fake_normalize_existing_schema)

    edges_df, elements_df = apply.init_existing_schema(
        tm1_service=tm1_service,
        dimension_name=dimension_name,
        old_orphan_parent_name=old_orphan_parent_name
    )
    if tm1_exists:
        if normalized_edges_df is None:
            assert edges_df is None
        else:
            pd.testing.assert_frame_equal(edges_df, normalized_edges_df)
        pd.testing.assert_frame_equal(elements_df, normalized_elements_df)
    else:
        assert edges_df is None
        assert elements_df is None


@parametrize_from_file
def test_resolve_schema_success(
        monkeypatch, dimension_name, input_edges_df, input_elements_df, existing_edges_df,
        existing_elements_df, mode, allow_type_changes, orphan_parent_name,
        normalized_edges_df, normalized_elements_df
):
    input_edges_df = pd.DataFrame(input_edges_df)
    input_elements_df = pd.DataFrame(input_elements_df)
    existing_edges_df = pd.DataFrame(existing_edges_df)
    existing_elements_df = pd.DataFrame(existing_elements_df)
    normalized_edges_df = pd.DataFrame(normalized_edges_df)
    normalized_elements_df = pd.DataFrame(normalized_elements_df)

    monkeypatch.setattr(apply, "validate_element_type_consistency", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(apply, "apply_updates", lambda **_kwargs: (normalized_edges_df, normalized_elements_df))
    monkeypatch.setattr(apply.normalize, "normalize_updated_schema", lambda *_args, **_kwargs: (normalized_edges_df, normalized_elements_df))
    monkeypatch.setattr(apply, "post_validate_schema", lambda *_args, **_kwargs: None)

    edges_df, elements_df = apply.resolve_schema(
        dimension_name=dimension_name,
        tm1_service=None,
        input_edges_df=input_edges_df,
        input_elements_df=input_elements_df,
        existing_edges_df=existing_edges_df,
        existing_elements_df=existing_elements_df,
        mode=mode,
        allow_type_changes=allow_type_changes,
        orphan_parent_name=orphan_parent_name
    )
    pd.testing.assert_frame_equal(edges_df, normalized_edges_df)
    pd.testing.assert_frame_equal(elements_df, normalized_elements_df)


@parametrize_from_file
def test_resolve_schema_allow_type_changes_success(
        monkeypatch, dimension_name, input_edges_df, input_elements_df, existing_edges_df,
        existing_elements_df, mode, allow_type_changes, orphan_parent_name,
        normalized_edges_df, normalized_elements_df, conflicts_df
):
    input_edges_df = pd.DataFrame(input_edges_df)
    input_elements_df = pd.DataFrame(input_elements_df)
    existing_edges_df = pd.DataFrame(existing_edges_df)
    existing_elements_df = pd.DataFrame(existing_elements_df)
    normalized_edges_df = pd.DataFrame(normalized_edges_df)
    normalized_elements_df = pd.DataFrame(normalized_elements_df)
    conflicts_df = pd.DataFrame(conflicts_df)

    delete_calls = {"count": 0}

    monkeypatch.setattr(apply, "validate_element_type_consistency", lambda *_args, **_kwargs: conflicts_df)
    monkeypatch.setattr(apply, "delete_conflicting_elements", lambda *_args, **_kwargs: delete_calls.update(count=delete_calls["count"] + 1))
    monkeypatch.setattr(apply, "apply_updates", lambda **_kwargs: (normalized_edges_df, normalized_elements_df))
    monkeypatch.setattr(apply.normalize, "normalize_updated_schema", lambda *_args, **_kwargs: (normalized_edges_df, normalized_elements_df))
    monkeypatch.setattr(apply, "post_validate_schema", lambda *_args, **_kwargs: None)

    edges_df, elements_df = apply.resolve_schema(
        dimension_name=dimension_name,
        tm1_service=None,
        input_edges_df=input_edges_df,
        input_elements_df=input_elements_df,
        existing_edges_df=existing_edges_df,
        existing_elements_df=existing_elements_df,
        mode=mode,
        allow_type_changes=allow_type_changes,
        orphan_parent_name=orphan_parent_name
    )
    assert delete_calls["count"] == 1
    pd.testing.assert_frame_equal(edges_df, normalized_edges_df)
    pd.testing.assert_frame_equal(elements_df, normalized_elements_df)


@parametrize_from_file
def test_build_dimension_object_success(monkeypatch, dimension_name, edges_df, elements_df):
    edges_df = pd.DataFrame(edges_df)
    elements_df = pd.DataFrame(elements_df)

    class DummyHierarchy:
        def __init__(self, name, dimension_name):
            self.name = name
            self.dimension_name = dimension_name
            self.element_attrs = []
            self.elements = []
            self.edges = []

        def add_element_attribute(self, name, attr_type):
            self.element_attrs.append((name, attr_type))

        def add_element(self, element_name, element_type):
            self.elements.append((element_name, element_type))

        def add_edge(self, parent, component, weight):
            self.edges.append((parent, component, weight))

    class DummyDimension:
        def __init__(self, name):
            self.name = name
            self.hierarchies = {}

        def add_hierarchy(self, hierarchy):
            self.hierarchies[hierarchy.name] = hierarchy

    monkeypatch.setattr(apply, "Hierarchy", DummyHierarchy)
    monkeypatch.setattr(apply, "Dimension", DummyDimension)

    dimension = apply.build_dimension_object(
        dimension_name=dimension_name,
        edges_df=edges_df,
        elements_df=elements_df
    )
    assert dimension.name == dimension_name
    assert set(dimension.hierarchies.keys()) == set(elements_df["Hierarchy"].unique())


@parametrize_from_file
def test_update_element_attributes_success(
        elements_df, dimension_name, expected_cube_name, expected_cube_dims, expected_attr_df
):
    elements_df = pd.DataFrame(elements_df)
    expected_attr_df = pd.DataFrame(expected_attr_df)

    attr_df, cube_name, cube_dims = apply.prepare_attributes_for_load(
        elements_df=elements_df,
        dimension_name=dimension_name
    )
    pd.testing.assert_frame_equal(attr_df, expected_attr_df)
    assert cube_name == expected_cube_name
    assert cube_dims == expected_cube_dims
