import configparser
import os
import re
from pathlib import Path

from TM1py.Exceptions import TM1pyRestException
from sqlalchemy.exc import OperationalError, InterfaceError
from sqlalchemy import text
from pandas.core.frame import DataFrame
import pandas as pd
import pytest
import parametrize_from_file
from TM1py import TM1Service

from TM1_bedrock_py import extractor, transformer, utility, basic_logger, loader


EXCEPTION_MAP = {
    "ValueError": ValueError,
    "TypeError": TypeError,
    "TM1pyRestException": TM1pyRestException,
    "OperationalError": OperationalError,
    "IndexError": IndexError,
    "KeyError": KeyError
}


@pytest.fixture(scope="session")
def tm1_connection():
    """Creates a TM1 connection before tests and closes it after all tests."""
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))
    try:
        tm1 = TM1Service(**config['tm1srv'])
        basic_logger.debug("Successfully connected to TM1.")
        yield tm1

        tm1.logout()
        basic_logger.debug("Connection closed.")

    except TM1pyRestException:
        basic_logger.error("Unable to connect to TM1: ", exc_info=True)


@pytest.fixture(scope="session")
def sql_engine():
    """Creates a SQL connector engine before tests and closes it after all tests."""
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))
    try:
        engine = utility.create_sql_engine(**config['mssqlsrv'])
        basic_logger.debug("SQL engine successfully created")
        yield engine

        engine.dispose()
        basic_logger.debug("SQL engine disposed.")

    except OperationalError or InterfaceError:
        basic_logger.error("Unable to connect to SQL: ", exc_info=True)


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_get_cube_name_from_mdx(mdx_query):
    cube_name = utility._get_cube_name_from_mdx(mdx_query)
    assert isinstance(cube_name, str)


@parametrize_from_file
def test_mdx_filter_to_dictionary(mdx_query):
    dimensions = utility._mdx_filter_to_dictionary(mdx_query)
    if mdx_query:
        for dim in dimensions:
            for elem in dim:
                assert isinstance(elem, str)
    else:
        assert dimensions == {}


@parametrize_from_file
def test_get_kwargs_dict_from_set_mdx_list_success(set_mdx_list, expected_kwargs):
    """
    Tests successful extraction of kwargs from various valid MDX lists.
    """
    kwargs = utility.__get_kwargs_dict_from_set_mdx_list(set_mdx_list)
    assert kwargs == expected_kwargs


# Test focusing on filtering, edge cases, and empty results
@parametrize_from_file
def test_get_kwargs_dict_from_set_mdx_list_filtering(set_mdx_list, expected_kwargs):
    """
    Tests filtering of invalid/non-matching strings and edge cases.
    """
    kwargs = utility.__get_kwargs_dict_from_set_mdx_list(set_mdx_list)
    assert kwargs == expected_kwargs


@parametrize_from_file
def test_get_dimensions_from_set_mdx_list_success(mdx_sets, expected_dimensions):
    """
    Tests successful extraction of first dimension names from MDX strings.
    """
    result = utility.__get_dimensions_from_set_mdx_list(mdx_sets)
    assert result == expected_dimensions


@parametrize_from_file
def test_get_dimensions_from_set_mdx_list_failure(mdx_sets, expected_exception, expected_message_part):
    """
    Tests type errors for invalid input.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.__get_dimensions_from_set_mdx_list(mdx_sets)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_generate_cartesian_product_success(list_of_lists, expected_product):
    """
    Tests successful generation of Cartesian products.
    """
    expected_tuples = [tuple(item) for item in expected_product]
    result = utility.__generate_cartesian_product(list_of_lists)
    assert result == expected_tuples


@parametrize_from_file
def test_generate_cartesian_product_failure(list_of_lists, expected_exception, expected_message_part):
    """
    Tests failing scenarios due to invalid input types.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.__generate_cartesian_product(list_of_lists)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_generate_element_lists_from_set_mdx_list_success(tm1_connection, set_mdx_list, expected_result):
    """
    Tests successful extraction of element lists using a fake TM1 service.
    """
    result = utility.__generate_element_lists_from_set_mdx_list(tm1_connection, set_mdx_list)
    assert result == expected_result


@parametrize_from_file
def test_generate_element_lists_from_set_mdx_list_failure(
        tm1_connection, use_none_service, set_mdx_list, expected_exception, expected_message_part):
    """
    Tests failing scenarios for element list extraction using fake or invalid service/inputs.
    """
    if use_none_service:
        test_service = None
    else:
        test_service = tm1_connection

    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.__generate_element_lists_from_set_mdx_list(test_service, set_mdx_list)

    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_utility_float_casting_values(input_value, expected_value):
    output_value = utility.force_float64_on_numeric_values(input_value)
    print(f"input value: {input_value}, output_value: {output_value}")
    assert output_value == expected_value


@parametrize_from_file
def test_utility_float_casting_types(input_value, expected_type):
    output_value = utility.force_float64_on_numeric_values(input_value)
    output_type = str(type(output_value))
    print(output_type)
    assert output_type == expected_type


@parametrize_from_file
def test_add_nonempty_to_mdx_all_modes(input_mdx, expected_mdx):
    output_mdx = utility.add_non_empty_to_mdx(input_mdx)

    assert "".join(output_mdx.split()) == "".join(expected_mdx.split())


@parametrize_from_file
def test_all_leaves_identifiers_to_dataframe(tm1_connection, dimname, expected):
    expected_df = pd.DataFrame(expected)
    df = utility.all_leaves_identifiers_to_dataframe(tm1_connection, dimname)
    pd.testing.assert_frame_equal(df, expected_df)


# ------------------------------------------------------------------------------------------------------------
# Utility: Cube metadata collection using input MDXs and/or other cubes
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_cube_name_success(tm1_connection, cube_name):
    """Collects metadata based on cube name and checks if the method's output is a Metadata object"""
    try:
        metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, cube_name=cube_name)
        print(metadata.to_dict())
        assert isinstance(metadata, utility.TM1CubeObjectMetadata)
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_cube_name_fail(tm1_connection, cube_name, exception):
    """Runs collect_metadata based with bad cube name and checks if the method's output is a Metadata object."""
    with pytest.raises(EXCEPTION_MAP[exception]):
        metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, cube_name=cube_name)
        print(metadata.to_dict())
        assert isinstance(metadata, utility.TM1CubeObjectMetadata)


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_mdx_name_success(tm1_connection, data_mdx):
    """Collects metadata based on MDX and checks if the method's output is a Metadata object"""
    try:
        assert isinstance(
            utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, mdx=data_mdx),
            utility.TM1CubeObjectMetadata
        )
    except TM1pyRestException as e:
        pytest.fail(f"Cube not found based on MDX: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_mdx_name_fail(tm1_connection, data_mdx, exception):
    """Runs collect_metadata with bad input for MDX and checks if the method's output is a Metadata object."""
    with pytest.raises(EXCEPTION_MAP[exception]):
        assert isinstance(
            utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, mdx=data_mdx),
            utility.TM1CubeObjectMetadata
        )


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_cube_dimensions_not_empty(tm1_connection, cube_name):
    """Collects metadata and verifies that cube dimensions are not empty."""
    try:
        metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, cube_name=cube_name)
        cube_dims = metadata.get_cube_dims()
        assert cube_dims != 0
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_cube_dimensions_match_dimensions(
        tm1_connection, cube_name, expected_dimensions
):
    """Collects metadata and verifies that cube dimensions match the expected dimensions."""
    try:
        metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, cube_name=cube_name)
        cube_dims = metadata.get_cube_dims()
        assert cube_dims == expected_dimensions
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_filter_dict_not_empty(tm1_connection, data_mdx):
    """Collects metadata and verifies that filter dimensions are not empty."""
    try:
        metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, mdx=data_mdx)
        filter_dims = metadata.get_filter_dict()
        assert filter_dims
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_filter_dict_match(tm1_connection, data_mdx, expected_filter_dict):
    """Collects metadata and verifies that filter dimensions are not empty."""
    try:
        metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=tm1_connection, mdx=data_mdx)
        filter_dict = metadata.get_filter_dict()
        assert filter_dict == expected_filter_dict
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_build_mdx_from_cube_filter_is_valid_format_true(tm1_connection, cube_filter, cube_name, expected_mdx):
    """Build MDX from cube name and dimension and check if the returned MDX matches the expected."""

    mdx = utility.build_mdx_from_cube_filter(tm1_service=tm1_connection, cube_name=cube_name, cube_filter=cube_filter)
    mdx = mdx.replace(" ", "")
    expected_mdx = expected_mdx.replace(" ", "")

    assert mdx == expected_mdx


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_success(tm1_connection, data_mdx):
    """Run MDX to dataframe function and verifies that the output is a DataFrame object."""
    try:
        df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_fail(tm1_connection, data_mdx):
    """Run MDX to dataframe function with bad input. Raises error."""
    with pytest.raises((TM1pyRestException, ValueError)):
        assert isinstance(
            extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx),
            DataFrame
        )


@parametrize_from_file
def test_normalize_dataframe_is_dataframe_true(tm1_connection, data_mdx):
    """Run normalize dataframe function and check for if output is dataframe"""
    try:
        df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_number_of_dimensions_success(tm1_connection, data_mdx, expected_dimensions):
    """Run normalize dataframe function and check if the output has the correct number of dimensions"""
    try:
        df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        df.keys()
        assert len(df.keys()) == expected_dimensions
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_dimensions_success(tm1_connection, data_mdx, expected_dimensions):
    """Runs normalize dataframe function and validates that the output's dimension keys match the expected"""
    try:
        df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        keys = [key for key in df.keys()]
        assert expected_dimensions == keys
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_build_mdx_from_cube_filter_create_dataframe_success(tm1_connection, cube_filter, cube_name):
    """Run MDX query created by the MDX builder and verifies that the output is a DataFrame object"""

    data_mdx = utility.build_mdx_from_cube_filter(
        tm1_service=tm1_connection, cube_name=cube_name, cube_filter=cube_filter
    )
    try:
        df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe transform utility functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_dataframe_filter_inplace(dataframe, filter_condition, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_filter_inplace(dataframe=df, filter_condition=filter_condition)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_filter(dataframe, filter_condition, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    filtered_df = transformer.dataframe_filter(dataframe=df, filter_condition=filter_condition)

    pd.testing.assert_frame_equal(filtered_df, expected_df)


@parametrize_from_file
def test_dataframe_drop_column(dataframe, column_list, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_drop_column(dataframe=df, column_list=column_list)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_redimension_scale_down(dataframe, filter_condition, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_drop_filtered_column(dataframe=df, filter_condition=filter_condition)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_relabel(dataframe, columns, expected_columns):
    df = pd.DataFrame(dataframe)
    transformer.dataframe_relabel(dataframe=df, columns=columns)
    new_columns = list(map(str, df.columns))
    assert new_columns == expected_columns


@parametrize_from_file
def test_dataframe_add_column_assign_value(dataframe, column_values, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_add_column_assign_value(dataframe=df, column_value=column_values)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_redimension_and_transform(
        dataframe, source_dim_mapping, related_dimensions, target_dim_mapping, expected_dataframe
):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_redimension_and_transform(
        df, source_dim_mapping, related_dimensions, target_dim_mapping
    )

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_reorder_dimensions(dataframe, cube_cols, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_reorder_dimensions(dataframe=df, cube_dimensions=cube_cols)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_force_float64_on_numerical_values(dataframe, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_force_float64_on_numeric_values(df)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_value_scale(dataframe, expected_dataframe):
    def add_one(x): return x + 1
    def multiply_by_two(x): return x * 2

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_value_scale(df, add_one)
    transformer.dataframe_value_scale(df, multiply_by_two)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_itemskip_elements(source, check1, check2, expected):
    utility.set_logging_level("DEBUG")
    df = pd.DataFrame(source)
    check_dfs = [pd.DataFrame(check1), pd.DataFrame(check2)]
    expected_df = pd.DataFrame(expected)
    transformer.dataframe_itemskip_elements(dataframe=df, check_dfs=check_dfs)
    pd.testing.assert_frame_equal(df, expected_df)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_dataframe_find_and_replace_success(dataframe, mapping, expected_dataframe):
    """Remaps elements based on literal mapping, without dimension manipulation and checks for successful execution"""

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_find_and_replace(dataframe=df, mapping=mapping)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_find_and_replace_fail(dataframe, mapping, expected_dataframe):
    """Tries to remap elements based on literal mapping, without dimension manipulation with bad input. Raises error."""
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    with pytest.raises(AssertionError):
        transformer.dataframe_find_and_replace(dataframe=df, mapping=mapping)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_map_and_replace_success(dataframe, mapping_dataframe, mapping_dimensions, expected_dataframe):
    df = pd.DataFrame(dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_map_and_replace(
        data_df=df,
        mapping_df=mapping_df,
        mapped_dimensions=mapping_dimensions)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_map_and_join_success(dataframe, joined_cols, mapping_dataframe, expected_dataframe):
    df = pd.DataFrame(dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_map_and_join(
        data_df=df,
        mapping_df=mapping_df,
        joined_columns=joined_cols
    )

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_execute_mappings_replace_success(mapping_steps):
    pass


# ------------------------------------------------------------------------------------------------------------
# Main: tests for sql connections and I/O processes
# ------------------------------------------------------------------------------------------------------------


def test_mssql_database_connection(sql_engine):
    with sql_engine.connect() as connection:
        assert connection.closed is False


def test_mssql_server_responds_to_query(sql_engine):
    with sql_engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        response = result.fetchone()
    assert response == (1,)


@parametrize_from_file
def test_mssql_extract_full_table(sql_engine, table_name, expected):
    df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_table_columns(sql_engine, table_name, columns, expected):
    df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name, table_columns=columns)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_query(sql_engine, query, expected):
    df = extractor.sql_to_dataframe(engine=sql_engine, sql_query=query)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssq_extract_query_with_chunksize(sql_engine, query, expected, chunksize):
    df = extractor.sql_to_dataframe(engine=sql_engine, sql_query=query, chunksize=chunksize)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_sql_normalize_relabel(sql_engine, dataframe, expected, column_mapping):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, column_mapping=column_mapping)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_sql_normalize_valuecol_assign(sql_engine, dataframe, expected, valuecol):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, value_column_name=valuecol)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_sql_normalize_keep_and_drop(sql_engine, dataframe, expected, keep, drop):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, columns_to_keep=keep, drop_other_columns=drop)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_loader_replace(sql_engine, dataframe, if_exists, table_name):
    df = pd.DataFrame(dataframe)
    loader.dataframe_to_sql(dataframe=df, engine=sql_engine, table_name=table_name, if_exists=if_exists, index=False)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for csv I/O processes
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_dataframe_to_csv(data_dataframe, expected_dataframe):
    """
        Loads data from DataFrame file to a CSV file then does the reverse. Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    csv_file_name = "sample_data.csv"
    try:
        data_dataframe["Price"] = [float(x) for x in data_dataframe["Price"]]
        data_dataframe["Quantity"] = [float(x) for x in data_dataframe["Quantity"]]

        data_df = pd.DataFrame(data_dataframe)
        dtype_mapping = data_df.dtypes.apply(lambda x: x.name).to_dict()

        loader.dataframe_to_csv(dataframe=data_df, csv_file_name=csv_file_name, decimal=".", mode="w")
        df = extractor.csv_to_dataframe(
            csv_file_path=f"./{csv_file_name}",
            decimal=".",
            dtype=dtype_mapping,
            keep_default_na=False,
            na_values=["NULL", "Nan"]
        )

        expected_df = pd.DataFrame(expected_dataframe)
        print(df)
        print(df.dtypes)
        print(expected_df)
        print(expected_df.dtypes)

        pd.testing.assert_frame_equal(df, expected_df, check_dtype=True)

    finally:
        if os.path.exists(csv_file_name):
            os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx(tm1_connection, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse. Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    csv_file_name = "sample_data.csv"
    try:
        expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        dtype_mapping = expected_df.dtypes.apply(lambda x: x.name).to_dict()

        transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=expected_df, mdx=data_mdx)

        loader.dataframe_to_csv(dataframe=expected_df, csv_file_name=csv_file_name, decimal=".", mode="a")
        df = extractor.csv_to_dataframe(csv_file_path=f"./{csv_file_name}", decimal=".", dtype=dtype_mapping)
        pd.testing.assert_frame_equal(df, expected_df)

    finally:
        if os.path.exists(csv_file_name):
            os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx_with_param_optimisation(tm1_connection, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse. Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    csv_file_name = "sample_data.csv"
    try:
        expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        dtype_mapping = expected_df.dtypes.apply(lambda x: x.name).to_dict()

        loader.dataframe_to_csv(dataframe=expected_df, csv_file_name=csv_file_name, decimal=".", mode="w")
        df = extractor.csv_to_dataframe(
                csv_file_path=f"./{csv_file_name}",
                decimal=".",
                dtype=dtype_mapping,
                chunksize= 204
        )

        print(df)
        pd.testing.assert_frame_equal(df, expected_df)

    finally:
        if os.path.exists(csv_file_name):
            os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx_fail(tm1_connection, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse. Checks if the DataFrame stayed the same after the operations.
        As the original data types are not passed to the function, the types differ.
        Expected to fail.
        Deletes CSV file after assertion.
    """
    csv_file_name = "sample_data.csv"
    with pytest.raises(AssertionError):
        try:
            expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)

            loader.dataframe_to_csv(dataframe=expected_df, csv_file_name=csv_file_name, decimal=".", mode="w")
            df = extractor.csv_to_dataframe(csv_file_path=f"./{csv_file_name}", decimal=".")

            pd.testing.assert_frame_equal(df, expected_df)

        finally:
            if os.path.exists(csv_file_name):
                os.remove(csv_file_name)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for input processes
# ------------------------------------------------------------------------------------------------------------

