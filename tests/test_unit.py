import os

import pandas as pd
import parametrize_from_file
import pytest
from TM1py.Exceptions import TM1pyRestException
from pandas.core.frame import DataFrame
from sqlalchemy import text
from sqlalchemy.exc import OperationalError

from TM1_bedrock_py import extractor, transformer, utility, loader
from TM1_bedrock_py.dimension_builder.exceptions import (LevelColumnInvalidRowError,
                                                         SchemaValidationError,
                                                         GraphValidationError)
from TM1_bedrock_py.dimension_builder import normalize, validate
from tests.config import tm1_connection_factory, sql_engine_factory

EXCEPTION_MAP = {
    "ValueError": ValueError,
    "TypeError": TypeError,
    "TM1pyRestException": TM1pyRestException,
    "OperationalError": OperationalError,
    "IndexError": IndexError,
    "KeyError": KeyError
}


def test_tm1_connection(tm1_connection_factory):
    with tm1_connection_factory("testbench") as conn:
        server_name = conn.server.get_server_name()
        print("Connection to TM1 established! Your server name is: {}".format(server_name))


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_get_cube_name_from_mdx(mdx_query):
    cube_name = utility.get_cube_name_from_mdx(mdx_query)
    assert isinstance(cube_name, str)


@parametrize_from_file
def test_mdx_filter_to_dictionary(mdx_query):
    dimensions = utility.mdx_filter_to_dictionary(mdx_query)
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
    kwargs = utility.get_kwargs_dict_from_set_mdx_list(set_mdx_list)
    assert kwargs == expected_kwargs


"""
# Test focusing on filtering, edge cases, and empty results
@parametrize_from_file
def test_get_kwargs_dict_from_set_mdx_list_filtering(set_mdx_list, expected_exception):
    
    Tests filtering of invalid/non-matching strings and edge cases.
    
    kwargs = utility.__get_kwargs_dict_from_set_mdx_list(set_mdx_list)
    assert kwargs == expected_exception
"""


@parametrize_from_file
def test_get_dimensions_from_set_mdx_list_success(mdx_sets, expected_dimensions):
    """
    Tests successful extraction of first dimension names from MDX strings.
    """
    result = utility.get_dimensions_from_set_mdx_list(mdx_sets)
    assert result == expected_dimensions


@parametrize_from_file
def test__get_kwargs_dict_from_set_mdx_list_fail(mdx_expressions, expected_exception):
    """
    Tests if the function raises a ValueError if at least one expression does not match.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type):
        utility.get_kwargs_dict_from_set_mdx_list(mdx_expressions)


@parametrize_from_file
def test_get_dimensions_from_set_mdx_list_failure(mdx_sets, expected_exception, expected_message_part):
    """
    Tests type errors for invalid input.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.get_dimensions_from_set_mdx_list(mdx_sets)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_generate_cartesian_product_success(list_of_lists, expected_product):
    """
    Tests successful generation of Cartesian products.
    """
    expected_tuples = [tuple(item) for item in expected_product]
    result = utility.generate_cartesian_product(list_of_lists)
    assert result == expected_tuples


@parametrize_from_file
def test_generate_cartesian_product_failure(list_of_lists, expected_exception, expected_message_part):
    """
    Tests failing scenarios due to invalid input types.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.generate_cartesian_product(list_of_lists)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_generate_element_lists_from_set_mdx_list_success(tm1_connection_factory, set_mdx_list, expected_result):
    """
    Tests successful extraction of element lists using a fake TM1 service.
    """
    with tm1_connection_factory("tm1srv") as conn:
        result = utility.generate_element_lists_from_set_mdx_list(conn, set_mdx_list)
        assert result == expected_result


@parametrize_from_file
def test_generate_element_lists_from_set_mdx_list_failure(
        tm1_connection_factory, use_none_service, set_mdx_list, expected_exception, expected_message_part):
    """
    Tests failing scenarios for element list extraction using fake or invalid service/inputs.
    """
    with tm1_connection_factory("tm1srv") as conn:
        if use_none_service:
            test_service = None
        else:
            test_service = conn
    
        exception_type = eval(expected_exception)
        with pytest.raises(exception_type) as excinfo:
            utility.generate_element_lists_from_set_mdx_list(test_service, set_mdx_list)
    
        assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_extract_mdx_components(input_mdx, expected_set_mdx_list):
    output_set_mdx_list = utility.extract_mdx_components(mdx=input_mdx)
    assert output_set_mdx_list == expected_set_mdx_list


@parametrize_from_file
def test_add_nonempty_to_mdx_all_modes(input_mdx, expected_mdx):
    output_mdx = utility.add_non_empty_to_mdx(input_mdx)
    assert "".join(output_mdx.split()) == "".join(expected_mdx.split())


@parametrize_from_file
def test_all_leaves_identifiers_to_dataframe(tm1_connection_factory, dimname, expected):
    with tm1_connection_factory("tm1srv") as conn:
        expected_df = pd.DataFrame(expected)
        df = utility.all_leaves_identifiers_to_dataframe(conn, dimname)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_rename_columns_with_reference(input_df, input_list, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = transformer.rename_columns_by_reference(dataframe=input_df, column_names=input_list)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_normalize_dict_strings(input_dict, expected_dict):
    output_dict = utility.normalize_structure_strings(input_dict)
    assert output_dict == expected_dict


@parametrize_from_file
def test_normalize_dataframe_strings(input_df, expected_df):
    output_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    utility.normalize_dataframe_strings(output_df)
    pd.testing.assert_frame_equal(output_df, expected_df)

# ------------------------------------------------------------------------------------------------------------
# Utility: Cube metadata collection using input MDXs and/or other cubes
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_cube_name_success(tm1_connection_factory, cube_name):
    """Collects metadata based on cube name and checks if the method's output is a Metadata object"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            print(metadata.to_dict())
            assert isinstance(metadata, utility.TM1CubeObjectMetadata)
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_cube_name_fail(tm1_connection_factory, cube_name, exception):
    """Runs collect_metadata based with bad cube name and checks if the method's output is a Metadata object."""
    with tm1_connection_factory("tm1srv") as conn:
        with pytest.raises(EXCEPTION_MAP[exception]):
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            print(metadata.to_dict())
            assert isinstance(metadata, utility.TM1CubeObjectMetadata)


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_mdx_name_success(tm1_connection_factory, data_mdx):
    """Collects metadata based on MDX and checks if the method's output is a Metadata object"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            assert isinstance(
                utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx),
                utility.TM1CubeObjectMetadata
            )
        except TM1pyRestException as e:
            pytest.fail(f"Cube not found based on MDX: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_mdx_name_fail(tm1_connection_factory, data_mdx, exception):
    """Runs collect_metadata with bad input for MDX and checks if the method's output is a Metadata object."""
    with tm1_connection_factory("tm1srv") as conn:
        with pytest.raises(EXCEPTION_MAP[exception]):
            assert isinstance(
                utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx),
                utility.TM1CubeObjectMetadata
            )


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_cube_dimensions_not_empty(tm1_connection_factory, cube_name):
    """Collects metadata and verifies that cube dimensions are not empty."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            cube_dims = metadata.get_cube_dims()
            assert cube_dims != 0
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_cube_dimensions_match_dimensions(
        tm1_connection_factory, cube_name, expected_dimensions
):
    """Collects metadata and verifies that cube dimensions match the expected dimensions."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            cube_dims = metadata.get_cube_dims()
            assert cube_dims == expected_dimensions
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_filter_dict_not_empty(tm1_connection_factory, data_mdx):
    """Collects metadata and verifies that filter dimensions are not empty."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx)
            filter_dims = metadata.get_filter_dict()
            assert filter_dims
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_filter_dict_match(tm1_connection_factory, data_mdx, expected_filter_dict):
    """Collects metadata and verifies that filter dimensions are not empty."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx)
            filter_dict = metadata.get_filter_dict()
            assert filter_dict == expected_filter_dict
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_success(tm1_connection_factory, data_mdx):
    """Run MDX to dataframe function and verifies that the output is a DataFrame object."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            assert isinstance(df, DataFrame)
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_fail(tm1_connection_factory, data_mdx):
    """Run MDX to dataframe function with bad input. Raises error."""
    with tm1_connection_factory("tm1srv") as conn:
        with pytest.raises((TM1pyRestException, ValueError)):
            assert isinstance(
                extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx),
                DataFrame
            )


@parametrize_from_file
def test_normalize_dataframe_is_dataframe_true(tm1_connection_factory, data_mdx):
    """Run normalize dataframe function and check for if output is dataframe"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            df = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=conn, mdx=data_mdx)
            assert isinstance(df, DataFrame)
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_number_of_dimensions_success(tm1_connection_factory, data_mdx, expected_dimensions):
    """Run normalize dataframe function and check if the output has the correct number of dimensions"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            df = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=conn, mdx=data_mdx)
            df.keys()
            assert len(df.keys()) == expected_dimensions
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_dimensions_success(tm1_connection_factory, data_mdx, expected_dimensions):
    """Runs normalize dataframe function and validates that the output's dimension keys match the expected"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            df = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=conn, mdx=data_mdx)
            keys = [key for key in df.keys()]
            assert expected_dimensions == keys
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
    df = transformer.dataframe_reorder_dimensions(dataframe=df, cube_dimensions=cube_cols)

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
def test_dataframe_itemskip_elements(source, check1, check2, defaults, expected):
    utility.set_logging_level("DEBUG")
    df = pd.DataFrame(source)
    check_dfs = [pd.DataFrame(check1), pd.DataFrame(check2)]
    expected_df = pd.DataFrame(expected)
    transformer.dataframe_itemskip_elements(dataframe=df, check_dfs=check_dfs, fallback_elements=defaults)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_validate_datatypes(measuredim, measuretypes, dataframe, expected):
    input_dataframe = pd.DataFrame(dataframe)
    expected_dataframe = pd.DataFrame(expected)
    transformer.dataframe_cast_value_by_measure_type(
        dataframe=input_dataframe,
        measure_dimension_name=measuredim,
        measure_element_types=measuretypes
    )
    pd.testing.assert_frame_equal(input_dataframe, expected_dataframe)

# ------------------------------------------------------------------------------------------------------------
# Main: tests for dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_dataframe_find_and_replace_success(dataframe, mapping, expected_dataframe):
    """Remaps elements based on literal mapping, without dimension manipulation and checks for successful execution"""

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_find_and_replace(dataframe=df, mapping=mapping)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_find_and_replace_fail(dataframe, mapping, expected_dataframe):
    """Tries to remap elements based on literal mapping, without dimension manipulation with bad input. Raises error."""
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    with pytest.raises(AssertionError):
        df = transformer.dataframe_find_and_replace(dataframe=df, mapping=mapping)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_map_and_replace_success(dataframe, mapping_dataframe, mapping_dimensions, expected_dataframe):
    df = pd.DataFrame(dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_map_and_replace(
        data_df=df,
        mapping_df=mapping_df,
        mapped_dimensions=mapping_dimensions)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_map_and_join_success(dataframe, joined_cols, mapping_dataframe, expected_dataframe):
    df = pd.DataFrame(dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_map_and_join(
        data_df=df,
        mapping_df=mapping_df,
        joined_columns=joined_cols
    )

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_execute_mappings_replace_success(dataframe, mapping_steps, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_execute_mappings(
        data_df=df,
        mapping_steps=mapping_steps,
    )

    pd.testing.assert_frame_equal(df, expected_df)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for sql connections and I/O processes
# ------------------------------------------------------------------------------------------------------------


def test_mssql_database_connection(sql_engine_factory):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        # assert sql_engine.closed is False
        assert str(sql_engine.url.get_backend_name()) == "mssql", f"Wrong backend: {sql_engine.url.get_backend_name()}"


def test_mssql_server_responds_to_query(sql_engine_factory):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        with sql_engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            response = result.fetchone()
    assert response == (1,)


@parametrize_from_file
def test_mssql_extract_full_table(sql_engine_factory, table_name, expected):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name)
        expected_df = pd.DataFrame(expected)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_table_columns(sql_engine_factory, table_name, columns, expected):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name, table_columns=columns)
        expected_df = pd.DataFrame(expected)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_query(sql_engine_factory, query, expected):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        df = extractor.sql_to_dataframe(engine=sql_engine, sql_query=query)
        expected_df = pd.DataFrame(expected)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_query_with_chunksize(sql_engine_factory, query, expected, chunksize):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        df = extractor.sql_to_dataframe(engine=sql_engine, sql_query=query, chunksize=chunksize)
        expected_df = pd.DataFrame(expected)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_sql_normalize_relabel(sql_engine_factory, dataframe, expected, column_mapping):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, column_mapping=column_mapping)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_sql_normalize_drop(sql_engine_factory, dataframe, expected, drop):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, columns_to_drop=drop)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_loader_replace(sql_engine_factory, dataframe, if_exists, table_name):
    with sql_engine_factory('mssqlsrv') as sql_engine:
        df = pd.DataFrame(dataframe)
        loader.dataframe_to_sql(
            dataframe=df, engine=sql_engine, table_name=table_name, if_exists=if_exists, index=False
        )


# ------------------------------------------------------------------------------------------------------------
# Main: tests for csv I/O processes
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_dataframe_casting_for_csv_file(dataframe, cube_dims):
    """ Tests utility.cast_coordinates_to_str() function. """
    df = pd.DataFrame(dataframe)
    for dim_col in cube_dims:
        if dim_col in df.columns:
            df[dim_col] = df[dim_col].astype(str)
            assert df[dim_col].apply(lambda v: isinstance(v, str)).all()


@parametrize_from_file
def test_dataframe_to_csv(data_dataframe, expected_dataframe):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    csv_file_name = "sample_data.csv"
    output_dir = "./dataframe_to_csv"
    csv_file_path = f"{output_dir}/{csv_file_name}"
    try:
        data_dataframe["Price"] = [float(x) for x in data_dataframe["Price"]]
        data_dataframe["Quantity"] = [float(x) for x in data_dataframe["Quantity"]]

        expected_dataframe["Price"] = [float(x) for x in expected_dataframe["Price"]]
        expected_dataframe["Quantity"] = [float(x) for x in expected_dataframe["Quantity"]]

        data_df = pd.DataFrame(data_dataframe)
        dtype_mapping = data_df.dtypes.apply(lambda x: x.name).to_dict()

        loader.dataframe_to_csv(dataframe=data_df, csv_file_name=csv_file_name, decimal=".")
        df = extractor.csv_to_dataframe(
            csv_file_path=csv_file_path,
            decimal=".",
            dtype=dtype_mapping,
            keep_default_na=False,
            na_values=["NULL", "Nan"]
        )

        expected_df = pd.DataFrame(expected_dataframe)

        pd.testing.assert_frame_equal(df, expected_df)

    finally:
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
            os.rmdir(output_dir)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx(tm1_connection_factory, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    with tm1_connection_factory("tm1srv") as conn:
        csv_file_name = "sample_data.csv"
        try:
            expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            dtype_mapping = expected_df.dtypes.apply(lambda x: x.name).to_dict()

            expected_df = transformer.normalize_dataframe_for_testing(dataframe=expected_df, tm1_service=conn, mdx=data_mdx)

            loader.dataframe_to_csv(
                dataframe=expected_df, csv_file_name=csv_file_name, csv_output_dir="./", decimal=".", mode="a"
            )
            df = extractor.csv_to_dataframe(csv_file_path=f"./{csv_file_name}", decimal=".", dtype=dtype_mapping)
            pd.testing.assert_frame_equal(df, expected_df)

        finally:
            if os.path.exists(csv_file_name):
                os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx_with_param_optimisation(tm1_connection_factory, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    with tm1_connection_factory("tm1srv") as conn:
        csv_file_name = "sample_data.csv"
        try:
            expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            dtype_mapping = expected_df.dtypes.apply(lambda x: x.name).to_dict()

            loader.dataframe_to_csv(
                dataframe=expected_df, csv_file_name=csv_file_name, csv_output_dir="./", decimal="."
            )
            df = extractor.csv_to_dataframe(
                    csv_file_path=f"./{csv_file_name}",
                    decimal=".",
                    dtype=dtype_mapping,
                    chunksize=204
            )

            pd.testing.assert_frame_equal(df, expected_df)

        finally:
            if os.path.exists(csv_file_name):
                os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx_fail(tm1_connection_factory, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        As the original data types are not passed to the function, the types differ.
        Expected to fail.
        Deletes CSV file after assertion.
    """
    with tm1_connection_factory("tm1srv") as conn:
        csv_file_name = "sample_data.csv"
        with pytest.raises(AssertionError):
            try:
                expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)

                loader.dataframe_to_csv(
                    dataframe=expected_df, csv_file_name=csv_file_name, csv_output_dir="./", decimal="."
                )
                df = extractor.csv_to_dataframe(csv_file_path=f"./{csv_file_name}", decimal=".")

                pd.testing.assert_frame_equal(df, expected_df)

            finally:
                if os.path.exists(csv_file_name):
                    os.remove(csv_file_name)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for airflow executor common functions
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_generate_mapping_queries_for_slice(kwargs, ms, sm, expected_ms, expected_sm):
    try:
        from TM1_bedrock_py.airflow_executor import common as airflow_common
        output_ms, output_sm = airflow_common.generate_mapping_queries_for_slice(
            expand_kwargs=kwargs,
            mapping_steps=ms,
            shared_mapping=sm
        )
        assert (output_ms, output_sm) == (expected_ms, expected_sm)
    except ModuleNotFoundError as e:
        print(f"Airflow executor sub-modul packages were not installed: {e}")


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
def test_separate_attr_df_columns(input_df, attr_columns, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)

    output_df = normalize.separate_attr_df_columns(
        input_df=input_df,
        attr_columns=attr_columns
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
