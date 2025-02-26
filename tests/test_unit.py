import configparser
from pathlib import Path

from TM1py.Exceptions import TM1pyRestException
from pandas.core.frame import DataFrame
import pandas as pd
import pytest
import parametrize_from_file
from TM1py import TM1Service

from TM1_bedrock_py import tm1_bedrock


EXCEPTION_MAP = {
    "ValueError": ValueError,
    "TypeError": TypeError,
    "TM1pyRestException": TM1pyRestException,
    "IndexError": IndexError,
    "KeyError": KeyError
}


@pytest.fixture(scope="session")
def tm1_connection():
    """Creates a TM1 connection before tests and closes it after all tests."""
    config = configparser.ConfigParser()
    config.read(Path(__file__).parent.joinpath('config.ini'))

    tm1 = TM1Service(**config['tm1srv'])
    yield tm1
    tm1.logout()


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_parse_from_clause(mdx_query):
    cube_name = tm1_bedrock.parse_from_clause(mdx_query)
    assert isinstance(cube_name, str)


@parametrize_from_file
def test_parse_where_clause(mdx_query):
    dimensions = tm1_bedrock.parse_where_clause(mdx_query)
    if mdx_query:
        for dim in dimensions:
            for elem in dim:
                assert isinstance(elem, str)
    else:
        assert dimensions == []

# ------------------------------------------------------------------------------------------------------------
# Utility: Cube metadata collection using input MDXs and/or other cubes
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_collect_metadata_based_on_cube_name_success(tm1_connection, cube_name):
    """Collects metadata based on cube name and checks if the method's output is a Metadata object"""
    try:
        assert isinstance(
            tm1_bedrock.collect_metadata(tm1_service=tm1_connection, cube_name=cube_name),
            tm1_bedrock.Metadata
        )
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_collect_metadata_based_on_cube_name_fail(tm1_connection, cube_name, exception):
    """Runs collect_metadata based with bad cube name and checks if the method's output is a Metadata object."""
    with pytest.raises(EXCEPTION_MAP[exception]):
        assert isinstance(
            tm1_bedrock.collect_metadata(tm1_service=tm1_connection, cube_name=cube_name),
            tm1_bedrock.Metadata
        )


@parametrize_from_file
def test_collect_metadata_based_on_mdx_success(tm1_connection, data_mdx):
    """Collects metadata based on MDX and checks if the method's output is a Metadata object"""
    try:
        assert isinstance(
            tm1_bedrock.collect_metadata(tm1_service=tm1_connection, mdx=data_mdx),
            tm1_bedrock.Metadata
        )
    except TM1pyRestException as e:
        pytest.fail(f"Cube not found based on MDX: {e}")


@parametrize_from_file
def test_collect_metadata_based_on_mdx_fail(tm1_connection, data_mdx, exception):
    """Runs collect_metadata with bad input for MDX and checks if the method's output is a Metadata object."""
    with pytest.raises(EXCEPTION_MAP[exception]):
        assert isinstance(
            tm1_bedrock.collect_metadata(tm1_service=tm1_connection, mdx=data_mdx),
            tm1_bedrock.Metadata
        )


@parametrize_from_file
def test_collect_metadata_cube_dimensions_not_empty(tm1_connection, cube_name):
    """Collects metadata and verifies that cube dimensions are not empty."""
    try:
        metadata = tm1_bedrock.collect_metadata(tm1_service=tm1_connection, cube_name=cube_name)
        cube_dims = metadata.get_cube_dims()
        assert cube_dims != 0
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_collect_metadata_cube_dimensions_match_dimensions(tm1_connection, cube_name, expected_dimensions):
    """Collects metadata and verifies that cube dimensions match the expected dimensions."""
    try:
        metadata = tm1_bedrock.collect_metadata(tm1_service=tm1_connection, cube_name=cube_name)
        cube_dims = metadata.get_cube_dims()
        assert cube_dims == expected_dimensions
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_collect_metadata_filter_dimensions_not_empty(tm1_connection, cube_name):
    """Collects metadata and verifies that filter dimensions are not empty."""
    try:
        metadata = tm1_bedrock.collect_metadata(tm1_service=tm1_connection, cube_name=cube_name)
        filter_dims = metadata["dimensions"].to_dict()
        assert bool(filter_dims)
    except TM1pyRestException as e:
        pytest.fail(f"Cube name not found: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_mdx_object_builder_is_valid_format_true(tm1_connection, dimensions, cube_name, expected_mdx):
    """Build MDX from cube name and dimension and check if the returned MDX matches the expected."""

    mdx = tm1_bedrock.mdx_object_builder(tm1_service=tm1_connection, cube_name=cube_name, dimension=dimensions)
    mdx = mdx.replace(" ", "")
    expected_mdx = expected_mdx.replace(" ", "")

    assert mdx == expected_mdx


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_success(tm1_connection, data_mdx):
    """Run MDX to dataframe function and verifies that the output is a DataFrame object."""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_fail(tm1_connection, data_mdx):
    """Run MDX to dataframe function with bad input. Raises error."""
    with pytest.raises(TM1pyRestException):
        assert isinstance(
            tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx),
            DataFrame
        )


@parametrize_from_file
def test_normalize_dataframe_is_dataframe_true(tm1_connection, data_mdx):
    """Run normalize dataframe function and check for if output is dataframe"""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        df = tm1_bedrock.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_number_of_dimensions_success(tm1_connection, data_mdx, expected_dimensions):
    """Run normalize dataframe function and check if the output has the correct number of dimensions"""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        df = tm1_bedrock.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        df.keys()
        assert len(df.keys()) == expected_dimensions
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_dimensions_success(tm1_connection, data_mdx, expected_dimensions):
    """Runs normalize dataframe function and validates that the output's dimension keys match the expected"""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        df = tm1_bedrock.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        keys = [key for key in df.keys()]
        assert expected_dimensions == keys
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_mdx_object_builder_create_dataframe_success(tm1_connection ,dimensions, cube_name):
    """Run MDX query created by the MDX builder and verifies that the output is a DataFrame object"""

    data_mdx = tm1_bedrock.mdx_object_builder(tm1_service=tm1_connection, cube_name=cube_name, dimension=dimensions)
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        df = tm1_bedrock.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_dataframe_literal_remap_success(dataframe, mapping, expected_dataframe):
    """Remaps elements based on literal mapping, without dimension manipulation and checks for successful execution"""

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    remapped_df = tm1_bedrock.dataframe_literal_remap(dataframe=df, mapping=mapping)

    pd.testing.assert_frame_equal(remapped_df, expected_df)


@parametrize_from_file
def test_dataframe_literal_remap_fail(dataframe, mapping, expected_dataframe):
    """Tries to remap elements based on literal mapping, without dimension manipulation with bad input. Raises error."""

    expected_df = pd.DataFrame(expected_dataframe)
    with pytest.raises(AssertionError):
        remapped_df = tm1_bedrock.dataframe_literal_remap(dataframe=pd.DataFrame(dataframe), mapping=mapping)
        pd.testing.assert_frame_equal(remapped_df, expected_df)
