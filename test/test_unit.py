import configparser
from pandas.core.frame import DataFrame
import pandas as pd
import pytest
import parametrize_from_file
from TM1py import TM1Service

from tm1_bedrock_py import tm1_bedrock


@pytest.fixture(scope="session")
def tm1_connection():
    """Creates a TM1 connection before tests and closes it after all tests."""
    config = configparser.ConfigParser()
    config.read(r'..\config.ini')

    tm1 = TM1Service(**config['tm1srv'])
    yield tm1
    tm1.logout()


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_mdx_object_builder_valid_format(tm1_connection, dimensions, cube_name, expected_mdx):
    """Build MDX from cube name and dimension and check if the returned MDX matches the expected."""

    mdx = tm1_bedrock.mdx_object_builder(tm1_service=tm1_connection, cube_name=cube_name, dimension=dimensions)
    mdx = mdx.replace(" ", "")
    expected_mdx = expected_mdx.replace(" ", "")

    assert mdx == expected_mdx


@parametrize_from_file
def test_mdx_to_dataframe_execute_query(tm1_connection, data_mdx):
    """Run MDX query created by the MDX builder and check for successful execution"""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_is_dataframe(tm1_connection, data_mdx):
    """Run normalize dataframe function and check for if output is dataframe"""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        df = tm1_bedrock.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        assert isinstance(df, DataFrame)
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_dimensions(tm1_connection, data_mdx, expected_dimensions):
    """Run normalize dataframe function and check if the output has the correct number of dimensions"""
    try:
        df = tm1_bedrock.mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=data_mdx)
        df = tm1_bedrock.normalize_dataframe(tm1_service=tm1_connection, dataframe=df, mdx=data_mdx)
        df.keys()
        assert len(df.keys()) == expected_dimensions
    except Exception as e:
        pytest.fail(f"MDX query execution failed: {e}")


# TODO: def test_normalize_dataframe_check_columns()


@parametrize_from_file
def test_mdx_builder_execute_query(tm1_connection ,dimensions, cube_name):
    """Run MDX query created by the MDX builder and check for successful execution"""

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
def test_dataframe_literal_remap(dataframe, mapping, expected_dataframe):
    """Remaps elements based on literal mapping, without dimension manipulation and checks for successful execution"""

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    remapped_df = tm1_bedrock.dataframe_literal_remap(dataframe=df, mapping=mapping)

    pd.testing.assert_frame_equal(remapped_df, expected_df)


@parametrize_from_file
def test_dataframe_settings_remap(main_dataframe, mapping_dataframe, target_mapping, expected_dataframe):
    """
    Remaps dimensions in the main DataFrame using values from the mapping DataFrame.
    Fails for 3rd run, where shared dimensions are missing in the main DataFrame.
    """

    main_df = pd.DataFrame(main_dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)

    try:
        remapped_df = tm1_bedrock.dataframe_settings_remap(main_dataframe=main_df, mapping_dataframe=mapping_df, target_mapping=target_mapping)
        expected_df = pd.DataFrame(expected_dataframe)
        pd.testing.assert_frame_equal(remapped_df, expected_df)
    except IndexError:
        tm1_bedrock.dataframe_settings_remap(main_dataframe=main_df, mapping_dataframe=mapping_df, target_mapping=target_mapping)
