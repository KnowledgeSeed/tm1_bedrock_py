import configparser
from pathlib import Path

from TM1py.Exceptions import TM1pyRestException
import pandas as pd
import pytest
import parametrize_from_file

from TM1py import TM1Service

from TM1_bedrock_py import bedrock, extractor, transformer


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


@parametrize_from_file
def test_data_copy_for_single_literal_remap(
        tm1_connection, base_data_mdx, mapping_steps, literal_mapping, output_data_mdx
):
    base_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=base_data_mdx)
    base_df = transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=base_df, mdx=base_data_mdx)
    base_df = transformer.dataframe_find_and_replace(dataframe=base_df, mapping=literal_mapping)

    bedrock.data_copy(tm1_service=tm1_connection, data_mdx=base_data_mdx, mapping_steps=mapping_steps, skip_zeros=True)

    copy_test_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=output_data_mdx)
    copy_test_df = transformer.normalize_dataframe(
        tm1_service=tm1_connection, dataframe=copy_test_df, mdx=output_data_mdx
    )

    pd.testing.assert_frame_equal(base_df, copy_test_df)


@parametrize_from_file
def test_data_copy_for_multiple_steps(
        tm1_connection, base_data_mdx, shared_mapping, mapping_steps
):
    bedrock.data_copy(
        tm1_service=tm1_connection,
        shared_mapping=shared_mapping,
        data_mdx=base_data_mdx,
        mapping_steps=mapping_steps,
        clear_target=True,
        clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
        skip_zeros=True,
        skip_consolidated_cells=True
    )

