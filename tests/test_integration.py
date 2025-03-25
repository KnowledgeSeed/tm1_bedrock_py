import configparser
from pathlib import Path

from TM1py.Exceptions import TM1pyRestException
import pandas as pd
import pytest
import parametrize_from_file

from TM1py import TM1Service

from TM1_bedrock_py import bedrock, extractor, transformer, basic_logger, utility


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

    try:
        tm1 = TM1Service(**config['tm1srv'])
        basic_logger.debug("Successfully connected to TM1.")
        yield tm1

        tm1.logout()
        basic_logger.debug("Connection closed.")

    except TM1pyRestException:
        basic_logger.error("Unable to connect to TM1: ", exc_info=True)


@parametrize_from_file
def test_data_copy_for_single_literal_remap(
        tm1_connection, base_data_mdx, mapping_steps, literal_mapping, output_data_mdx
):
    base_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=base_data_mdx)
    transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=base_df, mdx=base_data_mdx)
    transformer.dataframe_find_and_replace(dataframe=base_df, mapping=literal_mapping)

    data_metadata = utility.TM1CubeObjectMetadata.collect(
        tm1_service=tm1_connection,
        mdx=base_data_mdx
    )
    def metadata_func(**_kwargs): return data_metadata

    extractor.generate_step_specific_mapping_dataframes(
        mapping_steps=mapping_steps,
        tm1_service=tm1_connection
    )

    bedrock.data_copy(
        data_metadata_function=metadata_func,
        tm1_service=tm1_connection, data_mdx=base_data_mdx, mapping_steps=mapping_steps, skip_zeros=True
    )

    copy_test_df = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_connection, data_mdx=output_data_mdx)
    transformer.normalize_dataframe(tm1_service=tm1_connection, dataframe=copy_test_df, mdx=output_data_mdx)

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
        async_write=True,
        logging_level="DEBUG",
        _execution_id=1
    )
