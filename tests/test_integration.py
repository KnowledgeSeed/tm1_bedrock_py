import asyncio
import time

import pandas as pd
import parametrize_from_file
from TM1py.Exceptions import TM1pyRestException
from tm1_bench_py import tm1_bench
from sqlalchemy import text

from TM1_bedrock_py import bedrock, extractor, transformer, utility
from tests.config import tm1_connection_factory, sql_engine_factory
from tests import config as cfg


EXCEPTION_MAP = {
    "ValueError": ValueError,
    "TypeError": TypeError,
    "TM1pyRestException": TM1pyRestException,
    "IndexError": IndexError,
    "KeyError": KeyError
}


@parametrize_from_file
def test_data_copy_for_single_literal_remap(
        tm1_connection_factory, base_data_mdx, mapping_steps, literal_mapping, output_data_mdx
):
    with tm1_connection_factory("tm1srv") as conn:
        base_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=base_data_mdx)
        transformer.normalize_dataframe_for_testing(dataframe=base_df, tm1_service=conn, mdx=base_data_mdx)
        transformer.dataframe_find_and_replace(dataframe=base_df, mapping=literal_mapping)

        data_metadata = utility.TM1CubeObjectMetadata.collect(
            tm1_service=conn,
            mdx=base_data_mdx
        )
        def metadata_func(**_kwargs): return data_metadata

        extractor.generate_step_specific_mapping_dataframes(
            mapping_steps=mapping_steps,
            tm1_service=conn
        )

        bedrock.data_copy(
            target_metadata_function=metadata_func,
            tm1_service=conn, data_mdx=base_data_mdx, mapping_steps=mapping_steps, skip_zeros=True
        )

        copy_test_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=output_data_mdx)
        transformer.normalize_dataframe_for_testing(dataframe=copy_test_df, tm1_service=conn, mdx=output_data_mdx)

        pd.testing.assert_frame_equal(base_df, copy_test_df)


@parametrize_from_file
def test_data_copy_for_multiple_steps(
        tm1_connection_factory, base_data_mdx, shared_mapping, mapping_steps
):
    with tm1_connection_factory("tm1srv") as conn:
        bedrock.data_copy(
            tm1_service=conn,
            shared_mapping=shared_mapping,
            data_mdx=base_data_mdx,
            mapping_steps=mapping_steps,
            clear_target=True,
            target_clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
            skip_zeros=True,
            async_write=True,
            logging_level="WARNING",
            df_verbose_logging=False
        )


@parametrize_from_file
def test_data_copy_for_multiple_steps_with_lazy_input(
        tm1_connection_factory, base_data_mdx, shared_mapping, mapping_steps
):
    utility.set_logging_level("DEBUG")
    with tm1_connection_factory("tm1srv") as conn:
        bedrock.data_copy(
            tm1_service=conn,
            shared_mapping=shared_mapping,
            data_mdx=base_data_mdx,
            mapping_steps=mapping_steps,
            clear_target=True,
            target_clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
            skip_zeros=True,
            async_write=True,
            logging_level="DEBUG",
            case_and_space_insensitive_inputs=True
        )


@parametrize_from_file
def test_data_copy_intercube_for_multiple_steps_with_lazy_input(
        tm1_connection_factory, base_data_mdx, shared_mapping, mapping_steps, target_cube_name
):
    utility.set_logging_level("DEBUG")
    with tm1_connection_factory("tm1srv") as conn:
        bedrock.data_copy_intercube(
            tm1_service=conn,
            shared_mapping=shared_mapping,
            target_cube_name=target_cube_name,
            data_mdx=base_data_mdx,
            mapping_steps=mapping_steps,
            clear_target=True,
            target_clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
            skip_zeros=True,
            async_write=True,
            slice_size_of_dataframe=2,
            use_blob=True,
            logging_level="DEBUG",
            case_and_space_insensitive_inputs=True
        )


@parametrize_from_file
def test_data_copy_intercube_for_multiple_steps(
        tm1_connection_factory, base_data_mdx, shared_mapping, mapping_steps, target_cube_name
):
    with tm1_connection_factory("tm1srv") as conn:
        bedrock.data_copy_intercube(
            tm1_service=conn,
            shared_mapping=shared_mapping,
            target_cube_name=target_cube_name,
            data_mdx=base_data_mdx,
            mapping_steps=mapping_steps,
            clear_target=True,
            target_clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
            skip_zeros=True,
            async_write=True,
            slice_size_of_dataframe=2,
            use_blob=True,
            logging_level="DEBUG",

        )


@parametrize_from_file
def test_data_copy_intercube_for_multiple_steps_with_custom_function(
        tm1_connection_factory, base_data_mdx, shared_mapping, mapping_steps, target_cube_name
):
    def do_nothing(df: pd.DataFrame, arg1: int, arg2: int, kwarg1: int, kwarg2: int) -> pd.DataFrame:
        arg_kwarg_sum = arg1 + arg2 + kwarg1 + kwarg2
        print(str(arg_kwarg_sum))
        return df

    do_nothing_args = [1, 2]
    do_nothing_kwargs = {"kwarg1": 3, "kwarg2": 4}

    with tm1_connection_factory("tm1srv") as conn:
        bedrock.data_copy_intercube(
            tm1_service=conn,
            shared_mapping=shared_mapping,
            target_cube_name=target_cube_name,
            data_mdx=base_data_mdx,
            mapping_steps=mapping_steps,
            clear_target=True,
            target_clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
            skip_zeros=True,
            pre_load_function=do_nothing,
            pre_load_args=do_nothing_args,
            pre_load_kwargs=do_nothing_kwargs,
            async_write=True,
            slice_size_of_dataframe=2,
            use_blob=True,
            logging_level="DEBUG",

        )


@parametrize_from_file
def test_data_copy_intercube_for_multiple_steps_with_itemskip_fallback(
        tm1_connection_factory, base_data_mdx, shared_mapping, mapping_steps, target_cube_name
):
    with tm1_connection_factory("tm1srv") as conn:
        bedrock.data_copy_intercube(
            tm1_service=conn,
            shared_mapping=shared_mapping,
            target_cube_name=target_cube_name,
            data_mdx=base_data_mdx,
            mapping_steps=mapping_steps,
            clear_target=True,
            target_clear_set_mdx_list=["{[Versions].[Versions].[DataCopy Integration Test]}"],
            skip_zeros=True,
            ignore_missing_elements=True,
            fallback_elements={"Versions": "DataCopy Integration Test"},
            async_write=True,
            slice_size_of_dataframe=2,
            use_blob=True,
            logging_level="DEBUG",
            #verbose_logging_mode="print_console"
        )


@parametrize_from_file
def test_async_data_copy_intercube(
        tm1_connection_factory, param_set_mdx_list, data_mdx_template, target_clear_set_mdx_list,
        target_cube_name, shared_mapping, mapping_steps
):
    with tm1_connection_factory("tm1srv") as conn:
        utility.set_logging_level("DEBUG")
        start_time = time.gmtime()
        start_time_total = time.time()
        print('Start time: ')
        print(time.strftime('{%Y%m%d %H:%M}', start_time))
        asyncio.run(bedrock.async_executor_tm1(
            data_copy_function=bedrock.data_copy_intercube,
            tm1_service=conn,
            data_mdx_template=data_mdx_template,
            skip_zeros=True,
            skip_consolidated_cells=True,
            target_cube_name=target_cube_name,
            shared_mapping=shared_mapping,
            mapping_steps=mapping_steps,
            clear_target=True,
            async_write=True,
            logging_level="DEBUG",
            param_set_mdx_list=param_set_mdx_list,
            target_clear_set_mdx_list=target_clear_set_mdx_list,
            ignore_missing_elements=True,
            max_workers=8
        ))
        run_time = time.time() - start_time_total
        print('Time: {:.4f} sec'.format(run_time))


@parametrize_from_file
def test_async_data_copy_intercube_multi_parameter(
        tm1_connection_factory, param_set_mdx_list, data_mdx_template, target_clear_set_mdx_list,
        target_cube_name, shared_mapping, mapping_steps
):
    with tm1_connection_factory("tm1srv") as conn:
        utility.set_logging_level("DEBUG")
        start_time = time.gmtime()
        start_time_total = time.time()
        print('Start time: ')
        print(time.strftime('{%Y%m%d %H:%M}', start_time))
        asyncio.run(bedrock.async_executor_tm1(
            data_copy_function=bedrock.data_copy_intercube,
            tm1_service=conn,
            data_mdx_template=data_mdx_template,
            skip_zeros=True,
            skip_consolidated_cells=True,
            target_cube_name=target_cube_name,
            shared_mapping=shared_mapping,
            mapping_steps=mapping_steps,
            clear_target=True,
            logging_level="INFO",
            param_set_mdx_list=param_set_mdx_list,
            target_clear_set_mdx_list=target_clear_set_mdx_list,
            ignore_missing_elements=True,
            df_verbose_logging=False
        ))
        run_time = time.time() - start_time_total
        print('Time: {:.4f} sec'.format(run_time))


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> SQL data copy integration tests
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_load_tm1_cube_to_sql_table(
        tm1_connection_factory, sql_engine_factory, base_data_mdx, mapping_steps, related_dimensions
):
    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_postgres") as sql_engine:

            # >>> ADD THESE TWO LINES FOR DIAGNOSIS <<<
            print(f"DIALECT NAME: {sql_engine.dialect.name}")
            #print(f"FAST_EXECUTEMANY ENABLED: {sql_engine.dialect.fast_executemany}")
            # >>> END DIAGNOSIS <<<

            envname = 'bedrock_test_10000'
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
                bedrock.load_tm1_cube_to_sql_table(
                    tm1_service=conn,
                    target_table_name="testbench_sales",
                    sql_engine=sql_engine,
                    data_mdx=base_data_mdx,
                    mapping_steps=mapping_steps,
                    clear_target=True,
                    sql_delete_statement="TRUNCATE TABLE tm1_bedrock.testbench_sales",
                    sql_schema="tm1_bedrock",
                    skip_zeros=True,
                    logging_level="DEBUG",
                    index=False,
                    related_dimensions=related_dimensions,
                    method=None,
                )
                cnt = sql_engine.execute(text("SELECT count(*) FROM tm1_bedrock.testbench_sales")).scalar()
                print("rows:", cnt)
            finally:
                print("Execution ended.")
                tm1_bench.destroy_model(tm1=conn, schema=schema)


@parametrize_from_file
def test_load_sql_data_to_tm1_cube(
        tm1_connection_factory, sql_engine_factory, sql_query, mapping_steps, clear_set_mdx_list, sql_column_mapping, sql_columns_to_keep, expected_mdx
):
    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_postgres") as sql_engine:
            envname = 'bedrock_test_10000'
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
                bedrock.load_sql_data_to_tm1_cube(
                    tm1_service=conn,
                    target_cube_name="testbenchSales",
                    sql_schema="tm1_bedrock",
                    sql_engine=sql_engine,
                    sql_query=sql_query,
                    mapping_steps=mapping_steps,
                    target_clear_set_mdx_list=clear_set_mdx_list,
                    sql_column_mapping=sql_column_mapping,
                    sql_columns_to_keep=sql_columns_to_keep,
                    sql_value_column_name="testbench_value",
                    clear_target=True,
                    skip_zeros=True,
                    logging_level="DEBUG",
                    index=False
                )
                df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=expected_mdx)
                print(df)
            finally:
                print("Execution ended.")
                tm1_bench.destroy_model(tm1=conn, schema=schema)


@parametrize_from_file
def test_async_load_tm1_cube_to_sql_table(
        tm1_connection_factory, sql_engine_factory, data_mdx_template, mapping_steps, param_set_mdx_list, related_dimensions
):
    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_postgres") as sql_engine:
            envname = 'bedrock_test_10000'
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
                asyncio.run(bedrock.async_executor_tm1_to_sql(
                    data_copy_function=bedrock.load_tm1_cube_to_sql_table,
                    tm1_service=conn,
                    sql_engine=sql_engine,
                    target_table_name="testbench_sales",
                    sql_schema="tm1_bedrock",
                    data_mdx_template=data_mdx_template,
                    mapping_steps=mapping_steps,
                    param_set_mdx_list=param_set_mdx_list,
                    sql_delete_statement="TRUNCATE TABLE tm1_bedrock.testbench_sales",
                    related_dimensions=related_dimensions,
                    clear_target=True,
                    skip_zeros=True,
                    logging_level="DEBUG",
                    index=False,
                    decimal=",",
                    max_workers=8,
                ))
                cnt = sql_engine.execute(text("SELECT count(*) FROM tm1_bedrock.testbench_sales")).scalar()
                print("rows:", cnt)
            finally:
                print("Execution ended.")
                tm1_bench.destroy_model(tm1=conn, schema=schema)


@parametrize_from_file
def test_async_load_sql_data_to_tm1_cube(
        tm1_connection_factory, sql_engine_factory, sql_query_template, mapping_steps, param_set_mdx_list, sql_column_mapping, sql_columns_to_keep
):
    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_postgres") as sql_engine:
            envname = 'bedrock_test_10000'
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
                asyncio.run(bedrock.async_executor_sql_to_tm1(
                    data_copy_function=bedrock.load_sql_data_to_tm1_cube,
                    tm1_service=conn,
                    target_cube_name="testbenchSales",
                    sql_schema="tm1_bedrock",
                    sql_engine=sql_engine,
                    sql_query_template=sql_query_template,
                    sql_table_for_count="tm1_bedrock.testbench_sales",
                    slice_size=5000,
                    mapping_steps=mapping_steps,
                    target_clear_set_mdx_list=["{[testbenchVersion].[testbenchVersion].[ForeCast]}"],
                    param_set_mdx_list=param_set_mdx_list,
                    sql_column_mapping=sql_column_mapping,
                    sql_columns_to_keep=sql_columns_to_keep,
                    sql_value_column_name="testbench_value",
                    skip_zeros=True,
                    logging_level="DEBUG",
                    decimal=',',
                    index=False,
                    max_workers=8
                ))
            finally:
                print("Execution ended.")
                tm1_bench.destroy_model(tm1=conn, schema=schema)


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> CSV data copy integration tests
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_load_tm1_cube_to_csv_file(
        tm1_connection_factory, base_data_mdx, mapping_steps
):
    with tm1_connection_factory("testbench") as conn:
        envname = 'bedrock_test_10000'
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            server_name = conn.server.get_server_name()
            print("Connection to TM1 established! Your server name is: {}".format(server_name))

            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
            bedrock.load_tm1_cube_to_csv_file(
                tm1_service=conn,
                data_mdx=base_data_mdx,
                mapping_steps=mapping_steps,
                clear_target=True,
                skip_zeros=True,
                logging_level="DEBUG",
                target_csv_output_dir="./dataframe_to_csv",
                target_csv_file_name="sample_data.csv",
                decimal=".",
                delimiter=",",
                float_format='%.f'
            )
        finally:
            print("Execution ended.")
            tm1_bench.destroy_model(tm1=conn, schema=schema)


@parametrize_from_file
def test_load_csv_data_to_tm1_cube(
        tm1_connection_factory, base_data_mdx, mapping_steps, expected_mdx
):
    with tm1_connection_factory("testbench") as conn:
        envname = 'bedrock_test_10000'
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
            bedrock.load_csv_data_to_tm1_cube(
                tm1_service=conn,
                source_csv_file_path="dataframe_to_csv/sample_data.csv",
                data_mdx=base_data_mdx,
                mapping_steps=mapping_steps,
                target_cube_name="testbenchSales",
                clear_target=False,
                target_clear_set_mdx_list=["{[testbenchVersion].[testbenchVersion].[ForeCast]}"],
                skip_zeros=True,
                async_write=False,
                logging_level="DEBUG",
                decimal=".",
                delimiter=",",
                df_verbose_logging=False
            )
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=expected_mdx)
            print(df)
        finally:
            print("Execution ended.")
            tm1_bench.destroy_model(tm1=conn, schema=schema)


@parametrize_from_file
def test_async_load_csv_data_to_tm1_cube(
        tm1_connection_factory, data_mdx_template, mapping_steps, param_set_mdx_list, target_clear_set_mdx_list
):
    with tm1_connection_factory("testbench") as conn:
        envname = 'bedrock_test_10000'
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
            asyncio.run(bedrock.async_executor_csv_to_tm1(
                data_copy_function=bedrock.load_csv_data_to_tm1_cube,
                tm1_service=conn,
                source_directory="D:\\tm1-bedrock-benchmark\\tm1_to_csv\\8_10000_0",
                data_mdx_template=data_mdx_template,
                skip_zeros=True,
                skip_consolidated_cells=True,
                target_cube_name="testbenchSales",
                mapping_steps=mapping_steps,
                clear_target=True,
                logging_level="DEBUG",
                param_set_mdx_list=param_set_mdx_list,
                target_clear_set_mdx_list=target_clear_set_mdx_list,
                ignore_missing_elements=True,
                decimal=",",
                delimiter=";",
                df_verbose_logging=False,
                async_write=False,
                max_workers=8
            ))
        finally:
            print("Execution ended.")
            tm1_bench.destroy_model(tm1=conn, schema=schema)
