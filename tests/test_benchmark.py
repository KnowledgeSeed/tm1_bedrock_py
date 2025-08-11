import asyncio
import json
import logging
import statistics

import matplotlib.pyplot as plt
from sqlalchemy import types, text
import pytest
from tm1_bench_py import tm1_bench

from TM1_bedrock_py import bedrock, basic_logger, benchmark_metrics_logger
from tests import config as cfg
from tests.config import tm1_connection_factory, sql_engine_factory


# ---------------------------------------------------------------------------------------------------------------------
# Bench test utility
# ---------------------------------------------------------------------------------------------------------------------


def load_benchmark_logs(run_ids):
    file_path_template = "../logs/TM1_bedrock_py_benchmark_{run_id}.json.log"
    file_path = [file_path_template.format(run_id=run_id) for run_id in run_ids]
    data = {}

    for run_id, file in zip(run_ids, file_path):
        run_log_data = []
        try:
            with open(file, "r") as f:
                for line_number, line in enumerate(f, 1):
                    stripped_line = line.strip()
                    if not stripped_line:
                        continue

                    try:
                        json_object = json.loads(stripped_line)
                        run_log_data.append(json_object)

                    except json.JSONDecodeError as e:
                        print(f"Warning: Skipping invalid JSON in file '{file_path}' at line {line_number}: {e}")

        except FileNotFoundError:
            print(f"Error: File not found for run_id {run_id}: {file_path}")
        except Exception as e:
            print(f"Error reading file '{file_path}': {e}")

        data[run_id] = run_log_data

    return data


def get_exec_data(test_cases):
    run_ids = [f"{nr_core}_{nr_record}_{n}" for nr_core, nr_record, n in test_cases]
    number_of_runs = max([n for nr_core, nr_record, n in test_cases])
    data = load_benchmark_logs(run_ids)
    total_runtimes = {}
    for run_id in run_ids:
        run_logs = data.get(run_id)
        if len(run_logs) > 0:
            exec_time_msg = float(run_logs[-1].get("msg").split(" ")[1])
        else: exec_time_msg = None
        total_runtimes[run_id] = exec_time_msg

    partial_keys = list(set([f"{nr_core}_{nr_record}" for nr_core, nr_record, n in test_cases]))
    partial_keys = sorted(partial_keys, key=lambda x: int(x.replace('_', '')))
    identical_cases = {}
    for key in partial_keys:
        values = []
        for i in range(number_of_runs+1):
            values.append(total_runtimes.get(f"{key}_{i}"))
        identical_cases[key] = values

    return identical_cases


def calculate_average_run_time(results: list):
    if results:
        min_time = min(results)
        max_time = max(results)
        avg_time = statistics.mean(results)
        stdev_time = statistics.stdev(results) if len(results) > 1 else 0.0

        return {'min': min_time, 'max': max_time, 'avg': avg_time, 'stdev': stdev_time}
    else: return {}


def test_process_benchmark_results():
    combinations = cfg.benchmark_testcase_parameters()
    cases = get_exec_data(combinations)
    keys = cases.keys()
    for key in keys:
        cases[key] = calculate_average_run_time(cases.get(key))

    combinations = cfg.benchmark_testcase_parameters()
    cores = list(set([core for core, record, n in combinations]))

    plot_average_times(cases, combinations)

    for core in cores:
        results = {}
        for key in cases.keys():
            if key.startswith(f"{core}_"):
                results[key] = cases.get(key)
        plot_statistics_for_core(results, combinations, core)


def plot_average_times(cases, combinations):
    records = sorted(list(set([record for core, record, n in combinations])))
    cores = sorted(list(set([core for core, record, n in combinations])))
    fig, ax = plt.subplots(figsize=(10, 6))

    for core in cores:
        avg_times = []

        for i in range(len(records)):
            key = f"{core}_{records[i]}"
            if key in cases:
                avg_times.append(cases[key]['avg'])
                ax.annotate(f"{avg_times[i]:.2f} s", (records[i], avg_times[i]),
                            textcoords="offset points", xytext=(-5, 5), ha='center', fontsize=10)
            else:
                avg_times.append(None)

        ax.plot(records, avg_times, marker='o', linestyle='-', label=f'{core} Cores')

    ax.set_xlabel("Number of Records")
    ax.set_ylabel("Average Execution Time [s]")
    ax.set_title("Average Execution Time vs. Number of Records (All Cores)")
    ax.legend()
    ax.grid(True, linestyle='--', alpha=0.6)
    plt.tight_layout()
    plt.xscale('log', base=10)
    plt.yscale('log', base=10)

    plt.savefig("benchmark_results_avg_times.png")


def plot_statistics_for_core(results, combinations, nr_cores):
    if results:
        records = sorted(list(set([record for core, record, n in combinations])))

        min_times = []
        max_times = []
        avg_times = []
        stdev_times_pos = []
        stdev_times_neg = []

        fig, ax = plt.subplots(figsize=(10, 6))

        for key in results:
            values = results.get(key)
            min_times.append(values['min'])
            max_times.append(values['max'])
            avg_times.append(values['avg'])
            stdev_times_pos.append(values['avg'] + values['stdev'])
            stdev_times_neg.append(values['avg'] - values['stdev'])

        ax.plot(records, min_times, label='Minimum Time', marker='^', linestyle='', color='black')
        ax.plot(records, max_times, label='Maximum Time', marker='v', linestyle='', color='black')
        for i in range(len(records)):
            ax.plot([records[i], records[i]], [min_times[i], max_times[i]], color='black', linewidth=0.5)

            ax.annotate(f"{min_times[i]:.2f} s", (records[i], min_times[i]),
                        textcoords="offset points", xytext=(0, -15), ha='right', fontsize=10)
            ax.annotate(f"{max_times[i]:.2f} s", (records[i], max_times[i]),
                        textcoords="offset points", xytext=(0, 10), ha='right', fontsize=10)
            ax.annotate(f"{avg_times[i]:.2f} s", (records[i], avg_times[i]),
                        textcoords="offset points", xytext=(5, 5), ha='left', fontsize=10, fontweight="bold")

        ax.plot(records, stdev_times_pos, linestyle='', color='lightblue')
        ax.plot(records, stdev_times_neg, linestyle='', color='lightblue')
        ax.fill_between(records, stdev_times_neg, stdev_times_pos, color='lightblue', alpha=0.3, label='Standard Deviation')

        ax.plot(records, avg_times, label='Average Time', marker='o', linestyle='-', linewidth=2)


        ax.set_xlabel("Number of Records")
        ax.set_ylabel(f'Execution Time [s]')
        ax.set_title(f'Execution times based on number of records for {nr_cores} cores')

        ax.legend()
        ax.grid(True, linestyle='--', alpha=0.6)
        plt.tight_layout()
        plt.xscale('log', base=10)
        plt.yscale('log', base=10)

        plt.savefig(f"benchmark_for_{nr_cores}_cores.png")



def update_benchmark_log_handler(run_id):
    for handler in benchmark_metrics_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.close()
            handler.baseFilename = f"../logs/TM1_bedrock_py_benchmark_{run_id}.json.log"
            handler.stream = open(handler.baseFilename, handler.mode)
            break


# ---------------------------------------------------------------------------------------------------------------------
# Bench test main
# ---------------------------------------------------------------------------------------------------------------------


@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_single_benchmark_case(tm1_connection_factory, nr_of_cores, nr_of_records, n):
    with tm1_connection_factory("testbench") as conn:

        update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

        fix_kwargs = {
            "tm1_service": conn,
            "data_mdx_template": cfg.DATA_MDX_TEMPLATE,
            "skip_zeros": True,
            "skip_consolidated_cells": True,
            "target_cube_name": cfg.TARGET_CUBE_NAME,
            "mapping_steps": cfg.MAPPING_STEPS,
            "clear_target": True,
            "logging_level": "DEBUG",
            "param_set_mdx_list": cfg.PARAM_SET_MDX_LIST,
            "clear_param_templates": cfg.CLEAR_PARAM_TEMPLATES,
            "target_dim_mapping": cfg.TARGET_DIM_MAPPING
        }
        basic_logger.info(f"Execution starting with {nr_of_cores} workers")

        envname = 'bedrock_test_' + str(nr_of_records)
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)

            asyncio.run(bedrock.async_executor_tm1(
                data_copy_function=bedrock.data_copy_intercube,
                max_workers=nr_of_cores,
                df_verbose_logging=False,
                **fix_kwargs
            ))
        finally:
            print("exec ended")
            tm1_bench.destroy_model(tm1=conn, schema=schema)

        basic_logger.info(f"Execution ended with {nr_of_cores} workers")


@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_single_benchmark_case_data_copy(tm1_connection_factory, nr_of_cores, nr_of_records, n):

    with tm1_connection_factory("testbench") as conn:
        update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

        server_name = conn.server.get_server_name()
        print("Connection to TM1 established! Your server name is: {}".format(server_name))

        fix_kwargs = {
            "tm1_service": conn,
            "data_mdx_template": cfg.DATA_MDX_TEMPLATE,
            "skip_zeros": True,
            "skip_consolidated_cells": True,
            "target_cube_name": "testbenchSales",
            "mapping_steps": cfg.MAPPING_STEPS_INTRACUBE,
            "clear_target": True,
            "logging_level": "DEBUG",
            "use_blob": True,
            "param_set_mdx_list": cfg.PARAM_SET_MDX_LIST,
            "clear_param_templates": cfg.CLEAR_PARAM_TEMPLATES
        }
        basic_logger.info(f"Execution starting with {nr_of_cores} workers")

        envname = 'bedrock_test_' + str(nr_of_records)
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
            asyncio.run(bedrock.async_executor_tm1(
                data_copy_function=bedrock.data_copy,
                max_workers=nr_of_cores,
                df_verbose_logging=False,
                **fix_kwargs
            ))
        finally:
            print("exec ended")
            tm1_bench.destroy_model(tm1=conn, schema=schema)

        basic_logger.info(f"Execution ended with {nr_of_cores} workers")


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> CSV data copy bench tests
# ------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_single_benchmark_case_tm1_to_csv(tm1_connection_factory, nr_of_cores, nr_of_records, n):

    with tm1_connection_factory("testbench") as conn:
        update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

        server_name = conn.server.get_server_name()
        print("Connection to TM1 established! Your server name is: {}".format(server_name))

        fix_kwargs = {
            "tm1_service": conn,
            "data_mdx_template": cfg.DATA_MDX_TEMPLATE,
            "skip_zeros": True,
            "skip_consolidated_cells": True,
            "source_cube_name": "testbenchSales",
            "mapping_steps": cfg.MAPPING_STEPS_INTRACUBE,
            "clear_target": True,
            "logging_level": "DEBUG",
            "use_blob": True,
            "param_set_mdx_list": cfg.PARAM_SET_MDX_LIST,
            "clear_param_templates": cfg.CLEAR_PARAM_TEMPLATES,
            "target_csv_output_dir": f"../tests/tm1_to_csv/{nr_of_cores}_{nr_of_records}_{n}",
            "decimal": ",",
            "delimiter": ";"
        }
        basic_logger.info(f"Execution starting with {nr_of_cores} workers")

        envname = 'bedrock_test_' + str(nr_of_records)
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)

            asyncio.run(bedrock.async_executor_tm1(
                data_copy_function=bedrock.load_tm1_cube_to_csv_file,
                max_workers=nr_of_cores,
                df_verbose_logging=False,
                **fix_kwargs
            ))

        finally:
            print("exec ended")
            tm1_bench.destroy_model(tm1=conn, schema=schema)

        basic_logger.info(f"Execution ended with {nr_of_cores} workers")


@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_single_benchmark_case_csv_to_tm1(tm1_connection_factory, nr_of_cores, nr_of_records, n):
    update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

    with tm1_connection_factory("testbench") as conn:
        server_name = conn.server.get_server_name()
        print("Connection to TM1 established! Your server name is: {}".format(server_name))

        fix_kwargs = {
            "tm1_service": conn,
            "target_cube_name": "testbenchSales",
            "data_mdx_template": cfg.DATA_MDX_TEMPLATE,
            "skip_zeros": True,
            "skip_consolidated_cells": True,
            "mapping_steps": cfg.MAPPING_STEPS_INTRACUBE,
            "clear_target": True,
            "logging_level": "DEBUG",
            "use_blob": True,
            "param_set_mdx_list": cfg.PARAM_SET_MDX_LIST,
            "clear_param_templates": cfg.CLEAR_PARAM_TEMPLATES,
            "decimal": ",",
            "delimiter": ";",
            "async_write": False
        }
        basic_logger.info(f"Execution starting with {nr_of_cores} workers")

        envname = 'bedrock_test_' + str(nr_of_records)
        schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
        schema = schemaloader.load_schema()
        default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
        try:
            tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)

            asyncio.run(bedrock.async_executor_csv_to_tm1(
                data_copy_function=bedrock.load_csv_data_to_tm1_cube,
                source_directory=f"../tests/tm1_to_csv/tm1_to_csv/8_{nr_of_records}_0",
                max_workers=nr_of_cores,
                df_verbose_logging=False,
                **fix_kwargs
            ))

        finally:
            print("exec ended")
            tm1_bench.destroy_model(tm1=conn, schema=schema)

        basic_logger.info(f"Execution ended with {nr_of_cores} workers")


# ------------------------------------------------------------------------------------------------------------
# TM1 <-> SQL data copy bench tests
# ------------------------------------------------------------------------------------------------------------

@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_sync_benchmark_case_tm1_to_sql(tm1_connection_factory, sql_engine_factory, nr_of_cores, nr_of_records, n):
    update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_mssql") as sql_engine:

            # >>> ADD THESE TWO LINES FOR DIAGNOSIS <<<
            print(f"DIALECT NAME: {sql_engine.dialect.name}")
            print(f"FAST_EXECUTEMANY ENABLED: {sql_engine.dialect.fast_executemany}")
            # >>> END DIAGNOSIS <<<

            basic_logger.info(f"Execution starting with {nr_of_cores} workers")

            envname = 'bedrock_test_' + str(nr_of_records)
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)
                bedrock.load_tm1_cube_to_sql_table(
                    tm1_service=conn,
                    target_table_name="testbenchSales",
                    sql_engine=sql_engine,
                    data_mdx=cfg.BASE_DATA_MDX,
                    mapping_steps=cfg.MAPPING_STEPS_INTRACUBE,
                    clear_target=True,
                    sql_delete_statement="TRUNCATE TABLE testbenchSales",
                    sql_schema="dbo",
                    skip_zeros=True,
                    logging_level="DEBUG",
                    index=False,
                    related_dimensions={"Value": "testbenchValue"},
                    method=None,
                )
                cnt = sql_engine.execute(text("SELECT count(*) FROM testbenchSales")).scalar()
                basic_logger.info(f"rows: {cnt}")
            finally:
                print("Execution ended.")
                tm1_bench.destroy_model(tm1=conn, schema=schema)


@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_single_benchmark_case_tm1_to_sql(tm1_connection_factory, sql_engine_factory, nr_of_cores, nr_of_records, n):
    update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_mssql") as sql_engine:
            server_name = conn.server.get_server_name()
            print("Connection to TM1 established! Your server name is: {}".format(server_name))
            dtype = {
                'testbenchVersion': types.VARCHAR(50),
                'testbenchPeriod': types.VARCHAR(50),
                'testbenchMeasureSales': types.VARCHAR(50),
                'testbenchProduct': types.VARCHAR(50),
                'testbenchCustomer': types.VARCHAR(50),
                'testbenchKeyAccountManager': types.VARCHAR(50),
                'Value': types.FLOAT
            }

            fix_kwargs = {
                "tm1_service": conn,
                "target_table_name": "testbenchSales",
                "data_mdx_template": cfg.DATA_MDX_TEMPLATE,
                "skip_zeros": True,
                "skip_consolidated_cells": True,
                "mapping_steps": [],
                "clear_target": True,
                "logging_level": "DEBUG",
                "use_blob": True,
                "chunksize": 10000,
                "param_set_mdx_list": cfg.PARAM_SET_MDX_LIST,
                "clear_param_templates": cfg.CLEAR_PARAM_TEMPLATES,
                "dtype": dtype,
                "decimal": ",",
                "async_write": False,
                "sql_delete_statement": cfg.SQL_DELETE_STATEMENT,
                "index": False,
                "method": "multi"
            }
            basic_logger.info(f"Execution starting with {nr_of_cores} workers")

            envname = 'bedrock_test_' + str(nr_of_records)
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)

                asyncio.run(bedrock.async_executor_tm1_to_sql(
                    data_copy_function=bedrock.load_tm1_cube_to_sql_table,
                    sql_engine=sql_engine,
                    max_workers=nr_of_cores,
                    df_verbose_logging=False,
                    **fix_kwargs
                ))
            finally:
                print("exec ended")
                tm1_bench.destroy_model(tm1=conn, schema=schema)

            basic_logger.info(f"Execution ended with {nr_of_cores} workers")


@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", cfg.benchmark_testcase_parameters())
def test_run_single_benchmark_case_sql_to_tm1(tm1_connection_factory, sql_engine_factory, nr_of_cores, nr_of_records, n):
    update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

    with tm1_connection_factory("testbench") as conn:
        with sql_engine_factory("testbench_mssql") as sql_engine:
            server_name = conn.server.get_server_name()
            print("Connection to TM1 established! Your server name is: {}".format(server_name))

            fix_kwargs = {
                "tm1_service": conn,
                "target_cube_name": "testbenchSales",
                "sql_query_template": cfg.SQL_QUERY_TEMPLATE,
                "sql_table_for_count": "BENCHMARK.dbo.testbenchSales",
                "slice_size": 200000,
                "skip_zeros": True,
                "skip_consolidated_cells": True,
                "mapping_steps": cfg.MAPPING_STEPS_INTRACUBE,
                "logging_level": "DEBUG",
                "use_blob": True,
                "clear_target": True,
                "target_clear_set_mdx_list": ["{[testbenchVersion].[testbenchVersion].[ForeCast]}"],
                "decimal": ",",
                "async_write": False,
                "index": False
            }
            basic_logger.info(f"Execution starting with {nr_of_cores} workers")

            envname = 'bedrock_test_' + str(nr_of_records)
            schemaloader = tm1_bench.SchemaLoader(cfg.SCHEMA_DIR, envname)
            schema = schemaloader.load_schema()
            default_df_to_cube_kwargs = schema['config']['df_to_cube_default_kwargs']
            try:
                tm1_bench.build_model(tm1=conn, schema=schema, env=envname, system_defaults=default_df_to_cube_kwargs)

                asyncio.run(bedrock.async_executor_sql_to_tm1(
                    data_copy_function=bedrock.load_sql_data_to_tm1_cube,
                    sql_engine=sql_engine,
                    max_workers=nr_of_cores,
                    df_verbose_logging=False,
                    **fix_kwargs
                ))

            finally:
                print("exec ended")
                tm1_bench.destroy_model(tm1=conn, schema=schema)

            basic_logger.info(f"Execution ended with {nr_of_cores} workers")
