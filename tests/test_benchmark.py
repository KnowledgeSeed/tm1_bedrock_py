import configparser
import itertools
import logging
import os
import site
import sys
import tracemalloc
from pathlib import Path
from pipes import Template

import yaml
import json
from string import Template
from TM1py.Exceptions import TM1pyRestException
import pytest
import time
import asyncio
import statistics
import matplotlib.pyplot as plt

from TM1py import TM1Service
from matplotlib.testing.decorators import image_comparison

from TM1_bedrock_py import bedrock, basic_logger, benchmark_metrics_logger

benchmark_metrics_logger.setLevel("WARNING")

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


def input_data():
    num_runs = 2
    identical_run_ids = [i for i in range(num_runs)]
    number_of_cores = [4, 8]
    number_of_records = [10000, 25000]
    combinations = list(itertools.product(number_of_cores, number_of_records, identical_run_ids))

    return combinations


def load_config(yaml_filepath):
    """Loads parameter configuration from a YAML file."""
    try:
        with open(yaml_filepath, 'r') as f:
            config = yaml.safe_load(f)
            if not isinstance(config, dict):
                raise ValueError("YAML content should be a dictionary (mapping).")
            return config
    except FileNotFoundError:
        print(f"Error: YAML file not found at '{yaml_filepath}'", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"Error parsing YAML file '{yaml_filepath}': {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"Error in YAML structure: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"An unexpected error occurred loading the config: {e}", file=sys.stderr)
        sys.exit(1)


@pytest.fixture(scope="module")
def config():
    file_path = "test_benchmark.yaml"
    return load_config(file_path)


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
    data = load_benchmark_logs(run_ids)
    total_runtimes = {}
    for run_id in run_ids:
        exec_time_msg = data.get(run_id)[-1].get("msg").split(" ")
        total_runtimes[run_id] = float(exec_time_msg[1])

    partial_keys = [f"{nr_core}_{nr_record}" for nr_core, nr_record, n in test_cases]
    identical_cases = {}
    for key in partial_keys:
        identical_cases[key] = [val for k, val in total_runtimes.items() if key in k]

    return identical_cases


def calculate_average_run_time(results: list):
    min_time = min(results)
    max_time = max(results)
    avg_time = statistics.mean(results)
    stdev_time = statistics.stdev(results) if len(results) > 1 else 0.0

    return {'min': min_time, 'max': max_time, 'avg': avg_time, 'stdev': stdev_time}


def test_process_benchmark_results():
    combinations = input_data()
    cases = get_exec_data(combinations)
    keys = cases.keys()
    for key in keys:
        cases[key] = calculate_average_run_time(cases.get(key))
    print(cases)
    combinations = input_data()
    nr_cores = list(set([core for core, record, n in combinations]))
    plot_results(cases, nr_cores)




#@pytest.fixture(scope="session")
def plot_results(results, cores):
    #results = []
    #yield results

    if results:
        print(results)
        min_times = []
        max_times = []
        avg_times = []
        stdev_times = []

        fig, ax = plt.subplots(2,2, figsize=(10, 6))

        for key in results:
            print(key)
            values = results.get(key)
            min_times.append(values['min'])
            max_times.append(values['max'])
            avg_times.append(values['avg'])
            stdev_times.append(values['stdev'])
                #sweep.append(values['cores'])

            ax.plot(cores, avg_times, label='Average Time', marker='o', linestyle='-', linewidth=2)
            ax.plot(cores, min_times, label='Minimum Time', marker='^', linestyle='--', color='green')
            ax.plot(cores, max_times, label='Maximum Time', marker='v', linestyle=':', color='red')
            ax.plot(cores, stdev_times, label='Standard Deviation', marker='x', linestyle='-.', color='orange')

        ax.set_xlabel("max_workers")
        ax.set_ylabel(f'Execution Time [s]')
        ax.set_title(f'Execution times based on max_workers')

        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())

        #ax.legend()
        ax.grid(True, linestyle='--', alpha=0.6)

        fig.tight_layout()
        #plt.show()
        plt.savefig("benchmark.png")


def async_data_copy_intercube_multi_parameter(
        tm1_connection, param_set_mdx_list, data_mdx_template, clear_param_templates,
        target_cube_name, shared_mapping, mapping_steps, max_workers, **kwargs
):
    asyncio.run(bedrock.async_executor(
        data_copy_function=bedrock.data_copy_intercube,
        tm1_service=tm1_connection,
        data_mdx_template=data_mdx_template,
        skip_zeros=kwargs["skip_zeros"],
        skip_consolidated_cells=kwargs["skip_consolidated_cells"],
        target_cube_name=target_cube_name,
        shared_mapping=shared_mapping,
        mapping_steps=mapping_steps,
        clear_target=kwargs["clear_target"],
        async_write=kwargs["async_write"],
        logging_level=kwargs["logging_level"],
        param_set_mdx_list=param_set_mdx_list,
        clear_param_templates=clear_param_templates,
        ignore_missing_elements=kwargs["ignore_missing_elements"],
        max_workers=max_workers
    ))


def update_benchmark_log_handler(run_id):
    for handler in benchmark_metrics_logger.handlers:
        if isinstance(handler, logging.FileHandler):
            handler.close()  # Close the current file
            handler.baseFilename = f"../logs/TM1_bedrock_py_benchmark_{run_id}.json.log"
            handler.stream = open(handler.baseFilename, handler.mode)  # Re-open with new filename
            break


@pytest.mark.parametrize("nr_of_cores, nr_of_records, n", input_data())
def test_parallel_sweep(plot_results, config, tm1_connection, nr_of_cores, nr_of_records, n):

    update_benchmark_log_handler(f"{nr_of_cores}_{nr_of_records}_{n}")

    fix_kwargs = {
        "tm1_service": tm1_connection,
        "data_mdx_template": config.get("data_mdx_template")[0],
        "skip_zeros": True,
        "skip_consolidated_cells": True,
        "target_cube_name": config.get("target_cube_name")[0],
        "shared_mapping": config.get("shared_mapping")[0],
        "mapping_steps": config.get("mapping_steps")[0],
        "clear_target": True,
        "async_write": True,
        "logging_level": "DEBUG",
        "param_set_mdx_list": config.get("param_set_mdx_list")[0],
        "clear_param_templates": config.get("clear_param_templates")[0],
        "ignore_missing_elements": True
    }
    basic_logger.info(f"Execution starting with {nr_of_cores} workers")

    # number of records and test model build
    print(nr_of_records)

    # TODO: pass the test id generated from input tuple to benchmark logger

    asyncio.run(bedrock.async_executor(
        data_copy_function=bedrock.data_copy_intercube,
        max_workers=nr_of_cores,
        **fix_kwargs
    ))

    # destroy model
    print("model destroyed")

    basic_logger.info(f"Execution ended with {nr_of_cores} workers")
    """
    plot_results.append((
        {
            n: {
                "cores": nr_of_cores,
                "records": nr_of_records,
                "result": result
            }
        }
    ))
    """

#@parametrize_from_file
def test_trace_malloc(tm1_connection, param_set_mdx_list, data_mdx_template,
        clear_param_templates, target_cube_name, shared_mapping, mapping_steps, max_workers):
    fix_kwargs = {
        "tm1_connection": tm1_connection,
        "data_mdx_template": data_mdx_template,
        "skip_zeros": True,
        "skip_consolidated_cells": True,
        "target_cube_name": target_cube_name,
        "shared_mapping": shared_mapping,
        "mapping_steps": mapping_steps,
        "clear_target": True,
        "async_write": True,
        "logging_level": "WARNING",
        "param_set_mdx_list": param_set_mdx_list,
        "clear_param_templates": clear_param_templates,
        "ignore_missing_elements": True
    }

    tracemalloc.start()

    async_data_copy_intercube_multi_parameter(
        func_name=async_data_copy_intercube_multi_parameter,
        num_times=1,
        max_workers=max_workers,
        **fix_kwargs)

    snapshot = tracemalloc.take_snapshot()
    site_pkgs_dirs = site.getsitepackages()
    tests_dir = os.path.dirname(__file__)
    project_root = os.path.abspath(os.path.join(tests_dir, '..', 'TM1_bedrock_py'))

    include = tracemalloc.Filter(True, project_root + '/*')
    excludes = [tracemalloc.Filter(False, d + '/*') for d in site_pkgs_dirs]

    filtered = snapshot.filter_traces([include, *excludes])

    for stat in filtered.statistics('lineno')[:10]:
        print(stat)

    top_stats = filtered.statistics('traceback')

    print("\n[ tracemalloc top 10 – by function in my_package ]")
    for stat in top_stats[:10]:
        # stat.traceback is a Traceback object containing Frame records
        # You can format it to get human-readable lines (with function names)
        for line in stat.traceback.format():
            print(line)
        # And then show the size/count summary
        size_kb = stat.size / 1024
        print(f"↳ {size_kb:.1f} KiB in {stat.count} allocations\n")
