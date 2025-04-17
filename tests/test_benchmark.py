import configparser
import sys
from pathlib import Path

import yaml
from TM1py.Exceptions import TM1pyRestException
import pytest
import time
import asyncio
import statistics
import matplotlib.pyplot as plt
import parametrize_from_file

from TM1py import TM1Service

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


def calculate_average_run_time(func_name: callable, num_times: int, *args, **kwargs):
    """Runs benchmark for the async function."""
    results = []

    for n in range(num_times):
        start_time_total = time.time()
        func_name(*args, **kwargs)
        duration = time.time() - start_time_total
        results.append(duration)

    min_time = min(results) / num_times
    max_time = max(results) / num_times
    avg_time = statistics.mean(results) / num_times
    stdev_time = statistics.stdev(results) / num_times if len(results) > 1 else 0.0

    return {'min': min_time, 'max': max_time, 'avg': avg_time, 'stdev': stdev_time}


@pytest.fixture(scope="session")
def plot_results():
    results = []
    yield results

    if results:
        min_times = []
        max_times = []
        avg_times = []
        stdev_times = []
        sweep = []
        fig, ax = plt.subplots(figsize=(10, 6))
        for label, result in results:
            #plt.plot(result_data, label=label)
            min_times.append(result['min'])
            max_times.append(result['max'])
            avg_times.append(result['avg'])
            stdev_times.append(result['stdev'])
            sweep.append(label)

        ax.plot(sweep, avg_times, label='Average Time', marker='o', linestyle='-', linewidth=2)
        ax.plot(sweep, min_times, label='Minimum Time', marker='^', linestyle='--', color='green')
        ax.plot(sweep, max_times, label='Maximum Time', marker='v', linestyle=':', color='red')
        ax.plot(sweep, stdev_times, label='Standard Deviation', marker='x', linestyle='-.', color='orange')

        ax.set_xlabel("max_workers")
        ax.set_ylabel(f'Execution Time [s]')
        ax.set_title(f'Execution times based on max_workers')

        ax.get_xaxis().set_major_formatter(plt.ScalarFormatter())

        ax.legend()
        ax.grid(True, linestyle='--', alpha=0.6)

        fig.tight_layout()
        #plt.show()
        plt.savefig("benchmark.png")


def async_data_copy_intercube_multi_parameter(
        tm1_connection, param_set_mdx_list, data_mdx_template, clear_param_templates,
        target_cube_name, shared_mapping, mapping_steps, max_workers, **_kwargs
):
    asyncio.run(bedrock.async_executor(
        data_copy_function=bedrock.data_copy_intercube,
        tm1_service=tm1_connection,
        data_mdx_template=data_mdx_template,
        skip_zeros=True,
        skip_consolidated_cells=True,
        target_cube_name=target_cube_name,
        shared_mapping=shared_mapping,
        mapping_steps=mapping_steps,
        clear_target=True,
        async_write=True,
        logging_level="WARNING",
        param_set_mdx_list=param_set_mdx_list,
        clear_param_templates=clear_param_templates,
        ignore_missing_elements=True,
        max_workers=max_workers
    ))


@parametrize_from_file
def test_parallel_sweep(plot_results, tm1_connection, param_set_mdx_list, data_mdx_template,
        clear_param_templates, target_cube_name, shared_mapping, mapping_steps, max_workers):

    fix_kwargs = {
        "tm1_connection" : tm1_connection,
        "data_mdx_template" : data_mdx_template,
        "skip_zeros" : True,
        "skip_consolidated_cells" : True,
        "target_cube_name" : target_cube_name,
        "shared_mapping" : shared_mapping,
        "mapping_steps" : mapping_steps,
        "clear_target" : True,
        "async_write" : True,
        "logging_level" : "WARNING",
        "param_set_mdx_list" : param_set_mdx_list,
        "clear_param_templates" : clear_param_templates,
        "ignore_missing_elements" : True
    }

    results = []
    num_times = 2
    param_list = [4, 8]
    #for max_workers in param_list:
    basic_logger.info(f"Execution starting with {max_workers} workers")

    # number of records and test model build
    # print(num_records)

    result = calculate_average_run_time(
        func_name=async_data_copy_intercube_multi_parameter,
        num_times=num_times,
        max_workers=max_workers,
        **fix_kwargs
    )

    # destroy model
    # print(model destroyed)

    #results.append(result)

    basic_logger.info(f"Execution ended with {max_workers} workers")
    """
    min_times = [result['min'] for result in results]
    max_times = [result['max'] for result in results]
    avg_times = [result['avg'] for result in results]
    stdev_times = [result['stdev'] for result in results]
    """
    #plot_results(param_list, min_times, max_times, avg_times, stdev_times, "number of workers")
    plot_results.append((max_workers, result))