# TM1 Data Integration and Automation Toolkit

This project is a Python-based toolkit by Knowledgeseed, designed to streamline data integration and automation tasks with IBM Planning Analytics (TM1). It leverages the `TM1py` library to provide a high-level, configurable, and extensible framework for moving data between TM1 cubes, SQL databases, and CSV files. The toolkit is built with a focus on performance, offering features like asynchronous operations and detailed logging for debugging and optimization.

For further information about the project, please refer to [https://knowledgeseed.ch/blog/tmbedrockpy_python_library](https://knowledgeseed.ch/blog/tmbedrockpy_python_library).

The detailed documentation of the project is available at [https://tm1-bedrock-py.readthedocs.io/en/latest/](https://tm1-bedrock-py.readthedocs.io/en/latest/).

## Features

*   **Versatile Data Flows:**
    *   Copy data between TM1 cubes (inter-cube and intra-cube).
    *   Load data from SQL databases into TM1 cubes.
    *   Export data from TM1 cubes to SQL tables.
    *   Load data from CSV files into TM1 cubes.
    *   Export data from TM1 cubes to CSV files.
*   **Powerful Data Transformation:**
    *   Apply a series of mapping and transformation steps to your data in-flight.
    *   Support for shared and step-specific mapping rules.
    *   Methods for replacing, mapping and replacing, and mapping and joining data based on dimension values.
    *   Ability to redimension data by adding, removing, or renaming columns.
    *   Transform values using custom functions.
*   **Performance-Oriented:**
    *   Asynchronous data writing for improved performance on large datasets.
    *   Asynchronous execution of multiple data copy operations in parallel.
    *   Detailed performance metrics logging to identify bottlenecks.
*   **Robust and Debuggable:**
    *   Comprehensive logging framework with configurable verbosity.
    *   Option to log intermediate DataFrames to CSV files for detailed debugging of transformation steps.
    *   Validation utilities to ensure data integrity.
*   **Extensible:**
    *   Use custom functions for data extraction, transformation, and loading to fit your specific needs.

## How to Use

This toolkit is designed to be used as a library within your Python scripts for TM1 automation. The core functionality is exposed through a set of high-level functions in the `bedrock.py` module.

### Core Functions

*   `data_copy_intercube`: Copies data from a source TM1 cube to a target TM1 cube.
*   `data_copy`: Copies and transforms data within the same TM1 cube.
*   `load_sql_data_to_tm1_cube`: Loads data from a SQL query or table into a TM1 cube.
*   `load_tm1_cube_to_sql_table`: Exports data from a TM1 cube to a SQL table.
*   `load_csv_data_to_tm1_cube`: Loads data from a CSV file into a TM1 cube.
*   `load_tm1_cube_to_csv_file`: Exports data from a TM1 cube to a CSV file.
*   `async_executor_tm1`: Asynchronously executes multiple `data_copy`, `data_copy_intercube`, `load_tm1_cube_to_sql_table` or `load_csv_data_to_tm1_cube` operations.
*   `async_executor_tm1_to_sql`: Asynchronously executes multiple `load_tm1_cube_to_sql_table` operations.
*   `async_executor_csv_to_tm1`: Asynchronously executes multiple `load_csv_data_to_tm1_cube` operations.

### Create a connection in TM1 with at least the following parameters set:

* address
* user
* password
* port
* ssl

You can check your connection via running `example/check_connectivity.py`. You can configure your connection in `examples/config_example.ini` or if left empty, via user input from the terminal.

### Example: Copying Data Between Cubes with Transformations

Here is an example of how to use `data_copy_intercube` to copy data from a source cube to a target cube while applying some transformations. For further examples, please refer to [`tests/test_integration.py`](https://github.com/KnowledgeSeed/tm1_bedrock_py/blob/main/tests/test_integration.py) and [`tests/test_benchmark.py`](https://github.com/KnowledgeSeed/tm1_bedrock_py/blob/main/tests/test_benchmark.py).

```python
from TM1py import TM1Service
from TM1_bedrock_py.bedrock import data_copy_intercube

# --- 1. Connect to TM1 ---
# Establish your TM1Service connection
tm1 = TM1Service(address='localhost', port=8001, user='admin', password='apple', ssl=False)

# --- 2. Define the Data Copy Parameters ---
# MDX to select the source data
source_mdx = """
SELECT
{ [Version].[Actual] } ON COLUMNS,
{ [Year]. } ON ROWS
FROM [Sales]
WHERE ([Measures].[Amount])
"""

# Define mapping and transformation steps
mapping_steps = [
    {
        "method": "replace",
        "mapping": {
            "Region": {"Old Region": "New Region"}
        }
    },
    {
        "method": "map_and_replace",
        "mapping_mdx": "SELECT {[Product].[All Products].Children} ON COLUMNS FROM [Product]",
        "mapping_dimensions": {
            "Product": "Product"
        }
    }
]

# --- 3. Execute the Data Copy ---
data_copy_intercube(
    tm1_service=tm1,
    data_mdx=source_mdx,
    target_cube_name="Sales Target",
    mapping_steps=mapping_steps,
    clear_target=True,
    logging_level="INFO"
)
```
### Configuration of Mapping Steps
The `mapping_steps` parameter is a powerful feature that allows you to define a pipeline of transformations. Each step is a dictionary that specifies the transformation method and its parameters.
* `replace`: Performs a simple find-and-replace on dimension elements.
* `map_and_replace`: Uses a mapping DataFrame (from an MDX query, SQL, or CSV) to replace values in specified dimensions.
* `map_and_join`: Joins columns from a mapping DataFrame to the main data based on shared dimensions.

## Optimizing Performance
This toolkit provides several ways to optimize the performance of your data operations:
* **Asynchronous Operations:** For large data writes to TM1, set the `async_write=True` parameter in the core functions. This will write data in parallel, significantly reducing the overall execution time.
* **Asynchronous Executors:** When you need to run multiple independent data copy processes, use the `async_executor_` functions. These functions will execute the operations concurrently, making efficient use of available resources.
```python
import asyncio
from TM1py import TM1Service
from TM1_bedrock_py.bedrock import async_executor_tm1, data_copy_intercube

tm1 = TM1Service(address='localhost', port=8001, user='admin', password='apple', ssl=False)

asyncio.run(async_executor_tm1(
            data_copy_function=data_copy_intercube,
            tm1_service=tm1,
            data_mdx_template=data_mdx_template,
            skip_zeros=True,
            skip_consolidated_cells=True,
            target_cube_name=target_cube_name,
            shared_mapping=shared_mapping,
            mapping_steps=mapping_steps,
            clear_target=True,
            async_write=True,
            logging_level="DEBUG",
            use_blob=True,
            param_set_mdx_list=param_set_mdx_list,
            clear_param_templates=clear_param_templates,
            ignore_missing_elements=True,
            max_workers=8
        ))

```
### IMPORTANT NOTES ON PERFORMANCE
* **skip_zeros:** When extracting data from TM1, setting `skip_zeros=True` can reduce the amount of data transferred and processed, especially for sparse cubes.
* **use_blob:** Default value is `use_blob=False` as `True` needs `administrator` privileges. Setting the value to `True` improves performance significantly.
* **slice_size_of_dataframe:** Default value is `slice_size_of_dataframe=5000`.
* **Efficient MDX:** The performance of the entire process is heavily dependent on the efficiency of your MDX queries. Ensure your MDX is optimized for the source cube structure.

## Debugging Options
The toolkit includes a robust logging framework to help you debug your data integration processes.
* **Logging Level:** You can control the verbosity of the logs by setting the `logging_level` parameter in the core functions. The available levels are DEBUG, INFO, WARNING, ERROR, and CRITICAL.
* **DataFrame Logger:** This is a powerful debugging feature that allows you to inspect the state of your DataFrame at various stages of the transformation pipeline. To enable it, set `df_verbose_logging=True` in the core functions. This will save a CSV file of the DataFrame at each significant step (e.g., after each mapping step) into a dataframe_logs directory. This is invaluable for understanding how your data is being transformed and for troubleshooting mapping issues.
* **Performance Metrics:** The execution time of key functions is automatically logged. By setting the logging level to DEBUG, you can see the time taken for each major operation, which helps in identifying performance bottlenecks. The `logging.json` file can be configured to output these metrics to a separate file for analysis.

## Requirements
* TM1py >=2.1, <3.0
* pandas >=2.0.0, <2.2.0
* json_logging >=1.3.0, <2.0.0
* sqlalchemy >=1.4.42, <2.0.0
* pyodbc >=5.2.0, <6.0.0

## Installation
> Install without airflow
```
pip install tm1-bedrock-py
```
> Install with `airflow`
```
pip install tm1-bedrock-py[airflow]
```
> Additional options for `airflow` database connectors for Microsoft-MSSQL and Postgres
```
pip install tm1-bedrock-py[airflow, microsoft-mssql]
pip install tm1-bedrock-py[airflow, postgres]
```

## Development
### Windows
```
git clone https://github.com/KnowledgeSeed/tm1_bedrock_py.git
cd tm1_bedrock_py

python -m venv .env
.\.env\Scripts\activate
pip install -r requirements-dev.txt
python -m build
```

### Linux/macOS
```
git clone https://github.com/KnowledgeSeed/tm1_bedrock_py.git
cd tm1_bedrock_py

virtualenv .env
source .env/bin/activate
pip install -r requirements-dev.txt
python -m build
```

## Manual integration testing

Use `tests_integration/docker-compose.yaml` as a baseline, which spins up a TM1 provider and a base TM1 database and a PostgreSQL database to test against. Please note that `tm1-docker` image is properietary IBM product wrapped in Docker by Knowledgeseed and therefore it is only available internally for Knowledgeseed developers.

```
docker compose up -d tm1 test_db_postgres
```

To obtain a licensed IBM TM1 database for testing or production purpose, please see https://www.ibm.com/topics/tm1 for further details.

## License

See [LICENSE](https://github.com/KnowledgeSeed/tm1_bedrock_py/blob/main/LICENSE)
