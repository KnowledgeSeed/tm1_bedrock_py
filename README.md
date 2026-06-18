![Project Banner](banner.png)

# TM1 Bedrock for Python

`tm1_bedrock_py` is a high-level integration toolkit for IBM Planning Analytics
(TM1). It builds on TM1py and pandas to move, transform, validate, and load data
between TM1 cubes, SQL databases, CSV files, and dimension structures.

The public API lives in `TM1_bedrock_py.bedrock`.

## Main workflows

| Need | Function |
| --- | --- |
| Transform and write data within one cube | `data_copy` |
| Move data between cubes or TM1 servers | `data_copy_intercube` |
| Load SQL data into TM1 | `load_sql_data_to_tm1_cube` |
| Export TM1 data to SQL | `load_tm1_cube_to_sql_table` |
| Load CSV data into TM1 | `load_csv_data_to_tm1_cube` |
| Export TM1 data to CSV | `load_tm1_cube_to_csv_file` |
| Run TM1-sliced work in parallel | `async_executor_tm1` |
| Export TM1 slices to SQL in parallel | `async_executor_tm1_to_sql` |
| Load paginated SQL slices in parallel | `async_executor_sql_to_tm1` |
| Load a directory of CSV files in parallel | `async_executor_csv_to_tm1` |
| Build or copy cubes and dimensions | `cube_builder`, `dimension_builder`, `dimension_copy` |
| Build one hierarchy | `hierarchy_builder`, `hierarchy_build_from_attributes` |
| Export a dimension schema | `dimension_export` |
| Run a coordinate-domain calculation pipeline | `input_handler` |

## Installation

```bash
pip install tm1-bedrock-py
```

Airflow integration is optional:

```bash
pip install "tm1-bedrock-py[airflow]"
```

Python 3.9 or newer is required. The current dependency ranges are defined in
`pyproject.toml`.

## Quick start

```python
from TM1py import TM1Service
from TM1_bedrock_py import bedrock

source_mdx = """
SELECT
  {[Period].[Period].[2026-01]} ON 0
FROM [Sales]
WHERE ([Version].[Version].[Actual], [Measure].[Measure].[Amount])
"""

with TM1Service(
    address="tm1.example.com",
    port=12354,
    user="svc_bedrock",
    password="...",
    ssl=True,
) as tm1:
    bedrock.data_copy_intercube(
        tm1_service=tm1,
        data_mdx=source_mdx,
        target_cube_name="Sales Reporting",
        target_dim_mapping={"Scenario": "Reported"},
        clear_target=True,
        target_clear_set_mdx_list=[
            "{[Period].[Period].[2026-01]}",
            "{[Scenario].[Scenario].[Reported]}",
        ],
        async_write=True,
        skip_zeros=True,
    )
```

Transformation arguments such as `source_dim_mapping`, `related_dimensions`,
and `target_dim_mapping` are accepted through the wrappers' `**kwargs`.

## Mapping pipeline

`mapping_steps` is an ordered list. Supported methods are:

- `replace`
- `map_and_replace`
- `map_and_join`
- `cartesian`
- `pivot`
- `unpivot`
- `basic_reshaping`
- `cartesian_with_set`

Example:

```python
mapping_steps = [
    {
        "method": "replace",
        "mapping": {"Version": {"Working": "Budget"}},
    },
    {
        "method": "basic_reshaping",
        "new_columns_with_values": {"Load Source": "SQL"},
        "column_relabel_map": {"Amount": "Value"},
    },
]
```

Invalid mapping and calculation configurations fail before execution with
field-level validation messages.

## Failure behavior

Public `bedrock` functions preserve the original exception type and traceback
while adding operation and safe parameter context. Passwords, tokens, secrets,
credentials, API keys, MDX, and SQL query text are redacted or summarized in
Bedrock-generated error messages. Parallel executors wait for their workers and
raise if any worker failed; partial failures are not reported as success.

SQL and MDX strings are executable configuration. Only use statements from a
trusted source.

## Documentation

The full guide includes:

- current function selection and connection examples;
- complete `bedrock` API signatures and parameters;
- TM1, SQL, CSV, async, dimension, and Airflow workflows;
- mapping, clearing, performance, and error-handling guidance.

Read the hosted documentation at
[tm1-bedrock-py.readthedocs.io](https://tm1-bedrock-py.readthedocs.io/en/latest/).

## Validation scope

The release hardening suite covers public exception boundaries, offline TM1
wrapper orchestration through `MockTM1Service`, SQL/CSV fixtures, dimension
building, async failure propagation, query redaction, rollback, and cleanup.
Live TM1 behavior still requires validation against your Planning Analytics
environment because authentication, server configuration, privileges, and
version-specific REST behavior cannot be reproduced fully offline.

## Development

```bash
python -m venv .venv
.\.venv\Scripts\activate
pip install -r requirements-dev.txt
pytest
python -m build
```

The repository may contain tests that require a configured `tm1srv` connection.

## License

Apache-2.0. See [LICENSE](LICENSE).
