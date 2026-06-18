import asyncio

import pandas as pd
import pytest
from sqlalchemy import create_engine, text

from TM1_bedrock_py import bedrock
from tests.config import mock_tm1_service
from tests.mock_tm1_service import MockTM1Service, UnregisteredTM1QueryError


def test_mock_tm1_service_requires_registered_mdx(mock_tm1_service):
    with pytest.raises(UnregisteredTM1QueryError, match="No MDX response registered"):
        mock_tm1_service.cells.execute_mdx_dataframe("SELECT FROM [Missing]")


def test_mock_tm1_service_preserves_tm1_case_and_space_insensitive_names(
    mock_tm1_service,
):
    mock_tm1_service.add_dimension("Product Group", {"A": "Numeric"})
    mock_tm1_service.add_cube("Sales Cube", ["Product Group"])

    assert mock_tm1_service.dimensions.exists("productgroup")
    assert mock_tm1_service.cubes.exists("salescube")
    assert mock_tm1_service.cubes.get_dimension_names("SALES CUBE") == ["Product Group"]


def test_data_copy_wrapper_runs_without_live_tm1(mock_tm1_service):
    dimensions = ["Version", "Period", "Measure"]
    mock_tm1_service.add_dimension(
        "Version",
        {"Actual": "String", "Forecast": "String"},
    )
    mock_tm1_service.add_dimension("Period", {"202601": "String"})
    mock_tm1_service.add_dimension("Measure", {"Amount": "Numeric"})
    mock_tm1_service.add_cube("Planning", dimensions)

    mdx = """
        SELECT
          {[Version].[Version].[Actual]}
          * {[Period].[Period].[202601]}
          * {[Measure].[Measure].[Amount]} ON 0
        FROM [Planning]
    """
    mock_tm1_service.register_mdx(
        mdx,
        pd.DataFrame(
            {
                "Version": ["Actual"],
                "Period": ["202601"],
                "Measure": ["Amount"],
                "Value": [125.0],
            }
        ),
    )

    bedrock.data_copy(
        tm1_service=mock_tm1_service,
        data_mdx=mdx,
        mapping_steps=[
            {
                "method": "replace",
                "mapping": {"Version": {"Actual": "Forecast"}},
            }
        ],
        value_function=lambda value: float(value) * 2,
    )

    expected = pd.DataFrame(
        {
            "Version": ["Forecast"],
            "Period": ["202601"],
            "Measure": ["Amount"],
            "Value": [250.0],
        }
    )
    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), expected)


def test_dimension_builder_wrapper_updates_mock_metadata(mock_tm1_service):
    edges = pd.DataFrame(
        {
            "Parent": ["All Products", "All Products"],
            "Child": ["A", "B"],
            "Weight": [1.0, 1.0],
            "Dimension": ["Product", "Product"],
            "Hierarchy": ["Product", "Product"],
        }
    )
    elements = pd.DataFrame(
        {
            "ElementName": ["All Products", "A", "B"],
            "ElementType": ["Consolidated", "Numeric", "Numeric"],
            "Dimension": ["Product", "Product", "Product"],
            "Hierarchy": ["Product", "Product", "Product"],
        }
    )

    bedrock.dimension_builder(
        dimension_name="Product",
        input_format="parent_child",
        build_strategy="rebuild",
        tm1_service=mock_tm1_service,
        override_input_edges_df=edges,
        override_input_elements_df=elements,
    )

    hierarchy = mock_tm1_service.hierarchies.get("Product", "Product")
    assert set(hierarchy.elements) == {"All Products", "A", "B"}
    assert hierarchy.edges[("All Products", "A")] == 1.0


def test_mock_tm1_service_supports_filtered_cube_clear(mock_tm1_service):
    mock_tm1_service.add_dimension("Version", {"Actual": "String", "Forecast": "String"})
    mock_tm1_service.add_dimension("Measure", {"Amount": "Numeric"})
    mock_tm1_service.add_cube(
        "Planning",
        ["Version", "Measure"],
        data=pd.DataFrame(
            {
                "Version": ["Actual", "Forecast"],
                "Measure": ["Amount", "Amount"],
                "Value": [1.0, 2.0],
            }
        ),
    )
    actual_set = "{[Version].[Version].[Actual]}"
    mock_tm1_service.register_set_mdx(actual_set, ["Actual"])

    mock_tm1_service.cells.clear("Planning", Version=actual_set)

    expected = pd.DataFrame(
        {
            "Version": ["Forecast"],
            "Measure": ["Amount"],
            "Value": [2.0],
        }
    )
    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), expected)


def test_async_executor_tm1_runs_parallel_slices_without_live_tm1(mock_tm1_service):
    dimensions = ["Version", "Period", "Measure"]
    mock_tm1_service.add_dimension("Version", {"Actual": "String"})
    mock_tm1_service.add_dimension(
        "Period",
        {"202601": "String", "202602": "String"},
    )
    mock_tm1_service.add_dimension("Measure", {"Amount": "Numeric"})
    mock_tm1_service.add_cube("Planning", dimensions)

    parameter_mdx = "{[Period].[Period].[202601], [Period].[Period].[202602]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601", "202602"])
    data_mdx_template = (
        "SELECT {[Version].[Version].[Actual]} "
        "* {[Period].[Period].[$Period]} "
        "* {[Measure].[Measure].[Amount]} ON 0 FROM [Planning]"
    )
    for period, value in (("202601", 10), ("202602", 20)):
        data_mdx = data_mdx_template.replace("$Period", period)
        mock_tm1_service.register_mdx(
            data_mdx,
            pd.DataFrame(
                {
                    "Version": ["Actual"],
                    "Period": [period],
                    "Measure": ["Amount"],
                    "Value": [value],
                }
            ),
        )

    asyncio.run(
        bedrock.async_executor_tm1(
            tm1_service=mock_tm1_service,
            param_set_mdx_list=[parameter_mdx],
            data_mdx_template=data_mdx_template,
            max_workers=2,
        )
    )

    result = mock_tm1_service.cube_data("Planning").sort_values("Period").reset_index(drop=True)
    assert result["Period"].tolist() == ["202601", "202602"]
    assert result["Value"].tolist() == ["10", "20"]


def test_async_executor_csv_to_tm1_uses_csv_loader_by_default(mock_tm1_service, tmp_path):
    dimensions = ["Version", "Period", "Measure"]
    mock_tm1_service.add_dimension("Version", {"Actual": "String"})
    mock_tm1_service.add_dimension("Period", {"202601": "String"})
    mock_tm1_service.add_dimension("Measure", {"Amount": "Numeric"})
    mock_tm1_service.add_cube("Planning", dimensions)

    parameter_mdx = "{[Period].[Period].[202601]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601"])
    csv_path = tmp_path / "slice.csv"
    csv_path.write_text("Version,Period,Measure,Value\nActual,202601,Amount,7\n", encoding="utf-8")

    asyncio.run(
        bedrock.async_executor_csv_to_tm1(
            tm1_service=mock_tm1_service,
            target_cube_name="Planning",
            source_directory=str(tmp_path),
            param_set_mdx_list=[parameter_mdx],
            data_mdx_template="SELECT $Period FROM [Planning]",
            delimiter=",",
            max_workers=1,
        )
    )

    expected = pd.DataFrame(
        {
            "Version": ["Actual"],
            "Period": ["202601"],
            "Measure": ["Amount"],
            "Value": [7],
        }
    )
    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), expected)


def _sqlite_engine(tmp_path, name="wrapper.db"):
    return create_engine(f"sqlite:///{tmp_path / name}")


def _read_sql_table(engine, table_name):
    return pd.read_sql_query(text(f"SELECT * FROM {table_name}"), engine)


def _setup_planning_cube(tm1, periods, cube_name="Planning", values=None):
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Period", {period: "String" for period in periods})
    tm1.add_dimension("Measure", {"Amount": "Numeric"})
    data = None
    if values is not None:
        data = pd.DataFrame(
            {
                "Version": ["Actual"] * len(periods),
                "Period": list(periods),
                "Measure": ["Amount"] * len(periods),
                "Value": values,
            }
        )
    tm1.add_cube(cube_name, ["Version", "Period", "Measure"], data=data)


def test_load_sql_data_to_tm1_cube_runs_offline_with_mock_tm1(mock_tm1_service, tmp_path):
    _setup_planning_cube(mock_tm1_service, ["202601"], values=[999.0])
    mock_tm1_service.register_set_mdx("{[Period].[Period].[202601]}", ["202601"])
    engine = _sqlite_engine(tmp_path, "sql_to_tm1.db")
    pd.DataFrame(
        {
            "SourceVersion": ["Actual"],
            "PeriodCode": ["202601"],
            "MeasureName": ["Amount"],
            "Amount": [12.5],
        }
    ).to_sql("source_rows", engine, index=False, if_exists="replace")

    bedrock.load_sql_data_to_tm1_cube(
        tm1_service=mock_tm1_service,
        target_cube_name="Planning",
        sql_engine=engine,
        sql_table_name="source_rows",
        sql_column_mapping={
            "SourceVersion": "Version",
            "PeriodCode": "Period",
            "MeasureName": "Measure",
            "Amount": "Value",
        },
        clear_target=True,
        target_clear_set_mdx_list=["{[Period].[Period].[202601]}"],
    )

    expected = pd.DataFrame(
        {
            "Version": ["Actual"],
            "Period": ["202601"],
            "Measure": ["Amount"],
            "Value": [12.5],
        }
    )
    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), expected)
    pd.testing.assert_frame_equal(
        _read_sql_table(engine, "source_rows"),
        pd.DataFrame(
            {
                "SourceVersion": ["Actual"],
                "PeriodCode": ["202601"],
                "MeasureName": ["Amount"],
                "Amount": [12.5],
            }
        ),
    )


def test_load_tm1_cube_to_sql_table_runs_offline_with_mock_tm1(mock_tm1_service, tmp_path):
    _setup_planning_cube(mock_tm1_service, ["202601"], values=[100.0])
    mdx = (
        "SELECT {[Version].[Version].[Actual]} "
        "* {[Period].[Period].[202601]} "
        "* {[Measure].[Measure].[Amount]} ON 0 FROM [Planning]"
    )
    mock_tm1_service.register_mdx(
        mdx,
        pd.DataFrame(
            {
                "Version": ["Actual"],
                "Period": ["202601"],
                "Measure": ["Amount"],
                "Value": [15.0],
            }
        ),
    )
    mock_tm1_service.register_set_mdx("{[Period].[Period].[202601]}", ["202601"])
    engine = _sqlite_engine(tmp_path, "tm1_to_sql.db")
    pd.DataFrame(columns=["Version", "Period", "Measure", "Value"]).to_sql(
        "target_rows", engine, index=False, if_exists="replace"
    )

    bedrock.load_tm1_cube_to_sql_table(
        tm1_service=mock_tm1_service,
        target_table_name="target_rows",
        sql_engine=engine,
        data_mdx=mdx,
        clear_source=True,
        source_clear_set_mdx_list=["{[Period].[Period].[202601]}"],
    )

    expected = pd.DataFrame(
        {
            "Version": ["Actual"],
            "Period": ["202601"],
            "Measure": ["Amount"],
            "Value": [15.0],
        }
    )
    result = _read_sql_table(engine, "target_rows")
    assert result[["Version", "Period", "Measure"]].to_dict("records") == expected[
        ["Version", "Period", "Measure"]
    ].to_dict("records")
    assert pd.to_numeric(result["Value"]).tolist() == [15.0]
    assert mock_tm1_service.cube_data("Planning").empty


def test_load_csv_data_to_tm1_cube_runs_offline_with_mock_tm1(mock_tm1_service, tmp_path):
    _setup_planning_cube(mock_tm1_service, ["202601"], values=[999.0])
    mock_tm1_service.register_set_mdx("{[Period].[Period].[202601]}", ["202601"])
    csv_path = tmp_path / "source.csv"
    csv_path.write_text(
        "SourceVersion,PeriodCode,MeasureName,Amount\nActual,202601,Amount,8.5\n",
        encoding="utf-8",
    )

    bedrock.load_csv_data_to_tm1_cube(
        tm1_service=mock_tm1_service,
        target_cube_name="Planning",
        source_csv_file_path=str(csv_path),
        csv_column_mapping={
            "SourceVersion": "Version",
            "PeriodCode": "Period",
            "MeasureName": "Measure",
            "Amount": "Value",
        },
        clear_target=True,
        target_clear_set_mdx_list=["{[Period].[Period].[202601]}"],
    )

    expected = pd.DataFrame(
        {
            "Version": ["Actual"],
            "Period": ["202601"],
            "Measure": ["Amount"],
            "Value": [8.5],
        }
    )
    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), expected)


def test_load_tm1_cube_to_csv_file_runs_offline_with_mock_tm1(mock_tm1_service, tmp_path):
    _setup_planning_cube(mock_tm1_service, ["202601"], values=[100.0])
    mdx = (
        "SELECT {[Version].[Version].[Actual]} "
        "* {[Period].[Period].[202601]} "
        "* {[Measure].[Measure].[Amount]} ON 0 FROM [Planning]"
    )
    mock_tm1_service.register_mdx(
        mdx,
        pd.DataFrame(
            {
                "Version": ["Actual"],
                "Period": ["202601"],
                "Measure": ["Amount"],
                "Value": [9.0],
            }
        ),
    )
    mock_tm1_service.register_set_mdx("{[Period].[Period].[202601]}", ["202601"])

    bedrock.load_tm1_cube_to_csv_file(
        tm1_service=mock_tm1_service,
        data_mdx=mdx,
        target_csv_file_name="export.csv",
        target_csv_output_dir=str(tmp_path),
        clear_source=True,
        source_clear_set_mdx_list=["{[Period].[Period].[202601]}"],
    )

    expected = pd.DataFrame(
        {
            "Version": ["Actual"],
            "Period": ["202601"],
            "Measure": ["Amount"],
            "Value": [9.0],
        }
    )
    pd.testing.assert_frame_equal(
        pd.read_csv(tmp_path / "export.csv", dtype={"Version": str, "Period": str, "Measure": str}),
        expected,
    )
    assert mock_tm1_service.cube_data("Planning").empty


def test_async_executor_tm1_to_sql_runs_offline_with_mock_tm1(mock_tm1_service, tmp_path):
    _setup_planning_cube(mock_tm1_service, ["202601", "202602"])
    parameter_mdx = "{[Period].[Period].[202601], [Period].[Period].[202602]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601", "202602"])
    data_mdx_template = (
        "SELECT {[Version].[Version].[Actual]} "
        "* {[Period].[Period].[$Period]} "
        "* {[Measure].[Measure].[Amount]} ON 0 FROM [Planning]"
    )
    for period, value in (("202601", 10.0), ("202602", 20.0)):
        mock_tm1_service.register_mdx(
            data_mdx_template.replace("$Period", period),
            pd.DataFrame(
                {
                    "Version": ["Actual"],
                    "Period": [period],
                    "Measure": ["Amount"],
                    "Value": [value],
                }
            ),
        )
    engine = _sqlite_engine(tmp_path, "async_tm1_to_sql.db")
    pd.DataFrame(columns=["Version", "Period", "Measure", "Value"]).to_sql(
        "target_rows", engine, index=False, if_exists="replace"
    )

    asyncio.run(
        bedrock.async_executor_tm1_to_sql(
            tm1_service=mock_tm1_service,
            target_table_name="target_rows",
            sql_engine=engine,
            param_set_mdx_list=[parameter_mdx],
            data_mdx_template=data_mdx_template,
            max_workers=2,
        )
    )

    result = _read_sql_table(engine, "target_rows").sort_values("Period").reset_index(drop=True)
    assert result["Period"].tolist() == ["202601", "202602"]
    assert pd.to_numeric(result["Value"]).tolist() == [10.0, 20.0]


def test_async_executor_sql_to_tm1_runs_offline_with_mock_tm1(mock_tm1_service, tmp_path):
    _setup_planning_cube(mock_tm1_service, ["202601", "202602"], values=[999.0, 888.0])
    clear_periods_mdx = "{[Period].[Period].[202601], [Period].[Period].[202602]}"
    mock_tm1_service.register_set_mdx(clear_periods_mdx, ["202601", "202602"])
    engine = _sqlite_engine(tmp_path, "async_sql_to_tm1.db")
    pd.DataFrame(
        {
            "SourceVersion": ["Actual", "Actual"],
            "PeriodCode": ["202601", "202602"],
            "MeasureName": ["Amount", "Amount"],
            "Amount": [11.0, 22.0],
        }
    ).to_sql("source_rows", engine, index=False, if_exists="replace")

    asyncio.run(
        bedrock.async_executor_sql_to_tm1(
            tm1_service=mock_tm1_service,
            sql_engine=engine,
            sql_query_template=(
                "SELECT SourceVersion, PeriodCode, MeasureName, Amount "
                "FROM source_rows ORDER BY PeriodCode LIMIT {fetch} OFFSET {offset}"
            ),
            sql_table_for_count="source_rows",
            target_cube_name="Planning",
            sql_column_mapping={
                "SourceVersion": "Version",
                "PeriodCode": "Period",
                "MeasureName": "Measure",
                "Amount": "Value",
            },
            target_clear_set_mdx_list=[clear_periods_mdx],
            slice_size=1,
            max_workers=2,
        )
    )

    result = mock_tm1_service.cube_data("Planning").sort_values("Period").reset_index(drop=True)
    assert result["Period"].tolist() == ["202601", "202602"]
    assert pd.to_numeric(result["Value"]).tolist() == [11.0, 22.0]
