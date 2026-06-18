import asyncio
from pathlib import Path

import pandas as pd
import pytest

from TM1_bedrock_py import bedrock
from TM1_bedrock_py.dimension_builder.exceptions import MissingHierarchyError
from tests.config import mock_tm1_service


def _planning_service(tm1):
    dimensions = ["Version", "Period", "Measure"]
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Period", {"202601": "String"})
    tm1.add_dimension("Measure", {"Amount": "Numeric"})
    tm1.add_cube(
        "Planning",
        dimensions,
        data=pd.DataFrame(
            {
                "Version": ["Actual"],
                "Period": ["202601"],
                "Measure": ["Amount"],
                "Value": [999.0],
            }
        ),
    )
    mdx = (
        "SELECT {[Version].[Version].[Actual]} "
        "* {[Period].[Period].[202601]} "
        "* {[Measure].[Measure].[Amount]} ON 0 FROM [Planning]"
    )
    tm1.register_mdx(
        mdx,
        pd.DataFrame(
            {
                "Version": ["Actual"],
                "Period": ["202601"],
                "Measure": ["Amount"],
                "Value": [10.0],
            }
        ),
    )
    return mdx


@pytest.mark.parametrize(
    ("parameter_name", "call"),
    [
        (
            "build_mode",
            lambda tm1: bedrock.cube_builder(tm1_service=tm1, build_mode="invalid"),
        ),
        (
            "build_strategy",
            lambda tm1: bedrock.dimension_builder(
                dimension_name="Product",
                input_format="parent_child",
                build_strategy="invalid",
                tm1_service=tm1,
            ),
        ),
        (
            "output_mode",
            lambda tm1: bedrock.hierarchy_builder(
                dimension_name="Product",
                hierarchy_name="Product",
                input_format="parent_child",
                build_strategy="rebuild",
                output_mode="invalid",
                tm1_service=tm1,
            ),
        ),
        (
            "element_query_mode",
            lambda tm1: bedrock.data_copy(
                tm1_service=tm1,
                data_mdx="SELECT FROM [Planning]",
                element_query_mode="invalid",
            ),
        ),
    ],
)
def test_public_wrappers_reject_invalid_modes_early(
    mock_tm1_service, parameter_name, call
):
    with pytest.raises(ValueError, match=parameter_name):
        call(mock_tm1_service)


def test_dimension_export_rejects_empty_destinations():
    with pytest.raises(ValueError, match="target_destinations"):
        bedrock.dimension_export(
            dimension_name="Product",
            output_format="parent_child",
            target_destinations=[],
        )


def test_cube_builder_honors_missing_dimension_raise_strategy(mock_tm1_service):
    with pytest.raises(ValueError, match="MissingDimension"):
        bedrock.cube_builder(
            tm1_service=mock_tm1_service,
            cube_dimension_create_map={"Planning": ["MissingDimension"]},
            missing_dimension_strategy="raise_error",
        )

    assert not mock_tm1_service.cubes.exists("Planning")


def test_hierarchy_from_attributes_rejects_empty_attribute_list(mock_tm1_service):
    with pytest.raises(ValueError, match="non-empty list"):
        bedrock.hierarchy_build_from_attributes(
            tm1_service=mock_tm1_service,
            dimension_name="Product",
            attributes=[],
        )


def test_hierarchy_builder_supports_flat_hierarchy(mock_tm1_service):
    elements = pd.DataFrame(
        {
            "ElementName": ["A", "B"],
            "ElementType": ["Numeric", "String"],
            "Dimension": ["Product", "Product"],
            "Hierarchy": ["Flat", "Flat"],
        }
    )

    bedrock.hierarchy_builder(
        dimension_name="Product",
        hierarchy_name="Flat",
        input_format="parent_child",
        build_strategy="rebuild",
        tm1_service=mock_tm1_service,
        override_input_edges_df=pd.DataFrame(
            columns=["Parent", "Child", "Weight", "Dimension", "Hierarchy"]
        ),
        override_input_elements_df=elements,
    )

    hierarchy = mock_tm1_service.hierarchies.get("Product", "Flat")
    assert set(hierarchy.elements) == {"A", "B"}
    assert len(hierarchy.edges) == 0


def test_dimension_export_supports_flat_local_schema(tmp_path):
    output_path = tmp_path / "flat.json"
    elements = pd.DataFrame(
        {
            "ElementName": ["A", "B"],
            "ElementType": ["Numeric", "String"],
            "Dimension": ["Product", "Product"],
            "Hierarchy": ["Product", "Product"],
        }
    )

    bedrock.dimension_export(
        dimension_name="Product",
        output_format="parent_child",
        target_destinations=["json"],
        edges_df=None,
        elements_df=elements,
        file_path_destination=str(output_path),
    )

    exported = pd.read_json(output_path, lines=True)
    assert exported["Child"].tolist() == ["A", "B"]
    assert exported["Parent"].isna().all()


def test_dimension_modify_validates_callback_result(monkeypatch, mock_tm1_service):
    mock_tm1_service.add_dimension("Product", {"A": "Numeric"})
    elements = pd.DataFrame(
        {
            "ElementName": ["A"],
            "ElementType": ["Numeric"],
            "Dimension": ["Product"],
            "Hierarchy": ["Product"],
        }
    )
    monkeypatch.setattr(
        bedrock.apply,
        "init_existing_schema_full",
        lambda **_kwargs: (None, elements),
    )

    with pytest.raises(TypeError, match="two-item tuple"):
        bedrock.dimension_modify(
            tm1_service=mock_tm1_service,
            dimension_name="Product",
            modify_function=lambda *_args: None,
        )


def test_hierarchy_modify_reports_missing_hierarchy(mock_tm1_service):
    mock_tm1_service.add_dimension("Product", {"A": "Numeric"})

    with pytest.raises(MissingHierarchyError, match="Missing"):
        bedrock.hierarchy_modify(
            tm1_service=mock_tm1_service,
            dimension_name="Product",
            hierarchy_name="Missing",
            modify_function=lambda edges, elements: (edges, elements),
        )


def test_pre_load_failure_does_not_clear_target(mock_tm1_service):
    mdx = _planning_service(mock_tm1_service)
    before = mock_tm1_service.cube_data("Planning")

    with pytest.raises(TypeError, match="must return a pandas DataFrame"):
        bedrock.data_copy(
            tm1_service=mock_tm1_service,
            data_mdx=mdx,
            clear_target=True,
            pre_load_function=lambda _dataframe: None,
        )

    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), before)
    assert not any(
        call.service == "MockCellService" and call.method == "clear"
        for call in mock_tm1_service.calls
    )


def test_input_handler_dry_run_does_not_clear_target(mock_tm1_service):
    mdx = _planning_service(mock_tm1_service)
    before = mock_tm1_service.cube_data("Planning")

    result = bedrock.input_handler(
        tm1_service=mock_tm1_service,
        input_value=5,
        target_cube_name="Planning",
        domain_mdx=mdx,
        clear_target=True,
        do_write=False,
        output_final_state_dataframe=True,
    )

    assert isinstance(result, pd.DataFrame)
    pd.testing.assert_frame_equal(mock_tm1_service.cube_data("Planning"), before)
    assert not any(
        call.service == "MockCellService" and call.method == "clear"
        for call in mock_tm1_service.calls
    )


def test_async_tm1_rejects_non_positive_worker_count(mock_tm1_service):
    parameter_mdx = "{[Period].[Period].[202601]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601"])

    with pytest.raises(ValueError, match="max_workers"):
        asyncio.run(
            bedrock.async_executor_tm1(
                tm1_service=mock_tm1_service,
                param_set_mdx_list=[parameter_mdx],
                data_mdx_template="SELECT $Period FROM [Planning]",
                data_copy_function=lambda **_kwargs: None,
                max_workers=0,
            )
        )


def test_async_tm1_rejects_missing_template_placeholder(mock_tm1_service):
    parameter_mdx = "{[Period].[Period].[202601]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601"])

    with pytest.raises(ValueError, match="missing placeholders.*Period"):
        asyncio.run(
            bedrock.async_executor_tm1(
                tm1_service=mock_tm1_service,
                param_set_mdx_list=[parameter_mdx],
                data_mdx_template="SELECT FROM [Planning]",
                data_copy_function=lambda **_kwargs: None,
            )
        )


def test_async_tm1_rejects_empty_parameter_result(mock_tm1_service):
    parameter_mdx = "{[Period].[Period].Members}"
    mock_tm1_service.register_set_mdx(parameter_mdx, [])

    with pytest.raises(ValueError, match="produced no execution tuples"):
        asyncio.run(
            bedrock.async_executor_tm1(
                tm1_service=mock_tm1_service,
                param_set_mdx_list=[parameter_mdx],
                data_mdx_template="SELECT $Period FROM [Planning]",
                data_copy_function=lambda **_kwargs: None,
            )
        )


def test_async_tm1_custom_function_does_not_require_cube_metadata(mock_tm1_service):
    parameter_mdx = "{[Period].[Period].[202601]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601"])
    executions = []

    asyncio.run(
        bedrock.async_executor_tm1(
            tm1_service=mock_tm1_service,
            param_set_mdx_list=[parameter_mdx],
            data_mdx_template="SELECT $Period FROM [NotARealCube]",
            data_copy_function=lambda **kwargs: executions.append(kwargs["data_mdx"]),
        )
    )

    assert executions == ["SELECT 202601 FROM [NotARealCube]"]


def test_async_tm1_worker_failure_does_not_log_rendered_mdx(
    mock_tm1_service, caplog
):
    parameter_mdx = "{[Period].[Period].[SecretPeriod]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["SecretPeriod"])
    rendered_mdx = "SELECT SecretPeriod FROM [SensitiveCube]"

    def fail_with_query(**kwargs):
        raise ValueError(f"query failed: {kwargs['data_mdx']}")

    with pytest.raises(RuntimeError) as excinfo:
        asyncio.run(
            bedrock.async_executor_tm1(
                tm1_service=mock_tm1_service,
                param_set_mdx_list=[parameter_mdx],
                data_mdx_template="SELECT $Period FROM [SensitiveCube]",
                data_copy_function=fail_with_query,
            )
        )

    assert rendered_mdx not in str(excinfo.value)
    assert rendered_mdx not in caplog.text
    assert "<query text" in caplog.text


def test_async_intercube_clear_uses_target_service():
    from tests.mock_tm1_service import MockTM1Service

    source = MockTM1Service("source")
    target = MockTM1Service("target")
    source.add_dimension("Period", {"202601": "String"})
    source.add_cube("Source", ["Period"])
    target.add_dimension("Period", {"202601": "String"})
    target.add_cube(
        "Target",
        ["Period"],
        data=pd.DataFrame({"Period": ["202601"], "Value": [1]}),
    )
    parameter_mdx = "{[Period].[Period].[202601]}"
    source.register_set_mdx(parameter_mdx, ["202601"])
    target.register_set_mdx(parameter_mdx, ["202601"])

    asyncio.run(
        bedrock.async_executor_tm1(
            tm1_service=source,
            target_tm1_service=target,
            target_cube_name="Target",
            param_set_mdx_list=[parameter_mdx],
            data_mdx_template="SELECT $Period FROM [Source]",
            data_copy_function=lambda **_kwargs: None,
            target_clear_set_mdx_list=[parameter_mdx],
        )
    )

    source_clear_calls = [
        call for call in source.calls
        if call.service == "MockCellService" and call.method == "clear"
    ]
    target_clear_calls = [
        call for call in target.calls
        if call.service == "MockCellService" and call.method == "clear"
    ]
    assert source_clear_calls == []
    assert len(target_clear_calls) == 1


def test_async_sql_to_tm1_clear_uses_target_service(monkeypatch):
    from tests.mock_tm1_service import MockTM1Service

    source = MockTM1Service("source")
    target = MockTM1Service("target")
    target.add_dimension("Period", {"202601": "String"})
    target.add_cube(
        "Target",
        ["Period"],
        data=pd.DataFrame({"Period": ["202601"], "Value": [1]}),
    )
    target.register_set_mdx("{[Period].[Period].[202601]}", ["202601"])
    monkeypatch.setattr(bedrock.extractor, "_get_sql_table_count", lambda *_args: 1)

    asyncio.run(
        bedrock.async_executor_sql_to_tm1(
            tm1_service=source,
            target_tm1_service=target,
            sql_engine=object(),
            sql_query_template="SELECT * FROM Source OFFSET {offset} FETCH {fetch}",
            sql_table_for_count="Source",
            target_cube_name="Target",
            data_copy_function=lambda **_kwargs: None,
            target_clear_set_mdx_list=["{[Period].[Period].[202601]}"],
            slice_size=1,
        )
    )

    source_clear_calls = [
        call for call in source.calls
        if call.service == "MockCellService" and call.method == "clear"
    ]
    target_clear_calls = [
        call for call in target.calls
        if call.service == "MockCellService" and call.method == "clear"
    ]
    assert source_clear_calls == []
    assert len(target_clear_calls) == 1


@pytest.mark.parametrize(
    ("query_template", "slice_size", "expected_message"),
    [
        ("SELECT * FROM Source OFFSET {offset}", 100, r"\{fetch\}"),
        ("SELECT * FROM Source OFFSET {offset} FETCH {fetch}", 0, "slice_size"),
    ],
)
def test_async_sql_rejects_invalid_pagination_before_connecting(
    query_template, slice_size, expected_message
):
    with pytest.raises(ValueError, match=expected_message):
        asyncio.run(
            bedrock.async_executor_sql_to_tm1(
                tm1_service=object(),
                sql_engine=object(),
                sql_query_template=query_template,
                sql_table_for_count="Source",
                target_cube_name="Planning",
                slice_size=slice_size,
                data_copy_function=lambda **_kwargs: None,
            )
        )


def test_async_csv_rejects_missing_directory_before_clear(mock_tm1_service, tmp_path):
    parameter_mdx = "{[Period].[Period].[202601]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601"])

    with pytest.raises(ValueError, match="does not exist"):
        asyncio.run(
            bedrock.async_executor_csv_to_tm1(
                tm1_service=mock_tm1_service,
                target_cube_name="Planning",
                source_directory=str(tmp_path / "missing"),
                param_set_mdx_list=[parameter_mdx],
                data_mdx_template="SELECT $Period FROM [Planning]",
                data_copy_function=lambda **_kwargs: None,
                target_clear_set_mdx_list=["{[Version].[Version].[Actual]}"],
            )
        )

    assert not any(
        call.service == "MockCellService" and call.method == "clear"
        for call in mock_tm1_service.calls
    )


def test_async_csv_requires_exact_file_parameter_count(mock_tm1_service, tmp_path):
    parameter_mdx = "{[Period].[Period].[202601], [Period].[Period].[202602]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601", "202602"])
    (tmp_path / "only.csv").write_text("Value\n1\n", encoding="utf-8")

    with pytest.raises(ValueError, match=r"1 CSV file\(s\).*2 parameter tuple"):
        asyncio.run(
            bedrock.async_executor_csv_to_tm1(
                tm1_service=mock_tm1_service,
                target_cube_name="Planning",
                source_directory=str(tmp_path),
                param_set_mdx_list=[parameter_mdx],
                data_mdx_template="SELECT $Period FROM [Planning]",
                data_copy_function=lambda **_kwargs: None,
            )
        )


def test_async_csv_pairs_sorted_files_deterministically(mock_tm1_service, tmp_path):
    parameter_mdx = "{[Period].[Period].[202601], [Period].[Period].[202602]}"
    mock_tm1_service.register_set_mdx(parameter_mdx, ["202601", "202602"])
    (tmp_path / "z.csv").write_text("Value\n2\n", encoding="utf-8")
    (tmp_path / "a.csv").write_text("Value\n1\n", encoding="utf-8")
    assignments = {}

    def capture_assignment(**kwargs):
        assignments[kwargs["_execution_id"]] = Path(
            kwargs["source_csv_file_path"]
        ).name

    asyncio.run(
        bedrock.async_executor_csv_to_tm1(
            tm1_service=mock_tm1_service,
            target_cube_name="Planning",
            source_directory=str(tmp_path),
            param_set_mdx_list=[parameter_mdx],
            data_mdx_template="SELECT $Period FROM [Planning]",
            data_copy_function=capture_assignment,
            max_workers=2,
        )
    )

    assert assignments == {0: "a.csv", 1: "z.csv"}


def test_tm1_to_sql_skips_source_clear_when_pre_load_empties_dataframe(
    monkeypatch, mock_tm1_service
):
    mdx = _planning_service(mock_tm1_service)

    cleared = []
    monkeypatch.setattr(
        bedrock.loader,
        "clear_cube",
        lambda **kwargs: cleared.append(kwargs),
    )
    monkeypatch.setattr(
        bedrock.loader,
        "dataframe_to_sql",
        lambda **_kwargs: pytest.fail("SQL writer should not run for empty dataframe"),
    )

    bedrock.load_tm1_cube_to_sql_table(
        tm1_service=mock_tm1_service,
        target_table_name="Target",
        sql_engine=object(),
        data_mdx=mdx,
        clear_source=True,
        source_clear_set_mdx_list=["{[Version].[Version].[Actual]}"],
        pre_load_function=lambda dataframe: dataframe.iloc[0:0],
    )

    assert cleared == []


def test_tm1_to_csv_skips_source_clear_when_mapping_empties_dataframe(
    monkeypatch, mock_tm1_service, tmp_path
):
    mdx = _planning_service(mock_tm1_service)

    cleared = []
    monkeypatch.setattr(
        bedrock.loader,
        "clear_cube",
        lambda **kwargs: cleared.append(kwargs),
    )
    monkeypatch.setattr(
        bedrock.loader,
        "dataframe_to_csv",
        lambda **_kwargs: pytest.fail("CSV writer should not run for empty dataframe"),
    )

    bedrock.load_tm1_cube_to_csv_file(
        tm1_service=mock_tm1_service,
        data_mdx=mdx,
        target_csv_file_name="out.csv",
        target_csv_output_dir=str(tmp_path),
        clear_source=True,
        source_clear_set_mdx_list=["{[Version].[Version].[Actual]}"],
        mapping_steps=[
            {
                "method": "basic_reshaping",
                "filter_condition": {"Version": "DoesNotExist"},
            }
        ],
    )

    assert cleared == []
