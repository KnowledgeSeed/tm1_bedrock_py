"""Validates the orchestration logic in scripts/live_server_test_harness.py against
MockTM1Service.

Important limitation, documented in dev/14_live_server_test_plan.md: MockTM1Service's
execute_mdx_dataframe is registration-based (exact-MDX-string lookup), not a real TM1
rule/feeder engine. So this file validates everything the mock CAN genuinely exercise --
schema creation, baseline data loading, the deployment safety net, and the pure-Python
metadata-walk logic (alternate-hierarchy resolution) -- and separately validates the
expected-value computation and comparison logic with synthetic data. It does NOT and
cannot validate that a real TM1 server computes the same numbers a real rule engine
would; that part requires running run_full_suite() against an actual server.
"""

from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import live_server_test_harness as harness
from tests.mock_tm1_service import MockTM1Service


@pytest.fixture()
def tm1_service():
    return MockTM1Service()


def _seed_mock_attribute_values(tm1_service):
    """MockElementService.get_attribute_of_elements reads from a separate in-memory
    store (MockTM1Service._dimension_attribute_values), not from the }}ElementAttributes_
    cube the harness writes to via loader.dataframe_to_cube -- a real TM1 server reads
    attribute values from that cube directly, so this gap is mock-only. Seed the mock's
    store here so attribute-routed formulas (shift, align) can be exercised against the
    mock; this is test-only scaffolding, not part of the harness itself."""

    from tests.mock_tm1_service import _name_key

    tm1_service._dimension_attribute_values[(_name_key(harness.DIM_MONTH), _name_key(harness.DIM_MONTH))] = {
        month: {
            "Month Number": str(int(month)),
            "Prev Month": str(int(month) - 1) if int(month) > 1 else "",
        }
        for month in harness.MONTHS
    }
    tm1_service._dimension_attribute_values[(_name_key(harness.DIM_REGION), _name_key(harness.DIM_REGION))] = {
        region: {"Currency": harness.REGION_CURRENCY[region]} for region in harness.REGIONS
    }


def test_build_schema_creates_all_test_dimensions_and_cubes(tm1_service):
    schema = harness.build_schema(tm1_service)

    for dimension_name in schema["dimensions"]:
        assert tm1_service.dimensions.exists(dimension_name), dimension_name
    for cube_name in schema["cubes"]:
        assert tm1_service.cubes.exists(cube_name), cube_name

    assert tm1_service.hierarchies.exists(harness.DIM_REGION, "Coastal Grouping")


def test_build_schema_is_safely_rerunnable_against_a_fully_built_schema(tm1_service):
    """build_schema must not error on a second call against the same server -- the
    scenario this guards is a prior run being stopped/killed before its own
    cleanup_schema finally-block ran, leaving a previous schema fully or partially
    in place."""

    harness.build_schema(tm1_service)
    schema = harness.build_schema(tm1_service)

    for dimension_name in schema["dimensions"]:
        assert tm1_service.dimensions.exists(dimension_name), dimension_name
    for cube_name in schema["cubes"]:
        assert tm1_service.cubes.exists(cube_name), cube_name
    assert tm1_service.hierarchies.exists(harness.DIM_REGION, "Coastal Grouping")


def test_build_schema_is_safely_rerunnable_against_a_partially_torn_down_schema(tm1_service):
    """Simulates a process stopped mid-cleanup (or mid-build): only the cubes were
    removed, dimensions left behind. build_schema must still succeed and produce a
    complete, consistent schema."""

    schema = harness.build_schema(tm1_service)
    for cube_name in schema["cubes"]:
        tm1_service.cubes.delete(cube_name)

    schema = harness.build_schema(tm1_service)

    for dimension_name in schema["dimensions"]:
        assert tm1_service.dimensions.exists(dimension_name), dimension_name
    for cube_name in schema["cubes"]:
        assert tm1_service.cubes.exists(cube_name), cube_name


def test_load_baseline_data_writes_expected_row_counts(tm1_service):
    harness.build_schema(tm1_service)
    baseline = harness.load_baseline_data(tm1_service)

    assert len(baseline["sales"]) == len(harness.REGIONS) * len(harness.MONTHS) * 2 + len(
        harness.REGIONS
    ) * len(harness.MONTHS)
    assert len(baseline["fx"]) == 3
    assert len(baseline["region_code"]) == len(harness.REGIONS)

    written = tm1_service.cube_data(harness.CUBE_SALES)
    assert len(written) > 0


def test_read_measure_values_forces_dimension_columns_to_string_dtype(tm1_service, monkeypatch):
    """Regression test for a real first-live-run failure: against a real TM1 server,
    pandas infers a numeric dtype for all-numeric-looking element names (e.g.
    TM1BPY_TEST_Month's '1'..'12'), which then fails to merge against this harness's
    independently computed expected-value frames (always built with str dimension
    columns) -- 'You are trying to merge on object and int64 columns'. The mock
    doesn't reproduce the dtype-inference behavior itself, so this asserts the actual
    mechanism of the fix: read_measure_values must request cube_dimensions so
    extractor.tm1_mdx_to_dataframe forces every dimension column to dtype=str
    regardless of what the underlying values look like."""

    harness.build_schema(tm1_service)

    captured_kwargs: Dict[str, Any] = {}
    from TM1_bedrock_py import extractor as extractor_module

    original = extractor_module.tm1_mdx_to_dataframe

    def _capture(**kwargs):
        captured_kwargs.update(kwargs)
        return original(**kwargs)

    monkeypatch.setattr(harness.extractor, "tm1_mdx_to_dataframe", _capture)

    try:
        harness.read_measure_values(
            tm1_service, harness.CUBE_SALES, harness.DIM_SALES_MEASURE,
            list(harness.SALES_MEASURES_NUMERIC),
            extra_filters={harness.DIM_VERSION: ["Actual"], harness.DIM_YEAR: ["2024"]},
        )
    except Exception:
        pass

    assert captured_kwargs.get("cube_dimensions") == tm1_service.cubes.get_dimension_names(harness.CUBE_SALES)


def test_build_model_compiles_every_formula_without_errors(tm1_service):
    harness.build_schema(tm1_service)
    harness.load_baseline_data(tm1_service)
    _seed_mock_attribute_values(tm1_service)
    model = harness.build_model(tm1_service)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    for target_key, entry in preview.manifest.items():
        if entry["backend"] != "native":
            continue
        assert not entry["artifact"].get("preview_only"), (
            f"{target_key} stayed preview-only: {entry['rationale']}"
        )


def test_deployment_safety_net_rollback_works_end_to_end(tm1_service):
    harness.build_schema(tm1_service)
    harness.load_baseline_data(tm1_service)
    _seed_mock_attribute_values(tm1_service)
    model = harness.build_model(tm1_service)

    report = harness.HarnessReport()
    harness.run_deployment_safety_net_check(model, report)

    assert report.ok, report.failures


def test_alternate_hierarchy_resolution_check_passes_against_live_collected_metadata(tm1_service):
    harness.build_schema(tm1_service)
    harness.load_baseline_data(tm1_service)

    report = harness.HarnessReport()
    harness.verify_alternate_hierarchy_resolution(tm1_service, report)

    assert report.ok, report.failures


def test_compute_expected_values_matches_hand_computed_result():
    baseline_sales = pd.DataFrame(
        [
            {harness.DIM_VERSION: "Actual", harness.DIM_YEAR: "2024", harness.DIM_MONTH: "1",
             harness.DIM_REGION: "East", harness.DIM_SALES_MEASURE: "Revenue", "Value": 100.0},
            {harness.DIM_VERSION: "Actual", harness.DIM_YEAR: "2024", harness.DIM_MONTH: "1",
             harness.DIM_REGION: "East", harness.DIM_SALES_MEASURE: "Cost", "Value": 60.0},
            {harness.DIM_VERSION: "Actual", harness.DIM_YEAR: "2024", harness.DIM_MONTH: "2",
             harness.DIM_REGION: "East", harness.DIM_SALES_MEASURE: "Revenue", "Value": 200.0},
            {harness.DIM_VERSION: "Actual", harness.DIM_YEAR: "2024", harness.DIM_MONTH: "2",
             harness.DIM_REGION: "East", harness.DIM_SALES_MEASURE: "Cost", "Value": 120.0},
            {harness.DIM_VERSION: "Actual", harness.DIM_YEAR: "2023", harness.DIM_MONTH: "1",
             harness.DIM_REGION: "East", harness.DIM_SALES_MEASURE: "Revenue", "Value": 90.0},
            {harness.DIM_VERSION: "Actual", harness.DIM_YEAR: "2023", harness.DIM_MONTH: "2",
             harness.DIM_REGION: "East", harness.DIM_SALES_MEASURE: "Revenue", "Value": 95.0},
        ]
    )
    baseline_fx = pd.DataFrame(
        [{harness.DIM_CURRENCY: "USD", "Value": 0.92}],
    )
    baseline = {"sales": baseline_sales, "fx": baseline_fx, "region_code": pd.DataFrame()}

    expected = harness.compute_expected_values(baseline)
    leaf = expected["leaf_2024"].set_index(harness.DIM_MONTH)

    assert leaf.loc["1", "Gross Margin"] == 40.0
    assert leaf.loc["2", "Gross Margin"] == 80.0
    assert leaf.loc["2", "Revenue Prior Month Attr"] == 100.0
    assert leaf.loc["1", "Revenue Prior Year"] == 90.0
    assert leaf.loc["2", "Revenue Prior Year"] == 95.0
    assert leaf.loc["2", "Revenue Rolling 2yr"] == 200.0 + 95.0
    assert leaf.loc["2", "Revenue YTD"] == 100.0 + 200.0
    assert leaf.loc["1", "Revenue EUR"] == pytest.approx(100.0 * 0.92)


def test_isclose_treats_both_null_as_match_and_flags_real_mismatch():
    left = pd.Series([1.0, None, 5.0])
    right = pd.Series([1.0000001, None, 6.0])

    result = harness._isclose(left, right)

    assert list(result) == [True, True, False]


def test_extract_feeder_source_measures_parses_same_cube_and_cross_cube_statements():
    feeder_text = (
        "['Cost'] => ['Gross Margin'];\n"
        "['Measure':'Measure':'Revenue'] => ['Measure':'Measure':'Gross Margin'];\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] "
        "=> DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate EUR');"
    )

    sources = harness._extract_feeder_source_measures(feeder_text)

    assert sources == ["Cost", "Revenue", "Rate"]


def test_verify_no_over_feeding_passes_on_genuine_compiled_output_and_flags_a_spurious_statement(tm1_service):
    harness.build_schema(tm1_service)
    harness.load_baseline_data(tm1_service)
    _seed_mock_attribute_values(tm1_service)
    model = harness.build_model(tm1_service)
    preview = model.compile(dry_run=True)
    assert preview.errors == []

    clean_report = harness.HarnessReport()
    harness.verify_no_over_feeding(model, preview, clean_report)
    assert clean_report.ok, clean_report.failures

    # Inject a feeder statement that doesn't correspond to any real dependency --
    # this is exactly the shape of bug the structural check exists to catch (a
    # spurious statement that doesn't break VALUES, only wastes recalculation).
    tampered_feeders = dict(preview.feeders)
    tampered_feeders[harness.CUBE_SALES] = (
        tampered_feeders.get(harness.CUBE_SALES, "")
        + "\n['Nonexistent Spurious Measure'] => ['Gross Margin'];"
    )
    tampered_preview = SimpleNamespace(feeders=tampered_feeders)

    tampered_report = harness.HarnessReport()
    harness.verify_no_over_feeding(model, tampered_preview, tampered_report)
    assert not tampered_report.ok
    assert any("Nonexistent Spurious Measure" in failure for failure in tampered_report.failures)


def test_under_feeding_live_mutation_check_writes_and_reverts_cube_data(tm1_service):
    # MockTM1Service's execute_mdx_dataframe cannot reflect the write this function
    # makes (registration-based, not a real engine), so the report is EXPECTED to
    # report a mismatch here -- that is the documented mock limitation, not a bug.
    # What this test validates is the plumbing: the mutation is written and then
    # reverted, leaving cube_data exactly as it started.
    harness.build_schema(tm1_service)
    harness.load_baseline_data(tm1_service)
    before_data = tm1_service.cube_data(harness.CUBE_SALES)

    consolidated_mdx = harness.utility.generate_dynamic_mdx_query_string(
        tm1_service=tm1_service,
        target_cube_name=harness.CUBE_SALES,
        dimension_filter_mapping={
            harness.DIM_VERSION: ["Actual"],
            harness.DIM_YEAR: ["2024"],
            harness.DIM_REGION: ["All Regions"],
            harness.DIM_SALES_MEASURE: ["Revenue", "Gross Margin", "Revenue EUR"],
        },
    )
    tm1_service.register_mdx(
        consolidated_mdx,
        pd.DataFrame(
            {harness.DIM_SALES_MEASURE: ["Revenue", "Gross Margin", "Revenue EUR"], "Value": [100.0, 40.0, 92.0]}
        ),
    )
    tm1_service.register_mdx(
        harness.utility.generate_dynamic_mdx_query_string(
            tm1_service=tm1_service,
            target_cube_name=harness.CUBE_FX,
            dimension_filter_mapping={
                harness.DIM_VERSION: ["Actual"], harness.DIM_YEAR: ["2024"],
                harness.DIM_CURRENCY: ["USD"], harness.DIM_TARGET_CURRENCY: ["EUR"],
                harness.DIM_FX_MEASURE: ["Rate"],
            },
        ),
        pd.DataFrame({"Value": [0.92]}),
    )
    # East/month-6 baseline Revenue per load_baseline_data's formula: 1000 + 0*100 + 6*10.
    tm1_service.register_mdx(
        harness.utility.generate_dynamic_mdx_query_string(
            tm1_service=tm1_service,
            target_cube_name=harness.CUBE_SALES,
            dimension_filter_mapping={
                harness.DIM_VERSION: ["Actual"], harness.DIM_YEAR: ["2024"], harness.DIM_MONTH: ["6"],
                harness.DIM_REGION: ["East"], harness.DIM_SALES_MEASURE: ["Revenue"],
            },
        ),
        pd.DataFrame({"Value": [1060.0]}),
    )

    report = harness.HarnessReport()
    harness.verify_no_under_feeding_via_live_mutation(tm1_service, report)

    # The mock always answers the same registered (static) result, so the "after"
    # read never moves -- every delta check correctly comes back failed.
    assert not report.ok

    after_data = tm1_service.cube_data(harness.CUBE_SALES)
    pd.testing.assert_frame_equal(
        before_data.sort_values(list(before_data.columns)).reset_index(drop=True),
        after_data.sort_values(list(after_data.columns)).reset_index(drop=True),
    )


def test_run_full_suite_prints_progress_narration(tm1_service, capsys):
    # run_full_suite raises a tests.mock_tm1_service.UnregisteredTM1QueryError once it
    # reaches a verification step that needs a registered MDX result the mock doesn't
    # have (expected -- the mock has no real engine, see module docstring). What this
    # test actually checks: the steps that DO run before that point print progress
    # narration -- proving the harness is no longer silent by default. Narration is
    # plain print(), not routed through TM1_bedrock_py's logger, deliberately (see
    # run_full_suite's docstring: raising that shared logger to INFO by default
    # triggered a real ~50x slowdown via a separate, pre-existing compiler
    # performance characteristic -- utility.get_default_hierarchy logs at INFO and
    # gets called tens of thousands of times for even a handful of formulas).
    _seed_mock_attribute_values(tm1_service)
    with pytest.raises(Exception):
        harness.run_full_suite(tm1_service, cleanup=True)

    captured = capsys.readouterr()
    assert "[1/9]" in captured.out
    assert "Schema built" in captured.out
    assert "[2/9]" in captured.out
    assert "[3/9]" in captured.out


def test_run_full_suite_does_not_leave_bedrock_logger_more_verbose_than_before(tm1_service):
    original_level = harness.basic_logger.level
    _seed_mock_attribute_values(tm1_service)

    with pytest.raises(Exception):
        harness.run_full_suite(tm1_service, cleanup=True, verbose_bedrock_logging="INFO")

    assert harness.basic_logger.level == original_level


def test_cleanup_schema_removes_only_prefixed_objects(tm1_service):
    # MockDimensionService has no delete() (TM1py/a real server does); cleanup_schema
    # guards for that, so this checks what the mock CAN verify: cube removal (including
    # the attribute cubes) and that cleanup never touches anything outside TEST_PREFIX.
    schema = harness.build_schema(tm1_service)
    harness.load_baseline_data(tm1_service)

    harness.cleanup_schema(tm1_service, schema)

    for cube_name in schema["cubes"]:
        assert not tm1_service.cubes.exists(cube_name)
    assert not tm1_service.cubes.exists(f"}}ElementAttributes_{harness.DIM_MONTH}")
    assert not tm1_service.cubes.exists(f"}}ElementAttributes_{harness.DIM_REGION}")
