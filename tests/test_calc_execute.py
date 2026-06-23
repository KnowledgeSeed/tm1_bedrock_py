import pandas as pd
import pytest

from TM1_bedrock_py.calc import Model, StaticMetadataProvider, build_static_cube_metadata
from TM1_bedrock_py.calc.execute import (
    evaluate_formula,
    melt_wide_to_long,
    pivot_long_to_wide,
    unsupported_python_execution_reason,
)


def _sales_metadata():
    return build_static_cube_metadata(
        "Sales",
        ["Version", "Month", "Measure"],
        default_hierarchies={"Version": "Version", "Month": "Month", "Measure": "Measure"},
        measure_element_types={"Revenue": "Numeric", "Cost": "Numeric", "Margin": "Numeric"},
        dimension_hierarchies={"Version": ["Version"], "Month": ["Month"], "Measure": ["Measure"]},
        dimension_leaf_elements={"Month": ["Jan", "Feb", "Mar"]},
    )


def test_pivot_and_melt_round_trip():
    long_dataframe = pd.DataFrame(
        {
            "Version": ["Actual", "Actual"],
            "Month": ["Jan", "Jan"],
            "Measure": ["Revenue", "Cost"],
            "Value": [100, 40],
        }
    )
    wide, other_dims = pivot_long_to_wide(long_dataframe, "Measure")
    assert other_dims == ["Version", "Month"]
    assert wide.loc[0, "Revenue"] == 100
    assert wide.loc[0, "Cost"] == 40

    melted = melt_wide_to_long(wide, other_dims, "Measure", "Margin", pd.Series([60]), None)
    assert melted.iloc[0]["Measure"] == "Margin"
    assert melted.iloc[0]["Value"] == 60


def test_evaluate_formula_plain_arithmetic():
    cube = Model().cube("Sales")
    expr = cube["Revenue"] - cube["Cost"]

    frame = pd.DataFrame({"Revenue": [100, 200], "Cost": [40, 50]})
    value, mask = evaluate_formula(expr, frame, "Sales")
    assert list(value) == [60, 150]
    assert mask is None


def test_evaluate_formula_where_excludes_failing_rows():
    cube = Model().cube("Sales")
    expr = (cube["Revenue"] / cube["Cost"]).where(cube["Cost"] != 0)

    frame = pd.DataFrame({"Revenue": [100, 200], "Cost": [0, 50]})
    value, mask = evaluate_formula(expr, frame, "Sales")
    assert list(mask) == [False, True]

    write_frame = melt_wide_to_long(frame, ["Revenue", "Cost"], "Measure", "Ratio", value, mask)
    assert len(write_frame) == 1


def test_evaluate_formula_case():
    model = Model()
    cube = model.cube("Sales")
    expr = model.case(
        (cube["Revenue"] > 150, 3),
        (cube["Revenue"] > 50, 2),
        default=1,
    )

    frame = pd.DataFrame({"Revenue": [200, 100, 10]})
    value, mask = evaluate_formula(expr, frame, "Sales")
    assert list(value) == [3, 2, 1]
    assert mask is None


def test_evaluate_formula_rolling_sum():
    cube = Model().cube("Sales")
    expr = cube["Revenue"].rolling(Month=2).sum()
    metadata = _sales_metadata()

    frame = pd.DataFrame(
        {
            "Version": ["Actual", "Actual", "Actual"],
            "Month": ["Jan", "Feb", "Mar"],
            "Revenue": [10, 20, 30],
        }
    )
    value, mask = evaluate_formula(
        expr, frame, "Sales", metadata=metadata, dimension_columns=["Version", "Month"]
    )
    assert list(value) == [10, 30, 50]
    assert mask is None


def test_unsupported_python_execution_reason_rejects_cross_cube_and_ytd():
    cube = Model().cube("Sales")
    other = Model().cube("FX Rates")

    cross_cube_expr = cube["Revenue"] + other["Rate"]
    assert "cross-cube" in unsupported_python_execution_reason(cross_cube_expr, "Sales")

    ytd_expr = cube["Revenue"].ytd(dimension="Month", period_number_attribute="Month Number")
    reason = unsupported_python_execution_reason(ytd_expr, "Sales")
    assert "ytd" in reason and "native-only" in reason

    align_expr = cube["Revenue"].align(Currency="EUR")
    align_reason = unsupported_python_execution_reason(align_expr, "Sales")
    assert "align" in align_reason


def test_unsupported_python_execution_reason_accepts_supported_shapes():
    cube = Model().cube("Sales")
    assert unsupported_python_execution_reason(cube["Revenue"] - cube["Cost"], "Sales") is None
    assert unsupported_python_execution_reason(
        (cube["Revenue"] - cube["Cost"]).where(cube["Cost"] != 0), "Sales"
    ) is None
    assert unsupported_python_execution_reason(cube["Revenue"].rolling(Month=3).sum(), "Sales") is None


def test_model_execute_python_backend_end_to_end_with_injected_io():
    provider = StaticMetadataProvider({"Sales": _sales_metadata()})
    model = Model(metadata_provider=provider)
    cube = model.cube("Sales")

    cube["Revenue Per Two Months"] = cube["Revenue"].rolling(Month=2).sum()
    cube["Cost Per Two Months"] = cube["Cost"].rolling(Month=2).sum()
    # Native-eligible formula registered alongside the Python-only ones, to confirm the
    # executor leaves native-backend formulas alone rather than re-executing them.
    cube["Margin %"] = (cube["Revenue"] - cube["Cost"]).where(cube["Cost"] != 0)

    long_dataframe = pd.DataFrame(
        {
            "Version": ["Actual"] * 3,
            "Month": ["Jan", "Feb", "Mar"],
            "Measure": ["Revenue"] * 3,
            "Value": [10, 20, 30],
        }
    )
    cost_rows = pd.DataFrame(
        {
            "Version": ["Actual"] * 3,
            "Month": ["Jan", "Feb", "Mar"],
            "Measure": ["Cost"] * 3,
            "Value": [0, 5, 6],
        }
    )
    combined = pd.concat([long_dataframe, cost_rows], ignore_index=True)

    captured_mdx = {}

    def fake_read(tm1_service, mdx):
        captured_mdx["mdx"] = mdx
        return combined

    written = {}

    def fake_write(tm1_service, cube_name, dataframe, cube_dims):
        written["cube_name"] = cube_name
        written["dataframe"] = dataframe
        written["cube_dims"] = cube_dims

    report = model.execute_python_backend(read_function=fake_read, write_function=fake_write)

    assert sorted(report.executed) == sorted(["Sales:Cost Per Two Months", "Sales:Revenue Per Two Months"])
    assert report.rejected == []
    assert "Revenue" in captured_mdx["mdx"]
    assert written["cube_name"] == "Sales"
    written_measures = set(written["dataframe"]["Measure"])
    assert written_measures == {"Cost Per Two Months", "Revenue Per Two Months"}
    rolling_rows = written["dataframe"][written["dataframe"]["Measure"] == "Revenue Per Two Months"]
    assert len(rolling_rows) == 3


def test_model_execute_python_backend_reports_rejections():
    provider = StaticMetadataProvider(
        {
            "Sales": _sales_metadata(),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "Measure"],
                measure_element_types={"Rate": "Numeric"},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue YTD"] = sales["Revenue"].ytd(dimension="Month", period_number_attribute="Month Number")
    sales["Revenue Converted"] = sales["Revenue"] * fx["Rate"]

    report = model.execute_python_backend(read_function=lambda *a, **k: pd.DataFrame(), write=False)

    rejected_targets = {rejection.target for rejection in report.rejected}
    assert "Sales:Revenue YTD" in rejected_targets
    assert "Sales:Revenue Converted" in rejected_targets
