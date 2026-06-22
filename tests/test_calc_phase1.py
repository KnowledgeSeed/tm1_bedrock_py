import pytest

from TM1_bedrock_py.calc import Model


def test_registers_formulas_and_builds_deterministic_execution_order():
    model = Model()
    sales = model.cube("Sales")

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]
    sales["Gross Margin %"] = (
        sales["Gross Margin"] / sales["Revenue"]
    ).where(sales["Revenue"] != 0)

    report = model.validate()

    assert report.ok is True
    assert report.execution_order == [
        "Sales:Gross Margin",
        "Sales:Gross Margin %",
    ]

    explanation = model.explain("Sales:Gross Margin %")
    assert explanation["measure_dependencies"] == [
        "Sales:Gross Margin",
        "Sales:Revenue",
    ]
    assert explanation["execution_order"] == [
        "Sales:Gross Margin",
        "Sales:Gross Margin %",
    ]


def test_tracks_cross_cube_and_attribute_dependencies_in_explain():
    model = Model()
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = (
        sales["Revenue Local"]
        * fx["Rate"].align(
            Currency=sales.Company.attribute("Currency"),
            TargetCurrency="EUR",
        )
    )

    explanation = model.explain("Sales:Revenue EUR")

    assert explanation["measure_dependencies"] == [
        "FX Rates:Rate",
        "Sales:Revenue Local",
    ]
    assert explanation["attribute_dependencies"] == [
        "Sales.Company.attribute(Currency)",
    ]
    assert explanation["backend"] == "Python materialization backend"


def test_case_and_rolling_declarations_remain_symbolic():
    model = Model()
    sales = model.cube("Sales")

    sales["Commission Rate"] = model.case(
        (sales["Revenue"] >= 1_000_000, 0.08),
        (sales["Revenue"] >= 500_000, 0.06),
        default=0.04,
    )
    sales["Revenue Rolling 12M"] = sales["Revenue"].rolling(months=12).sum()

    explanation = model.explain()

    assert "case(" in explanation["formulas"]["Sales:Commission Rate"]["expression"]
    assert ".rolling.sum(" in explanation["formulas"]["Sales:Revenue Rolling 12M"]["expression"]


def test_growth_with_string_baseline_adds_semantic_dependency_to_execution_plan():
    model = Model()
    sales = model.cube("Sales")

    sales["Revenue Prior Year"] = sales["Revenue Input PY"]
    sales["Revenue Growth %"] = sales["Revenue"].growth(vs="Revenue Prior Year")

    report = model.validate()
    explanation = model.explain("Sales:Revenue Growth %")

    assert report.execution_order == [
        "Sales:Revenue Prior Year",
        "Sales:Revenue Growth %",
    ]
    assert explanation["measure_dependencies"] == [
        "Sales:Revenue",
        "Sales:Revenue Prior Year",
    ]


def test_detects_cyclic_dependencies():
    model = Model()
    sales = model.cube("Sales")

    sales["A"] = sales["B"] + 1
    sales["B"] = sales["A"] + 1

    report = model.validate()

    assert report.ok is False
    assert report.errors == [
        "Cyclic formula dependencies detected for targets: Sales:A, Sales:B"
    ]


def test_duplicate_formula_targets_fail_fast():
    model = Model()
    sales = model.cube("Sales")

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

    with pytest.raises(ValueError, match="already registered"):
        sales["Gross Margin"] = sales["Revenue"]
