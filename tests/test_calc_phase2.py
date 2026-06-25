import pytest

from TM1_bedrock_py.calc import (
    DeploymentError,
    Model,
    StaticMetadataProvider,
    TM1ServiceMetadataProvider,
    build_static_cube_metadata,
)
from tests.mock_tm1_service import MockTM1Service


def test_compile_preview_emits_tm1_equality_operator_not_python_equality():
    model = Model()
    sales = model.cube("Sales")

    sales["Is Closed"] = sales["Revenue"].where(sales["Status"] == 1)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert preview.rules["Sales"] == "['Is Closed'] = N: IF((['Status'] = 1), ['Revenue'], STET);"


def test_compile_preview_emits_rules_and_feeders_for_native_subset():
    model = Model()
    sales = model.cube("Sales")

    sales["Gross Margin"] = (sales["Revenue"] - sales["Cost"]).native(feeder_mode="safe_union")
    sales["Gross Margin %"] = (
        sales["Gross Margin"] / sales["Revenue"]
    ).where(sales["Revenue"] != 0).native(feeder_mode="safe_union")

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Gross Margin'] = N: (['Revenue'] - ['Cost']);\n"
        "['Gross Margin %'] = N: IF((['Revenue'] <> 0), (['Gross Margin'] / ['Revenue']), STET);"
    )
    assert preview.feeders["Sales"] == (
        "['Cost'] => ['Gross Margin %'];\n"
        "['Cost'] => ['Gross Margin'];\n"
        "['Revenue'] => ['Gross Margin %'];\n"
        "['Revenue'] => ['Gross Margin'];"
    )
    assert preview.deployment["Sales"] == {
        "mode": "dry_run",
        "deployed": False,
        "formula_targets": [
            "Sales:Gross Margin",
            "Sales:Gross Margin %",
        ],
        "rule_line_count": 2,
        "feeder_count": 4,
        "artifact_preview": (
            "['Gross Margin'] = N: (['Revenue'] - ['Cost']);\n"
            "['Gross Margin %'] = N: IF((['Revenue'] <> 0), (['Gross Margin'] / ['Revenue']), STET);"
        ),
        "check_rules_status": None,
    }
    assert preview.manifest["Sales:Gross Margin"]["backend"] == "native-rule backend"
    assert preview.manifest["Sales:Gross Margin %"]["backend"] == "native-rule backend"
    assert preview.manifest["Sales:Gross Margin"]["artifact"] == {
        "rule_statement": "['Gross Margin'] = N: (['Revenue'] - ['Cost']);",
        "feeder_statements": [
            "['Cost'] => ['Gross Margin'];",
            "['Revenue'] => ['Gross Margin'];",
        ],
        "feeder_strategy": "same_cube_traceback",
        "feeder_rationale": (
            "Same-cube feeder planning traces rule-calculated intermediates back to their original "
            "same-cube sparse source measures and feeds the final target directly."
        ),
        "feeder_deployment_cube": "Sales",
        "deployment_cube": "Sales",
        "preview_only": False,
    }


def test_simple_feeder_infer_traces_rule_calculated_cells_back_to_original_source():
    model = Model()
    sales = model.cube("Sales")

    sales["B"] = sales["A"]
    sales["C"] = sales["B"]
    sales["D"] = sales["C"]

    preview = model.compile(dry_run=True)

    assert preview.feeders["Sales"] == (
        "['A'] => ['B'];\n"
        "['A'] => ['C'];\n"
        "['A'] => ['D'];"
    )


def test_compile_preview_falls_back_to_python_for_cross_cube_alignment_without_metadata():
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
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "Python materialization backend"
    assert "needs source and target cube metadata" in explanation["rationale"]
    assert "Sales" not in preview.rules
    assert preview.manifest["Sales:Revenue EUR"]["backend"] == "Python materialization backend"


def test_compile_explains_ytd_falls_back_to_python_without_time_dimension_metadata():
    model = Model()
    sales = model.cube("Sales")

    sales["Revenue YTD"] = sales["Revenue"].ytd(
        dimension="Month", period_number_attribute="Month Number"
    )

    explanation = model.explain("Sales:Revenue YTD")

    assert explanation["backend"] == "Python materialization backend"
    assert "needs cube metadata to resolve the time dimension" in explanation["rationale"]


def test_compile_explains_rolling_as_intentionally_deferred_to_python_backend():
    model = Model()
    sales = model.cube("Sales")

    sales["Revenue Rolling 12M"] = sales["Revenue"].rolling(months=12)

    explanation = model.explain("Sales:Revenue Rolling 12M")

    assert explanation["backend"] == "Python materialization backend"
    assert "multi-cell aggregation" in explanation["rationale"]


def test_compile_preview_lowers_case_to_nested_tm1_if():
    model = Model()
    sales = model.cube("Sales")

    sales["Commission Rate"] = model.case(
        (sales["Revenue"] >= 1_000_000, 0.08),
        (sales["Revenue"] >= 500_000, 0.06),
        default=0.04,
    )

    preview = model.compile(dry_run=True)

    assert preview.rules["Sales"] == (
        "['Commission Rate'] = N: IF((['Revenue'] >= 1000000), 0.08, "
        "IF((['Revenue'] >= 500000), 0.06, 0.04));"
    )
    assert preview.feeders["Sales"] == "['Revenue'] => ['Commission Rate'];"


def test_compile_preview_lowers_growth_helper_with_string_baseline():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Period", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Period": "Period",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue Prior Year": "Numeric",
                    "Revenue Growth %": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Period": ["Period"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue Growth %"] = sales["Revenue"].growth(vs="Revenue Prior Year").native(feeder_mode="safe_union")

    preview = model.compile(dry_run=True)

    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue Growth %'] = N: "
        "IF((['Measure':'Measure':'Revenue Prior Year'] <> 0), "
        "((['Measure':'Measure':'Revenue'] - ['Measure':'Measure':'Revenue Prior Year']) / "
        "['Measure':'Measure':'Revenue Prior Year']), STET);"
    )
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Revenue Prior Year'] => ['Measure':'Measure':'Revenue Growth %'];\n"
        "['Measure':'Measure':'Revenue'] => ['Measure':'Measure':'Revenue Growth %'];"
    )


def test_compile_preview_lowers_shift_helper_to_attrs_lookup_with_safe_feeder():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue Prior Month": "Numeric",
                },
                dimension_attributes={"Month": ["Prev Month"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["Jan", "Feb", "Mar"],
                },
                dimension_attribute_values={
                    "Month": {
                        "Jan": {"Prev Month": "Dec"},
                        "Feb": {"Prev Month": "Jan"},
                        "Mar": {"Prev Month": "Feb"},
                    },
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue Prior Month"] = sales["Revenue"].shift(Month="Prev Month")

    explanation = model.explain("Sales:Revenue Prior Month")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "same_cube_attribute_shift"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue Prior Month'] = N: "
        "DB('Sales', !Version, ATTRS('Month', !Month, 'Prev Month'), 'Measure':'Measure':'Revenue');"
    )
    assert preview.feeders["Sales"] == (
        "[!Version, 'Month':'Month':'Dec', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'Jan', 'Measure':'Measure':'Revenue Prior Month');\n"
        "[!Version, 'Month':'Month':'Feb', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'Mar', 'Measure':'Measure':'Revenue Prior Month');\n"
        "[!Version, 'Month':'Month':'Jan', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'Feb', 'Measure':'Measure':'Revenue Prior Month');"
    )
    assert preview.manifest["Sales:Revenue Prior Month"]["artifact"]["feeder_strategy"] == "same_cube_attribute_shift"
    assert preview.manifest["Sales:Revenue Prior Month"]["artifact"]["preview_only"] is False


def test_compile_preview_lowers_shift_helper_to_numeric_offset_lookup_with_safe_feeder():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Year", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Year": "Year",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue Prior Year": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Year": ["Year"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Year": ["2023", "2024", "2025"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue Prior Year"] = sales["Revenue"].shift(Year=-1)

    explanation = model.explain("Sales:Revenue Prior Year")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "same_cube_numeric_shift"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue Prior Year'] = N: "
        "DB('Sales', !Version, STR(NUMBR(!Year) + (-1)), 'Measure':'Measure':'Revenue');"
    )
    assert preview.feeders["Sales"] == (
        "[!Version, 'Year':'Year':'2023', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2024', 'Measure':'Measure':'Revenue Prior Year');\n"
        "[!Version, 'Year':'Year':'2024', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2025', 'Measure':'Measure':'Revenue Prior Year');"
    )
    assert preview.manifest["Sales:Revenue Prior Year"]["artifact"]["feeder_strategy"] == "same_cube_numeric_shift"
    assert preview.manifest["Sales:Revenue Prior Year"]["artifact"]["preview_only"] is False


def test_compile_keeps_shift_helper_preview_only_when_offset_dimension_is_not_numeric():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue Prior Month": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["Jan", "Feb"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue Prior Month"] = sales["Revenue"].shift(Month=-1)

    explanation = model.explain("Sales:Revenue Prior Month")
    assert explanation["backend"] == "Python materialization backend"


def test_compile_preview_lowers_rolling_sum_to_unrolled_shift_sum_with_safe_feeder():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Year", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Year": "Year",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue 2yr Sum": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Year": ["Year"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Year": ["2023", "2024", "2025"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue 2yr Sum"] = sales["Revenue"].rolling(Year=2).sum()

    explanation = model.explain("Sales:Revenue 2yr Sum")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "same_cube_rolling_window"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue 2yr Sum'] = N: "
        "DB('Sales', !Version, STR(NUMBR(!Year) + (0)), 'Measure':'Measure':'Revenue') + "
        "DB('Sales', !Version, STR(NUMBR(!Year) + (-1)), 'Measure':'Measure':'Revenue');"
    )
    assert preview.feeders["Sales"] == (
        "[!Version, 'Year':'Year':'2023', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2023', 'Measure':'Measure':'Revenue 2yr Sum');\n"
        "[!Version, 'Year':'Year':'2023', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2024', 'Measure':'Measure':'Revenue 2yr Sum');\n"
        "[!Version, 'Year':'Year':'2024', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2024', 'Measure':'Measure':'Revenue 2yr Sum');\n"
        "[!Version, 'Year':'Year':'2024', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2025', 'Measure':'Measure':'Revenue 2yr Sum');\n"
        "[!Version, 'Year':'Year':'2025', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Year':'Year':'2025', 'Measure':'Measure':'Revenue 2yr Sum');"
    )
    assert preview.manifest["Sales:Revenue 2yr Sum"]["artifact"]["feeder_strategy"] == "same_cube_rolling_window"
    assert preview.manifest["Sales:Revenue 2yr Sum"]["artifact"]["preview_only"] is False


def test_compile_deploys_rolling_sum_when_dimension_is_proven_numeric():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Year", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Year": "Year",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue 2yr Sum": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Year": ["Year"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Year": ["2023", "2024", "2025"],
                },
            ),
        }
    )
    tm1_service = MockTM1Service()
    model = Model(metadata_provider=provider, tm1=tm1_service)
    sales = model.cube("Sales")

    sales["Revenue 2yr Sum"] = sales["Revenue"].rolling(Year=2).sum()

    deployment = model.compile(dry_run=False)

    assert deployment.errors == []
    assert deployment.deployment["Sales"]["deployed"] is True
    assert deployment.deployment["Sales"]["check_rules_status"] == 200


def test_compile_keeps_rolling_sum_preview_only_when_dimension_is_not_numeric():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue 3mo Sum": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["Jan", "Feb", "Mar"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue 3mo Sum"] = sales["Revenue"].rolling(Month=3).sum()

    explanation = model.explain("Sales:Revenue 3mo Sum")
    assert explanation["backend"] == "Python materialization backend"


def _ytd_metadata_provider():
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue YTD": "Numeric",
                },
                dimension_attributes={"Month": ["Month Number"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["1", "2", "3"],
                },
                dimension_attribute_values={
                    "Month": {
                        "1": {"Month Number": "1"},
                        "2": {"Month Number": "2"},
                        "3": {"Month Number": "3"},
                    },
                },
            ),
        }
    )


def test_compile_preview_lowers_ytd_to_gated_unrolled_shift_sum_with_safe_feeder():
    model = Model(metadata_provider=_ytd_metadata_provider())
    sales = model.cube("Sales")

    sales["Revenue YTD"] = sales["Revenue"].ytd(dimension="Month", period_number_attribute="Month Number")

    explanation = model.explain("Sales:Revenue YTD")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "same_cube_ytd_window"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue YTD'] = N: "
        "IF((NUMBR(ATTRS('Month', !Month, 'Month Number')) > 0), "
        "DB('Sales', !Version, STR(NUMBR(!Month) + (0)), 'Measure':'Measure':'Revenue'), 0) + "
        "IF((NUMBR(ATTRS('Month', !Month, 'Month Number')) > 1), "
        "DB('Sales', !Version, STR(NUMBR(!Month) + (-1)), 'Measure':'Measure':'Revenue'), 0) + "
        "IF((NUMBR(ATTRS('Month', !Month, 'Month Number')) > 2), "
        "DB('Sales', !Version, STR(NUMBR(!Month) + (-2)), 'Measure':'Measure':'Revenue'), 0);"
    )
    assert preview.feeders["Sales"] == (
        "[!Version, 'Month':'Month':'1', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'1', 'Measure':'Measure':'Revenue YTD');\n"
        "[!Version, 'Month':'Month':'1', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'2', 'Measure':'Measure':'Revenue YTD');\n"
        "[!Version, 'Month':'Month':'1', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'3', 'Measure':'Measure':'Revenue YTD');\n"
        "[!Version, 'Month':'Month':'2', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'2', 'Measure':'Measure':'Revenue YTD');\n"
        "[!Version, 'Month':'Month':'2', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'3', 'Measure':'Measure':'Revenue YTD');\n"
        "[!Version, 'Month':'Month':'3', 'Measure':'Measure':'Revenue'] => "
        "DB('Sales', !Version, 'Month':'Month':'3', 'Measure':'Measure':'Revenue YTD');"
    )
    assert preview.manifest["Sales:Revenue YTD"]["artifact"]["feeder_strategy"] == "same_cube_ytd_window"
    assert preview.manifest["Sales:Revenue YTD"]["artifact"]["preview_only"] is False


def test_compile_deploys_ytd_when_period_number_attribute_is_proven():
    model = Model(metadata_provider=_ytd_metadata_provider(), tm1=MockTM1Service())
    sales = model.cube("Sales")

    sales["Revenue YTD"] = sales["Revenue"].ytd(dimension="Month", period_number_attribute="Month Number")

    deployment = model.compile(dry_run=False)

    assert deployment.errors == []
    assert deployment.deployment["Sales"]["deployed"] is True
    assert deployment.deployment["Sales"]["check_rules_status"] == 200


def test_compile_keeps_ytd_preview_only_when_dimension_is_not_numeric():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue YTD": "Numeric",
                },
                dimension_attributes={"Month": ["Month Number"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["Jan", "Feb", "Mar"],
                },
                dimension_attribute_values={
                    "Month": {
                        "Jan": {"Month Number": "1"},
                        "Feb": {"Month Number": "2"},
                        "Mar": {"Month Number": "3"},
                    },
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue YTD"] = sales["Revenue"].ytd(dimension="Month", period_number_attribute="Month Number")

    explanation = model.explain("Sales:Revenue YTD")
    assert explanation["backend"] == "Python materialization backend"


def test_compile_deploys_shift_helper_when_reverse_attribute_mapping_is_proven():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue Prior Month": "Numeric",
                },
                dimension_attributes={"Month": ["Prev Month"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["Jan", "Feb"],
                },
                dimension_attribute_values={
                    "Month": {
                        "Feb": {"Prev Month": "Jan"},
                    },
                },
            ),
        }
    )
    tm1 = MockTM1Service()
    model = Model(metadata_provider=provider, tm1=tm1)
    sales = model.cube("Sales")

    sales["Revenue Prior Month"] = sales["Revenue"].shift(Month="Prev Month")

    preview = model.compile(dry_run=False)

    assert preview.deployment["Sales"]["deployed"] is True


def test_compile_keeps_shift_helper_preview_only_without_reverse_attribute_metadata():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Month": "Month",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Revenue Prior Month": "Numeric",
                },
                dimension_attributes={"Month": ["Prev Month"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Month": ["Month"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Month": ["Jan", "Feb"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Revenue Prior Month"] = sales["Revenue"].shift(Month="Prev Month")

    explanation = model.explain("Sales:Revenue Prior Month")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_same_cube_shift"
    assert preview.manifest["Sales:Revenue Prior Month"]["artifact"]["preview_only"] is True

    blocked_model = Model(metadata_provider=provider, tm1=MockTM1Service())
    blocked_sales = blocked_model.cube("Sales")
    blocked_sales["Revenue Prior Month"] = blocked_sales["Revenue"].shift(Month="Prev Month")
    with pytest.raises(DeploymentError):
        blocked_model.compile(dry_run=False)


def test_validate_warns_when_native_formula_is_not_deployment_ready():
    model = Model()
    sales = model.cube("Sales")

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

    report = model.validate()

    assert any(
        "native-targeted but not deployment-ready because metadata is missing" in warning
        for warning in report.warnings
    )


def test_validate_warns_when_cross_cube_native_preview_is_still_live_blocked():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    report = model.validate()

    assert any(
        "live deployment stays blocked until a safe cross-cube feeder plan is proven" in warning
        for warning in report.warnings
    )


def test_phase2_readiness_reports_missing_metadata_without_provider():
    model = Model()
    sales = model.cube("Sales")

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

    readiness = model.phase2_readiness("Sales:Gross Margin")

    assert readiness["backend"] == "native-rule backend"
    assert readiness["metadata_ready"] is False
    assert readiness["missing_metadata"] == [
        "cube_dimensions",
        "measure_dimension",
        "measure_types",
        "measure_exists",
        "numeric_measure",
        "measure_exists",
        "numeric_measure",
        "measure_exists",
        "numeric_measure",
    ]
    assert readiness["native_eligibility"] == {
        "status": "metadata_incomplete",
        "code": "native_metadata_missing",
        "category": "metadata",
        "detail": "Native compilation remains the intended path, but required metadata is missing.",
    }


def test_phase2_readiness_uses_tm1_metadata_when_available():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Period", {"2026-JAN": "String"})
    tm1.add_dimension("Measure", {"Revenue": "Numeric", "Cost": "Numeric", "Gross Margin": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Period", "Measure"])

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

    readiness = model.phase2_readiness("Sales:Gross Margin")

    assert readiness["metadata_ready"] is True
    assert readiness["missing_metadata"] == []


def test_phase2_readiness_reports_align_prerequisites_when_mapping_is_incomplete():
    provider = StaticMetadataProvider(
        {
            "Sales Extended": build_static_cube_metadata(
                "Sales Extended",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric", "FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Region", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Region": "Region",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Region": ["Region"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales Extended")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = (
        sales["Revenue Local"]
        * fx["Rate"].align(
            Currency=sales.Company.attribute("Currency"),
            TargetCurrency="EUR",
        )
    )

    readiness = model.phase2_readiness("Sales Extended:Revenue EUR")

    assert readiness["metadata_ready"] is False
    assert "alignment_mapping" in readiness["missing_metadata"]
    assert "dimension_attribute" not in readiness["missing_metadata"]
    assert "measure_types" not in readiness["missing_metadata"]
    assert "measure_exists" not in readiness["missing_metadata"]
    assert "numeric_measure" not in readiness["missing_metadata"]


def test_phase2_readiness_marks_constrained_align_ready_when_metadata_proves_mapping():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric", "FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = (
        sales["Revenue Local"]
        * fx["Rate"].align(
            Currency=sales.Company.attribute("Currency"),
            TargetCurrency="EUR",
        )
    )

    readiness = model.phase2_readiness("Sales:Revenue EUR")

    assert readiness["backend"] == "native-rule backend"
    assert readiness["metadata_ready"] is True
    assert readiness["missing_metadata"] == []


def test_compile_preview_lowers_constrained_cross_cube_align_to_db_lookup():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
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
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "cross_cube_local_driver"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue EUR'] = N: "
        "(['Measure':'Measure':'Revenue Local'] * "
        "DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'));"
    )
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Revenue Local'] => ['Measure':'Measure':'Revenue EUR'];"
    )
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["feeder_strategy"] == "cross_cube_local_driver"
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["preview_only"] is False


def test_phase2_readiness_blocks_align_when_fixed_element_hierarchy_is_ambiguous():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency", "Reporting Currency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = (
        sales["Revenue Local"]
        * fx["Rate"].align(
            Currency=sales.Company.attribute("Currency"),
            TargetCurrency="EUR",
        )
    )

    readiness = model.phase2_readiness("Sales:Revenue EUR")
    explanation = model.explain("Sales:Revenue EUR")

    assert readiness["backend"] == "Python materialization backend"
    assert "hierarchy_resolution" in readiness["missing_metadata"]
    assert "multiple hierarchies" in explanation["rationale"]


def test_compile_preview_uses_explicit_hierarchy_when_fixed_element_requests_it():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency", "Reporting Currency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = (
        sales["Revenue Local"]
        * fx["Rate"].align(
            Currency=sales.Company.attribute("Currency"),
            TargetCurrency=fx.TargetCurrency.element("EUR", hierarchy="Reporting Currency"),
        )
    )

    preview = model.compile(dry_run=True)

    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue EUR'] = N: "
        "(['Measure':'Measure':'Revenue Local'] * "
        "DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "'TargetCurrency':'Reporting Currency':'EUR', 'Measure':'Measure':'Rate'));"
    )


def test_compile_preview_emits_true_cross_cube_feeders_for_direct_lookup_case():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate EUR"] = fx["Rate"].align(TargetCurrency="EUR")

    explanation = model.explain("Sales:FX Rate EUR")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "cross_cube_source_lookup"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'FX Rate EUR'] = N: "
        "DB('FX Rates', !Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate');"
    )
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', !Version, !Company, "
        "'Measure':'Measure':'FX Rate EUR');"
    )
    assert preview.deployment["FX Rates"]["rule_line_count"] == 0
    assert preview.deployment["FX Rates"]["feeder_count"] == 1
    assert preview.manifest["Sales:FX Rate EUR"]["artifact"]["feeder_strategy"] == "cross_cube_source_lookup"
    assert preview.manifest["Sales:FX Rate EUR"]["artifact"]["feeder_deployment_cube"] == "FX Rates"
    assert preview.manifest["Sales:FX Rate EUR"]["artifact"]["preview_only"] is False


def test_compile_preview_feeds_nested_branches_traced_intermediates_and_multiple_cross_cube_origins():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Cost": "Numeric",
                    "Base Margin": "Numeric",
                    "Use A": "Numeric",
                    "Fallback": "Numeric",
                    "Routed Value": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "A FX": build_static_cube_metadata(
                "A FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"TargetCurrency": ["EUR"]},
            ),
            "B FX": build_static_cube_metadata(
                "B FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"TargetCurrency": ["USD"]},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    a_fx = model.cube("A FX")
    b_fx = model.cube("B FX")

    # Stress case for feeder-origin direction: a same-cube rule-calculated intermediate ("Base
    # Margin") used as a nested-case selector must trace back to its original sources (Revenue, Cost)
    # rather than chain through the intermediate; "Use A" and "Fallback" are genuine branch
    # selector/value inputs; "A FX" and "B FX" are two independent cross-cube origins selected by
    # different nested branches. All of these must feed "Routed Value" directly and exactly once.
    sales["Base Margin"] = (sales["Revenue"] - sales["Cost"]).native(feeder_mode="safe_union")
    sales["Routed Value"] = model.case(
        (sales["Use A"] != 0, a_fx["Rate"].align(TargetCurrency="EUR")),
        default=model.case(
            (sales["Base Margin"] != 0, b_fx["Rate"].align(TargetCurrency="USD")),
            default=sales["Fallback"],
        ),
    ).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:Routed Value")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Cost'] => ['Measure':'Measure':'Base Margin'];\n"
        "['Measure':'Measure':'Cost'] => ['Measure':'Measure':'Routed Value'];\n"
        "['Measure':'Measure':'Fallback'] => ['Measure':'Measure':'Routed Value'];\n"
        "['Measure':'Measure':'Revenue'] => ['Measure':'Measure':'Base Margin'];\n"
        "['Measure':'Measure':'Revenue'] => ['Measure':'Measure':'Routed Value'];\n"
        "['Measure':'Measure':'Use A'] => ['Measure':'Measure':'Routed Value'];"
    )
    assert preview.feeders["A FX"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Routed Value');"
    )
    assert preview.feeders["B FX"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'USD', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Routed Value');"
    )
    assert preview.manifest["Sales:Routed Value"]["artifact"]["feeder_deployment_cubes"] == [
        "A FX",
        "B FX",
        "Sales",
    ]
    assert preview.manifest["Sales:Routed Value"]["artifact"]["preview_only"] is False


def test_compile_preview_feeds_both_same_cube_driver_and_cross_cube_origin():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    # "Revenue Local" is a genuine value-composing, zero-gating same-cube driver, AND the cross-cube
    # align is independently resolvable as a direct invertible lookup. Both sides of the formula
    # contribute to the result, so both must feed the target: missing either one is under-feeding
    # (e.g. a Revenue Local update or a Rate update would otherwise silently fail to trigger recalc).
    sales["Revenue EUR"] = sales["Revenue Local"] * fx["Rate"].align(TargetCurrency="EUR")

    explanation = model.explain("Sales:Revenue EUR")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Revenue EUR');"
    )
    assert "Sales" not in preview.feeders
    assert explanation["feeder_rationale"].startswith(
        "Feeder origin ranking selected 'FX Rates' over 'Sales' by higher dimension count, "
        "following the original rule order (higher dimension count first; equal-dimension ties by rarer fill)."
    )
    assert explanation["selected_feeder_origin"] == "FX Rates::Rate::TargetCurrency=EUR"
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["feeder_deployment_cube"] == "FX Rates"
    assert preview.manifest["Sales:Revenue EUR"]["artifact"].get("feeder_deployment_cubes") in (None, ["FX Rates"])
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["preview_only"] is False


def test_compile_preview_ranks_equal_dimension_candidates_by_rarer_fill(monkeypatch):
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Amount": "Numeric", "Revenue EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = sales["Amount"] * fx["Rate"].align()

    def _fake_count(candidate):
        return 3 if candidate.cube_name == "Sales" else 9

    monkeypatch.setattr(model, "_count_candidate_origin_nonzero_cells", _fake_count)

    explanation = model.explain("Sales:Revenue EUR")
    preview = model.compile(dry_run=True)

    assert preview.feeders["Sales"] == "['Measure':'Measure':'Amount'] => ['Measure':'Measure':'Revenue EUR'];"
    assert "FX Rates" not in preview.feeders
    assert "rarer fill probability" in explanation["feeder_rationale"]
    assert "Winner nonzero count=3; loser nonzero count=9." in explanation["feeder_rationale"]
    assert explanation["selected_feeder_origin"] == "Sales::Amount::"


def test_compile_preview_keeps_equal_dimension_ranked_origin_preview_only_without_tm1():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Amount": "Numeric", "Revenue EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = sales["Amount"] * fx["Rate"].align()

    explanation = model.explain("Sales:Revenue EUR")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_ranked_origin"
    assert explanation["native_eligibility"]["code"] == "native_live_feeder_ranking_required"
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["preview_only"] is True


def test_compile_preview_keeps_equal_dimension_ranked_origin_preview_only_when_live_rarity_read_fails(monkeypatch):
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Amount": "Numeric", "Revenue EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = sales["Amount"] * fx["Rate"].align()

    monkeypatch.setattr(model, "_count_candidate_origin_nonzero_cells", lambda _candidate: None)

    explanation = model.explain("Sales:Revenue EUR")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_ranked_origin"
    assert explanation["native_eligibility"]["code"] == "native_live_feeder_ranking_required"
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["preview_only"] is True


def test_compile_preview_same_cube_multi_source_defaults_to_ranked_single_and_preview_only_without_tm1():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Cost": "Numeric",
                    "Gross Margin": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

    explanation = model.explain("Sales:Gross Margin")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_mode"] == "ranked_single"
    assert explanation["feeder_strategy"] == "preview_only_ranked_origin"
    assert explanation["native_eligibility"]["code"] == "native_live_feeder_ranking_required"
    assert preview.manifest["Sales:Gross Margin"]["artifact"]["preview_only"] is True


def test_compile_preview_same_cube_multi_source_safe_union_keeps_all_feeders():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Cost": "Numeric",
                    "Gross Margin": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Gross Margin"] = (sales["Revenue"] - sales["Cost"]).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:Gross Margin")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_mode"] == "safe_union"
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Cost'] => ['Measure':'Measure':'Gross Margin'];\n"
        "['Measure':'Measure':'Revenue'] => ['Measure':'Measure':'Gross Margin'];"
    )


def test_compile_preview_ranks_conditional_branch_origins_in_ranked_single_mode(monkeypatch):
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Use Corporate Rate": "Numeric",
                    "FX Rate Routed": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "Corporate FX": build_static_cube_metadata(
                "Corporate FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"TargetCurrency": ["EUR"]},
            ),
            "Market FX": build_static_cube_metadata(
                "Market FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"TargetCurrency": ["EUR"]},
            ),
        }
    )
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")
    corporate_fx = model.cube("Corporate FX")
    market_fx = model.cube("Market FX")

    sales["FX Rate Routed"] = model.case(
        (sales["Use Corporate Rate"] != 0, corporate_fx["Rate"].align(TargetCurrency="EUR")),
        default=market_fx["Rate"].align(TargetCurrency="EUR"),
    )

    def _fake_count(candidate):
        return 1 if candidate.cube_name == "Corporate FX" else 9

    monkeypatch.setattr(model, "_count_candidate_origin_nonzero_cells", _fake_count)

    explanation = model.explain("Sales:FX Rate Routed")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_mode"] == "ranked_single"
    assert explanation["feeder_strategy"] == "ranked_cross_cube_direct_origin"
    assert explanation["selected_feeder_origin"] == "Corporate FX::Rate::TargetCurrency=EUR"
    assert "Corporate FX" in preview.feeders
    assert "Market FX" not in preview.feeders


def test_compile_preview_prefers_more_fixed_coordinates_when_equal_dimension_and_fill(monkeypatch):
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Amount": "Numeric", "Revenue EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"Company": ["ACME"]},
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"Currency": ["EUR"]},
            ),
        }
    )
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = sales["Amount"] * fx["Rate"].align(Currency="EUR")

    def _fake_count(_candidate):
        return 5

    monkeypatch.setattr(model, "_count_candidate_origin_nonzero_cells", _fake_count)

    explanation = model.explain("Sales:Revenue EUR")
    preview = model.compile(dry_run=True)

    assert "more fixed/current coordinates" in explanation["feeder_rationale"]
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, 'Company':'Company':'ACME', 'Measure':'Measure':'Revenue EUR');"
    )
    assert "Sales" not in preview.feeders


def test_compile_preview_prefers_same_cube_when_equal_dimension_fill_and_fixed(monkeypatch):
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Amount": "Numeric", "Revenue EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = sales["Amount"] * fx["Rate"].align()

    def _fake_count(_candidate):
        return 5

    monkeypatch.setattr(model, "_count_candidate_origin_nonzero_cells", _fake_count)

    explanation = model.explain("Sales:Revenue EUR")
    preview = model.compile(dry_run=True)

    assert "same-cube preference" in explanation["feeder_rationale"]
    assert preview.feeders["Sales"] == "['Measure':'Measure':'Amount'] => ['Measure':'Measure':'Revenue EUR'];"
    assert "FX Rates" not in preview.feeders


def test_ranked_origin_traceback_uses_true_upstream_base_driver(monkeypatch):
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Revenue": "Numeric",
                    "Base Amount": "Numeric",
                    "Revenue EUR": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Base Amount"] = sales["Revenue"]
    sales["Revenue EUR"] = sales["Base Amount"] * fx["Rate"].align()

    def _fake_count(candidate):
        return 1 if candidate.cube_name == "Sales" else 10

    monkeypatch.setattr(model, "_count_candidate_origin_nonzero_cells", _fake_count)

    explanation = model.explain("Sales:Revenue EUR")

    same_cube_candidate = next(
        candidate for candidate in explanation["feeder_origin_candidates"]
        if candidate["cube"] == "Sales"
    )
    assert same_cube_candidate["measures"] == ["Revenue"]


def test_compile_preview_expands_consolidated_fixed_source_elements_to_leaf_feeders():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Group": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR", "USD"],
                },
                dimension_element_leaf_expansions={
                    "TargetCurrency": {
                        "All Reporting": ["EUR", "USD"],
                    },
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Group"] = fx["Rate"].align(TargetCurrency="All Reporting")

    explanation = model.explain("Sales:FX Rate Group")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "cross_cube_source_lookup"
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Group');\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'USD', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Group');"
    )
    assert preview.manifest["Sales:FX Rate Group"]["artifact"]["preview_only"] is False


def test_compile_keeps_consolidated_fixed_source_elements_preview_only_without_leaf_expansion_metadata():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Group": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR", "USD"],
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Group"] = fx["Rate"].align(TargetCurrency="All Reporting")

    explanation = model.explain("Sales:FX Rate Group")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate Group"]["artifact"]["preview_only"] is True


def test_compile_preview_emits_attribute_routed_cross_cube_feeders_when_leaf_mapping_is_proven():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA", "GAMMA"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "USD"},
                        "GAMMA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "cross_cube_attribute_lookup"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'GAMMA', "
        "'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'FX Rate'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'FX Rate');"
    )
    assert preview.deployment["FX Rates"]["rule_line_count"] == 0
    assert preview.deployment["FX Rates"]["feeder_count"] == 2
    assert preview.manifest["Sales:FX Rate"]["artifact"]["feeder_strategy"] == "cross_cube_attribute_lookup"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["feeder_deployment_cube"] == "FX Rates"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is False


def test_phase2_readiness_reports_reverse_mapping_gap_for_attribute_routed_align():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "GBP"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")

    assert readiness["metadata_ready"] is False
    assert "native_align_reverse_mapping_unproven" in readiness["missing_metadata"]
    assert readiness["native_eligibility"] == {
        "status": "metadata_incomplete",
        "code": "native_align_reverse_mapping_unproven",
        "category": "metadata",
        "detail": "Native rule lowering remains the intended path, but metadata does not yet prove the reverse attribute mapping from source lookup elements to target leaf elements, including any required composite multi-attribute route.",
    }


def test_phase2_readiness_reports_target_leaf_scope_gap_for_attribute_routed_broadcast_align():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Region": ["Region", "Alt Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                    "Region": ["East", "West"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")

    assert readiness["backend"] == "native-rule backend"
    assert readiness["metadata_ready"] is False
    assert "native_align_target_leaf_scope_unproven" in readiness["missing_metadata"]
    assert readiness["native_eligibility"] == {
        "status": "metadata_incomplete",
        "code": "native_align_target_leaf_scope_unproven",
        "category": "metadata",
        "detail": "Native rule lowering remains the intended path, but metadata does not yet prove the target-only broadcast leaf scope.",
    }


def _multi_attribute_align_metadata_provider(
    *,
    include_target_region: bool = False,
    ambiguous_target_region: bool = False,
    missing_company_region_code: bool = False,
    invalid_company_region_code: bool = False,
):
    company_attributes = {
        "ACME": {"Currency": "USD", "RegionCode": "NA"},
        "BETA": {"Currency": "USD", "RegionCode": "EU"},
        "DELTA": {"Currency": "USD", "RegionCode": "EU"},
        "GAMMA": {"Currency": "EUR", "RegionCode": "EU"},
    }
    if missing_company_region_code:
        company_attributes["DELTA"] = {"Currency": "USD"}
    if invalid_company_region_code:
        company_attributes["GAMMA"] = {"Currency": "EUR", "RegionCode": "APAC"}

    sales_dimensions = ["Version", "Company"]
    if include_target_region:
        sales_dimensions.append("Region")
    sales_dimensions.append("Measure")

    sales_default_hierarchies = {
        "Version": "Version",
        "Company": "Company",
        "Measure": "Measure",
    }
    if include_target_region and not ambiguous_target_region:
        sales_default_hierarchies["Region"] = "Region"

    sales_hierarchies = {
        "Version": ["Version"],
        "Company": ["Company"],
        "Measure": ["Measure"],
    }
    if include_target_region:
        sales_hierarchies["Region"] = ["Region", "Alt Region"] if ambiguous_target_region else ["Region"]

    sales_leaf_elements = {
        "Company": ["ACME", "BETA", "DELTA", "GAMMA"],
    }
    if include_target_region:
        sales_leaf_elements["Region"] = ["East", "West"]

    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                sales_dimensions,
                default_hierarchies=sales_default_hierarchies,
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency", "RegionCode"]},
                dimension_hierarchies=sales_hierarchies,
                dimension_leaf_elements=sales_leaf_elements,
                dimension_attribute_values={"Company": company_attributes},
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "RegionCode", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "RegionCode": "RegionCode",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "RegionCode": ["RegionCode"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "RegionCode": ["NA", "EU"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )


def test_phase2_readiness_marks_multi_attribute_same_target_dimension_align_ready_when_metadata_proves_reverse_map():
    provider = _multi_attribute_align_metadata_provider()
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")
    explanation = model.explain("Sales:FX Rate")

    assert readiness["metadata_ready"] is True
    assert readiness["native_eligibility"]["status"] == "deployable"
    assert explanation["feeder_strategy"] == "cross_cube_attribute_lookup"


def test_compile_preview_emits_multi_attribute_same_target_dimension_cross_cube_feeders():
    provider = _multi_attribute_align_metadata_provider()
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    preview = model.compile(dry_run=True)

    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'RegionCode':'RegionCode':'EU', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'GAMMA', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'RegionCode':'RegionCode':'EU', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'BETA', 'Measure':'Measure':'FX Rate'), DB('Sales', "
        "!Version, 'Company':'Company':'DELTA', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'RegionCode':'RegionCode':'NA', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'ACME', 'Measure':'Measure':'FX Rate');"
    )
    assert preview.manifest["Sales:FX Rate"]["artifact"]["feeder_strategy"] == "cross_cube_attribute_lookup"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is False


def test_compile_deploys_multi_attribute_cross_cube_feeders_from_tm1_metadata():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension(
        "Company",
        {
            "ACME": "String",
            "BETA": "String",
            "DELTA": "String",
            "GAMMA": "String",
        },
        element_attributes={"Currency": "String", "RegionCode": "String"},
        attribute_values={
            "ACME": {"Currency": "USD", "RegionCode": "NA"},
            "BETA": {"Currency": "USD", "RegionCode": "EU"},
            "DELTA": {"Currency": "USD", "RegionCode": "EU"},
            "GAMMA": {"Currency": "EUR", "RegionCode": "EU"},
        },
    )
    tm1.add_dimension("Measure", {"FX Rate": "Numeric"})
    tm1.add_dimension("Currency", {"USD": "String", "EUR": "String"})
    tm1.add_dimension("RegionCode", {"NA": "String", "EU": "String"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_dimension("FX Measure", {"Rate": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "Currency", "RegionCode", "TargetCurrency", "FX Measure"])

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    result = model.compile(dry_run=False)

    assert result.deployment["Sales"]["deployed"] is True
    assert result.deployment["FX Rates"]["deployed"] is True
    assert result.deployment["FX Rates"]["feeder_count"] == 3
    assert tm1.cube_rules("Sales") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: Sales\n"
        "SKIPCHECK;\n"
        "\n"
        "['Measure':'Measure':'FX Rate'] = N: DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "ATTRS('Company', !Company, 'RegionCode'), 'TargetCurrency':'TargetCurrency':'EUR', "
        "'FX Measure':'FX Measure':'Rate');\n"
    )


def test_compile_preview_emits_multi_attribute_broadcast_cross_cube_feeders_when_leaf_scope_is_proven():
    provider = _multi_attribute_align_metadata_provider(include_target_region=True)
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    preview = model.compile(dry_run=True)

    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'RegionCode':'RegionCode':'EU', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'GAMMA', 'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), "
        "DB('Sales', !Version, 'Company':'Company':'GAMMA', 'Region':'Region':'West', "
        "'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'RegionCode':'RegionCode':'EU', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'BETA', 'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), "
        "DB('Sales', !Version, 'Company':'Company':'BETA', 'Region':'Region':'West', "
        "'Measure':'Measure':'FX Rate'), DB('Sales', !Version, 'Company':'Company':'DELTA', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'DELTA', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'RegionCode':'RegionCode':'NA', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'ACME', 'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), "
        "DB('Sales', !Version, 'Company':'Company':'ACME', 'Region':'Region':'West', "
        "'Measure':'Measure':'FX Rate');"
    )
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is False


def test_phase2_readiness_reports_reverse_mapping_gap_for_multi_attribute_align_when_target_leaf_attribute_is_missing():
    provider = _multi_attribute_align_metadata_provider(missing_company_region_code=True)
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")

    assert readiness["metadata_ready"] is False
    assert readiness["native_eligibility"]["code"] == "native_align_reverse_mapping_unproven"


def test_phase2_readiness_reports_reverse_mapping_gap_for_multi_attribute_align_when_attribute_value_is_not_a_source_leaf():
    provider = _multi_attribute_align_metadata_provider(invalid_company_region_code=True)
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")

    assert readiness["metadata_ready"] is False
    assert readiness["native_eligibility"]["code"] == "native_align_reverse_mapping_unproven"


def test_phase2_readiness_reports_target_leaf_scope_gap_for_multi_attribute_broadcast_align():
    provider = _multi_attribute_align_metadata_provider(
        include_target_region=True,
        ambiguous_target_region=True,
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")

    assert readiness["metadata_ready"] is False
    assert readiness["native_eligibility"]["code"] == "native_align_target_leaf_scope_unproven"


def test_align_mapping_rejects_fixed_element_plus_attribute_routing_outside_supported_slice():
    provider = _multi_attribute_align_metadata_provider()
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.element("ACME"),
        TargetCurrency="EUR",
    )

    explanation = model.explain("Sales:FX Rate")

    assert explanation["backend"] == "Python materialization backend"
    assert "must target the same dimension" in explanation["rationale"]


def test_compile_preview_emits_attribute_routed_broadcast_cross_cube_feeders_when_leaf_scope_is_proven():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Region": "Region",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Region": ["Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                    "Region": ["East", "West"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "cross_cube_attribute_lookup"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'BETA', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'ACME', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');"
    )
    assert preview.manifest["Sales:FX Rate"]["artifact"]["feeder_strategy"] == "cross_cube_attribute_lookup"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is False


def test_compile_preview_emits_conditional_cross_cube_branch_feeders_for_case_expression():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Use EUR": "Numeric",
                    "FX Rate Chosen": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR", "USD"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Chosen"] = model.case(
        (sales["Use EUR"] != 0, fx["Rate"].align(TargetCurrency="EUR")),
        default=fx["Rate"].align(TargetCurrency="USD"),
    ).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:FX Rate Chosen")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Chosen');\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'USD', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Chosen');"
    )
    assert preview.manifest["Sales:FX Rate Chosen"]["artifact"]["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.manifest["Sales:FX Rate Chosen"]["artifact"]["preview_only"] is False


def test_compile_preview_emits_conditional_cross_cube_branch_feeders_for_where_expression():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Enable FX": "Numeric",
                    "FX Rate Guarded": "Numeric",
                },
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA", "GAMMA"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "USD"},
                        "GAMMA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Guarded"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    ).where(sales["Enable FX"] != 0).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:FX Rate Guarded")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'GAMMA', "
        "'Measure':'Measure':'FX Rate Guarded');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'FX Rate Guarded'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'FX Rate Guarded');"
    )
    assert preview.manifest["Sales:FX Rate Guarded"]["artifact"]["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.manifest["Sales:FX Rate Guarded"]["artifact"]["preview_only"] is False


def test_compile_preview_emits_mixed_conditional_cross_cube_branch_feeders_from_same_source_cube():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Use Corporate Rate": "Numeric",
                    "FX Rate Mixed": "Numeric",
                },
                dimension_attributes={"Company": ["Region"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA", "GAMMA"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Region": "NA"},
                        "BETA": {"Region": "NA"},
                        "GAMMA": {"Region": "EU"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Region", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Region": "Region",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Region": ["Region"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Region": ["Global", "NA", "EU"],
                    "TargetCurrency": ["EUR", "USD"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Mixed"] = model.case(
        (sales["Use Corporate Rate"] != 0, fx["Rate"].align(Region="Global", TargetCurrency="EUR")),
        default=fx["Rate"].align(
            Region=sales.Company.attribute("Region"),
            TargetCurrency="USD",
        ),
    ).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:FX Rate Mixed")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Region':'Region':'EU', 'TargetCurrency':'TargetCurrency':'USD', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'GAMMA', "
        "'Measure':'Measure':'FX Rate Mixed');\n"
        "[!Version, 'Region':'Region':'Global', 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, 'Company':'Company':'ACME', 'Measure':'Measure':'FX Rate Mixed'), "
        "DB('Sales', !Version, 'Company':'Company':'BETA', 'Measure':'Measure':'FX Rate Mixed'), "
        "DB('Sales', !Version, 'Company':'Company':'GAMMA', 'Measure':'Measure':'FX Rate Mixed');\n"
        "[!Version, 'Region':'Region':'NA', 'TargetCurrency':'TargetCurrency':'USD', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'FX Rate Mixed'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'FX Rate Mixed');"
    )
    assert preview.manifest["Sales:FX Rate Mixed"]["artifact"]["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.manifest["Sales:FX Rate Mixed"]["artifact"]["preview_only"] is False


def test_compile_preview_emits_conditional_cross_cube_branch_feeders_from_multiple_source_cubes():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Use Corporate Rate": "Numeric",
                    "FX Rate Routed": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "Corporate FX": build_static_cube_metadata(
                "Corporate FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
            "Market FX": build_static_cube_metadata(
                "Market FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    corporate_fx = model.cube("Corporate FX")
    market_fx = model.cube("Market FX")

    sales["FX Rate Routed"] = model.case(
        (sales["Use Corporate Rate"] != 0, corporate_fx["Rate"].align(TargetCurrency="EUR")),
        default=market_fx["Rate"].align(TargetCurrency="EUR"),
    ).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:FX Rate Routed")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.feeders["Corporate FX"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Routed');"
    )
    assert preview.feeders["Market FX"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Routed');"
    )
    # "Use Corporate Rate" is the branch selector: it genuinely changes the computed value (which FX
    # cube's rate is used), so it must also feed the target directly, alongside both cross-cube origins.
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Use Corporate Rate'] => ['Measure':'Measure':'FX Rate Routed'];"
    )
    assert preview.manifest["Sales:FX Rate Routed"]["artifact"]["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.manifest["Sales:FX Rate Routed"]["artifact"]["feeder_deployment_cube"] is None
    assert preview.manifest["Sales:FX Rate Routed"]["artifact"]["feeder_deployment_cubes"] == [
        "Corporate FX",
        "Market FX",
        "Sales",
    ]
    assert preview.manifest["Sales:FX Rate Routed"]["artifact"]["preview_only"] is False


def test_compile_preview_emits_conditional_cross_cube_branch_feeders_for_parameter_cube_gated_condition():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Local Rate": "Numeric",
                    "Fallback Rate": "Numeric",
                    "FX Rate Param Gated": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Use Flag": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Param Gated"] = model.case(
        (fx["Use Flag"].align(TargetCurrency="EUR") != 0, sales["Local Rate"]),
        default=sales["Fallback Rate"],
    ).native(feeder_mode="safe_union")

    explanation = model.explain("Sales:FX Rate Param Gated")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Use Flag'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Param Gated');"
    )
    # "Local Rate" and "Fallback Rate" are the branch values themselves: they genuinely determine the
    # computed result, so both must also feed the target directly alongside the parameter-cube gate.
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Fallback Rate'] => ['Measure':'Measure':'FX Rate Param Gated'];\n"
        "['Measure':'Measure':'Local Rate'] => ['Measure':'Measure':'FX Rate Param Gated'];"
    )
    assert preview.manifest["Sales:FX Rate Param Gated"]["artifact"]["feeder_strategy"] == "conditional_cross_cube_branches"
    assert preview.manifest["Sales:FX Rate Param Gated"]["artifact"]["feeder_deployment_cube"] is None
    assert preview.manifest["Sales:FX Rate Param Gated"]["artifact"]["feeder_deployment_cubes"] == [
        "FX Rates",
        "Sales",
    ]
    assert preview.manifest["Sales:FX Rate Param Gated"]["artifact"]["preview_only"] is False


def test_compile_rejects_conditional_branch_union_when_one_branch_is_structurally_unsafe():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Use EUR": "Numeric",
                    "FX Rate Group": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR", "USD"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    # "Use EUR" is a pure branch selector here, not a value-composing or zero-gating driver of the
    # whole cross-cube term, and the default branch's "All Reporting" target is consolidated with no
    # leaf-expansion metadata. The conditional branch union must therefore reject the whole formula
    # for live deployment rather than fall back to a same-cube-only feeder plan that would silently
    # ignore the FX Rates dependency for the resolvable branch and hide the unresolvable branch.
    sales["FX Rate Group"] = model.case(
        (sales["Use EUR"] != 0, fx["Rate"].align(TargetCurrency="EUR")),
        default=fx["Rate"].align(TargetCurrency="All Reporting"),
    )

    explanation = model.explain("Sales:FX Rate Group")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate Group"]["artifact"]["preview_only"] is True

    blocked_model = Model(metadata_provider=provider, tm1=MockTM1Service())
    blocked_sales = blocked_model.cube("Sales")
    blocked_fx = blocked_model.cube("FX Rates")
    blocked_sales["FX Rate Group"] = model.case(
        (blocked_sales["Use EUR"] != 0, blocked_fx["Rate"].align(TargetCurrency="EUR")),
        default=blocked_fx["Rate"].align(TargetCurrency="All Reporting"),
    )
    with pytest.raises(DeploymentError):
        blocked_model.compile(dry_run=False)


def test_align_mapping_rejects_cross_cube_value_driven_routing_as_ambiguous():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Routed": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
            "Routing Params": build_static_cube_metadata(
                "Routing Params",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Currency Code": "String"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")
    routing = model.cube("Routing Params")

    # The routing target currency is determined by a cell VALUE in a parameter cube, not by a static
    # dimension attribute. This is exactly the "many-to-many ambiguous routing driven by a parameter
    # cube value" pattern the project documents flag as unsafe to automate: the planner cannot derive
    # a bounded, metadata-proven reverse feeder mapping from a runtime cell value, so it must fall back
    # to the Python materialization backend rather than guess.
    sales["FX Rate Routed"] = fx["Rate"].align(
        TargetCurrency=routing["Currency Code"],
    )

    explanation = model.explain("Sales:FX Rate Routed")

    assert explanation["backend"] == "Python materialization backend"
    assert "literal" in explanation["rationale"] or "attribute reference" in explanation["rationale"]
    native_eligibility = explanation["native_eligibility"]
    assert native_eligibility["status"] == "unsupported"
    assert native_eligibility["code"] == "native_value_driven_mapping"
    assert native_eligibility["category"] == "mapping_shape"
    assert "align(...)" in native_eligibility["detail"]
    assert "value-driven" in native_eligibility["detail"]
    assert "confirmed permanent exclusion" in native_eligibility["detail"]
    assert "Recommended workaround" in native_eligibility["detail"]


def test_align_mapping_rejects_nested_align_as_ambiguous_routing():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Routed": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
            "Routing Params": build_static_cube_metadata(
                "Routing Params",
                ["Version", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Mapped Currency": "String"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")
    routing = model.cube("Routing Params")

    # A mapping value that is itself another align(...) lookup is a two-hop, value-driven routing
    # chain. It must be rejected the same way a direct cross-cube value reference is.
    sales["FX Rate Routed"] = fx["Rate"].align(
        TargetCurrency=routing["Mapped Currency"].align(TargetCurrency="EUR"),
    )

    explanation = model.explain("Sales:FX Rate Routed")

    assert explanation["backend"] == "Python materialization backend"


def test_compile_blocks_live_deployment_for_preview_only_cross_cube_align_without_local_driver():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric", "FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["native_eligibility"] == {
        "status": "metadata_incomplete",
        "code": "native_align_reverse_mapping_unproven",
        "category": "metadata",
        "detail": "Native rule lowering remains the intended path, but metadata does not yet prove the reverse attribute mapping from source lookup elements to target leaf elements, including any required composite multi-attribute route.",
    }
    assert preview.manifest["Sales:FX Rate"]["artifact"]["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is True

    with pytest.raises(DeploymentError, match="native_align_reverse_mapping_unproven"):
        model.compile(dry_run=False)


def test_compile_deploys_constrained_cross_cube_align_when_local_driver_feeders_exist():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Company", {"ACME": "String"})
    tm1.add_dimension("Measure", {"Revenue Local": "Numeric", "Revenue EUR": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_dimension("Currency", {"USD": "String"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_cube("FX Rates", ["Version", "Currency", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1, metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Revenue EUR"] = (
        sales["Revenue Local"]
        * fx["Rate"].align(
            Currency=sales.Company.attribute("Currency"),
            TargetCurrency="EUR",
        )
    )

    result = model.compile(dry_run=False)

    assert result.deployment["Sales"]["mode"] == "live"
    assert result.deployment["Sales"]["deployed"] is True
    assert result.deployment["Sales"]["formula_targets"] == ["Sales:Revenue EUR"]
    assert result.deployment["Sales"]["check_rules_status"] == 200
    assert tm1.cube_rules("Sales") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: Sales\n"
        "SKIPCHECK;\n"
        "\n"
        "['Measure':'Measure':'Revenue EUR'] = N: "
        "(['Measure':'Measure':'Revenue Local'] * "
        "DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'));\n"
        "\n"
        "FEEDERS;\n"
        "['Measure':'Measure':'Revenue Local'] => ['Measure':'Measure':'Revenue EUR'];\n"
    )


def test_compile_deploys_true_cross_cube_feeders_for_direct_lookup_case():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate EUR": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Company", {"ACME": "String"})
    tm1.add_dimension("Measure", {"FX Rate EUR": "Numeric", "Rate": "Numeric"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "Company", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1, metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate EUR"] = fx["Rate"].align(TargetCurrency="EUR")

    result = model.compile(dry_run=False)

    assert result.deployment["Sales"]["deployed"] is True
    assert result.deployment["FX Rates"]["deployed"] is True
    assert result.deployment["FX Rates"]["rule_line_count"] == 0
    assert result.deployment["FX Rates"]["feeder_count"] == 1
    assert tm1.cube_rules("Sales") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: Sales\n"
        "SKIPCHECK;\n"
        "\n"
        "['Measure':'Measure':'FX Rate EUR'] = N: "
        "DB('FX Rates', !Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate');\n"
    )
    assert tm1.cube_rules("FX Rates") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: FX Rates\n"
        "\n"
        "FEEDERS;\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', !Version, !Company, "
        "'Measure':'Measure':'FX Rate EUR');\n"
    )


def test_compile_deploys_attribute_routed_cross_cube_feeders_when_leaf_mapping_is_proven():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA", "GAMMA"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "USD"},
                        "GAMMA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Company", {"ACME": "String", "BETA": "String", "GAMMA": "String"})
    tm1.add_dimension("Measure", {"FX Rate": "Numeric", "Rate": "Numeric"})
    tm1.add_dimension("Currency", {"USD": "String", "EUR": "String"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "Currency", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1, metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    result = model.compile(dry_run=False)

    assert result.deployment["FX Rates"]["deployed"] is True
    assert result.deployment["FX Rates"]["rule_line_count"] == 0
    assert result.deployment["FX Rates"]["feeder_count"] == 2
    assert tm1.cube_rules("FX Rates") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: FX Rates\n"
        "\n"
        "FEEDERS;\n"
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'GAMMA', "
        "'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'FX Rate'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'FX Rate');\n"
    )


def test_compile_deploys_attribute_routed_broadcast_cross_cube_feeders_when_leaf_scope_is_proven():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Region": "Region",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Region": ["Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                    "Region": ["East", "West"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension(
        "Company",
        {"ACME": "String", "BETA": "String"},
        element_attributes={"Currency": "String"},
        attribute_values={
            "ACME": {"Currency": "USD"},
            "BETA": {"Currency": "EUR"},
        },
    )
    tm1.add_dimension("Region", {"East": "String", "West": "String"})
    tm1.add_dimension("Measure", {"FX Rate": "Numeric", "Rate": "Numeric"})
    tm1.add_dimension("Currency", {"USD": "String", "EUR": "String"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_cube("Sales", ["Version", "Company", "Region", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "Currency", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1, metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    result = model.compile(dry_run=False)

    assert result.deployment["FX Rates"]["deployed"] is True
    assert result.deployment["FX Rates"]["feeder_count"] == 2
    assert tm1.cube_rules("FX Rates") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: FX Rates\n"
        "\n"
        "FEEDERS;\n"
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'BETA', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'ACME', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');\n"
    )


def test_compile_deploys_conditional_cross_cube_branch_feeders_from_multiple_source_cubes():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Use Corporate Rate": "Numeric",
                    "FX Rate Routed": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "Corporate FX": build_static_cube_metadata(
                "Corporate FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
            "Market FX": build_static_cube_metadata(
                "Market FX",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Company", {"ACME": "String"})
    tm1.add_dimension("Measure", {"Use Corporate Rate": "Numeric", "FX Rate Routed": "Numeric", "Rate": "Numeric"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_cube("Corporate FX", ["Version", "Company", "TargetCurrency", "Measure"])
    tm1.add_cube("Market FX", ["Version", "Company", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1, metadata_provider=provider)
    sales = model.cube("Sales")
    corporate_fx = model.cube("Corporate FX")
    market_fx = model.cube("Market FX")

    sales["FX Rate Routed"] = model.case(
        (sales["Use Corporate Rate"] != 0, corporate_fx["Rate"].align(TargetCurrency="EUR")),
        default=market_fx["Rate"].align(TargetCurrency="EUR"),
    ).native(feeder_mode="safe_union")

    result = model.compile(dry_run=False)

    assert result.deployment["Sales"]["deployed"] is True
    assert result.deployment["Corporate FX"]["deployed"] is True
    assert result.deployment["Market FX"]["deployed"] is True
    assert result.deployment["Corporate FX"]["rule_line_count"] == 0
    assert result.deployment["Market FX"]["rule_line_count"] == 0
    assert result.deployment["Corporate FX"]["feeder_count"] == 1
    assert result.deployment["Market FX"]["feeder_count"] == 1
    assert tm1.cube_rules("Corporate FX") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: Corporate FX\n"
        "\n"
        "FEEDERS;\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', !Version, !Company, "
        "'Measure':'Measure':'FX Rate Routed');\n"
    )
    assert tm1.cube_rules("Market FX") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: Market FX\n"
        "\n"
        "FEEDERS;\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', !Version, !Company, "
        "'Measure':'Measure':'FX Rate Routed');\n"
    )


def test_compile_deploys_native_rule_artifacts_when_metadata_is_ready():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Period", {"2026-JAN": "String"})
    tm1.add_dimension("Measure", {"Revenue": "Numeric", "Cost": "Numeric", "Gross Margin": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Period", "Measure"])

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    sales["Gross Margin"] = (sales["Revenue"] - sales["Cost"]).native(feeder_mode="safe_union")

    result = model.compile(dry_run=False)

    assert result.deployment["Sales"]["mode"] == "live"
    assert result.deployment["Sales"]["deployed"] is True
    assert result.deployment["Sales"]["formula_targets"] == ["Sales:Gross Margin"]
    assert result.deployment["Sales"]["check_rules_status"] == 200
    assert tm1.cube_rules("Sales") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: Sales\n"
        "SKIPCHECK;\n"
        "\n"
        "['Measure':'Measure':'Gross Margin'] = N: "
        "(['Measure':'Measure':'Revenue'] - ['Measure':'Measure':'Cost']);\n"
        "\n"
        "FEEDERS;\n"
        "['Measure':'Measure':'Cost'] => ['Measure':'Measure':'Gross Margin'];\n"
        "['Measure':'Measure':'Revenue'] => ['Measure':'Measure':'Gross Margin'];\n"
    )


def test_compile_blocks_live_deployment_when_native_metadata_is_missing():
    model = Model(tm1=MockTM1Service())
    sales = model.cube("Sales")
    sales["Gross Margin"] = (sales["Revenue"] - sales["Cost"]).native(feeder_mode="safe_union")

    with pytest.raises(DeploymentError, match="required metadata is missing"):
        model.compile(dry_run=False)


def test_compile_raises_when_tm1_rule_check_fails():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Period", {"2026-JAN": "String"})
    tm1.add_dimension("Measure", {"Revenue": "Numeric", "Cost": "Numeric", "Gross Margin": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Period", "Measure"])
    tm1.set_cube_rule_check_result("Sales", 400, "Syntax error")

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    sales["Gross Margin"] = (sales["Revenue"] - sales["Cost"]).native(feeder_mode="safe_union")

    with pytest.raises(DeploymentError, match="TM1 rule validation failed"):
        model.compile(dry_run=False)


def test_phase2_readiness_rejects_string_measure_for_native_rule_target():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Period", "Measure"],
                measure_element_types={
                    "Revenue": "Numeric",
                    "Comment": "String",
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    sales["Comment"] = sales["Revenue"]

    readiness = model.phase2_readiness("Sales:Comment")

    assert readiness["backend"] == "native-rule backend"
    assert readiness["metadata_ready"] is False
    assert "numeric_measure" in readiness["missing_metadata"]


def test_metadata_provider_collects_element_types_and_children_from_tm1():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension(
        "TargetCurrency",
        {"All Reporting": "Consolidated", "EUR": "Numeric", "USD": "Numeric"},
        edges={("All Reporting", "EUR"): 1, ("All Reporting", "USD"): 1},
    )
    tm1.add_dimension("Measure", {"Rate": "Numeric"})
    tm1.add_cube("FX Rates", ["Version", "TargetCurrency", "Measure"])

    provider = TM1ServiceMetadataProvider(tm1)
    metadata = provider.get_cube_metadata("FX Rates")

    assert metadata.dimension_element_types["TargetCurrency"] == {
        "All Reporting": "Consolidated",
        "EUR": "Numeric",
        "USD": "Numeric",
    }
    assert set(metadata.dimension_leaf_elements["TargetCurrency"]) == {"EUR", "USD"}
    assert metadata.dimension_children["TargetCurrency"]["All Reporting"] == ("EUR", "USD")


def test_metadata_provider_collects_dimension_attribute_values_from_tm1():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension(
        "Company",
        {"ACME": "Numeric", "BETA": "Numeric"},
        element_attributes={"Currency": "String"},
        attribute_values={
            "ACME": {"Currency": "USD"},
            "BETA": {"Currency": "EUR"},
        },
    )
    tm1.add_dimension("Measure", {"Value": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])

    provider = TM1ServiceMetadataProvider(tm1)
    metadata = provider.get_cube_metadata("Sales")

    assert metadata.dimension_attributes["Company"] == ("Currency",)
    assert metadata.dimension_attribute_values["Company"] == {
        "ACME": {"Currency": "USD"},
        "BETA": {"Currency": "EUR"},
    }


def test_compile_deploys_attribute_routed_broadcast_cross_cube_feeders_from_tm1_metadata():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension(
        "Company",
        {"ACME": "String", "BETA": "String"},
        element_attributes={"Currency": "String"},
        attribute_values={
            "ACME": {"Currency": "USD"},
            "BETA": {"Currency": "EUR"},
        },
    )
    tm1.add_dimension("Region", {"East": "String", "West": "String"})
    tm1.add_dimension("Measure", {"FX Rate": "Numeric", "Rate": "Numeric"})
    tm1.add_dimension("Currency", {"USD": "String", "EUR": "String"})
    tm1.add_dimension("TargetCurrency", {"EUR": "String"})
    tm1.add_cube("Sales", ["Version", "Company", "Region", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "Currency", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    result = model.compile(dry_run=False)

    assert result.deployment["Sales"]["deployed"] is True
    assert result.deployment["FX Rates"]["deployed"] is True
    assert result.deployment["FX Rates"]["feeder_count"] == 2
    assert tm1.cube_rules("FX Rates") == (
        "# Generated by TM1_bedrock_py calc Phase 2 compiler\n"
        "# Cube: FX Rates\n"
        "\n"
        "FEEDERS;\n"
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'BETA', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), DB('Sales', !Version, "
        "'Company':'Company':'ACME', 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');\n"
    )


def test_compile_deploys_fixed_cross_cube_origin_via_structural_leaf_expansion():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Group": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR", "USD"],
                },
                dimension_element_types={
                    "TargetCurrency": {
                        "All Reporting": "Consolidated",
                        "EUR": "Numeric",
                        "USD": "Numeric",
                    },
                },
                dimension_children={
                    "TargetCurrency": {
                        "All Reporting": ["EUR", "USD"],
                    },
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Group"] = fx["Rate"].align(TargetCurrency="All Reporting")

    explanation = model.explain("Sales:FX Rate Group")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "cross_cube_source_lookup"
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Group');\n"
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'USD', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'FX Rate Group');"
    )
    assert preview.manifest["Sales:FX Rate Group"]["artifact"]["preview_only"] is False


def test_compile_rejects_consolidated_feeder_origin_with_explicit_reason():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Group": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR", "USD"],
                },
                dimension_element_types={
                    "TargetCurrency": {
                        "All Reporting": "Consolidated",
                        "EUR": "Numeric",
                        "USD": "Numeric",
                    },
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Group"] = fx["Rate"].align(TargetCurrency="All Reporting")

    explanation = model.explain("Sales:FX Rate Group")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate Group"]["artifact"]["preview_only"] is True
    assert "is consolidated and has no provable leaf expansion" in explanation["feeder_rationale"]
    assert "'All Reporting'" in explanation["feeder_rationale"]


def test_compile_rejects_partial_structural_expansion_as_unsafe():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate Group": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Company", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
                dimension_element_types={
                    "TargetCurrency": {
                        "All Reporting": "Consolidated",
                        "Europe": "Consolidated",
                        "EUR": "Numeric",
                        "GBP": "Consolidated",
                    },
                },
                dimension_children={
                    "TargetCurrency": {
                        "All Reporting": ["Europe", "GBP"],
                        "Europe": ["EUR"],
                    },
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate Group"] = fx["Rate"].align(TargetCurrency="All Reporting")

    explanation = model.explain("Sales:FX Rate Group")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate Group"]["artifact"]["preview_only"] is True


def test_compile_rejects_target_only_broadcast_dimension_with_ambiguous_hierarchy():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Region": ["Region", "Alt Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Region": ["East", "West"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(TargetCurrency="EUR")

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is True
    assert "Region" in explanation["feeder_rationale"]
    assert "multiple hierarchies" in explanation["feeder_rationale"]


def test_compile_keeps_attribute_routed_broadcast_preview_only_when_target_leaf_scope_is_ambiguous():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Region": ["Region", "Alt Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                    "Region": ["East", "West"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert explanation["native_eligibility"]["code"] == "native_align_target_leaf_scope_unproven"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is True


def test_compile_keeps_attribute_routed_align_preview_only_when_reverse_mapping_is_incomplete():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "GBP"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD", "EUR"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    )

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_cross_cube"
    assert explanation["native_eligibility"]["code"] == "native_align_reverse_mapping_unproven"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is True


def test_align_mapping_rejects_non_target_cube_attribute_routing_as_unresolved():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Currency": "Currency",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_attributes={"Currency": ["Alias"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Currency": ["Currency"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Currency": ["USD"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        Currency=fx.Currency.attribute("Alias"),
        TargetCurrency="EUR",
    )

    readiness = model.phase2_readiness("Sales:FX Rate")

    assert readiness["backend"] == "Python materialization backend"
    assert readiness["native_eligibility"]["code"] == "native_mapping_unresolved"


def test_compile_deploys_target_only_broadcast_dimension_when_hierarchy_is_unambiguous():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Measure": "Measure",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Region": ["Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Region": ["East", "West"],
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(TargetCurrency="EUR")

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "cross_cube_source_lookup"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is False
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, 'Region':'Region':'East', 'Measure':'Measure':'FX Rate'), "
        "DB('Sales', !Version, 'Region':'Region':'West', 'Measure':'Measure':'FX Rate');"
    )


def _chained_attribute_metadata_provider():
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                    "Region": "Region",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Region Code"], "Region": ["Default Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                    "Region": ["Region"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA", "GAMMA"],
                    "Region": ["NA", "EU"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Region Code": "NA"},
                        "BETA": {"Region Code": "NA"},
                        "GAMMA": {"Region Code": "EU"},
                    },
                    "Region": {
                        "NA": {"Default Currency": "USD"},
                        "EU": {"Default Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["USD", "EUR"],
                },
            ),
        }
    )


def test_compile_preview_emits_nested_attrs_for_two_hop_chained_attribute_align():
    provider = _chained_attribute_metadata_provider()
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        TargetCurrency=sales.Company.attribute_chain(
            "Region Code", via_dimension="Region", attribute="Default Currency"
        ),
    )

    explanation = model.explain("Sales:FX Rate")
    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'FX Rate'] = N: DB('FX Rates', !Version, "
        "ATTRS('Region', ATTRS('Company', !Company, 'Region Code'), 'Default Currency'), "
        "'Measure':'Measure':'Rate');"
    )
    assert explanation["feeder_strategy"] == "cross_cube_attribute_lookup"
    assert explanation["native_eligibility"]["status"] == "deployable"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is False
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, 'Company':'Company':'GAMMA', 'Measure':'Measure':'FX Rate');\n"
        "[!Version, 'TargetCurrency':'TargetCurrency':'USD', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, 'Company':'Company':'ACME', 'Measure':'Measure':'FX Rate'), "
        "DB('Sales', !Version, 'Company':'Company':'BETA', 'Measure':'Measure':'FX Rate');"
    )


def test_compile_deploys_two_hop_chained_attribute_align_via_mock_tm1_service():
    provider = _chained_attribute_metadata_provider()
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Company", {"ACME": "String", "BETA": "String", "GAMMA": "String"})
    tm1.add_dimension("Measure", {"FX Rate": "Numeric", "Rate": "Numeric"})
    tm1.add_dimension("TargetCurrency", {"USD": "String", "EUR": "String"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "TargetCurrency", "Measure"])

    model = Model(tm1=tm1, metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        TargetCurrency=sales.Company.attribute_chain(
            "Region Code", via_dimension="Region", attribute="Default Currency"
        ),
    )

    result = model.compile(dry_run=False)

    assert result.deployment["FX Rates"]["deployed"] is True
    assert result.deployment["FX Rates"]["feeder_count"] == 2


def test_compile_keeps_chained_attribute_align_preview_only_when_intermediate_value_unresolved():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                    "Region": "Region",
                },
                measure_element_types={"FX Rate": "Numeric"},
                dimension_attributes={"Company": ["Region Code"], "Region": ["Default Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                    "Region": ["Region"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA", "GAMMA"],
                    "Region": ["NA", "EU"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Region Code": "NA"},
                        "BETA": {"Region Code": "NA"},
                        # GAMMA's "Region Code" does not resolve to a real Region leaf element --
                        # the chain must fail closed, not silently drop GAMMA's feeder coverage.
                        "GAMMA": {"Region Code": "UNKNOWN"},
                    },
                    "Region": {
                        "NA": {"Default Currency": "USD"},
                        "EU": {"Default Currency": "EUR"},
                    },
                },
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "TargetCurrency", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "TargetCurrency": "TargetCurrency",
                    "Measure": "Measure",
                },
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "TargetCurrency": ["TargetCurrency"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "TargetCurrency": ["USD", "EUR"],
                },
            ),
        }
    )

    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["FX Rate"] = fx["Rate"].align(
        TargetCurrency=sales.Company.attribute_chain(
            "Region Code", via_dimension="Region", attribute="Default Currency"
        ),
    )

    explanation = model.explain("Sales:FX Rate")

    assert explanation["native_eligibility"]["status"] == "metadata_incomplete"
    assert explanation["native_eligibility"]["code"] == "native_align_reverse_mapping_unproven"


def test_compile_deploys_fan_out_allocation_of_a_broadcast_total_weighted_by_a_same_cube_ratio():
    """Verifies dev/13_native_parity_plan.md Work Item 5 (fan-out/allocation).

    Representative shape: a parameter-cube total (one cell per Department) is allocated
    across many CostCenter leaves, weighted by a same-cube "Weight Pct" driver measure.
    From each target leaf's point of view this is exactly the existing bounded
    target-only-broadcast align(...) primitive (Department maps straight through,
    CostCenter is target-only and broadcasts to every leaf) combined with the existing
    same-cube-driver-merge logic (Weight Pct is a genuine value-composing driver, not a
    selector, so it must be fed alongside the cross-cube origin). No new compiler or
    feeder-planner code is needed -- this test exists to prove that, not to add a feature.
    """
    provider = StaticMetadataProvider(
        {
            "CostCenter": build_static_cube_metadata(
                "CostCenter",
                ["Version", "Department", "CostCenter", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Department": "Department",
                    "CostCenter": "CostCenter",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Weight Pct": "Numeric",
                    "Allocated Amount": "Numeric",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Department": ["Department"],
                    "CostCenter": ["CostCenter"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "CostCenter": ["CC100", "CC200"],
                },
            ),
            "Budget": build_static_cube_metadata(
                "Budget",
                ["Version", "Department", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Department": "Department",
                    "Measure": "Measure",
                },
                measure_element_types={"Total Amount": "Numeric"},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Department": ["Department"],
                    "Measure": ["Measure"],
                },
            ),
        }
    )
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    cost_center = model.cube("CostCenter")
    budget = model.cube("Budget")

    cost_center["Allocated Amount"] = budget["Total Amount"].align() * cost_center["Weight Pct"]

    explanation = model.explain("CostCenter:Allocated Amount")
    preview = model.compile(dry_run=True)

    assert explanation["native_eligibility"]["status"] == "deployable"
    assert preview.errors == []
    assert preview.rules["CostCenter"] == (
        "['Measure':'Measure':'Allocated Amount'] = N: (DB('Budget', !Version, !Department, "
        "'Measure':'Measure':'Total Amount') * ['Measure':'Measure':'Weight Pct']);"
    )
    assert preview.feeders["CostCenter"] == (
        "['Measure':'Measure':'Weight Pct'] => ['Measure':'Measure':'Allocated Amount'];"
    )
    assert "Budget" not in preview.feeders
    assert "higher dimension count" in explanation["feeder_rationale"]
    assert preview.manifest["CostCenter:Allocated Amount"]["artifact"]["preview_only"] is False

    deployment = model.compile(dry_run=False)
    assert deployment.deployment["CostCenter"]["deployed"] is True
    assert "Budget" not in deployment.deployment
