import pytest

from TM1_bedrock_py.calc import DeploymentError, Model, StaticMetadataProvider, build_static_cube_metadata
from tests.mock_tm1_service import MockTM1Service


def test_compile_preview_emits_rules_and_feeders_for_native_subset():
    model = Model()
    sales = model.cube("Sales")

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]
    sales["Gross Margin %"] = (
        sales["Gross Margin"] / sales["Revenue"]
    ).where(sales["Revenue"] != 0)

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

    sales["Revenue Growth %"] = sales["Revenue"].growth(vs="Revenue Prior Year")

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

    preview = model.compile(dry_run=True)

    assert preview.manifest["Sales:FX Rate"]["artifact"]["feeder_strategy"] == "preview_only_cross_cube"
    assert preview.manifest["Sales:FX Rate"]["artifact"]["preview_only"] is True

    with pytest.raises(DeploymentError, match="preview-only cross-cube compilation"):
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


def test_compile_deploys_native_rule_artifacts_when_metadata_is_ready():
    tm1 = MockTM1Service()
    tm1.add_dimension("Version", {"Actual": "String"})
    tm1.add_dimension("Period", {"2026-JAN": "String"})
    tm1.add_dimension("Measure", {"Revenue": "Numeric", "Cost": "Numeric", "Gross Margin": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Period", "Measure"])

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

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
    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

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
    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]

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
