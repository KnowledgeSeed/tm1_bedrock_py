from TM1_bedrock_py.calc import Model, StaticMetadataProvider, build_static_cube_metadata
from TM1_bedrock_py.calc.ingest import parse_native_rule_text
from tests.mock_tm1_service import MockTM1Service


def test_ingest_round_trips_simple_arithmetic_and_guarded_ratio():
    rule_text = (
        "SKIPCHECK;\n"
        "['Gross Margin'] = N: (['Revenue'] - ['Cost']);\n"
        "['Gross Margin %'] = N: IF((['Revenue'] <> 0), (['Gross Margin'] / ['Revenue']), STET);\n"
        "FEEDERS;\n"
        "['Cost'] => ['Gross Margin'];\n"
    )

    model = Model()
    model.cube("Sales")
    report = model.import_native_rule_text("Sales", rule_text)

    assert report.rejected == []
    assert sorted(report.ingested) == ["Gross Margin", "Gross Margin %"]

    preview = model.compile(dry_run=True)
    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Gross Margin'] = N: (['Revenue'] - ['Cost']);\n"
        "['Gross Margin %'] = N: IF((['Revenue'] <> 0), (['Gross Margin'] / ['Revenue']), STET);"
    )


def test_ingest_where_reconstruction_from_if_stet():
    parsed, rejected = parse_native_rule_text(
        "Sales", "['Ratio'] = N: IF((['Revenue'] <> 0), (['Margin'] / ['Revenue']), STET);"
    )
    assert rejected == []
    assert len(parsed) == 1
    assert parsed[0].measure_name == "Ratio"
    assert parsed[0].expression.describe() == "(Sales.Margin / Sales.Revenue).where((Sales.Revenue != 0))"


def test_ingest_case_reconstruction_from_multi_branch_if_chain():
    rule_text = (
        "['Tier'] = N: IF((['Score'] > 90), 3, IF((['Score'] > 50), 2, 1));"
    )
    parsed, rejected = parse_native_rule_text("Sales", rule_text)
    assert rejected == []
    assert len(parsed) == 1
    expression = parsed[0].expression
    assert expression.describe() == (
        "case(((Sales.Score > 90) -> 3), ((Sales.Score > 50) -> 2), default=1)"
    )


def test_ingest_consolidated_scope_statement():
    parsed, rejected = parse_native_rule_text(
        "Sales", "['Total Cost'] = C: (['Cost A'] + ['Cost B']);"
    )
    assert rejected == []
    assert parsed[0].scope == "C"
    assert parsed[0].measure_name == "Total Cost"


def test_ingest_rejects_unsupported_constructs_but_keeps_other_statements():
    rule_text = (
        "['Good'] = N: (['Revenue'] - ['Cost']);\n"
        "['StringRule'] = S: 'literal text';\n"
        "['CrossCube'] = N: DB('FX Rates', !Version, 'EUR', 'Rate');\n"
        "['BadCall'] = N: NOTAFUNCTION(['Revenue']);\n"
    )
    model = Model()
    model.cube("Sales")
    report = model.import_native_rule_text("Sales", rule_text)

    assert report.ingested == ["Good"]
    rejected_measures = {rejection.measure_name: rejection.reason for rejection in report.rejected}
    assert "StringRule" in rejected_measures
    assert "S:" in rejected_measures["StringRule"]
    assert "CrossCube" in rejected_measures
    assert "DB" in rejected_measures["CrossCube"]
    assert "BadCall" in rejected_measures


def test_ingest_rejects_duplicate_target_without_raising():
    model = Model()
    model.cube("Sales")
    model.cube("Sales")["Existing"] = model.cube("Sales")["Revenue"] - model.cube("Sales")["Cost"]

    rule_text = "['Existing'] = N: (['Revenue'] * 2);"
    report = model.import_native_rule_text("Sales", rule_text)

    assert report.ingested == []
    assert len(report.rejected) == 1
    assert "already registered" in report.rejected[0].reason


def test_ingest_stops_at_feeders_section_and_ignores_garbage_after():
    rule_text = (
        "['Good'] = N: ['Revenue'];\n"
        "FEEDERS;\n"
        "this is not even valid rule syntax ###\n"
    )
    parsed, rejected = parse_native_rule_text("Sales", rule_text)
    assert rejected == []
    assert [statement.measure_name for statement in parsed] == ["Good"]


def test_import_cube_rules_from_tm1_round_trips_through_mock_service():
    tm1 = MockTM1Service()
    tm1.cubes.update_or_create_rules(
        "Sales", "['Gross Margin'] = N: (['Revenue'] - ['Cost']);"
    )

    model = Model(tm1=tm1)
    model.cube("Sales")
    report = model.import_cube_rules_from_tm1("Sales")

    assert report.ingested == ["Gross Margin"]
    assert report.rejected == []


def _fx_alignment_metadata_provider() -> StaticMetadataProvider:
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Measure"],
                default_hierarchies={"Version": "Version", "Company": "Company", "Measure": "Measure"},
                measure_element_types={"Revenue Local": "Numeric", "Revenue EUR": "Numeric"},
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={"Version": ["Version"], "Company": ["Company"], "Measure": ["Measure"]},
                dimension_leaf_elements={"Company": ["ACME"]},
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


def test_ingest_reconstructs_cross_cube_align_with_attribute_and_literal_mapping():
    provider = _fx_alignment_metadata_provider()
    model = Model(metadata_provider=provider)
    model.cube("Sales")
    model.cube("FX Rates")

    rule_text = (
        "['Measure':'Measure':'Revenue EUR'] = N: "
        "(['Measure':'Measure':'Revenue Local'] * "
        "DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'));"
    )
    report = model.import_native_rule_text("Sales", rule_text)
    assert report.rejected == []
    assert report.ingested == ["Revenue EUR"]

    preview = model.compile(dry_run=True)
    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Revenue EUR'] = N: "
        "(['Measure':'Measure':'Revenue Local'] * "
        "DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'));"
    )
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["feeder_strategy"] == "cross_cube_local_driver"
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["preview_only"] is False


def test_ingest_reconstructs_same_cube_attribute_shift():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                default_hierarchies={"Version": "Version", "Month": "Month", "Measure": "Measure"},
                measure_element_types={"Revenue": "Numeric", "Revenue Prior Month": "Numeric"},
                dimension_attributes={"Month": ["Prev Month"]},
                dimension_hierarchies={"Version": ["Version"], "Month": ["Month"], "Measure": ["Measure"]},
                dimension_leaf_elements={"Month": ["Jan", "Feb", "Mar"]},
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
    model.cube("Sales")

    rule_text = (
        "['Measure':'Measure':'Revenue Prior Month'] = N: "
        "DB('Sales', !Version, ATTRS('Month', !Month, 'Prev Month'), 'Measure':'Measure':'Revenue');"
    )
    report = model.import_native_rule_text("Sales", rule_text)
    assert report.rejected == []
    assert report.ingested == ["Revenue Prior Month"]

    explanation = model.explain("Sales:Revenue Prior Month")
    preview = model.compile(dry_run=True)
    assert explanation["feeder_strategy"] == "same_cube_attribute_shift"
    assert preview.rules["Sales"] == rule_text
    assert preview.manifest["Sales:Revenue Prior Month"]["artifact"]["preview_only"] is False


def test_ingest_reconstructs_same_cube_numeric_offset_shift():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Year", "Measure"],
                default_hierarchies={"Version": "Version", "Year": "Year", "Measure": "Measure"},
                measure_element_types={"Revenue": "Numeric", "Revenue Prior Year": "Numeric"},
                dimension_hierarchies={"Version": ["Version"], "Year": ["Year"], "Measure": ["Measure"]},
                dimension_leaf_elements={"Year": ["2023", "2024", "2025"]},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    model.cube("Sales")

    rule_text = (
        "['Measure':'Measure':'Revenue Prior Year'] = N: "
        "DB('Sales', !Version, STR(NUMBR(!Year) + (-1)), 'Measure':'Measure':'Revenue');"
    )
    report = model.import_native_rule_text("Sales", rule_text)
    assert report.rejected == []
    assert report.ingested == ["Revenue Prior Year"]

    explanation = model.explain("Sales:Revenue Prior Year")
    preview = model.compile(dry_run=True)
    assert explanation["feeder_strategy"] == "same_cube_numeric_shift"
    assert preview.rules["Sales"] == rule_text
    assert preview.manifest["Sales:Revenue Prior Year"]["artifact"]["preview_only"] is False


def test_ingest_reconstructs_pure_passthrough_db_as_plain_measure_reference():
    provider = _fx_alignment_metadata_provider()
    model = Model(metadata_provider=provider)
    model.cube("Sales")

    rule_text = "['Revenue EUR Copy'] = N: DB('Sales', !Version, !Company, 'Measure':'Measure':'Revenue Local');"
    parsed, rejected = parse_native_rule_text("Sales", rule_text, metadata_provider=provider)
    assert rejected == []
    assert parsed[0].expression.describe() == "Sales.Revenue Local"


def test_ingest_rejects_cross_cube_db_without_metadata_provider():
    rule_text = "['Revenue EUR'] = N: DB('FX Rates', !Version, 'EUR', 'Rate');"
    parsed, rejected = parse_native_rule_text("Sales", rule_text)
    assert parsed == []
    assert len(rejected) == 1
    assert "metadata provider" in rejected[0].reason


def test_ingest_rejects_db_referencing_unknown_source_cube():
    provider = _fx_alignment_metadata_provider()
    rule_text = "['Revenue EUR'] = N: DB('Unknown Cube', !Version, 'EUR', 'Rate');"
    parsed, rejected = parse_native_rule_text("Sales", rule_text, metadata_provider=provider)
    assert parsed == []
    assert len(rejected) == 1
    assert "Unknown Cube" in rejected[0].reason


def test_ingest_rejects_db_coordinate_count_mismatch():
    provider = _fx_alignment_metadata_provider()
    rule_text = "['Revenue EUR'] = N: DB('FX Rates', !Version, 'EUR', 'Rate');"
    parsed, rejected = parse_native_rule_text("Sales", rule_text, metadata_provider=provider)
    assert parsed == []
    assert len(rejected) == 1
    assert "coordinate count" in rejected[0].reason


def test_import_cube_rules_from_tm1_round_trips_cross_cube_align():
    tm1 = MockTM1Service()
    tm1.cubes.update_or_create_rules(
        "Sales",
        "['Measure':'Measure':'Revenue EUR'] = N: "
        "(['Measure':'Measure':'Revenue Local'] * "
        "DB('FX Rates', !Version, ATTRS('Company', !Company, 'Currency'), "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'));",
    )

    provider = _fx_alignment_metadata_provider()
    model = Model(tm1=tm1, metadata_provider=provider)
    model.cube("Sales")
    model.cube("FX Rates")

    report = model.import_cube_rules_from_tm1("Sales")
    assert report.rejected == []
    assert report.ingested == ["Revenue EUR"]

    preview = model.compile(dry_run=True)
    assert preview.manifest["Sales:Revenue EUR"]["artifact"]["preview_only"] is False
