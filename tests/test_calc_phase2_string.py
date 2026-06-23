from TM1_bedrock_py.calc import (
    Model,
    StaticMetadataProvider,
    build_static_cube_metadata,
)
from tests.mock_tm1_service import MockTM1Service


def _string_sales_metadata_provider():
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={"Version": "Version", "Region": "Region", "Measure": "Measure"},
                measure_element_types={
                    "First Name": "String",
                    "Last Name": "String",
                    "Full Name": "String",
                    "Revenue": "Numeric",
                },
                dimension_hierarchies={"Version": ["Version"], "Region": ["Region"], "Measure": ["Measure"]},
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "Measure"],
                measure_element_types={"Label": "String"},
            ),
        }
    )


def test_compile_deploys_string_concat_with_same_cube_traceback_feeders():
    provider = _string_sales_metadata_provider()
    model = Model(metadata_provider=provider, tm1=MockTM1Service())
    sales = model.cube("Sales")

    sales["Full Name"] = (
        sales["First Name"].concat(" ").concat(sales["Last Name"])
    ).native(scope="string")

    explanation = model.explain("Sales:Full Name")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "same_cube_traceback"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Full Name'] = S: "
        "((['Measure':'Measure':'First Name'] | ' ') | ['Measure':'Measure':'Last Name']);"
    )
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'First Name'] => ['Measure':'Measure':'Full Name'];\n"
        "['Measure':'Measure':'Last Name'] => ['Measure':'Measure':'Full Name'];"
    )
    assert preview.manifest["Sales:Full Name"]["rule_area"] == "string"
    assert preview.manifest["Sales:Full Name"]["artifact"]["preview_only"] is False

    deployment = model.compile(dry_run=False)
    assert deployment.errors == []
    assert deployment.deployment["Sales"]["deployed"] is True
    assert deployment.deployment["Sales"]["check_rules_status"] == 200


def test_compile_lowers_string_upper_and_where_for_s_scope():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={"Version": "Version", "Region": "Region", "Measure": "Measure"},
                measure_element_types={
                    "Status": "String",
                    "Status Upper": "String",
                },
                dimension_hierarchies={"Version": ["Version"], "Region": ["Region"], "Measure": ["Measure"]},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Status Upper"] = sales["Status"].upper().where(sales["Status"] != "").native(scope="string")

    explanation = model.explain("Sales:Status Upper")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Status Upper'] = S: "
        "IF((['Measure':'Measure':'Status'] <> ''), UPPER(['Measure':'Measure':'Status']), STET);"
    )


def test_compile_lowers_string_case_for_s_scope():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={"Version": "Version", "Region": "Region", "Measure": "Measure"},
                measure_element_types={
                    "Status": "String",
                    "Status Label": "String",
                },
                dimension_hierarchies={"Version": ["Version"], "Region": ["Region"], "Measure": ["Measure"]},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Status Label"] = model.case(
        (sales["Status"] != "Open", sales["Status"]),
        default="Open",
    ).native(scope="string")

    explanation = model.explain("Sales:Status Label")
    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "same_cube_traceback"


def test_compile_rejects_string_target_when_measure_is_not_string_typed():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                measure_element_types={"Revenue": "Numeric", "Status Upper": "Numeric"},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Status Upper"] = sales["Revenue"].native(scope="string")

    explanation = model.explain("Sales:Status Upper")
    assert explanation["backend"] == "Python materialization backend"
    assert "String element type" in explanation["rationale"]


def test_compile_rejects_string_formula_referencing_numeric_measure():
    provider = _string_sales_metadata_provider()
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Full Name"] = (sales["First Name"] + sales["Revenue"]).native(scope="string")

    explanation = model.explain("Sales:Full Name")
    assert explanation["backend"] == "Python materialization backend"
    assert "Revenue" in explanation["rationale"]
    assert "String element type" in explanation["rationale"]


def test_compile_rejects_cross_cube_align_inside_string_scope():
    provider = _string_sales_metadata_provider()
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["First Name"] = fx["Label"].align(Currency="USD").native(scope="string")

    explanation = model.explain("Sales:First Name")
    assert explanation["backend"] == "Python materialization backend"
    assert "does not support align(...)" in explanation["rationale"]


def test_compile_rejects_time_intelligence_methods_inside_string_scope():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Month", "Measure"],
                measure_element_types={"Status": "String"},
                dimension_leaf_elements={"Month": ["1", "2", "3"]},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Status"] = sales["Status"].shift(Month=-1).native(scope="string")

    explanation = model.explain("Sales:Status")
    assert explanation["backend"] == "Python materialization backend"
    assert "does not support align(...)" in explanation["rationale"]


def test_register_formula_rejects_unsupported_scope():
    model = Model()
    sales = model.cube("Sales")

    try:
        sales["Status"] = sales["Status"].native(scope="X")
    except ValueError as error:
        assert "Unsupported rule-area scope" in str(error)
    else:
        raise AssertionError("Expected ValueError for unsupported scope")
