from TM1_bedrock_py.calc import Model, StaticMetadataProvider, build_static_cube_metadata
from TM1_bedrock_py.calc.model import Backend
from tests.mock_tm1_service import MockTM1Service


def _sales_metadata_provider(measure_types):
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Department", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Department": "Department",
                    "Region": "Region",
                    "Measure": "Measure",
                },
                measure_element_types=measure_types,
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Department": ["Department"],
                    "Region": ["Region"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={"Region": ["East", "West"]},
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={"Rate": "Numeric"},
                dimension_hierarchies={"Version": ["Version"], "Measure": ["Measure"]},
            ),
        }
    )


def test_numeric_functions_compile_native():
    model = Model()
    sales = model.cube("Sales")

    sales["Rounded Margin"] = (sales["Revenue"] - sales["Cost"]).round(2)
    sales["Abs Variance"] = (sales["Revenue"] - sales["Cost"]).abs()
    sales["Remainder"] = sales["Revenue"].mod(7)
    sales["Whole Units"] = sales["Revenue"].int_part()

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Abs Variance'] = N: ABS((['Revenue'] - ['Cost']));\n"
        "['Remainder'] = N: MOD(['Revenue'], 7);\n"
        "['Rounded Margin'] = N: ROUND((['Revenue'] - ['Cost']), 2);\n"
        "['Whole Units'] = N: INT(['Revenue'])"
        + ";"
    )


def test_string_functions_compile_native():
    model = Model()
    sales = model.cube("Sales")

    sales["Region Upper"] = sales["Region Code"].upper()
    sales["Region Lower"] = sales["Region Code"].lower()
    sales["Region Trimmed"] = sales["Region Code"].trim()
    sales["Region Prefix"] = sales["Region Code"].substring(1, 3)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "UPPER(['Region Code'])" in preview.rules["Sales"]
    assert "LOWER(['Region Code'])" in preview.rules["Sales"]
    assert "TRIM(['Region Code'])" in preview.rules["Sales"]
    assert "SUBST(['Region Code'], 1, 3)" in preview.rules["Sales"]


def test_dimension_introspection_functions_compile_native_with_literal_element():
    model = Model()
    sales = model.cube("Sales")

    sales["Product Level"] = sales.Product.level("Widget")
    sales["Product Parent Count"] = sales.Product.parent_count("Widget")
    sales["Product Child Count"] = sales.Product.child_count("Total Products")
    sales["First Child"] = sales.Product.child(1, "Total Products")
    sales["Is Under Total"] = sales.Product.is_component_of("Total Products", "Widget")

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "ELLEV('Product', 'Widget')" in preview.rules["Sales"]
    assert "ELPARN('Product', 'Widget')" in preview.rules["Sales"]
    assert "ELCOMPN('Product', 'Total Products')" in preview.rules["Sales"]
    assert "ELCOMP('Product', 'Total Products', 1)" in preview.rules["Sales"]
    assert "ELISCOMP('Product', 'Widget', 'Total Products')" in preview.rules["Sales"]


def test_dimension_introspection_function_defaults_to_current_element():
    model = Model()
    sales = model.cube("Sales")

    sales["Current Product Level"] = sales.Product.level()

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "ELLEV('Product', !Product)" in preview.rules["Sales"]


def test_dimension_function_rejects_cross_cube_reference_to_python_backend():
    model = Model()
    sales = model.cube("Sales")
    other = model.cube("FX Rates")

    sales["Bad"] = other.Currency.level("EUR")

    explanation = model.explain("Sales:Bad")
    assert explanation["backend"] == Backend.PYTHON.value
    assert "outside the native subset" in explanation["rationale"]
    assert "cross-cube" in explanation["rationale"]


def test_dimension_relationship_functions_compile_native():
    model = Model()
    sales = model.cube("Sales")

    sales["Grandparent"] = sales.Product.parent(2, "Widget")
    sales["Is Direct Parent"] = sales.Product.is_parent_of("Widget", "Total Products")
    sales["Is Ancestor"] = sales.Product.is_ancestor_of("Widget", "Total Products")
    sales["Widget Weight"] = sales.Product.weight("Total Products", "Widget")

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "ELPAR('Product', 'Widget', 2)" in preview.rules["Sales"]
    assert "ELISPAR('Product', 'Total Products', 'Widget')" in preview.rules["Sales"]
    assert "ELISANC('Product', 'Total Products', 'Widget')" in preview.rules["Sales"]
    assert "ELWEIGHT('Product', 'Total Products', 'Widget')" in preview.rules["Sales"]


def test_dimension_relationship_functions_default_to_current_element():
    model = Model()
    sales = model.cube("Sales")

    sales["Weight Of Self"] = sales.Product.weight("Total Products")

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "ELWEIGHT('Product', 'Total Products', !Product)" in preview.rules["Sales"]


def test_additional_string_functions_compile_native():
    model = Model()
    sales = model.cube("Sales")

    sales["Region Trimmed Start"] = sales["Region Code"].delete(1, 1)
    sales["Region Match Position"] = sales["Region Code"].scan("EU")
    sales["Region Length"] = sales["Region Code"].length()
    sales["Region First Char Code"] = sales["Region Code"].char_code(1)
    sales["Region Char From Code"] = sales["Region Char Code"].to_char()

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "DELET(['Region Code'], 1, 1)" in preview.rules["Sales"]
    assert "SCAN('EU', ['Region Code'])" in preview.rules["Sales"]
    assert "LONG(['Region Code'])" in preview.rules["Sales"]
    assert "CODE(['Region Code'], 1)" in preview.rules["Sales"]
    assert "CHAR(['Region Char Code'])" in preview.rules["Sales"]


def test_insrt_function_compiles_native_with_confirmed_argument_order():
    """INSRT(String1, String2, Location) inserts String1 into String2 at Location -- confirmed
    via a real worked REPLACE-style rule example, not the ambiguous function-library page alone.
    """
    model = Model()
    sales = model.cube("Sales")

    sales["Patched Code"] = sales["Region Code"].insert(sales["New Fragment"], 3)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "INSRT(['New Fragment'], ['Region Code'], 3)" in preview.rules["Sales"]


def test_math_and_trig_functions_compile_native():
    model = Model()
    sales = model.cube("Sales")

    sales["Exp Value"] = sales["Rate"].exp()
    sales["Ln Value"] = sales["Rate"].ln()
    sales["Log Value"] = sales["Rate"].log10()
    sales["Sqrt Value"] = sales["Rate"].sqrt()
    sales["Sin Value"] = sales["Angle"].sin()
    sales["Cos Value"] = sales["Angle"].cos()
    sales["Tan Value"] = sales["Angle"].tan()
    sales["Asin Value"] = sales["Ratio"].asin()
    sales["Acos Value"] = sales["Ratio"].acos()
    sales["Atan Value"] = sales["Ratio"].atan()

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "EXP(['Rate'])" in preview.rules["Sales"]
    assert "LN(['Rate'])" in preview.rules["Sales"]
    assert "LOG(['Rate'])" in preview.rules["Sales"]
    assert "SQRT(['Rate'])" in preview.rules["Sales"]
    assert "SIN(['Angle'])" in preview.rules["Sales"]
    assert "COS(['Angle'])" in preview.rules["Sales"]
    assert "TAN(['Angle'])" in preview.rules["Sales"]
    assert "ASIN(['Ratio'])" in preview.rules["Sales"]
    assert "ACOS(['Ratio'])" in preview.rules["Sales"]
    assert "ATAN(['Ratio'])" in preview.rules["Sales"]


def test_date_functions_compile_native():
    model = Model()
    sales = model.cube("Sales")

    sales["Serial"] = model.dates(sales["Year"], sales["Month"], sales["Day"])
    sales["Day Of Month"] = model.day(sales["Date String"])
    sales["Serial From String"] = model.dayno(sales["Date String"])
    sales["Date Text"] = model.date(sales["Serial"], 1)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "DATES(['Year'], ['Month'], ['Day'])" in preview.rules["Sales"]
    assert "DAY(['Date String'])" in preview.rules["Sales"]
    assert "DAYNO(['Date String'])" in preview.rules["Sales"]
    assert "DATE(['Serial'], 1)" in preview.rules["Sales"]


def test_consolidated_max_compiles_native_with_current_element_passthrough():
    provider = _sales_metadata_provider({"Department Cost": "Numeric"})
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Department Cost"] = sales["Department Cost"].consolidated_max(flag=2)

    explanation = model.explain("Sales:Department Cost")
    preview = model.compile(dry_run=True)

    assert explanation["native_eligibility"]["status"] == "deployable"
    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Department Cost'] = N: ConsolidatedMax(2, 'Sales', !Version, "
        "!Department, !Region, 'Measure':'Measure':'Department Cost');"
    )
    assert preview.manifest["Sales:Department Cost"]["artifact"]["feeder_strategy"] == (
        "consolidated_aggregate_self_consolidating"
    )
    assert preview.manifest["Sales:Department Cost"]["artifact"]["preview_only"] is False
    # The aggregated measure is the formula's own target (a non-additive consolidation
    # override) -- confirms the self-reference cycle guard in
    # _resolve_simple_feeder_source_refs prevents infinite recursion and correctly excludes
    # the self-feed from the emitted feeder statements (self-feeding is invalid in TM1).
    assert preview.feeders.get("Sales", "") == ""


def test_consolidated_max_self_reference_deploys_live_without_dependency_cycle_error():
    """Regression test: a formula referencing its own target (the realistic shape for a
    non-additive consolidation override) used to make _build_plan's topological sort see a
    false self-loop cycle, and would also have crashed _resolve_simple_feeder_source_refs via
    infinite recursion before the cycle guard fix. Confirms both are fixed end-to-end."""
    provider = _sales_metadata_provider({"Department Cost": "Numeric"})
    model = Model(tm1=MockTM1Service(), metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Department Cost"] = sales["Department Cost"].consolidated_max(flag=2)

    deployment = model.compile(dry_run=False)

    assert deployment.errors == []
    assert deployment.deployment["Sales"]["deployed"] is True


def test_consolidated_avg_compiles_native_with_dimension_override():
    provider = _sales_metadata_provider({"Revenue": "Numeric", "Average Other Region": "Numeric"})
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Average Other Region"] = sales["Revenue"].consolidated_avg(flag=0, Region="West")

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert "ConsolidatedAvg(0, 'Sales', !Version, !Department, 'Region':'Region':'West', " in preview.rules["Sales"]


def test_consolidated_aggregate_rejects_non_numeric_measure():
    provider = _sales_metadata_provider({"Region Code": "String", "Bad": "Numeric"})
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")

    sales["Bad"] = sales["Region Code"].consolidated_max(flag=0)

    explanation = model.explain("Sales:Bad")
    assert explanation["backend"] == Backend.PYTHON.value
    assert "Numeric element type" in explanation["rationale"]


def test_consolidated_aggregate_cross_cube_base_compiles_native_with_passthrough_dimension():
    """Closed 2026-06-25 (dev/19_ordering_consolidation_and_indirect_db_plan.md Work Item 2):
    cross-cube non-additive consolidation is now supported wherever every source dimension other
    than the measure dimension resolves via same-named passthrough, a literal override, or a
    target-cube attribute mapping. 'FX Rates' only has a 'Version' dimension besides its measure
    dimension, and 'Version' exists in the target cube 'Sales' too, so it resolves by passthrough
    with no override needed."""
    provider = _sales_metadata_provider({"Bad": "Numeric"})
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx_rates = model.cube("FX Rates")

    sales["Bad"] = fx_rates["Rate"].consolidated_max(flag=0)

    explanation = model.explain("Sales:Bad")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == Backend.NATIVE.value
    assert explanation["feeder_strategy"] == "consolidated_aggregate_cross_cube_mapped"
    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Bad'] = N: ConsolidatedMax(0, 'FX Rates', !Version, "
        "'Measure':'Measure':'Rate');"
    )
    # A consolidation read, not a rule-derived lookup -- no feeder statements are needed even
    # cross-cube, per the same IBM/Cubewise documentation already confirmed for the same-cube case.
    assert preview.feeders.get("Sales", "") == ""


def test_consolidated_aggregate_cross_cube_base_without_metadata_falls_back_to_python():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={"Bad": "Numeric"},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    other = model.cube("Unknown Cube")

    sales["Bad"] = other["Rate"].consolidated_max(flag=0)

    explanation = model.explain("Sales:Bad")
    assert explanation["backend"] == Backend.PYTHON.value
    assert "measure dimension metadata" in explanation["rationale"]


def test_consolidated_aggregate_cross_cube_unmapped_dimension_falls_back_to_python():
    # The cross-cube source cube's only non-measure dimension ("Currency") has no counterpart in
    # the target cube "Sales" and is given no override, so it cannot be resolved.
    cross_cube_provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={"Bad Cross": "Numeric"},
            ),
            "FX Rates Extended": build_static_cube_metadata(
                "FX Rates Extended",
                ["Currency", "Measure"],
                default_hierarchies={"Currency": "Currency", "Measure": "Measure"},
                measure_element_types={"Rate": "Numeric"},
            ),
        }
    )
    cross_model = Model(metadata_provider=cross_cube_provider)
    cross_sales = cross_model.cube("Sales")
    fx_extended = cross_model.cube("FX Rates Extended")

    cross_sales["Bad Cross"] = fx_extended["Rate"].consolidated_max(flag=0)

    explanation = cross_model.explain("Sales:Bad Cross")
    assert explanation["backend"] == Backend.PYTHON.value
    assert "cannot infer source dimension" in explanation["rationale"]
    assert "Currency" in explanation["rationale"]
    # Regression guard: this used to fall through to the generic "native_blocked" catch-all
    # because the eligibility classifier's substring match was align(...)-specific (looked for
    # "cannot infer source dimensions", which doesn't match consolidated_max's own "dimension(s)"
    # wording) -- it must now classify correctly as a mapping_shape failure.
    native_eligibility = explanation["native_eligibility"]
    assert native_eligibility["code"] == "native_mapping_unresolved"
    assert native_eligibility["category"] == "mapping_shape"


def test_consolidated_aggregate_cross_cube_attribute_routed_dimension_compiles_native():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={"Version": "Version", "Region": "Region", "Measure": "Measure"},
                measure_element_types={"Max Rate": "Numeric"},
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "Measure"],
                default_hierarchies={"Version": "Version", "Currency": "Currency", "Measure": "Measure"},
                measure_element_types={"Rate": "Numeric"},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx_rates = model.cube("FX Rates")

    sales["Max Rate"] = fx_rates["Rate"].consolidated_max(
        flag=0, Currency=sales.Region.attribute("Currency")
    )

    explanation = model.explain("Sales:Max Rate")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == Backend.NATIVE.value
    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Max Rate'] = N: ConsolidatedMax(0, 'FX Rates', !Version, "
        "ATTRS('Region', !Region, 'Currency'), 'Measure':'Measure':'Rate');"
    )


def test_consolidated_aggregate_cross_cube_value_driven_dimension_falls_back_to_python():
    provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={"Version": "Version", "Region": "Region", "Measure": "Measure"},
                measure_element_types={"Max Rate": "Numeric", "Currency Code": "String"},
            ),
            "FX Rates": build_static_cube_metadata(
                "FX Rates",
                ["Version", "Currency", "Measure"],
                default_hierarchies={"Version": "Version", "Currency": "Currency", "Measure": "Measure"},
                measure_element_types={"Rate": "Numeric"},
            ),
        }
    )
    model = Model(metadata_provider=provider)
    sales = model.cube("Sales")
    fx_rates = model.cube("FX Rates")

    sales["Max Rate"] = fx_rates["Rate"].consolidated_max(flag=0, Currency=sales["Currency Code"])

    explanation = model.explain("Sales:Max Rate")
    assert explanation["backend"] == Backend.PYTHON.value
    assert "not enumerable from cube/dimension metadata" in explanation["rationale"]

    native_eligibility = explanation["native_eligibility"]
    assert native_eligibility["status"] == "unsupported"
    assert native_eligibility["code"] == "native_value_driven_mapping"
    assert native_eligibility["category"] == "mapping_shape"
    # Regression guard for the mislabeling this slice fixed: the construct named in the detail
    # must be the one that actually triggered it (consolidated_max), not a hardcoded "align(...)".
    assert "consolidated_max(...)" in native_eligibility["detail"]
    assert "align(...)" not in native_eligibility["detail"].split("Multi-hop")[0]
    assert "Recommended workaround" in native_eligibility["detail"]
