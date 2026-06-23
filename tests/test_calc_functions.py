from TM1_bedrock_py.calc import Model
from TM1_bedrock_py.calc.model import Backend


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
