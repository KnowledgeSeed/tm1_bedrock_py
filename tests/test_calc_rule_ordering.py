import pytest

from TM1_bedrock_py.calc import Model, StaticMetadataProvider, build_static_cube_metadata


def _provider():
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Region": "Region",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Bonus": "Numeric",
                    "Other": "Numeric",
                },
            ),
        }
    )


def test_area_target_compiles_fixed_lhs_coordinate_alongside_measure():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales.area("Bonus", Region="US").set(100)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Bonus', 'Region':'Region':'US'] = N: 100;"
    )


def test_overlapping_areas_without_distinct_priority_fail_closed():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales["Bonus"] = 50
    sales.area("Bonus", Region="US").set(100)

    report = model.validate()

    assert not report.ok
    assert any("overlapping rule areas" in error for error in report.errors)

    preview = model.compile(dry_run=True)
    assert preview.rules == {}
    assert any("overlapping rule areas" in error for error in preview.errors)


def test_overlapping_areas_with_distinct_priority_order_emission_by_priority():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales["Bonus"] = 50
    sales.area("Bonus", Region="US").set(100, priority=-1)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    statements = preview.rules["Sales"].splitlines()
    assert statements == [
        "['Measure':'Measure':'Bonus', 'Region':'Region':'US'] = N: 100;",
        "['Measure':'Measure':'Bonus'] = N: 50;",
    ]


def test_overlapping_areas_with_priority_in_the_other_order_still_orders_by_priority():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    # Registered blanket-first this time -- emission order must still follow priority,
    # not registration order or alphabetical key order.
    sales.area("Bonus", Region="US").set(100, priority=5)
    sales["Bonus"] = 50

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    statements = preview.rules["Sales"].splitlines()
    assert statements == [
        "['Measure':'Measure':'Bonus'] = N: 50;",
        "['Measure':'Measure':'Bonus', 'Region':'Region':'US'] = N: 100;",
    ]


def test_non_overlapping_areas_on_the_same_measure_compile_independently_with_equal_priority():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales.area("Bonus", Region="US").set(100)
    sales.area("Bonus", Region="EU").set(200)

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    statements = set(preview.rules["Sales"].splitlines())
    assert statements == {
        "['Measure':'Measure':'Bonus', 'Region':'Region':'US'] = N: 100;",
        "['Measure':'Measure':'Bonus', 'Region':'Region':'EU'] = N: 200;",
    }


def test_downstream_formula_depends_on_every_area_variant_of_a_referenced_measure():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales.area("Bonus", Region="US").set(100, priority=-1)
    sales["Bonus"] = 50
    sales["Other"] = sales["Bonus"] * 2

    preview = model.compile(dry_run=True)

    assert preview.errors == []
    statements = preview.rules["Sales"].splitlines()
    # Both Bonus variants must precede the formula that references the bare "Bonus" measure.
    assert statements.index("['Measure':'Measure':'Other'] = N: (['Measure':'Measure':'Bonus'] * 2);") == 2


def test_explain_surfaces_area_overlap_and_resolved_precedence():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales["Bonus"] = 50
    sales.area("Bonus", Region="US").set(100, priority=-1)

    explanation = model.explain("Sales:Bonus|Region=US")

    assert explanation["area"] == {"Region": "US"}
    assert explanation["priority"] == -1
    overlaps = explanation["area_overlaps"]
    assert len(overlaps) == 1
    assert overlaps[0]["target"] == "Sales:Bonus"
    assert overlaps[0]["resolution"] == "this formula is emitted first (lower priority)"


def test_registering_identical_area_twice_still_raises_as_a_true_duplicate():
    model = Model(metadata_provider=_provider())
    sales = model.cube("Sales")

    sales.area("Bonus", Region="US").set(100)
    with pytest.raises(ValueError):
        sales.area("Bonus", Region="US").set(200)
