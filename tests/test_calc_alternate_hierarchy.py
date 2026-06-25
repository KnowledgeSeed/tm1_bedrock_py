from TM1_bedrock_py.calc import Model, StaticMetadataProvider, build_static_cube_metadata
from tests.mock_tm1_service import MockTM1Service


def _alt_hierarchy_metadata():
    return build_static_cube_metadata(
        "Sales",
        ["Region", "Measure"],
        default_hierarchies={"Region": "Region", "Measure": "Measure"},
        measure_element_types={"Revenue": "Numeric", "Total Revenue": "Numeric"},
        dimension_hierarchies={"Region": ("Region", "Alt Region"), "Measure": ("Measure",)},
        dimension_leaf_elements={"Region": ("East", "West")},
        dimension_element_types={"Region": {"East Coast": "Consolidated"}},
        dimension_children_by_hierarchy={
            "Region": {
                "Alt Region": {"East Coast": ("East", "West")},
            },
        },
    )


def test_structural_leaf_expansion_falls_through_to_alternate_hierarchy_when_default_has_no_children():
    model = Model()
    metadata = _alt_hierarchy_metadata()

    result = model._expand_consolidated_element_to_leaves(
        metadata=metadata,
        dimension_name="Region",
        element_name="East Coast",
        explicit_hierarchy_name=None,
    )

    assert result == ("East", "West")


def test_structural_leaf_expansion_pinned_to_explicit_hierarchy_does_not_leak_into_other_hierarchies():
    model = Model()
    metadata = _alt_hierarchy_metadata()

    result = model._expand_consolidated_element_to_leaves(
        metadata=metadata,
        dimension_name="Region",
        element_name="East Coast",
        explicit_hierarchy_name="Region",
    )

    assert result is None


def test_structural_leaf_expansion_pinned_to_explicit_hierarchy_resolves_when_that_hierarchy_has_children():
    model = Model()
    metadata = _alt_hierarchy_metadata()

    result = model._expand_consolidated_element_to_leaves(
        metadata=metadata,
        dimension_name="Region",
        element_name="East Coast",
        explicit_hierarchy_name="Alt Region",
    )

    assert result == ("East", "West")


def test_consolidated_feeder_rejection_reason_resolves_via_alternate_hierarchy():
    provider = StaticMetadataProvider({"Sales": _alt_hierarchy_metadata()})
    model = Model(metadata_provider=provider, tm1=MockTM1Service())

    reason = model._consolidated_feeder_rejection_reason(
        cube_name="Sales",
        dimension_name="Region",
        element_name="East Coast",
    )

    assert reason is None
