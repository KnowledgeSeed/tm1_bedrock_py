from TM1_bedrock_py.calc import (
    Model,
    StaticMetadataProvider,
    build_static_cube_metadata,
)
from tests.mock_tm1_service import MockTM1Service


def _consolidated_sales_metadata(children=None, leaf_elements=None, element_types=None):
    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={
                    "Driver": "Numeric",
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_hierarchies={"Version": ["Version"], "Measure": ["Measure"]},
                dimension_leaf_elements={
                    "Measure": leaf_elements
                    if leaf_elements is not None
                    else ["Driver", "Cost A", "Cost B"]
                },
                dimension_element_types={
                    "Measure": element_types
                    if element_types is not None
                    else {
                        "Driver": "Numeric",
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={
                    "Measure": children
                    if children is not None
                    else {"Total Cost": ["Cost A", "Cost B"]}
                },
            )
        }
    )


def _consolidated_multi_attribute_align_metadata_provider(
    *,
    include_target_region: bool = False,
    ambiguous_target_region: bool = False,
):
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
        "Measure": ["Cost A", "Cost B"],
    }
    if include_target_region:
        sales_leaf_elements["Region"] = ["East", "West"]

    return StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                sales_dimensions,
                default_hierarchies=sales_default_hierarchies,
                measure_element_types={
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_attributes={"Company": ["Currency", "RegionCode"]},
                dimension_hierarchies=sales_hierarchies,
                dimension_leaf_elements=sales_leaf_elements,
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD", "RegionCode": "NA"},
                        "BETA": {"Currency": "USD", "RegionCode": "EU"},
                        "DELTA": {"Currency": "USD", "RegionCode": "EU"},
                        "GAMMA": {"Currency": "EUR", "RegionCode": "EU"},
                    }
                },
                dimension_element_types={
                    "Measure": {
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={"Measure": {"Total Cost": ["Cost A", "Cost B"]}},
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


def test_compile_deploys_consolidated_target_with_per_leaf_feeders():
    metadata_provider = _consolidated_sales_metadata()
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")

    sales["Total Cost"] = (sales["Driver"] * 2).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    assert explanation["backend"] == "native-rule backend"

    readiness = model.phase2_readiness("Sales:Total Cost")
    assert readiness["metadata_ready"] is True

    preview = model.compile(dry_run=True)
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Total Cost'] = C: (['Measure':'Measure':'Driver'] * 2);"
    )
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost B'];"
    )

    manifest = preview.manifest["Sales:Total Cost"]
    assert manifest["rule_area"] == "C"
    assert manifest["artifact"]["feeder_strategy"] == "consolidated_target_leaf_feeders"
    assert manifest["artifact"]["preview_only"] is False


def test_compile_deploys_consolidated_target_live():
    metadata_provider = _consolidated_sales_metadata()
    mock = MockTM1Service()
    model = Model(tm1=mock, metadata_provider=metadata_provider)
    sales = model.cube("Sales")

    sales["Total Cost"] = (sales["Driver"] * 2).native(scope="C")

    preview = model.compile(dry_run=False)

    assert preview.deployment["Sales"]["deployed"] is True
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_attribute_routed_cross_cube_align_deploys_live_from_tm1_metadata():
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
    tm1.add_dimension(
        "Measure",
        {
            "Cost A": "Numeric",
            "Cost B": "Numeric",
            "Total Cost": "Consolidated",
        },
        edges={("Total Cost", "Cost A"): 1, ("Total Cost", "Cost B"): 1},
    )
    tm1.add_dimension("Currency", {"USD": "Numeric", "EUR": "Numeric"})
    tm1.add_dimension("TargetCurrency", {"EUR": "Numeric"})
    tm1.add_dimension("FX Measure", {"Rate": "Numeric"})
    tm1.add_cube("Sales", ["Version", "Company", "Measure"])
    tm1.add_cube("FX Rates", ["Version", "Currency", "TargetCurrency", "FX Measure"])

    model = Model(tm1=tm1)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=False)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "consolidated_target_leaf_cross_cube"
    assert preview.deployment["Sales"]["deployed"] is True
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'FX Measure':'FX Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'Cost A'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'Cost B');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'FX Measure':'FX Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'Cost A'), DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'Cost B');"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_multi_attribute_same_target_dimension_align_deploys_leaf_level_feeders():
    metadata_provider = _consolidated_multi_attribute_align_metadata_provider()
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "consolidated_target_leaf_cross_cube"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'RegionCode':'RegionCode':'EU', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'GAMMA', 'Measure':'Measure':'Cost A'), DB('Sales', "
        "!Version, 'Company':'Company':'GAMMA', 'Measure':'Measure':'Cost B');\n"
        "[!Version, 'Currency':'Currency':'USD', 'RegionCode':'RegionCode':'EU', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'BETA', 'Measure':'Measure':'Cost A'), DB('Sales', "
        "!Version, 'Company':'Company':'DELTA', 'Measure':'Measure':'Cost A'), DB('Sales', "
        "!Version, 'Company':'Company':'BETA', 'Measure':'Measure':'Cost B'), DB('Sales', "
        "!Version, 'Company':'Company':'DELTA', 'Measure':'Measure':'Cost B');\n"
        "[!Version, 'Currency':'Currency':'USD', 'RegionCode':'RegionCode':'NA', "
        "'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => DB('Sales', "
        "!Version, 'Company':'Company':'ACME', 'Measure':'Measure':'Cost A'), DB('Sales', "
        "!Version, 'Company':'Company':'ACME', 'Measure':'Measure':'Cost B');"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_multi_attribute_broadcast_align_stays_preview_only_without_leaf_scope():
    metadata_provider = _consolidated_multi_attribute_align_metadata_provider(
        include_target_region=True,
        ambiguous_target_region=True,
    )
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        RegionCode=sales.Company.attribute("RegionCode"),
        TargetCurrency="EUR",
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_consolidated_cross_cube"
    assert explanation["native_eligibility"]["code"] == "native_align_target_leaf_scope_unproven"
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is True


def test_consolidated_target_expands_through_non_default_hierarchy_when_default_lacks_children():
    metadata_provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={
                    "Driver": "Numeric",
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Measure": ["Measure", "Reporting Measure"],
                },
                dimension_leaf_elements={
                    "Measure": ["Driver", "Cost A", "Cost B"]
                },
                dimension_element_types={
                    "Measure": {
                        "Driver": "Numeric",
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={},
                dimension_children_by_hierarchy={
                    "Measure": {
                        "Reporting Measure": {
                            "Total Cost": ["Cost A", "Cost B"],
                        }
                    }
                },
            )
        }
    )
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")

    sales["Total Cost"] = sales["Driver"].native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost B'];"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_case_feeds_all_branch_drivers_to_each_leaf_measure():
    metadata_provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Measure"],
                default_hierarchies={"Version": "Version", "Measure": "Measure"},
                measure_element_types={
                    "Use Alt": "Numeric",
                    "Driver": "Numeric",
                    "Fallback": "Numeric",
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_hierarchies={"Version": ["Version"], "Measure": ["Measure"]},
                dimension_leaf_elements={
                    "Measure": ["Use Alt", "Driver", "Fallback", "Cost A", "Cost B"]
                },
                dimension_element_types={
                    "Measure": {
                        "Use Alt": "Numeric",
                        "Driver": "Numeric",
                        "Fallback": "Numeric",
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={"Measure": {"Total Cost": ["Cost A", "Cost B"]}},
            )
        }
    )
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")

    sales["Total Cost"] = model.case(
        (sales["Use Alt"] != 0, sales["Driver"]),
        default=sales["Fallback"],
    ).native(scope="C")

    preview = model.compile(dry_run=True)

    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Total Cost'] = C: IF((['Measure':'Measure':'Use Alt'] <> 0), "
        "['Measure':'Measure':'Driver'], ['Measure':'Measure':'Fallback']);"
    )
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost B'];\n"
        "['Measure':'Measure':'Fallback'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Fallback'] => ['Measure':'Measure':'Cost B'];\n"
        "['Measure':'Measure':'Use Alt'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Use Alt'] => ['Measure':'Measure':'Cost B'];"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_strategy"] == (
        "consolidated_target_leaf_feeders"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_cross_cube_align_deploys_leaf_level_source_feeders():
    metadata_provider = StaticMetadataProvider(
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
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                    "Measure": ["Cost A", "Cost B"],
                },
                dimension_element_types={
                    "Measure": {
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={
                    "Measure": {"Total Cost": ["Cost A", "Cost B"]}
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
                    "Company": ["ACME"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(TargetCurrency="EUR").native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["backend"] == "native-rule backend"
    assert explanation["feeder_strategy"] == "consolidated_target_leaf_cross_cube"
    assert preview.rules["Sales"] == (
        "['Measure':'Measure':'Total Cost'] = C: "
        "DB('FX Rates', !Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate');"
    )
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost A'), "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost B');"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_strategy"] == (
        "consolidated_target_leaf_cross_cube"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_deployment_cube"] == "FX Rates"
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_attribute_routed_cross_cube_align_deploys_leaf_level_feeders():
    metadata_provider = StaticMetadataProvider(
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
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_attributes={"Company": ["Currency"]},
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME", "BETA"],
                    "Measure": ["Cost A", "Cost B"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    }
                },
                dimension_element_types={
                    "Measure": {
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={"Measure": {"Total Cost": ["Cost A", "Cost B"]}},
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
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "consolidated_target_leaf_cross_cube"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'Cost A'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Measure':'Measure':'Cost B');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'Cost A'), DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Measure':'Measure':'Cost B');"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_deployment_cube"] == "FX Rates"
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_attribute_routed_broadcast_cross_cube_align_deploys_leaf_level_feeders():
    metadata_provider = StaticMetadataProvider(
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
                measure_element_types={
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
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
                    "Measure": ["Cost A", "Cost B"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    }
                },
                dimension_element_types={
                    "Measure": {
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={"Measure": {"Total Cost": ["Cost A", "Cost B"]}},
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
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "consolidated_target_leaf_cross_cube"
    assert preview.feeders["FX Rates"] == (
        "[!Version, 'Currency':'Currency':'EUR', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Region':'Region':'East', 'Measure':'Measure':'Cost A'), DB('Sales', !Version, "
        "'Company':'Company':'BETA', 'Region':'Region':'West', 'Measure':'Measure':'Cost A'), "
        "DB('Sales', !Version, 'Company':'Company':'BETA', 'Region':'Region':'East', "
        "'Measure':'Measure':'Cost B'), DB('Sales', !Version, 'Company':'Company':'BETA', "
        "'Region':'Region':'West', 'Measure':'Measure':'Cost B');\n"
        "[!Version, 'Currency':'Currency':'USD', 'TargetCurrency':'TargetCurrency':'EUR', "
        "'Measure':'Measure':'Rate'] => DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Region':'Region':'East', 'Measure':'Measure':'Cost A'), DB('Sales', !Version, "
        "'Company':'Company':'ACME', 'Region':'Region':'West', 'Measure':'Measure':'Cost A'), "
        "DB('Sales', !Version, 'Company':'Company':'ACME', 'Region':'Region':'East', "
        "'Measure':'Measure':'Cost B'), DB('Sales', !Version, 'Company':'Company':'ACME', "
        "'Region':'Region':'West', 'Measure':'Measure':'Cost B');"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_cross_cube_align_keeps_same_cube_driver_feeders_too():
    metadata_provider = StaticMetadataProvider(
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
                    "Driver": "Numeric",
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                    "Measure": ["Driver", "Cost A", "Cost B"],
                },
                dimension_element_types={
                    "Measure": {
                        "Driver": "Numeric",
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={
                    "Measure": {"Total Cost": ["Cost A", "Cost B"]}
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
                    "Company": ["ACME"],
                    "TargetCurrency": ["EUR"],
                },
            ),
        }
    )
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = (sales["Driver"] * fx["Rate"].align(TargetCurrency="EUR")).native(scope="C")

    preview = model.compile(dry_run=True)

    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Driver'] => ['Measure':'Measure':'Cost B'];"
    )
    assert preview.feeders["FX Rates"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost A'), "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost B');"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_deployment_cube"] is None
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_deployment_cubes"] == [
        "FX Rates",
        "Sales",
    ]
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_with_conditional_cross_cube_branches_unions_leaf_level_feeders():
    metadata_provider = StaticMetadataProvider(
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
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
                dimension_hierarchies={
                    "Version": ["Version"],
                    "Company": ["Company"],
                    "Measure": ["Measure"],
                },
                dimension_leaf_elements={
                    "Company": ["ACME"],
                    "Measure": ["Use Corporate Rate", "Cost A", "Cost B"],
                },
                dimension_element_types={
                    "Measure": {
                        "Use Corporate Rate": "Numeric",
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={"Measure": {"Total Cost": ["Cost A", "Cost B"]}},
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
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    corporate_fx = model.cube("Corporate FX")
    market_fx = model.cube("Market FX")

    sales["Total Cost"] = model.case(
        (sales["Use Corporate Rate"] != 0, corporate_fx["Rate"].align(TargetCurrency="EUR")),
        default=market_fx["Rate"].align(TargetCurrency="EUR"),
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "consolidated_target_leaf_cross_cube"
    assert preview.feeders["Corporate FX"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost A'), "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost B');"
    )
    assert preview.feeders["Market FX"] == (
        "[!Version, !Company, 'TargetCurrency':'TargetCurrency':'EUR', 'Measure':'Measure':'Rate'] => "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost A'), "
        "DB('Sales', !Version, !Company, 'Measure':'Measure':'Cost B');"
    )
    assert preview.feeders["Sales"] == (
        "['Measure':'Measure':'Use Corporate Rate'] => ['Measure':'Measure':'Cost A'];\n"
        "['Measure':'Measure':'Use Corporate Rate'] => ['Measure':'Measure':'Cost B'];"
    )
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_deployment_cube"] is None
    assert preview.manifest["Sales:Total Cost"]["artifact"]["feeder_deployment_cubes"] == [
        "Corporate FX",
        "Market FX",
        "Sales",
    ]
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is False


def test_consolidated_target_rejected_when_measure_is_not_consolidated_type():
    metadata_provider = _consolidated_sales_metadata()
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")

    sales["Driver Copy"] = sales["Driver"].native(scope="C")

    explanation = model.explain("Sales:Driver Copy")

    assert explanation["backend"] == "Python materialization backend"
    assert "Consolidated element type" in explanation["rationale"]


def test_consolidated_target_keeps_attribute_routed_broadcast_cross_cube_preview_only_without_leaf_scope():
    metadata_provider = StaticMetadataProvider(
        {
            "Sales": build_static_cube_metadata(
                "Sales",
                ["Version", "Company", "Region", "Measure"],
                default_hierarchies={
                    "Version": "Version",
                    "Company": "Company",
                    "Measure": "Measure",
                },
                measure_element_types={
                    "Cost A": "Numeric",
                    "Cost B": "Numeric",
                    "Total Cost": "Consolidated",
                },
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
                    "Measure": ["Cost A", "Cost B"],
                },
                dimension_attribute_values={
                    "Company": {
                        "ACME": {"Currency": "USD"},
                        "BETA": {"Currency": "EUR"},
                    }
                },
                dimension_element_types={
                    "Measure": {
                        "Cost A": "Numeric",
                        "Cost B": "Numeric",
                        "Total Cost": "Consolidated",
                    }
                },
                dimension_children={"Measure": {"Total Cost": ["Cost A", "Cost B"]}},
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
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")
    fx = model.cube("FX Rates")

    sales["Total Cost"] = fx["Rate"].align(
        Currency=sales.Company.attribute("Currency"),
        TargetCurrency="EUR",
    ).native(scope="C")

    explanation = model.explain("Sales:Total Cost")
    preview = model.compile(dry_run=True)

    assert explanation["feeder_strategy"] == "preview_only_consolidated_cross_cube"
    assert explanation["native_eligibility"]["code"] == "native_align_target_leaf_scope_unproven"
    assert preview.manifest["Sales:Total Cost"]["artifact"]["preview_only"] is True


def test_consolidated_target_rejected_when_leaf_expansion_is_partial():
    # "Other Cost" has no children metadata and is not itself a leaf, so the whole
    # expansion must fail closed instead of silently feeding only "Cost A".
    metadata_provider = _consolidated_sales_metadata(
        children={"Total Cost": ["Cost A", "Other Cost"]},
    )
    model = Model(metadata_provider=metadata_provider)
    sales = model.cube("Sales")

    sales["Total Cost"] = sales["Driver"].native(scope="C")

    explanation = model.explain("Sales:Total Cost")

    assert explanation["backend"] == "Python materialization backend"
    assert "bounded leaf-measure expansion" in explanation["rationale"]


def test_consolidated_target_rejected_when_leaf_count_exceeds_ceiling():
    metadata_provider = _consolidated_sales_metadata()
    model = Model(metadata_provider=metadata_provider)
    model._CONSOLIDATED_FEEDER_LEAF_CEILING = 1
    sales = model.cube("Sales")

    sales["Total Cost"] = sales["Driver"].native(scope="C")

    explanation = model.explain("Sales:Total Cost")

    assert explanation["backend"] == "Python materialization backend"
    assert "beyond the bounded ceiling" in explanation["rationale"]


def test_consolidated_target_default_leaf_ceiling_is_2000():
    model = Model(metadata_provider=_consolidated_sales_metadata())

    assert model._CONSOLIDATED_FEEDER_LEAF_CEILING == 2000
