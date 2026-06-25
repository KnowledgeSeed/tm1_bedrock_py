"""Live-TM1-server functional test harness for the calc native-rule compiler.

See dev/14_live_server_test_plan.md for the full specification, design rationale,
and how to run this against a real TM1 server. This module is intentionally
self-contained: it builds its own test dimensions/cubes (all prefixed with
TEST_PREFIX), loads deterministic baseline data, deploys a representative set of
native-rule formula shapes via TM1_bedrock_py.calc.Model, reads the results back
through TM1_bedrock_py's own extractor/loader functions, and compares against
independently-computed expected values.

Entry point: run_full_suite(tm1_service).
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import pandas as pd
from TM1py.Objects import Cube, Dimension, Element, ElementAttribute, Hierarchy

from TM1_bedrock_py import bedrock, extractor, loader, utility
from TM1_bedrock_py.calc import Model, TM1ServiceMetadataProvider
from TM1_bedrock_py.dimension_builder import apply as dimension_builder_apply

TEST_PREFIX = "TM1BPY_TEST_"

DIM_VERSION = f"{TEST_PREFIX}Version"
DIM_YEAR = f"{TEST_PREFIX}Year"
DIM_MONTH = f"{TEST_PREFIX}Month"
DIM_REGION = f"{TEST_PREFIX}Region"
DIM_CURRENCY = f"{TEST_PREFIX}Currency"
DIM_TARGET_CURRENCY = f"{TEST_PREFIX}TargetCurrency"
DIM_SALES_MEASURE = f"{TEST_PREFIX}SalesMeasure"
DIM_FX_MEASURE = f"{TEST_PREFIX}FXMeasure"

CUBE_SALES = f"{TEST_PREFIX}Sales"
CUBE_FX = f"{TEST_PREFIX}FXRates"

REGIONS = ("East", "West", "North", "South")
REGION_CURRENCY = {"East": "USD", "West": "USD", "North": "EUR", "South": "GBP"}
REGION_CODE = {"East": "E", "West": "W", "North": "N", "South": "S"}
YEARS = ("2023", "2024", "2025")
MONTHS = tuple(str(month) for month in range(1, 13))
COASTAL_REGIONS = ("East", "West")


@dataclass
class HarnessReport:
    schema: Dict[str, Any] = field(default_factory=dict)
    checks: List[Dict[str, Any]] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)

    def record(self, name: str, passed: bool, detail: str = "") -> None:
        self.checks.append({"check": name, "passed": passed, "detail": detail})
        if not passed:
            self.failures.append(f"{name}: {detail}")

    @property
    def ok(self) -> bool:
        return not self.failures


# ----------------------------------------------------------------------------------
# Schema creation
# ----------------------------------------------------------------------------------


def build_schema(tm1_service: Any) -> Dict[str, Any]:
    """Creates all test dimensions and cubes. Safe to call against a real server:
    every object name is prefixed with TEST_PREFIX."""

    _create_simple_dimension(tm1_service, DIM_VERSION, {"Actual": "Numeric", "Budget": "Numeric"})

    _create_simple_dimension(
        tm1_service, DIM_YEAR, {year: "Numeric" for year in YEARS}
    )

    _create_month_dimension(tm1_service)
    _create_region_dimension(tm1_service)

    _create_simple_dimension(tm1_service, DIM_CURRENCY, {"USD": "Numeric", "EUR": "Numeric", "GBP": "Numeric"})
    _create_simple_dimension(
        tm1_service, DIM_TARGET_CURRENCY, {"USD": "Numeric", "EUR": "Numeric", "GBP": "Numeric"}
    )

    _create_sales_measure_dimension(tm1_service)
    _create_simple_dimension(tm1_service, DIM_FX_MEASURE, {"Rate": "Numeric"})

    bedrock.cube_builder(
        tm1_service=tm1_service,
        build_mode="create_from_map",
        if_cube_exist_strategy="rebuild",
        cube_dimension_create_map={
            CUBE_SALES: [DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION, DIM_SALES_MEASURE],
            CUBE_FX: [DIM_VERSION, DIM_YEAR, DIM_CURRENCY, DIM_TARGET_CURRENCY, DIM_FX_MEASURE],
        },
    )

    return {
        "dimensions": [
            DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION,
            DIM_CURRENCY, DIM_TARGET_CURRENCY, DIM_SALES_MEASURE, DIM_FX_MEASURE,
        ],
        "cubes": [CUBE_SALES, CUBE_FX],
    }


def _create_simple_dimension(tm1_service: Any, dimension_name: str, elements: Dict[str, str]) -> None:
    hierarchy = Hierarchy(
        name=dimension_name,
        dimension_name=dimension_name,
        elements=[Element(name, element_type) for name, element_type in elements.items()],
    )
    dimension = Dimension(name=dimension_name, hierarchies=[hierarchy])
    tm1_service.dimensions.update_or_create(dimension)


def _create_month_dimension(tm1_service: Any) -> None:
    hierarchy = Hierarchy(
        name=DIM_MONTH,
        dimension_name=DIM_MONTH,
        elements=[Element(month, "Numeric") for month in MONTHS],
    )
    hierarchy.add_element_attribute("Month Number", "Numeric")
    hierarchy.add_element_attribute("Prev Month", "String")
    dimension = Dimension(name=DIM_MONTH, hierarchies=[hierarchy])
    tm1_service.dimensions.update_or_create(dimension)

    # Attribute values are written through the standard }ElementAttributes_ cube,
    # same mechanism TM1py/TM1 itself uses; write via cells.write_dataframe for
    # portability across the mock and a real server.
    _write_attribute_values(
        tm1_service,
        dimension_name=DIM_MONTH,
        attribute_names=["Month Number", "Prev Month"],
        rows=[
            {DIM_MONTH: month, "Month Number": str(int(month)), "Prev Month": (
                str(int(month) - 1) if int(month) > 1 else ""
            )}
            for month in MONTHS
        ],
    )


def _create_region_dimension(tm1_service: Any) -> None:
    default_hierarchy = Hierarchy(
        name=DIM_REGION,
        dimension_name=DIM_REGION,
        elements=(
            [Element(region, "Numeric") for region in REGIONS]
            + [Element("All Regions", "Consolidated")]
        ),
        edges={("All Regions", region): 1.0 for region in REGIONS},
    )
    default_hierarchy.add_element_attribute("Currency", "String")
    dimension = Dimension(name=DIM_REGION, hierarchies=[default_hierarchy])
    tm1_service.dimensions.update_or_create(dimension)

    # Alternate hierarchy: groups only East/West under "Coastal" -- deliberately
    # omits North/South so the consolidated element's leaf set differs from the
    # default hierarchy. This is the live-server regression target for the
    # explicit-hierarchy-pinning fix in Model._expand_consolidated_element_to_leaves
    # (dev/12_current_gaps.md gap #6).
    coastal_hierarchy = Hierarchy(
        name="Coastal Grouping",
        dimension_name=DIM_REGION,
        elements=(
            [Element(region, "Numeric") for region in REGIONS]
            + [Element("Coastal", "Consolidated")]
        ),
        edges={("Coastal", region): 1.0 for region in COASTAL_REGIONS},
    )
    tm1_service.hierarchies.update_or_create(coastal_hierarchy)

    _write_attribute_values(
        tm1_service,
        dimension_name=DIM_REGION,
        attribute_names=["Currency"],
        rows=[{DIM_REGION: region, "Currency": REGION_CURRENCY[region]} for region in REGIONS],
    )


def _write_attribute_values(
    tm1_service: Any,
    dimension_name: str,
    attribute_names: List[str],
    rows: List[Dict[str, Any]],
) -> None:
    """Writes element attribute values the same way bedrock.dimension_builder does
    internally (apply.create_attribute_structure + loader.dataframe_to_cube into the
    standard }}ElementAttributes_<dim> cube) -- this is the real, server-correct path
    TM1 itself uses to store attribute values, not a harness shortcut."""

    attribute_cube_name = "}" + f"ElementAttributes_{dimension_name}"
    dimension_builder_apply.create_attribute_structure(
        tm1_service=tm1_service,
        attr_cols=[f"{attribute_name}:String" for attribute_name in attribute_names],
        attr_cube_name=attribute_cube_name,
        dimension_name=dimension_name,
    )
    long_rows = [
        {dimension_name: row[dimension_name], attribute_cube_name: attribute_name, "Value": row[attribute_name]}
        for row in rows
        for attribute_name in attribute_names
    ]
    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=pd.DataFrame(long_rows),
        cube_name=attribute_cube_name,
        cube_dims=[dimension_name, attribute_cube_name],
        use_blob=False,
    )


SALES_MEASURES_NUMERIC = (
    "Revenue",
    "Cost",
    "Gross Margin",
    "Gross Margin Pct",
    "Revenue Prior Month Attr",
    "Revenue Prior Month Num",
    "Revenue Prior Year",
    "Revenue Rolling 2yr",
    "Revenue YTD",
    "Revenue EUR",
    "Department Cost Max Override",
)
SALES_MEASURES_STRING = ("Region Code", "Region Status")


def _create_sales_measure_dimension(tm1_service: Any) -> None:
    elements = [Element(name, "Numeric") for name in SALES_MEASURES_NUMERIC]
    elements += [Element(name, "String") for name in SALES_MEASURES_STRING]
    hierarchy = Hierarchy(name=DIM_SALES_MEASURE, dimension_name=DIM_SALES_MEASURE, elements=elements)
    dimension = Dimension(name=DIM_SALES_MEASURE, hierarchies=[hierarchy])
    tm1_service.dimensions.update_or_create(dimension)


# ----------------------------------------------------------------------------------
# Baseline data
# ----------------------------------------------------------------------------------


def load_baseline_data(tm1_service: Any) -> Dict[str, pd.DataFrame]:
    """Writes deterministic leaf data via TM1_bedrock_py.loader.dataframe_to_cube.
    Returns the frames used, so expected values can be recomputed independently."""

    revenue_rows = []
    for region_index, region in enumerate(REGIONS):
        for month in MONTHS:
            month_number = int(month)
            revenue = 1000.0 + region_index * 100.0 + month_number * 10.0
            revenue_rows.append(
                {
                    DIM_VERSION: "Actual",
                    DIM_YEAR: "2024",
                    DIM_MONTH: month,
                    DIM_REGION: region,
                    DIM_SALES_MEASURE: "Revenue",
                    "Value": revenue,
                }
            )
            revenue_rows.append(
                {
                    DIM_VERSION: "Actual",
                    DIM_YEAR: "2024",
                    DIM_MONTH: month,
                    DIM_REGION: region,
                    DIM_SALES_MEASURE: "Cost",
                    "Value": revenue * 0.6,
                }
            )
    # A second year, so the numeric-offset shift(Year=-1) test has a predecessor to read.
    for region_index, region in enumerate(REGIONS):
        for month in MONTHS:
            month_number = int(month)
            revenue = 900.0 + region_index * 90.0 + month_number * 9.0
            revenue_rows.append(
                {
                    DIM_VERSION: "Actual",
                    DIM_YEAR: "2023",
                    DIM_MONTH: month,
                    DIM_REGION: region,
                    DIM_SALES_MEASURE: "Revenue",
                    "Value": revenue,
                }
            )

    region_code_rows = [
        {
            DIM_VERSION: "Actual",
            DIM_YEAR: "2024",
            DIM_MONTH: "1",
            DIM_REGION: region,
            DIM_SALES_MEASURE: "Region Code",
            "Value": REGION_CODE[region],
        }
        for region in REGIONS
    ]

    sales_df = pd.DataFrame(revenue_rows)
    region_code_df = pd.DataFrame(region_code_rows)

    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=sales_df,
        cube_name=CUBE_SALES,
        cube_dims=[DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION, DIM_SALES_MEASURE],
        use_blob=False,
    )
    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=region_code_df,
        cube_name=CUBE_SALES,
        cube_dims=[DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION, DIM_SALES_MEASURE],
        use_blob=False,
    )

    fx_rows = [
        {DIM_VERSION: "Actual", DIM_YEAR: "2024", DIM_CURRENCY: "USD", DIM_TARGET_CURRENCY: "EUR",
         DIM_FX_MEASURE: "Rate", "Value": 0.92},
        {DIM_VERSION: "Actual", DIM_YEAR: "2024", DIM_CURRENCY: "EUR", DIM_TARGET_CURRENCY: "EUR",
         DIM_FX_MEASURE: "Rate", "Value": 1.0},
        {DIM_VERSION: "Actual", DIM_YEAR: "2024", DIM_CURRENCY: "GBP", DIM_TARGET_CURRENCY: "EUR",
         DIM_FX_MEASURE: "Rate", "Value": 1.17},
    ]
    fx_df = pd.DataFrame(fx_rows)
    loader.dataframe_to_cube(
        tm1_service=tm1_service,
        dataframe=fx_df,
        cube_name=CUBE_FX,
        cube_dims=[DIM_VERSION, DIM_YEAR, DIM_CURRENCY, DIM_TARGET_CURRENCY, DIM_FX_MEASURE],
        use_blob=False,
    )

    return {"sales": sales_df, "region_code": region_code_df, "fx": fx_df}


# ----------------------------------------------------------------------------------
# Model / formulas
# ----------------------------------------------------------------------------------


def build_model(tm1_service: Any) -> Model:
    model = Model(metadata_provider=TM1ServiceMetadataProvider(tm1_service), tm1=tm1_service)
    sales = model.cube(CUBE_SALES)
    fx = model.cube(CUBE_FX)

    sales["Gross Margin"] = sales["Revenue"] - sales["Cost"]
    sales["Gross Margin Pct"] = (sales["Gross Margin"] / sales["Revenue"]).where(sales["Revenue"] != 0)
    sales["Revenue Prior Month Attr"] = sales["Revenue"].shift(**{DIM_MONTH: "Prev Month"})
    sales["Revenue Prior Month Num"] = sales["Revenue"].shift(**{DIM_MONTH: -1})
    sales["Revenue Prior Year"] = sales["Revenue"].shift(**{DIM_YEAR: -1})
    sales["Revenue Rolling 2yr"] = sales["Revenue"].rolling(**{DIM_YEAR: 2}).sum()
    sales["Revenue YTD"] = sales["Revenue"].ytd(dimension=DIM_MONTH, period_number_attribute="Month Number")
    sales["Revenue EUR"] = sales["Revenue"] * fx["Rate"].align(
        **{DIM_CURRENCY: getattr(sales, DIM_REGION).attribute("Currency"), DIM_TARGET_CURRENCY: "EUR"}
    )
    sales["Region Status"] = (sales["Region Code"].concat(" REGION")).upper().native(scope="string")
    sales["Department Cost Max Override"] = sales["Department Cost Max Override"].consolidated_max(flag=2)

    return model


# ----------------------------------------------------------------------------------
# Expected-value computation (independent of TM1 -- pure pandas re-implementation)
# ----------------------------------------------------------------------------------


def compute_expected_values(baseline: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    sales = baseline["sales"]
    pivot = sales[sales[DIM_SALES_MEASURE].isin(["Revenue", "Cost"])].pivot_table(
        index=[DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION],
        columns=DIM_SALES_MEASURE,
        values="Value",
    ).reset_index()

    pivot["Gross Margin"] = pivot["Revenue"] - pivot["Cost"]
    pivot["Gross Margin Pct"] = pivot.apply(
        lambda row: (row["Gross Margin"] / row["Revenue"]) if row["Revenue"] != 0 else None, axis=1
    )

    revenue_2024 = pivot[pivot[DIM_YEAR] == "2024"].copy()
    revenue_2024["Month Int"] = revenue_2024[DIM_MONTH].astype(int)

    def _prior_month_attr(row: pd.Series) -> Optional[float]:
        prior_month = row["Month Int"] - 1
        if prior_month < 1:
            return None
        match = revenue_2024[
            (revenue_2024[DIM_REGION] == row[DIM_REGION]) & (revenue_2024["Month Int"] == prior_month)
        ]
        return float(match["Revenue"].iloc[0]) if not match.empty else None

    revenue_2024["Revenue Prior Month Attr"] = revenue_2024.apply(_prior_month_attr, axis=1)
    revenue_2024["Revenue Prior Month Num"] = revenue_2024["Revenue Prior Month Attr"]

    prior_year = pivot[pivot[DIM_YEAR] == "2023"][[DIM_REGION, DIM_MONTH, "Revenue"]].rename(
        columns={"Revenue": "Revenue Prior Year"}
    )
    revenue_2024 = revenue_2024.merge(prior_year, on=[DIM_REGION, DIM_MONTH], how="left")

    revenue_by_year_region_month = pivot.set_index([DIM_YEAR, DIM_REGION, DIM_MONTH])["Revenue"]

    def _rolling_2yr(row: pd.Series) -> float:
        current = row["Revenue"]
        prior = revenue_by_year_region_month.get(("2023", row[DIM_REGION], row[DIM_MONTH]), 0.0)
        return current + prior

    revenue_2024["Revenue Rolling 2yr"] = revenue_2024.apply(_rolling_2yr, axis=1)

    def _ytd(row: pd.Series) -> float:
        region = row[DIM_REGION]
        month_int = row["Month Int"]
        months_to_sum = [str(month) for month in range(1, month_int + 1)]
        subset = revenue_2024[
            (revenue_2024[DIM_REGION] == region) & (revenue_2024[DIM_MONTH].isin(months_to_sum))
        ]
        return float(subset["Revenue"].sum())

    revenue_2024["Revenue YTD"] = revenue_2024.apply(_ytd, axis=1)

    fx_rate_by_currency = baseline["fx"].set_index(DIM_CURRENCY)["Value"]

    def _revenue_eur(row: pd.Series) -> float:
        currency = REGION_CURRENCY[row[DIM_REGION]]
        rate = float(fx_rate_by_currency.get(currency, 0.0))
        return float(row["Revenue"]) * rate

    revenue_2024["Revenue EUR"] = revenue_2024.apply(_revenue_eur, axis=1)

    region_status = pd.DataFrame(
        {
            DIM_REGION: list(REGIONS),
            "Region Status": [f"{REGION_CODE[region]} REGION".upper() for region in REGIONS],
        }
    )

    return {
        "leaf_2024": revenue_2024,
        "region_status": region_status,
    }


# ----------------------------------------------------------------------------------
# Reading actual values back via TM1_bedrock_py's own extractor
# ----------------------------------------------------------------------------------


def read_measure_values(tm1_service: Any, cube_name: str, measure_dimension: str, measure_names: List[str],
                         extra_filters: Optional[Dict[str, List[str]]] = None) -> pd.DataFrame:
    dimension_filter_mapping = dict(extra_filters or {})
    dimension_filter_mapping[measure_dimension] = list(measure_names)

    mdx = utility.generate_dynamic_mdx_query_string(
        tm1_service=tm1_service,
        target_cube_name=cube_name,
        dimension_filter_mapping=dimension_filter_mapping,
    )
    return extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service,
        data_mdx=mdx,
        default_returned_value_type=float,
        skip_zeros=False,
    )


# ----------------------------------------------------------------------------------
# Verification
# ----------------------------------------------------------------------------------


def verify_leaf_calculations(tm1_service: Any, expected: Dict[str, pd.DataFrame], report: HarnessReport) -> None:
    actual = read_measure_values(
        tm1_service,
        CUBE_SALES,
        DIM_SALES_MEASURE,
        list(SALES_MEASURES_NUMERIC),
        extra_filters={DIM_VERSION: ["Actual"], DIM_YEAR: ["2024"]},
    )
    actual_pivot = actual.pivot_table(
        index=[DIM_MONTH, DIM_REGION], columns=DIM_SALES_MEASURE, values="Value"
    ).reset_index()

    expected_leaf = expected["leaf_2024"]
    merged = expected_leaf.merge(
        actual_pivot, on=[DIM_MONTH, DIM_REGION], suffixes=("_expected", "_actual")
    )

    for measure in (
        "Gross Margin", "Gross Margin Pct", "Revenue Prior Month Attr", "Revenue Prior Month Num",
        "Revenue Prior Year", "Revenue Rolling 2yr", "Revenue YTD", "Revenue EUR",
    ):
        expected_col = f"{measure}_expected" if f"{measure}_expected" in merged.columns else measure
        actual_col = f"{measure}_actual" if f"{measure}_actual" in merged.columns else measure
        mismatches = merged[~_isclose(merged[expected_col], merged[actual_col])]
        report.record(
            f"leaf_calculation:{measure}",
            mismatches.empty,
            "" if mismatches.empty else f"{len(mismatches)} mismatched rows",
        )


def _isclose(left: pd.Series, right: pd.Series) -> pd.Series:
    left_num = pd.to_numeric(left, errors="coerce")
    right_num = pd.to_numeric(right, errors="coerce")
    both_null = left_num.isna() & right_num.isna()
    return both_null | (left_num.sub(right_num).abs() < 1e-6)


def verify_string_calculations(tm1_service: Any, expected: Dict[str, pd.DataFrame], report: HarnessReport) -> None:
    actual = read_measure_values(
        tm1_service,
        CUBE_SALES,
        DIM_SALES_MEASURE,
        ["Region Status"],
        extra_filters={DIM_VERSION: ["Actual"], DIM_YEAR: ["2024"], DIM_MONTH: ["1"]},
    )
    actual_by_region = actual.set_index(DIM_REGION)["Value"]
    expected_by_region = expected["region_status"].set_index(DIM_REGION)["Region Status"]

    mismatches = [
        region for region in REGIONS
        if str(actual_by_region.get(region)) != str(expected_by_region.get(region))
    ]
    report.record(
        "string_calculation:Region Status",
        not mismatches,
        "" if not mismatches else f"mismatched regions: {mismatches}",
    )


def verify_consolidated_feeders(tm1_service: Any, report: HarnessReport) -> None:
    """Confirms feeders actually fired by checking a CONSOLIDATED total against the sum of
    its leaves -- a rule-derived leaf cell always recalculates correctly on direct query
    regardless of feeders, so this is the only query shape that actually exercises feeder
    correctness (an unfed cell consolidates as if it were blank)."""

    leaf_actual = read_measure_values(
        tm1_service,
        CUBE_SALES,
        DIM_SALES_MEASURE,
        ["Revenue", "Gross Margin", "Revenue EUR"],
        extra_filters={DIM_VERSION: ["Actual"], DIM_YEAR: ["2024"], DIM_REGION: list(REGIONS)},
    )
    leaf_sum_by_measure = leaf_actual.groupby(DIM_SALES_MEASURE)["Value"].sum()

    consolidated_mdx = utility.generate_dynamic_mdx_query_string(
        tm1_service=tm1_service,
        target_cube_name=CUBE_SALES,
        dimension_filter_mapping={
            DIM_VERSION: ["Actual"],
            DIM_YEAR: ["2024"],
            DIM_REGION: ["All Regions"],
            DIM_SALES_MEASURE: ["Revenue", "Gross Margin", "Revenue EUR"],
        },
    )
    consolidated_actual = extractor.tm1_mdx_to_dataframe(
        tm1_service=tm1_service, data_mdx=consolidated_mdx, default_returned_value_type=float,
    )
    consolidated_sum_by_measure = consolidated_actual.groupby(DIM_SALES_MEASURE)["Value"].sum()

    for measure in ("Revenue", "Gross Margin", "Revenue EUR"):
        expected_total = float(leaf_sum_by_measure.get(measure, 0.0))
        actual_total = float(consolidated_sum_by_measure.get(measure, 0.0))
        report.record(
            f"feeder:{measure}@All Regions",
            abs(expected_total - actual_total) < 1e-6,
            f"expected {expected_total}, got {actual_total} -- unfed cells would show 0/short here",
        )


_FEEDER_SOURCE_PATTERN = re.compile(r"^\[(?P<source>.*?)\]\s*=>", re.MULTILINE)
_QUOTED_TOKEN_PATTERN = re.compile(r"'([^']*)'")


def _extract_feeder_source_measures(feeder_text: str) -> List[str]:
    """Every feeder statement is `[source coordinates] => target;`. The source
    measure is always the last single-quoted element name in the source bracket --
    either bare ('Cost') or hierarchy-qualified ('Measure':'Measure':'Cost')."""

    measures = []
    for statement in feeder_text.splitlines():
        statement = statement.strip()
        if not statement:
            continue
        match = _FEEDER_SOURCE_PATTERN.match(statement)
        if not match:
            continue
        quoted_tokens = _QUOTED_TOKEN_PATTERN.findall(match.group("source"))
        if quoted_tokens:
            measures.append(quoted_tokens[-1])
    return measures


def verify_no_over_feeding(model: Model, preview: Any, report: HarnessReport) -> None:
    """Structural, server-independent over-feeding check: every feeder statement's
    source measure must be a genuine upstream dependency of some deployed formula
    whose target lives in the same cube the feeder is filed under. A spurious feeder
    referencing a measure nothing actually depends on would pass the sum-of-leaves
    under-feeding check in verify_consolidated_feeders (values stay correct) while
    still costing unnecessary TM1 recalculation -- this is the only check that would
    catch that. Run after compile(); does not require a live server."""

    allowed_source_measures_by_cube: Dict[str, set] = {}
    for target_key in model._build_plan().formulas:
        explanation = model.explain(target_key)
        for dependency_key in explanation["measure_dependencies"]:
            dependency_cube, _, dependency_measure = dependency_key.partition(":")
            allowed_source_measures_by_cube.setdefault(dependency_cube, set()).add(dependency_measure)

    for cube_name, feeder_text in preview.feeders.items():
        allowed = allowed_source_measures_by_cube.get(cube_name, set())
        actual_sources = set(_extract_feeder_source_measures(feeder_text))
        extraneous = actual_sources - allowed
        report.record(
            f"over_feeding:{cube_name}",
            not extraneous,
            f"feeder statements reference {extraneous}, which no deployed formula in this "
            f"model actually depends on -- allowed set was {allowed}",
        )


def verify_no_under_feeding_via_live_mutation(tm1_service: Any, report: HarnessReport) -> None:
    """The strongest under-feeding check: change a real leaf SOURCE value after
    deployment, then re-read the CONSOLIDATED total for every measure that depends on
    it, without issuing any explicit recalculation. If a dependent rule-derived cell
    was not fed, the consolidated total will not reflect the change -- this is
    distinct from verify_consolidated_feeders (which only checks a static snapshot
    is internally consistent) because it tests that feeders react to a genuinely new
    write, not just that they were correct for the data loaded at deploy time."""

    mutated_region = "East"
    mutated_month = "6"
    delta = 50.0
    currency = REGION_CURRENCY[mutated_region]

    before = _read_consolidated_totals(tm1_service)

    # Absolute (not incremental) write: read the current leaf value first, then write
    # old+delta. Avoids relying on TM1's increment semantics (which the mock cannot
    # safely emulate cube-wide alongside non-numeric measures in the same cube) and
    # keeps this check's own mutation/revert logic identical on a real server.
    leaf_before = read_measure_values(
        tm1_service, CUBE_SALES, DIM_SALES_MEASURE, ["Revenue"],
        extra_filters={DIM_VERSION: ["Actual"], DIM_YEAR: ["2024"], DIM_MONTH: [mutated_month],
                       DIM_REGION: [mutated_region]},
    )
    original_value = float(leaf_before["Value"].iloc[0]) if not leaf_before.empty else 0.0

    mutation_row = pd.DataFrame([{
        DIM_VERSION: "Actual", DIM_YEAR: "2024", DIM_MONTH: mutated_month,
        DIM_REGION: mutated_region, DIM_SALES_MEASURE: "Revenue", "Value": original_value + delta,
    }])
    loader.dataframe_to_cube(
        tm1_service=tm1_service, dataframe=mutation_row, cube_name=CUBE_SALES,
        cube_dims=[DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION, DIM_SALES_MEASURE],
        use_blob=False,
    )

    try:
        after = _read_consolidated_totals(tm1_service)

        fx_rate_lookup = read_measure_values(
            tm1_service, CUBE_FX, DIM_FX_MEASURE, ["Rate"],
            extra_filters={DIM_VERSION: ["Actual"], DIM_YEAR: ["2024"], DIM_CURRENCY: [currency],
                           DIM_TARGET_CURRENCY: ["EUR"]},
        )
        fx_rate = float(fx_rate_lookup["Value"].iloc[0]) if not fx_rate_lookup.empty else 0.0

        expected_deltas = {
            "Revenue": delta,
            "Gross Margin": delta,
            "Revenue EUR": delta * fx_rate,
        }
        for measure, expected_delta in expected_deltas.items():
            actual_delta = after.get(measure, 0.0) - before.get(measure, 0.0)
            report.record(
                f"under_feeding_live_mutation:{measure}@All Regions",
                abs(actual_delta - expected_delta) < 1e-6,
                f"after writing +{delta} to Revenue at {mutated_region}/{mutated_month}, expected "
                f"consolidated {measure} to move by {expected_delta}, moved by {actual_delta} -- "
                f"a smaller-than-expected move means the new leaf write was not fed into 'All Regions'",
            )
    finally:
        revert_row = mutation_row.copy()
        revert_row["Value"] = original_value
        loader.dataframe_to_cube(
            tm1_service=tm1_service, dataframe=revert_row, cube_name=CUBE_SALES,
            cube_dims=[DIM_VERSION, DIM_YEAR, DIM_MONTH, DIM_REGION, DIM_SALES_MEASURE],
            use_blob=False,
        )


def _read_consolidated_totals(tm1_service: Any) -> Dict[str, float]:
    mdx = utility.generate_dynamic_mdx_query_string(
        tm1_service=tm1_service,
        target_cube_name=CUBE_SALES,
        dimension_filter_mapping={
            DIM_VERSION: ["Actual"],
            DIM_YEAR: ["2024"],
            DIM_REGION: ["All Regions"],
            DIM_SALES_MEASURE: ["Revenue", "Gross Margin", "Revenue EUR"],
        },
    )
    actual = extractor.tm1_mdx_to_dataframe(tm1_service=tm1_service, data_mdx=mdx, default_returned_value_type=float)
    return actual.groupby(DIM_SALES_MEASURE)["Value"].sum().to_dict()


def verify_alternate_hierarchy_resolution(tm1_service: Any, report: HarnessReport) -> None:
    """Exercises the gap #6 fix directly against live-collected metadata: a consolidated
    element ('Coastal') that only has children in a non-default hierarchy ('Coastal
    Grouping') must resolve via that hierarchy when explicitly pinned, and must NOT
    resolve via a different, unrequested hierarchy."""

    provider = TM1ServiceMetadataProvider(tm1_service)
    metadata = provider.get_cube_metadata(CUBE_SALES)
    model = Model()

    resolved_via_pinned_alt_hierarchy = model._expand_consolidated_element_to_leaves(
        metadata=metadata,
        dimension_name=DIM_REGION,
        element_name="Coastal",
        explicit_hierarchy_name="Coastal Grouping",
    )
    report.record(
        "alt_hierarchy:pinned_resolution",
        resolved_via_pinned_alt_hierarchy is not None
        and set(resolved_via_pinned_alt_hierarchy) == set(COASTAL_REGIONS),
        f"got {resolved_via_pinned_alt_hierarchy}",
    )

    resolved_via_pinned_default_hierarchy = model._expand_consolidated_element_to_leaves(
        metadata=metadata,
        dimension_name=DIM_REGION,
        element_name="Coastal",
        explicit_hierarchy_name=DIM_REGION,
    )
    report.record(
        "alt_hierarchy:pinned_default_does_not_leak",
        resolved_via_pinned_default_hierarchy is None,
        f"expected None (Coastal has no children in the default hierarchy), got "
        f"{resolved_via_pinned_default_hierarchy}",
    )


def run_deployment_safety_net_check(model: Model, report: HarnessReport) -> None:
    """Model formulas can only be registered once per target, so v2 (a deliberate
    rule-text change) is built as a fresh Model against the same already-deployed cube
    rather than mutated in place."""

    tm1_service = model.tm1
    preview_v1 = model.compile(dry_run=False)
    if preview_v1.errors:
        report.record("deployment_safety_net:initial_deploy", False, str(preview_v1.errors))
        return

    rule_text_v1 = tm1_service.cubes.get(CUBE_SALES).rules.text

    model_v2 = build_model(tm1_service)
    sales_v2 = model_v2.cube(CUBE_SALES)
    sales_v2._model._formulas.pop(f"{CUBE_SALES}:Gross Margin", None)
    sales_v2["Gross Margin"] = (sales_v2["Revenue"] - sales_v2["Cost"]) * 1  # trivial rule-text change

    preview_v2 = model_v2.compile(dry_run=False)
    if preview_v2.errors:
        report.record("deployment_safety_net:v2_deploy", False, str(preview_v2.errors))
        return

    manifest_path_v2 = preview_v2.deployment_manifest_path
    model_v2.rollback_deployment(manifest_path_v2)
    rule_text_after_rollback = tm1_service.cubes.get(CUBE_SALES).rules.text

    report.record(
        "deployment_safety_net:rollback_restores_prior_text",
        rule_text_after_rollback == rule_text_v1,
        "rule text after rollback did not match the pre-v2 rule text",
    )


# ----------------------------------------------------------------------------------
# Cleanup
# ----------------------------------------------------------------------------------


def cleanup_schema(tm1_service: Any, schema: Dict[str, Any]) -> None:
    for cube_name in schema.get("cubes", []):
        if not cube_name.startswith(TEST_PREFIX):
            continue
        if tm1_service.cubes.exists(cube_name):
            tm1_service.cubes.delete(cube_name)

    for dimension_name in schema.get("dimensions", []):
        if not dimension_name.startswith(TEST_PREFIX):
            continue
        attribute_cube = f"}}ElementAttributes_{dimension_name}"
        if tm1_service.cubes.exists(attribute_cube):
            tm1_service.cubes.delete(attribute_cube)
        if tm1_service.dimensions.exists(dimension_name) and hasattr(tm1_service.dimensions, "delete"):
            tm1_service.dimensions.delete(dimension_name)


# ----------------------------------------------------------------------------------
# Orchestration
# ----------------------------------------------------------------------------------


def run_full_suite(tm1_service: Any, cleanup: bool = True) -> HarnessReport:
    report = HarnessReport()
    schema = build_schema(tm1_service)
    report.schema = schema

    try:
        baseline = load_baseline_data(tm1_service)
        model = build_model(tm1_service)
        preview = model.compile(dry_run=False)
        report.record("deploy:no_errors", not preview.errors, str(preview.errors))
        report.record(
            "deploy:no_preview_only_targets",
            all(
                not entry["artifact"].get("preview_only")
                for entry in preview.manifest.values()
                if entry["backend"] == "native"
            ),
            "one or more native formulas stayed preview-only; check preview.manifest",
        )

        if preview.errors:
            return report

        expected = compute_expected_values(baseline)
        verify_leaf_calculations(tm1_service, expected, report)
        verify_string_calculations(tm1_service, expected, report)
        verify_consolidated_feeders(tm1_service, report)
        verify_no_over_feeding(model, preview, report)
        verify_no_under_feeding_via_live_mutation(tm1_service, report)
        verify_alternate_hierarchy_resolution(tm1_service, report)
        run_deployment_safety_net_check(model, report)
    finally:
        if cleanup:
            cleanup_schema(tm1_service, schema)

    return report
