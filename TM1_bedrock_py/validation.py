from string import Template
from typing import List, Dict, Any, Literal, Union, Optional, Type, Callable
from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError
import pandas as pd


def validate_choice(parameter_name: str, value: Any, allowed_values: set[Any]) -> None:
    if value not in allowed_values:
        formatted_values = ", ".join(repr(item) for item in sorted(allowed_values))
        raise ValueError(
            f"Unsupported '{parameter_name}' value {value!r}. "
            f"Expected one of: {formatted_values}."
        )


def validate_positive_integer(parameter_name: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"'{parameter_name}' must be a positive integer, got {value!r}.")


def validate_callable(parameter_name: str, value: Any, allow_none: bool = False) -> None:
    if value is None and allow_none:
        return
    if not callable(value):
        raise TypeError(f"'{parameter_name}' must be callable, got {type(value).__name__}.")


def validate_dataframe_callback_result(callback_name: str, result: Any) -> pd.DataFrame:
    if not isinstance(result, pd.DataFrame):
        raise TypeError(
            f"'{callback_name}' must return a pandas DataFrame, "
            f"got {type(result).__name__}."
        )
    return result


def validate_schema_callback_result(
    callback_name: str, result: Any
) -> tuple[Optional[pd.DataFrame], pd.DataFrame]:
    if not isinstance(result, tuple) or len(result) != 2:
        raise TypeError(
            f"'{callback_name}' must return a two-item tuple of "
            "(edges_dataframe, elements_dataframe)."
        )

    edges_dataframe, elements_dataframe = result
    if edges_dataframe is not None and not isinstance(edges_dataframe, pd.DataFrame):
        raise TypeError(
            f"'{callback_name}' returned {type(edges_dataframe).__name__} for edges; "
            "expected a pandas DataFrame or None."
        )
    if not isinstance(elements_dataframe, pd.DataFrame):
        raise TypeError(
            f"'{callback_name}' returned {type(elements_dataframe).__name__} for elements; "
            "expected a pandas DataFrame."
        )
    return edges_dataframe, elements_dataframe


def validate_parallel_mdx_inputs(
    param_names: list[str],
    param_tuples: list[tuple[Any, ...]],
    data_mdx_template: str,
    data_copy_function: Callable[..., Any],
    max_workers: int,
) -> None:
    validate_positive_integer("max_workers", max_workers)
    validate_callable("data_copy_function", data_copy_function)
    if not isinstance(data_mdx_template, str) or not data_mdx_template.strip():
        raise ValueError("'data_mdx_template' must be a non-empty string.")
    if not param_tuples:
        raise ValueError(
            "The parameter set MDX queries produced no execution tuples; "
            "parallel execution would perform no work."
        )

    template_identifiers = set()
    for match in Template.pattern.finditer(data_mdx_template):
        identifier = match.group("named") or match.group("braced")
        if identifier:
            template_identifiers.add(identifier)
        elif match.group("invalid"):
            raise ValueError(
                "The data_mdx_template contains an invalid '$' placeholder."
            )

    missing_identifiers = [
        parameter_name
        for parameter_name in param_names
        if parameter_name not in template_identifiers
    ]
    if missing_identifiers:
        raise ValueError(
            "The data_mdx_template is missing placeholders for parameter "
            f"dimensions: {', '.join(missing_identifiers)}."
        )
    unexpected_identifiers = sorted(template_identifiers - set(param_names))
    if unexpected_identifiers:
        raise ValueError(
            "The data_mdx_template contains placeholders without matching "
            f"parameter dimensions: {', '.join(unexpected_identifiers)}."
        )


def validate_sql_pagination_inputs(
    sql_query_template: str,
    slice_size: int,
    data_copy_function: Callable[..., Any],
    max_workers: int,
) -> None:
    validate_positive_integer("slice_size", slice_size)
    validate_positive_integer("max_workers", max_workers)
    validate_callable("data_copy_function", data_copy_function)
    if not isinstance(sql_query_template, str) or not sql_query_template.strip():
        raise ValueError("'sql_query_template' must be a non-empty string.")

    missing_placeholders = [
        placeholder
        for placeholder in ("offset", "fetch")
        if "{" + placeholder + "}" not in sql_query_template
    ]
    if missing_placeholders:
        raise ValueError(
            "The sql_query_template is missing pagination placeholders: "
            + ", ".join("{" + name + "}" for name in missing_placeholders)
            + "."
        )


def _format_validation_error(configuration_name: str, validation_error: ValidationError) -> str:
    errors = validation_error.errors()
    formatted_errors = []
    for error in errors[:10]:
        location = ".".join(str(part) for part in error.get("loc", ())) or "<root>"
        formatted_errors.append(f"  - {location}: {error.get('msg', 'Invalid value')}")

    remaining_count = len(errors) - len(formatted_errors)
    if remaining_count > 0:
        formatted_errors.append(f"  - ... and {remaining_count} more validation error(s)")

    return (
        f"The {configuration_name} configuration is invalid:\n"
        + "\n".join(formatted_errors)
    )


# ------------------------------------------------------------------------------------------------------------
# validation system for bedrock data copy functions (ETL)
# ------------------------------------------------------------------------------------------------------------


class BaseMappingStep(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")


class ReplaceStep(BaseMappingStep):
    method: Literal["replace"]
    mapping: Dict[str, Any]


class MapAndReplaceStep(BaseMappingStep):
    method: Literal["map_and_replace"]
    mapping_dimensions: Union[Dict[str, str], List[str]]
    mapping_df: Optional[pd.DataFrame] = None
    mapping_filter: Optional[Dict[str, Any]] = None
    include_mapped_in_join: Optional[Union[bool, List[str]]] = None
    relabel_dimensions: Optional[bool] = False


class MapAndJoinStep(BaseMappingStep):
    method: Literal["map_and_join"]
    joined_columns: Union[List[str], str]
    mapping_df: Optional[pd.DataFrame] = None
    mapping_filter: Optional[Dict[str, Any]] = None
    dropped_columns: Optional[List[str]] = None


class CartesianStep(BaseMappingStep):
    method: Literal["cartesian"]
    joined_columns: Union[List[str], str]
    mapping_df: Optional[pd.DataFrame] = None
    mapping_filter: Optional[Dict[str, Any]] = None


class PivotStep(BaseMappingStep):
    method: Literal["pivot"]
    index: Union[str, List[str]]
    columns: Union[str, List[str]]
    values: Union[str, List[str]]


class UnpivotStep(BaseMappingStep):
    method: Literal["unpivot"]
    id_vars: Union[str, List[str]]
    var_name: str
    value_name: str


class BasicReshapingStep(BaseMappingStep):
    method: Literal["basic_reshaping"]
    filter_condition: Optional[Dict[str, Any]] = None
    columns_to_drop: Optional[Dict[str, Any]] = None
    new_columns_with_values: Optional[Dict[str, Any]] = None
    column_relabel_map: Optional[Dict[str, str]] = None


class CartesianWithSetStep(BaseMappingStep):
    method: Literal["cartesian_with_set"]
    mapping_df: pd.DataFrame
    set_mdx: Optional[str] = None


MappingStep = Union[
    ReplaceStep,
    MapAndReplaceStep,
    MapAndJoinStep,
    CartesianStep,
    PivotStep,
    UnpivotStep,
    BasicReshapingStep,
    CartesianWithSetStep
]

mapping_pipeline_adapter: TypeAdapter = TypeAdapter(List[MappingStep])


def validate_mapping_configuration(raw_mapping_steps: List[Dict[str, Any]], logger: Any) -> None:
    """
    Validates the mapping pipeline configuration.
    Raises pydantic.ValidationError if invalid.
    """
    try:
        mapping_pipeline_adapter.validate_python(raw_mapping_steps)
    except ValidationError as validation_error:
        logger.error(validation_error.json(indent=4))
        raise ValueError(
            _format_validation_error("mapping_steps", validation_error)
        ) from validation_error


# ------------------------------------------------------------------------------------------------------------
# validation system for bedrock input handler
# ------------------------------------------------------------------------------------------------------------


class BaseCalculationStep(BaseModel):
    model_config = ConfigDict(extra="allow")
    name: str


class ConstantStep(BaseCalculationStep):
    method: Literal["constant"]
    value: float


class SumStep(BaseCalculationStep):
    method: Literal["sum"]
    column_to_sum: str


class SumGroupStep(BaseCalculationStep):
    method: Literal["sum_group"]
    column_to_sum: str
    group_column_list: list[str]


class SumIfStep(BaseCalculationStep):
    method: Literal["sumif"]
    column_to_sum: str
    statement: str


class CountStep(BaseCalculationStep):
    method: Literal["count"]


class CountGroupStep(BaseCalculationStep):
    method: Literal["count_group"]
    group_column_list: list[str]


class CountIfStep(BaseCalculationStep):
    method: Literal["countif"]
    statement: str


class CountUniqueStep(BaseCalculationStep):
    method: Literal["count_unique"]
    group_column_list: list[str]


class RankStep(BaseCalculationStep):
    method: Union[Literal["rank_over"], Literal["index_group"]]
    group_column_list: List[str]
    start_index: int = 1
    direction: Literal["asc", "desc"] = "asc"


class IndexStep(BaseCalculationStep):
    method: Literal["index"]
    start_index: int = 1
    direction: Literal["asc", "desc"] = "asc"


class CubeDataStep(BaseCalculationStep):
    model_config = ConfigDict(arbitrary_types_allowed=True, extra="allow")
    method: Union[Literal["cube_data"], Literal["query"]]
    calc_mdx: str


class IfStep(BaseCalculationStep):
    method: Union[Literal["if"], Literal["condition"]]
    if_then: Dict[str, Any]
    fallback: Any | None = None


class FormulaStep(BaseCalculationStep):
    method: Literal["formula"]
    formula: str


class StringtemplateStep(BaseCalculationStep):
    method: Literal["string", "template", "string_template"]
    template_string: str


class CustomStep(BaseCalculationStep):
    method: Literal["custom"]
    custom_callable: Callable
    custom_args: list[Any] = None
    custom_kwargs: dict[str, Any] = None


CalculationStep = Union[
    ConstantStep,
    SumStep,
    SumIfStep,
    SumGroupStep,
    CountStep,
    CountIfStep,
    CountGroupStep,
    CountUniqueStep,
    IndexStep,
    RankStep,
    CubeDataStep,
    IfStep,
    FormulaStep,
    CustomStep,
    StringtemplateStep
]

pipeline_adapter: TypeAdapter = TypeAdapter(List[CalculationStep])


def validate_calculation_pipeline_configuration(raw_configuration_steps: List[Dict[str, Any]], logger: Any) -> None:
    try:
        pipeline_adapter.validate_python(raw_configuration_steps)
    except ValidationError as validation_error:
        logger.error(validation_error.json(indent=4))
        raise ValueError(
            _format_validation_error("calculation_steps", validation_error)
        ) from validation_error
