from typing import List, Dict, Any, Literal, Union, Optional, Type, Callable
from pydantic import BaseModel, ConfigDict, TypeAdapter, ValidationError
import pandas as pd


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
        raise ValueError("The mapping_steps configuration is invalid. Execution aborted.")


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
        raise ValueError("The calculation_steps configuration is invalid. Execution aborted.")


# ------------------------------------------------------------------------------------------------------------
# Runtime input validation — fail-fast guard clauses
# ------------------------------------------------------------------------------------------------------------


def validate_dimension_builder_input(
    dimension_name: str,
    input_format: str,
    raw_input_df: Optional[pd.DataFrame] = None,
    level_columns: Optional[list] = None,
    build_strategy: Optional[str] = None,
    input_datasource: Optional[str] = None,
) -> None:
    """Validate dimension_builder inputs before execution.

    Raises ValueError with a specific message for each validation failure.
    """
    if not dimension_name or not isinstance(dimension_name, str):
        raise ValueError("dimension_builder: dimension_name is required and must be a string")

    if not input_format or input_format not in ("indented_levels", "parent_child"):
        raise ValueError(
            f"dimension_builder: input_format must be 'indented_levels' or 'parent_child', "
            f"got '{input_format}'"
        )

    if build_strategy and build_strategy not in ("rebuild", "safe_rebuild", "update"):
        raise ValueError(
            f"dimension_builder: build_strategy must be 'rebuild', 'safe_rebuild', or 'update', "
            f"got '{build_strategy}'"
        )

    # At least one data source must be provided
    if raw_input_df is None and input_datasource is None:
        raise ValueError(
            "dimension_builder: either raw_input_df or input_datasource must be provided"
        )

    if raw_input_df is not None:
        if not isinstance(raw_input_df, pd.DataFrame):
            raise ValueError(
                f"dimension_builder: raw_input_df must be a pandas DataFrame, "
                f"got {type(raw_input_df).__name__}"
            )
        if len(raw_input_df.columns) == 0:
            raise ValueError(
                "dimension_builder: input DataFrame has no columns"
            )
        if len(raw_input_df) == 0:
            raise ValueError(
                "dimension_builder: input DataFrame has no rows"
            )

    if level_columns:
        if not isinstance(level_columns, list):
            raise ValueError(
                f"dimension_builder: level_columns must be a list, got {type(level_columns).__name__}"
            )
        if raw_input_df is not None and len(raw_input_df.columns) > 0:
            missing = [col for col in level_columns if col not in raw_input_df.columns]
            if missing:
                raise ValueError(
                    f"dimension_builder: level_columns {missing} not found in input DataFrame. "
                    f"Available columns: {list(raw_input_df.columns)}"
                )


def validate_data_copy_input(
    target_cube_name: str,
    data_mdx: Optional[str] = None,
    mdx_function: Optional[Any] = None,
    data_mdx_list: Optional[list] = None,
    sql_engine: Optional[Any] = None,
    sql_function: Optional[Any] = None,
    csv_function: Optional[Any] = None,
    mapping_steps: Optional[list] = None,
) -> None:
    """Validate data_copy_intercube / data_copy inputs before execution."""
    if not target_cube_name or not isinstance(target_cube_name, str):
        raise ValueError("data_copy: target_cube_name is required and must be a string")

    # At least one data source must be specified
    sources = [
        data_mdx is not None and len(data_mdx.strip()) > 0,
        mdx_function is not None,
        data_mdx_list is not None and len(data_mdx_list) > 0,
        sql_engine is not None,
        sql_function is not None,
        csv_function is not None,
    ]
    if not any(sources):
        raise ValueError(
            "data_copy: at least one data source must be specified "
            "(data_mdx, mdx_function, data_mdx_list, sql_engine, sql_function, or csv_function)"
        )

    if mapping_steps is not None:
        if not isinstance(mapping_steps, list):
            raise ValueError(
                f"data_copy: mapping_steps must be a list, got {type(mapping_steps).__name__}"
            )
        for i, step in enumerate(mapping_steps):
            if not isinstance(step, dict):
                raise ValueError(
                    f"data_copy: mapping_steps[{i}] must be a dict, got {type(step).__name__}"
                )
            if "method" not in step:
                raise ValueError(
                    f"data_copy: mapping_steps[{i}] is missing required 'method' key"
                )


def validate_cube_builder_input(
    cube_dimension_create_map: Optional[dict] = None,
    build_mode: Optional[str] = None,
    copy_source_cubes: Optional[Any] = None,
    copy_source_tm1_service: Optional[Any] = None,
) -> None:
    """Validate cube_builder inputs before execution."""
    if build_mode == "copy_from_source":
        if not copy_source_tm1_service:
            raise ValueError(
                "cube_builder: copy_source_tm1_service is required when build_mode='copy_from_source'"
            )
        if not copy_source_cubes:
            raise ValueError(
                "cube_builder: copy_source_cubes is required when build_mode='copy_from_source'"
            )
    elif cube_dimension_create_map is None or len(cube_dimension_create_map) == 0:
        raise ValueError(
            "cube_builder: cube_dimension_create_map must be a non-empty dict "
            "when not using build_mode='copy_from_source'"
        )
    elif not isinstance(cube_dimension_create_map, dict):
        raise ValueError(
            f"cube_builder: cube_dimension_create_map must be a dict, "
            f"got {type(cube_dimension_create_map).__name__}"
        )


def validate_input_handler_input(
    input_value: Any,
    target_cube_name: str,
    domain_coordinates: Optional[dict] = None,
    domain_mdx: Optional[str] = None,
    calculation_steps: Optional[list] = None,
) -> None:
    """Validate input_handler inputs before execution."""
    if not target_cube_name or not isinstance(target_cube_name, str):
        raise ValueError("input_handler: target_cube_name is required and must be a string")

    if domain_coordinates is None and domain_mdx is None:
        raise ValueError(
            "input_handler: either domain_coordinates or domain_mdx must be specified"
        )

    if input_value is None:
        raise ValueError("input_handler: input_value is required")

    if not isinstance(input_value, (int, float)):
        raise ValueError(
            f"input_handler: input_value must be numeric, got {type(input_value).__name__}"
        )

    if calculation_steps is not None:
        if not isinstance(calculation_steps, list):
            raise ValueError(
                f"input_handler: calculation_steps must be a list, "
                f"got {type(calculation_steps).__name__}"
            )
        valid_methods = {
            "count", "sum", "sumif", "countif", "index", "rank",
            "formula", "if", "cube_data", "string_template",
            "sum_group", "count_group", "count_unique", "custom"
        }
        for i, step in enumerate(calculation_steps):
            if not isinstance(step, dict):
                raise ValueError(
                    f"input_handler: calculation_steps[{i}] must be a dict, "
                    f"got {type(step).__name__}"
                )
            method = step.get("method")
            if not method:
                raise ValueError(
                    f"input_handler: calculation_steps[{i}] is missing required 'method' key"
                )
            if method not in valid_methods:
                raise ValueError(
                    f"input_handler: calculation_steps[{i}] has unknown method '{method}'. "
                    f"Valid methods: {sorted(valid_methods)}"
                )


def validate_async_executor_input(
    param_set_mdx_list: Optional[list] = None,
    max_workers: Optional[int] = None,
    data_copy_function: Optional[Callable] = None,
) -> None:
    """Validate async_executor inputs before execution."""
    if param_set_mdx_list is None or len(param_set_mdx_list) == 0:
        raise ValueError(
            "async_executor: param_set_mdx_list must be a non-empty list of MDX set expressions"
        )
    if not isinstance(param_set_mdx_list, list):
        raise ValueError(
            f"async_executor: param_set_mdx_list must be a list, "
            f"got {type(param_set_mdx_list).__name__}"
        )
    if max_workers is not None and (not isinstance(max_workers, int) or max_workers < 1):
        raise ValueError(
            f"async_executor: max_workers must be a positive integer, got {max_workers}"
        )
    if data_copy_function is not None and not callable(data_copy_function):
        raise ValueError(
            f"async_executor: data_copy_function must be callable, "
            f"got {type(data_copy_function).__name__}"
        )
