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


class SumIfStep(BaseCalculationStep):
    method: Literal["sumif"]
    column_to_sum: str
    group_column_list: list[str]


class CountStep(BaseCalculationStep):
    method: Literal["count"]


class CountIfStep(BaseCalculationStep):
    method: Literal["countif"]
    group_column_list: list[str]


class RankStep(BaseCalculationStep):
    method: Union[Literal["rank_over"], Literal["indexif"]]
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
    dimension_map: Dict[str, str] | None = None
    ignore_in_join: List[str] | None = None
    omit_where_from_df: bool = False
    value_type: Type | None = None


class IfStep(BaseCalculationStep):
    method: Union[Literal["if"], Literal["condition"]]
    if_then: Dict[str, Any]
    fallback: Any | None = None


class FormulaStep(BaseCalculationStep):
    method: Literal["formula"]
    formula: str


class CustomStep(BaseCalculationStep):
    method: Literal["custom"]
    custom_callable: Callable
    custom_args: list[Any] = None
    custom_kwargs: dict[str, Any] = None


CalculationStep = Union[
    ConstantStep,
    SumStep,
    SumIfStep,
    CountStep,
    CountIfStep,
    IndexStep,
    RankStep,
    CubeDataStep,
    IfStep,
    FormulaStep,
    CustomStep
]

pipeline_adapter: TypeAdapter = TypeAdapter(List[CalculationStep])


def validate_calculation_pipeline_configuration(raw_configuration_steps: List[Dict[str, Any]], logger: Any) -> None:
    try:
        pipeline_adapter.validate_python(raw_configuration_steps)
    except ValidationError as validation_error:
        logger.error(validation_error.json(indent=4))
        raise ValueError("The calculation_steps configuration is invalid. Execution aborted.")
