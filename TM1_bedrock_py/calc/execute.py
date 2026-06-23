from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from .model import (
    BinaryExpression,
    CaseExpression,
    ComparisonExpression,
    Expression,
    LiteralExpression,
    MeasureExpression,
    MethodExpression,
    UnaryExpression,
)

_VALUE_OPERATORS = {
    "+": lambda left, right: left + right,
    "-": lambda left, right: left - right,
    "*": lambda left, right: left * right,
    "/": lambda left, right: left / right,
    "==": lambda left, right: left == right,
    "!=": lambda left, right: left != right,
    ">": lambda left, right: left > right,
    ">=": lambda left, right: left >= right,
    "<": lambda left, right: left < right,
    "<=": lambda left, right: left <= right,
}


class PythonExecutionError(Exception):
    pass


@dataclass(frozen=True)
class PythonExecutionRejection:
    target: str
    reason: str


@dataclass
class PythonExecutionReport:
    executed: List[str] = field(default_factory=list)
    rejected: List[PythonExecutionRejection] = field(default_factory=list)
    written_rows: Dict[str, int] = field(default_factory=dict)


def _is_plain_value_expression(expression: Expression) -> bool:
    if isinstance(expression, (LiteralExpression, MeasureExpression)):
        return True
    if isinstance(expression, UnaryExpression):
        return _is_plain_value_expression(expression.operand)
    if isinstance(expression, (BinaryExpression, ComparisonExpression)):
        return _is_plain_value_expression(expression.left) and _is_plain_value_expression(expression.right)
    return False


def unsupported_python_execution_reason(expression: Expression, target_cube_name: str) -> Optional[str]:
    for measure_ref in expression.iter_measure_refs():
        if measure_ref.cube_name != target_cube_name:
            return (
                f"cross-cube measure reference '{measure_ref.label}' is not yet supported by the "
                "Python execution MVP (same-cube only)"
            )

    if isinstance(expression, MethodExpression):
        if expression.method_name == "where":
            if _is_plain_value_expression(expression.base) and _is_plain_value_expression(expression.args[0]):
                return None
            return "where(...) is only supported when its value and condition are plain arithmetic/comparison expressions"
        if expression.method_name == "rolling.sum":
            if len(expression.kwargs) != 1:
                return "rolling(...).sum() requires exactly one dimension=window keyword argument"
            (_dimension_name, window_expr) = next(iter(expression.kwargs.items()))
            if not isinstance(window_expr, LiteralExpression) or not isinstance(window_expr.value, int):
                return "rolling(...).sum() window must be an integer literal"
            if not _is_plain_value_expression(expression.base):
                return "rolling(...).sum() base must be a plain same-cube measure expression"
            return None
        if expression.method_name == "ytd":
            return (
                "ytd() cannot be executed by any backend in its current form: the DSL's ytd() takes no "
                "parameters, so there is no way to know which dimension represents time. This is a DSL-level "
                "gap, not an executor limitation -- see dev/12_current_gaps.md."
            )
        if expression.method_name in ("align", "shift", "growth"):
            return (
                f"{expression.method_name}(...) inside a Python-backend formula is not supported by this "
                "execution MVP; these are native-only constructs and should be fixed at the native-compiler "
                "or metadata level instead of executed in Python"
            )
        return f"method '{expression.method_name}' is not supported by the Python execution MVP"

    if isinstance(expression, CaseExpression):
        if all(
            _is_plain_value_expression(condition) and _is_plain_value_expression(value)
            for condition, value in expression.cases
        ) and _is_plain_value_expression(expression.default):
            return None
        return "case(...) is only supported when every condition/value/default is a plain arithmetic/comparison expression"

    if _is_plain_value_expression(expression):
        return None

    return f"expression shape '{type(expression).__name__}' is not supported by the Python execution MVP"


def _evaluate_value(expression: Expression, frame: pd.DataFrame, cube_name: str) -> pd.Series:
    if isinstance(expression, LiteralExpression):
        return pd.Series(expression.value, index=frame.index)

    if isinstance(expression, MeasureExpression):
        if expression.ref.cube_name != cube_name:
            raise PythonExecutionError(f"cross-cube reference '{expression.ref.label}' cannot be evaluated here")
        if expression.ref.measure_name not in frame.columns:
            raise PythonExecutionError(f"measure '{expression.ref.measure_name}' is not present in the input frame")
        return pd.to_numeric(frame[expression.ref.measure_name], errors="coerce").fillna(0.0)

    if isinstance(expression, UnaryExpression):
        operand = _evaluate_value(expression.operand, frame, cube_name)
        if expression.operator == "-":
            return -operand
        raise PythonExecutionError(f"unsupported unary operator '{expression.operator}'")

    if isinstance(expression, (BinaryExpression, ComparisonExpression)):
        left = _evaluate_value(expression.left, frame, cube_name)
        right = _evaluate_value(expression.right, frame, cube_name)
        operator = _VALUE_OPERATORS.get(expression.operator)
        if operator is None:
            raise PythonExecutionError(f"unsupported operator '{expression.operator}'")
        return operator(left, right)

    raise PythonExecutionError(f"expression shape '{type(expression).__name__}' cannot be evaluated as a value")


def evaluate_formula(
    expression: Expression,
    frame: pd.DataFrame,
    cube_name: str,
    metadata: Any = None,
    dimension_columns: Optional[List[str]] = None,
) -> Tuple[pd.Series, Optional[pd.Series]]:
    if isinstance(expression, MethodExpression) and expression.method_name == "where":
        value = _evaluate_value(expression.base, frame, cube_name)
        mask = _evaluate_value(expression.args[0], frame, cube_name).astype(bool)
        return value, mask

    if isinstance(expression, MethodExpression) and expression.method_name == "rolling.sum":
        return _evaluate_rolling_sum(expression, frame, cube_name, metadata, dimension_columns or []), None

    if isinstance(expression, CaseExpression):
        condition_list = [
            _evaluate_value(condition, frame, cube_name).to_numpy()
            for condition, _value in expression.cases
        ]
        choice_list = [_evaluate_value(value, frame, cube_name).to_numpy() for _condition, value in expression.cases]
        default = _evaluate_value(expression.default, frame, cube_name).to_numpy()
        result = np.select(condition_list, choice_list, default=default)
        return pd.Series(result, index=frame.index), None

    return _evaluate_value(expression, frame, cube_name), None


def _evaluate_rolling_sum(
    expression: MethodExpression,
    frame: pd.DataFrame,
    cube_name: str,
    metadata: Any,
    dimension_columns: List[str],
) -> pd.Series:
    if metadata is None:
        raise PythonExecutionError("rolling(...).sum() requires cube metadata to resolve the dimension's leaf order")

    dimension_name, window_expr = next(iter(expression.kwargs.items()))
    window = int(window_expr.value)
    leaf_order = tuple(metadata.dimension_leaf_elements.get(dimension_name, ()))
    if not leaf_order:
        raise PythonExecutionError(
            f"rolling(...).sum() requires known leaf elements for dimension '{dimension_name}'"
        )
    if dimension_name not in frame.columns:
        raise PythonExecutionError(f"dimension '{dimension_name}' is not present in the input frame")

    order_index = {element_name: position for position, element_name in enumerate(leaf_order)}
    base_values = _evaluate_value(expression.base, frame, cube_name)

    working = frame.copy()
    working["_rolling_value"] = base_values.values
    working["_rolling_order"] = working[dimension_name].map(order_index)

    grouping_columns = [column for column in dimension_columns if column != dimension_name]

    result = pd.Series(index=frame.index, dtype="float64")
    if grouping_columns:
        groups = working.groupby(grouping_columns, dropna=False, sort=False)
    else:
        groups = [(None, working)]
    for _key, group in groups:
        ordered = group.sort_values("_rolling_order")
        rolled = ordered["_rolling_value"].rolling(window=window, min_periods=1).sum()
        result.loc[ordered.index] = rolled.values
    return result


def pivot_long_to_wide(long_dataframe: pd.DataFrame, measure_dimension_name: str) -> Tuple[pd.DataFrame, List[str]]:
    other_dimensions = [
        column for column in long_dataframe.columns if column not in (measure_dimension_name, "Value")
    ]
    wide = long_dataframe.pivot_table(
        index=other_dimensions,
        columns=measure_dimension_name,
        values="Value",
        aggfunc="first",
    ).reset_index()
    wide.columns = [str(column) for column in wide.columns]
    return wide, other_dimensions


def melt_wide_to_long(
    wide_dataframe: pd.DataFrame,
    other_dimensions: List[str],
    measure_dimension_name: str,
    measure_name: str,
    value_series: pd.Series,
    mask: Optional[pd.Series],
) -> pd.DataFrame:
    long_dataframe = wide_dataframe[other_dimensions].copy()
    long_dataframe[measure_dimension_name] = measure_name
    long_dataframe["Value"] = value_series.values
    if mask is not None:
        long_dataframe = long_dataframe.loc[mask.values]
    return long_dataframe
