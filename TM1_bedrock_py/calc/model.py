from __future__ import annotations

import difflib
import json
import os
import re
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from itertools import product
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

from TM1_bedrock_py import basic_logger
from TM1_bedrock_py import extractor, utility
import pandas as pd
from .metadata import MetadataProvider, TM1ServiceMetadataProvider


def _ensure_expression(value: Any) -> "Expression":
    if isinstance(value, Expression):
        return value
    return LiteralExpression(value=value)


@dataclass(frozen=True)
class MeasureRef:
    cube_name: str
    measure_name: str
    # Fixed dimension-element overrides narrowing this target to a sub-area of the measure's
    # cell space (e.g. (("Region", "US"),)), mirroring TM1's own rule-LHS area syntax
    # (['MeasureElement', 'OtherElement', ...]). Empty by default: the whole-measure target,
    # unchanged from this project's original single-area-per-measure behavior.
    area: Tuple[Tuple[str, str], ...] = ()

    @property
    def base_key(self) -> str:
        """The plain 'cube:measure' identity, ignoring any area -- this is what a measure
        *reference* inside another formula's expression always resolves to, since references
        never carry an area of their own; see Model._build_plan's base_measure_keys grouping."""
        return f"{self.cube_name}:{self.measure_name}"

    @property
    def key(self) -> str:
        if not self.area:
            return self.base_key
        area_suffix = ",".join(f"{dimension_name}={element_name}" for dimension_name, element_name in self.area)
        return f"{self.base_key}|{area_suffix}"

    @property
    def label(self) -> str:
        if not self.area:
            return f"{self.cube_name}.{self.measure_name}"
        area_suffix = ", ".join(f"{dimension_name}={element_name}" for dimension_name, element_name in self.area)
        return f"{self.cube_name}.{self.measure_name}[{area_suffix}]"


@dataclass(frozen=True)
class AttributeRef:
    cube_name: str
    dimension_name: str
    attribute_name: str

    @property
    def label(self) -> str:
        return f"{self.cube_name}.{self.dimension_name}.attribute({self.attribute_name})"


@dataclass(frozen=True)
class ChainedAttributeRef:
    cube_name: str
    first_dimension_name: str
    first_attribute_name: str
    second_dimension_name: str
    second_attribute_name: str

    @property
    def label(self) -> str:
        return (
            f"{self.cube_name}.{self.first_dimension_name}.attribute_chain("
            f"{self.first_attribute_name}, via_dimension={self.second_dimension_name}, "
            f"attribute={self.second_attribute_name})"
        )


@dataclass(frozen=True)
class ElementRef:
    cube_name: str
    dimension_name: str
    element_name: str
    hierarchy_name: Optional[str] = None

    @property
    def label(self) -> str:
        hierarchy_suffix = (
            f", hierarchy={self.hierarchy_name}"
            if self.hierarchy_name is not None
            else ""
        )
        return (
            f"{self.cube_name}.{self.dimension_name}.element("
            f"{self.element_name}{hierarchy_suffix})"
        )


class Expression:
    def __add__(self, other: Any) -> "Expression":
        return BinaryExpression("+", self, _ensure_expression(other))

    def __radd__(self, other: Any) -> "Expression":
        return BinaryExpression("+", _ensure_expression(other), self)

    def __sub__(self, other: Any) -> "Expression":
        return BinaryExpression("-", self, _ensure_expression(other))

    def __rsub__(self, other: Any) -> "Expression":
        return BinaryExpression("-", _ensure_expression(other), self)

    def __mul__(self, other: Any) -> "Expression":
        return BinaryExpression("*", self, _ensure_expression(other))

    def __rmul__(self, other: Any) -> "Expression":
        return BinaryExpression("*", _ensure_expression(other), self)

    def __truediv__(self, other: Any) -> "Expression":
        return BinaryExpression("/", self, _ensure_expression(other))

    def __rtruediv__(self, other: Any) -> "Expression":
        return BinaryExpression("/", _ensure_expression(other), self)

    def __neg__(self) -> "Expression":
        return UnaryExpression("-", self)

    def __eq__(self, other: Any) -> "Expression":  # type: ignore[override]
        return ComparisonExpression("==", self, _ensure_expression(other))

    def __ne__(self, other: Any) -> "Expression":  # type: ignore[override]
        return ComparisonExpression("!=", self, _ensure_expression(other))

    def __gt__(self, other: Any) -> "Expression":
        return ComparisonExpression(">", self, _ensure_expression(other))

    def __ge__(self, other: Any) -> "Expression":
        return ComparisonExpression(">=", self, _ensure_expression(other))

    def __lt__(self, other: Any) -> "Expression":
        return ComparisonExpression("<", self, _ensure_expression(other))

    def __le__(self, other: Any) -> "Expression":
        return ComparisonExpression("<=", self, _ensure_expression(other))

    def __bool__(self) -> bool:
        raise TypeError("Symbolic TM1 expressions cannot be used as Python booleans")

    def where(self, condition: Any) -> "Expression":
        return MethodExpression("where", self, args=(_ensure_expression(condition),))

    def align(self, **mappings: Any) -> "Expression":
        normalized = {key: _ensure_expression(value) for key, value in mappings.items()}
        return MethodExpression("align", self, kwargs=normalized)

    def shift(self, **offsets: Any) -> "Expression":
        return MethodExpression("shift", self, kwargs={key: _ensure_expression(value) for key, value in offsets.items()})

    def _consolidated_aggregate(self, method_name: str, flag: int, **dimension_overrides: Any) -> "Expression":
        kwargs = {key: _ensure_expression(value) for key, value in dimension_overrides.items()}
        kwargs["__flag__"] = LiteralExpression(flag)
        return MethodExpression(method_name, self, kwargs=kwargs)

    def consolidated_max(self, flag: int = 0, **dimension_overrides: Any) -> "Expression":
        """Lowers to TM1's native `ConsolidatedMax(...)` -- a non-additive consolidation
        override that returns the maximum leaf value beneath the current (or overridden)
        coordinate of this same-cube measure, instead of the default sum. `flag` follows TM1's
        own convention: 0 = plain, 1 = consolidation-weighted, 2 = ignore zero values,
        3 = weighted and ignore zero values."""
        return self._consolidated_aggregate("consolidated_max", flag, **dimension_overrides)

    def consolidated_min(self, flag: int = 0, **dimension_overrides: Any) -> "Expression":
        """Lowers to TM1's native `ConsolidatedMin(...)`. See `consolidated_max` for `flag`."""
        return self._consolidated_aggregate("consolidated_min", flag, **dimension_overrides)

    def consolidated_avg(self, flag: int = 0, **dimension_overrides: Any) -> "Expression":
        """Lowers to TM1's native `ConsolidatedAvg(...)`. See `consolidated_max` for `flag`."""
        return self._consolidated_aggregate("consolidated_avg", flag, **dimension_overrides)

    def consolidated_count(self, flag: int = 0, **dimension_overrides: Any) -> "Expression":
        """Lowers to TM1's native `ConsolidatedCount(...)`. See `consolidated_max` for `flag`."""
        return self._consolidated_aggregate("consolidated_count", flag, **dimension_overrides)

    def consolidated_count_unique(self, flag: int = 0, **dimension_overrides: Any) -> "Expression":
        """Lowers to TM1's native `ConsolidatedCountUnique(...)`. See `consolidated_max` for `flag`."""
        return self._consolidated_aggregate("consolidated_count_unique", flag, **dimension_overrides)

    def ytd(self, dimension: str, period_number_attribute: str) -> "Expression":
        return MethodExpression(
            "ytd",
            self,
            kwargs={
                "dimension": LiteralExpression(dimension),
                "period_number_attribute": LiteralExpression(period_number_attribute),
            },
        )

    def growth(self, **kwargs: Any) -> "Expression":
        return MethodExpression("growth", self, kwargs={key: _ensure_expression(value) for key, value in kwargs.items()})

    def rolling(self, **kwargs: Any) -> "RollingExpression":
        return RollingExpression(self, kwargs={key: _ensure_expression(value) for key, value in kwargs.items()})

    def native(self, scope: str = "leaf", feeder_mode: str = "ranked_single") -> "ScopedExpression":
        return ScopedExpression(inner=self, scope=scope, feeder_mode=feeder_mode)

    def abs(self) -> "Expression":
        return MethodExpression("abs", self)

    def round(self, ndigits: Any = 0) -> "Expression":
        return MethodExpression("round", self, args=(_ensure_expression(ndigits),))

    def mod(self, divisor: Any) -> "Expression":
        return MethodExpression("mod", self, args=(_ensure_expression(divisor),))

    def int_part(self) -> "Expression":
        return MethodExpression("int_part", self)

    def exp(self) -> "Expression":
        """Lowers to TM1's native `EXP(x)` -- the natural anti-log (e^x) of this value."""
        return MethodExpression("exp", self)

    def ln(self) -> "Expression":
        """Lowers to TM1's native `LN(x)` -- the natural base-e logarithm. TM1 requires a
        positive input; this DSL does not validate that at compile time, matching the existing
        posture for `mod`/`int_part`/etc., which also trust the caller's domain."""
        return MethodExpression("ln", self)

    def log10(self) -> "Expression":
        """Lowers to TM1's native `LOG(x)` -- the base-10 logarithm of a positive number."""
        return MethodExpression("log10", self)

    def sqrt(self) -> "Expression":
        """Lowers to TM1's native `SQRT(x)` -- the square root."""
        return MethodExpression("sqrt", self)

    def sin(self) -> "Expression":
        """Lowers to TM1's native `SIN(x)` -- sine of an angle expressed in radians."""
        return MethodExpression("sin", self)

    def cos(self) -> "Expression":
        """Lowers to TM1's native `COS(x)` -- cosine of an angle expressed in radians."""
        return MethodExpression("cos", self)

    def tan(self) -> "Expression":
        """Lowers to TM1's native `TAN(x)` -- tangent of an angle expressed in radians."""
        return MethodExpression("tan", self)

    def asin(self) -> "Expression":
        """Lowers to TM1's native `ASIN(x)` -- the angle, in radians, whose sine is x."""
        return MethodExpression("asin", self)

    def acos(self) -> "Expression":
        """Lowers to TM1's native `ACOS(x)` -- the angle, in radians, whose cosine is x."""
        return MethodExpression("acos", self)

    def atan(self) -> "Expression":
        """Lowers to TM1's native `ATAN(x)` -- the angle, in radians, whose tangent is x."""
        return MethodExpression("atan", self)

    def insert(self, text: Any, position: Any) -> "Expression":
        """Lowers to TM1's native `INSRT(String1, String2, Location)`, where `String1` is the
        string being inserted (`text`), `String2` is the string it's inserted into (`self`), and
        `Location` is the 1-based insertion position. Confirmed against a real worked example
        (a REPLACE-style rule combining SCAN/DELET/LONG/INSRT) since the function-library pages
        alone never stated which string is inserted into which -- see
        dev/07_handover_status.md's "INSRT" entry."""
        return MethodExpression(
            "insert", self, args=(_ensure_expression(text), _ensure_expression(position))
        )

    def upper(self) -> "Expression":
        return MethodExpression("upper", self)

    def lower(self) -> "Expression":
        return MethodExpression("lower", self)

    def trim(self) -> "Expression":
        return MethodExpression("trim", self)

    def substring(self, start: Any, length: Any) -> "Expression":
        return MethodExpression("substring", self, args=(_ensure_expression(start), _ensure_expression(length)))

    def delete(self, start: Any, length: Any) -> "Expression":
        """Lowers to TM1's native `DELET(string, start, number)` -- deletes `length` characters
        starting at position `start` (1-based) from the string."""
        return MethodExpression("delete", self, args=(_ensure_expression(start), _ensure_expression(length)))

    def scan(self, substring: Any) -> "Expression":
        """Lowers to TM1's native `SCAN(substring, string)` -- returns the 1-based position of
        the first occurrence of `substring` within this string, or 0 if not found."""
        return MethodExpression("scan", self, args=(_ensure_expression(substring),))

    def length(self) -> "Expression":
        """Lowers to TM1's native `LONG(string)` -- returns the character length of the string."""
        return MethodExpression("length", self)

    def char_code(self, position: Any = 1) -> "Expression":
        """Lowers to TM1's native `CODE(string, position)` -- returns the ASCII code of the
        character at `position` (1-based) in the string."""
        return MethodExpression("char_code", self, args=(_ensure_expression(position),))

    def to_char(self) -> "Expression":
        """Lowers to TM1's native `CHAR(number)` -- the inverse of `char_code`, returning the
        single character associated with this numeric ASCII code."""
        return MethodExpression("to_char", self)

    def to_string(self, length: Any, decimals: Any = 0) -> "Expression":
        """Numeric-to-string conversion, lowering to TM1's native `STR(value, length, decimals)`.

        Bounded MVP: only allowed when applied directly to a same-cube measure reference (see
        `Model._measure_type_conversion_overrides`), mirroring this project's narrow-first
        posture for every other widening. This is what lets an `S:` formula reference a Numeric
        measure, or an `N:` formula reference a String measure, without the type-checking gate
        rejecting the cross-type reference outright.
        """
        return MethodExpression(
            "to_string", self, args=(_ensure_expression(length), _ensure_expression(decimals))
        )

    def to_number(self) -> "Expression":
        """String-to-number conversion, lowering to TM1's native `NUMBR(value)`.

        Same bounded-MVP posture as `to_string`: only allowed when applied directly to a
        same-cube measure reference.
        """
        return MethodExpression("to_number", self)

    def concat(self, other: Any) -> "Expression":
        """String concatenation, lowering to TM1's native `|` operator.

        Deliberately a separate method rather than overloading `+`/`__add__`: `+` already means
        numeric addition for this DSL's `BinaryExpression`, and inferring "string concat" from
        runtime operand types would reintroduce the same "just a flag" inference problem this
        project's scope design explicitly rejected (see dev/07_handover_status.md, "Design
        Decision Reaffirmed: Why S:/C: Aren't 'Just A Flag'").
        """
        return BinaryExpression("|", self, _ensure_expression(other))

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        return iter(())

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        return iter(())

    def iter_element_refs(self) -> Iterator[ElementRef]:
        return iter(())

    def describe(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True, eq=False)
class LiteralExpression(Expression):
    value: Any

    def describe(self) -> str:
        return repr(self.value)


@dataclass(frozen=True, eq=False)
class MeasureExpression(Expression):
    ref: MeasureRef

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        yield self.ref

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True, eq=False)
class AttributeExpression(Expression):
    ref: AttributeRef

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        yield self.ref

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True, eq=False)
class ChainedAttributeExpression(Expression):
    ref: ChainedAttributeRef

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True, eq=False)
class ElementExpression(Expression):
    ref: ElementRef

    def iter_element_refs(self) -> Iterator[ElementRef]:
        yield self.ref

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True)
class DimensionFunctionRef:
    cube_name: str
    dimension_name: str
    function_name: str
    element_name: Optional[str] = None
    args: Tuple[Any, ...] = ()

    @property
    def label(self) -> str:
        element_part = self.element_name if self.element_name is not None else f"!{self.dimension_name}"
        extra = f", {', '.join(repr(arg) for arg in self.args)}" if self.args else ""
        return f"{self.cube_name}.{self.dimension_name}.{self.function_name}({element_part}{extra})"


@dataclass(frozen=True, eq=False)
class DimensionFunctionExpression(Expression):
    ref: DimensionFunctionRef

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True, eq=False)
class UnaryExpression(Expression):
    operator: str
    operand: Expression

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        yield from self.operand.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        yield from self.operand.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        yield from self.operand.iter_element_refs()

    def describe(self) -> str:
        return f"({self.operator}{self.operand.describe()})"


@dataclass(frozen=True, eq=False)
class BinaryExpression(Expression):
    operator: str
    left: Expression
    right: Expression

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        yield from self.left.iter_measure_refs()
        yield from self.right.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        yield from self.left.iter_attribute_refs()
        yield from self.right.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        yield from self.left.iter_element_refs()
        yield from self.right.iter_element_refs()

    def describe(self) -> str:
        return f"({self.left.describe()} {self.operator} {self.right.describe()})"


@dataclass(frozen=True, eq=False)
class ComparisonExpression(BinaryExpression):
    pass


@dataclass(frozen=True, eq=False)
class MethodExpression(Expression):
    method_name: str
    base: Expression
    args: Tuple[Expression, ...] = ()
    kwargs: Mapping[str, Expression] = field(default_factory=dict)

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        yield from self.base.iter_measure_refs()
        for arg in self.args:
            yield from arg.iter_measure_refs()
        for value in self.kwargs.values():
            yield from value.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        yield from self.base.iter_attribute_refs()
        for arg in self.args:
            yield from arg.iter_attribute_refs()
        for value in self.kwargs.values():
            yield from value.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        yield from self.base.iter_element_refs()
        for arg in self.args:
            yield from arg.iter_element_refs()
        for value in self.kwargs.values():
            yield from value.iter_element_refs()

    def describe(self) -> str:
        parts = [arg.describe() for arg in self.args]
        parts.extend(f"{key}={value.describe()}" for key, value in self.kwargs.items())
        inner = ", ".join(parts)
        return f"{self.base.describe()}.{self.method_name}({inner})"


@dataclass(frozen=True, eq=False)
class RollingExpression(Expression):
    base: Expression
    kwargs: Mapping[str, Expression] = field(default_factory=dict)

    def sum(self) -> Expression:
        return MethodExpression("rolling.sum", self.base, kwargs=self.kwargs)

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        yield from self.base.iter_measure_refs()
        for value in self.kwargs.values():
            yield from value.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        yield from self.base.iter_attribute_refs()
        for value in self.kwargs.values():
            yield from value.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        yield from self.base.iter_element_refs()
        for value in self.kwargs.values():
            yield from value.iter_element_refs()

    def describe(self) -> str:
        inner = ", ".join(f"{key}={value.describe()}" for key, value in self.kwargs.items())
        return f"{self.base.describe()}.rolling({inner})"


@dataclass(frozen=True, eq=False)
class FunctionCallExpression(Expression):
    """A free-standing native function call not bound to any particular measure/dimension
    receiver -- e.g. TM1's date functions (`DATE`/`DATES`/`DAY`/`DAYNO`), which take plain
    value arguments rather than operating on a `.base` expression the way `.abs()`/`.upper()`
    etc. do. Built via `Model.date(...)`/`Model.dates(...)`/`Model.day(...)`/`Model.dayno(...)`."""

    function_name: str
    args: Tuple[Expression, ...]

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        for arg in self.args:
            yield from arg.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        for arg in self.args:
            yield from arg.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        for arg in self.args:
            yield from arg.iter_element_refs()

    def describe(self) -> str:
        inner = ", ".join(arg.describe() for arg in self.args)
        return f"{self.function_name}({inner})"


@dataclass(frozen=True, eq=False)
class CaseExpression(Expression):
    cases: Tuple[Tuple[Expression, Expression], ...]
    default: Expression

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        for condition, value in self.cases:
            yield from condition.iter_measure_refs()
            yield from value.iter_measure_refs()
        yield from self.default.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        for condition, value in self.cases:
            yield from condition.iter_attribute_refs()
            yield from value.iter_attribute_refs()
        yield from self.default.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        for condition, value in self.cases:
            yield from condition.iter_element_refs()
            yield from value.iter_element_refs()
        yield from self.default.iter_element_refs()

    def describe(self) -> str:
        parts = [
            f"({condition.describe()} -> {value.describe()})"
            for condition, value in self.cases
        ]
        parts.append(f"default={self.default.describe()}")
        return f"case({', '.join(parts)})"


@dataclass(frozen=True, eq=False)
class ScopedExpression(Expression):
    """Marks an explicit rule-area hint (e.g. ``scope="consolidated"``) before formula registration.

    This wrapper is unwrapped by ``Model.register_formula`` and never appears inside a
    stored ``Formula.expression`` tree, so the rest of the compiler stays scope-agnostic.
    """

    inner: Expression
    scope: str
    feeder_mode: str = "ranked_single"

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        return self.inner.iter_measure_refs()

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        return self.inner.iter_attribute_refs()

    def iter_element_refs(self) -> Iterator[ElementRef]:
        return self.inner.iter_element_refs()

    def describe(self) -> str:
        if self.feeder_mode == "ranked_single":
            return f"{self.inner.describe()}.native(scope={self.scope!r})"
        return f"{self.inner.describe()}.native(scope={self.scope!r}, feeder_mode={self.feeder_mode!r})"


@dataclass(frozen=True)
class Formula:
    target: MeasureRef
    expression: Expression
    scope: str = "leaf"
    feeder_mode: str = "ranked_single"
    # Resolves emission order between two area-scoped formulas on the same measure whose areas
    # overlap (e.g. a blanket default plus a narrower exception): lower priority is emitted
    # earlier in the cube's rule text, matching TM1's documented "first matching rule statement
    # in file order wins" behavior for overlapping areas. Default 0; irrelevant for formulas
    # whose area never overlaps another formula's area on the same measure.
    priority: int = 0

    @property
    def key(self) -> str:
        return self.target.key


@dataclass
class ValidationReport:
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    execution_order: List[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


@dataclass
class BuildPlan:
    formulas: Dict[str, Formula]
    graph: Dict[str, List[str]]
    dependencies: Dict[str, List[str]]
    order: List[str]
    report: ValidationReport


class Backend(str, Enum):
    NATIVE = "native-rule backend"
    PYTHON = "Python materialization backend"


@dataclass(frozen=True)
class BackendDecision:
    backend: Backend
    rationale: str


@dataclass
class CompilePreview:
    rules: Dict[str, str]
    feeders: Dict[str, str]
    manifest: Dict[str, Dict[str, Any]]
    deployment: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    deployment_manifest_path: Optional[str] = None


@dataclass(frozen=True)
class MetadataRequirement:
    code: str
    description: str
    cube_name: str
    detail: str
    satisfied: bool


@dataclass
class Phase2ReadinessReport:
    formulas: Dict[str, Dict[str, Any]]
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)


class DeploymentError(RuntimeError):
    pass


@dataclass(frozen=True)
class FeederPlan:
    statements: tuple[str, ...]
    strategy: str
    rationale: str
    deployment_cube: Optional[str] = None
    deployment_statements: tuple[tuple[str, tuple[str, ...]], ...] = ()
    preview_only: bool = False
    origin_candidates: tuple["FeederOriginCandidate", ...] = ()
    selected_origin_key: Optional[str] = None

    def iter_deployment_statements(self, default_cube: Optional[str] = None) -> Iterator[tuple[str, tuple[str, ...]]]:
        if self.deployment_statements:
            yield from self.deployment_statements
            return
        cube_name = self.deployment_cube or default_cube
        if cube_name is None or not self.statements:
            return
        yield cube_name, self.statements

    def deployment_cubes(self, default_cube: Optional[str] = None) -> tuple[str, ...]:
        return tuple(
            cube_name
            for cube_name, _statements in self.iter_deployment_statements(default_cube=default_cube)
        )


@dataclass(frozen=True)
class FeederOriginCandidate:
    cube_name: str
    measure_names: tuple[str, ...]
    fixed_coordinates: tuple[tuple[str, tuple[str, ...]], ...]
    dimension_count: int
    fixed_coordinate_count: int
    same_cube: bool
    description: str
    candidate_kind: str = "same_cube"
    statements: tuple[str, ...] = ()
    deployment_cube: Optional[str] = None
    deployment_statements: tuple[tuple[str, tuple[str, ...]], ...] = ()

    @property
    def key(self) -> str:
        fixed_part = ",".join(
            f"{dimension_name}={'|'.join(str(element_name) for element_name in element_names)}"
            for dimension_name, element_names in self.fixed_coordinates
        )
        measures_part = ",".join(self.measure_names)
        return f"{self.cube_name}::{measures_part}::{fixed_part}"


class DimensionProxy:
    def __init__(self, cube_name: str, dimension_name: str):
        self._cube_name = cube_name
        self._dimension_name = dimension_name

    def attribute(self, attribute_name: str) -> AttributeExpression:
        return AttributeExpression(
            ref=AttributeRef(
                cube_name=self._cube_name,
                dimension_name=self._dimension_name,
                attribute_name=attribute_name,
            )
        )

    def attribute_chain(self, first_attribute: str, via_dimension: str, attribute: str) -> ChainedAttributeExpression:
        return ChainedAttributeExpression(
            ref=ChainedAttributeRef(
                cube_name=self._cube_name,
                first_dimension_name=self._dimension_name,
                first_attribute_name=first_attribute,
                second_dimension_name=via_dimension,
                second_attribute_name=attribute,
            )
        )

    def element(self, element_name: str, hierarchy: Optional[str] = None) -> ElementExpression:
        return ElementExpression(
            ref=ElementRef(
                cube_name=self._cube_name,
                dimension_name=self._dimension_name,
                element_name=element_name,
                hierarchy_name=hierarchy,
            )
        )

    def _dimension_function(
        self, function_name: str, element: Optional[str], args: Tuple[Any, ...] = ()
    ) -> DimensionFunctionExpression:
        return DimensionFunctionExpression(
            ref=DimensionFunctionRef(
                cube_name=self._cube_name,
                dimension_name=self._dimension_name,
                function_name=function_name,
                element_name=element,
                args=args,
            )
        )

    def level(self, element: Optional[str] = None) -> DimensionFunctionExpression:
        return self._dimension_function("ellev", element)

    def parent_count(self, element: Optional[str] = None) -> DimensionFunctionExpression:
        return self._dimension_function("elparn", element)

    def child_count(self, element: Optional[str] = None) -> DimensionFunctionExpression:
        return self._dimension_function("elcompn", element)

    def child(self, position: int, element: Optional[str] = None) -> DimensionFunctionExpression:
        return self._dimension_function("elcomp", element, args=(position,))

    def is_component_of(self, parent_element: str, element: Optional[str] = None) -> DimensionFunctionExpression:
        return self._dimension_function("eliscomp", element, args=(parent_element,))

    def parent(self, index: int = 1, element: Optional[str] = None) -> DimensionFunctionExpression:
        """Lowers to TM1's native `ELPAR(Dimension, Element, Index)` -- returns the name of the
        `index`-th (1-based) direct parent of `element` (defaults to the current element)."""
        return self._dimension_function("elpar", element, args=(index,))

    def is_parent_of(self, child_element: str, element: Optional[str] = None) -> DimensionFunctionExpression:
        """Lowers to TM1's native `ELISPAR(Dimension, Parent, Child)` -- true if `element`
        (defaults to the current element) is the *direct* parent of `child_element`."""
        return self._dimension_function("elispar", element, args=(child_element,))

    def is_ancestor_of(self, descendant_element: str, element: Optional[str] = None) -> DimensionFunctionExpression:
        """Lowers to TM1's native `ELISANC(Dimension, Ancestor, Descendant)` -- true if
        `element` (defaults to the current element) is an ancestor, at any level, of
        `descendant_element`."""
        return self._dimension_function("elisanc", element, args=(descendant_element,))

    def weight(self, parent_element: str, element: Optional[str] = None) -> DimensionFunctionExpression:
        """Lowers to TM1's native `ELWEIGHT(Dimension, Parent, Child)` -- returns the
        consolidation weight of `element` (defaults to the current element, the "child" role)
        within `parent_element`."""
        return self._dimension_function("elweight", element, args=(parent_element,))


class CubeRef:
    def __init__(self, model: "Model", cube_name: str):
        self._model = model
        self._cube_name = cube_name

    def __getitem__(self, measure_name: str) -> MeasureExpression:
        return MeasureExpression(ref=MeasureRef(cube_name=self._cube_name, measure_name=measure_name))

    def __setitem__(self, measure_name: str, expression: Any) -> None:
        self._model.register_formula(self[measure_name], _ensure_expression(expression))

    def area(self, measure_name: str, **fixed_elements: str) -> "AreaTarget":
        """Targets a sub-area of `measure_name`'s cell space, fixed to literal elements on the
        given dimensions (every other dimension stays a passthrough wildcard, exactly like a
        whole-measure target). Use this -- instead of `cube[measure_name] = ...` -- to author
        TM1's "blanket rule plus narrower exception" pattern as separate rule statements, e.g.:

            sales.area("Revenue", Region="US").set(special_expr, priority=-1)
            sales["Revenue"] = blanket_expr

        Both target the same measure; their areas overlap (the "US" area is a subset of the
        unconstrained blanket area), so an explicit, distinct `priority` is required -- the
        model fails closed at compile time otherwise. Lower priority is emitted earlier in the
        cube's rule text, matching TM1's documented first-matching-statement-wins behavior for
        overlapping rule areas.
        """
        ref = MeasureRef(
            cube_name=self._cube_name,
            measure_name=measure_name,
            area=tuple(sorted(fixed_elements.items())),
        )
        return AreaTarget(self._model, ref)

    def __getattr__(self, name: str) -> DimensionProxy:
        if name.startswith("_"):
            raise AttributeError(name)
        return DimensionProxy(self._cube_name, name)


class AreaTarget:
    """Returned by `CubeRef.area(...)`; binds an area-scoped `MeasureRef` to `.set(...)` so the
    formula can be registered with an explicit `priority` without overloading `__setitem__`."""

    def __init__(self, model: "Model", ref: MeasureRef):
        self._model = model
        self._ref = ref

    def set(self, expression: Any, priority: int = 0) -> None:
        self._model.register_formula(
            MeasureExpression(ref=self._ref), _ensure_expression(expression), priority=priority
        )


class Model:
    _SUPPORTED_RULE_AREA_SCOPES = ("leaf", "consolidated", "string")
    _SUPPORTED_FEEDER_MODES = ("ranked_single", "safe_union")
    _RULE_AREA_KEYWORDS = {"leaf": "N", "consolidated": "C", "string": "S"}
    _CONSOLIDATED_FEEDER_LEAF_CEILING = 2000
    _CONSOLIDATED_AGGREGATE_FUNCTIONS = {
        "consolidated_max": "ConsolidatedMax",
        "consolidated_min": "ConsolidatedMin",
        "consolidated_avg": "ConsolidatedAvg",
        "consolidated_count": "ConsolidatedCount",
        "consolidated_count_unique": "ConsolidatedCountUnique",
    }
    _SINGLE_ARG_MATH_FUNCTIONS = {
        "exp": "EXP",
        "ln": "LN",
        "log10": "LOG",
        "sqrt": "SQRT",
        "sin": "SIN",
        "cos": "COS",
        "tan": "TAN",
        "asin": "ASIN",
        "acos": "ACOS",
        "atan": "ATAN",
    }

    def __init__(self, tm1: Any = None, metadata_provider: Optional[MetadataProvider] = None):
        self.tm1 = tm1
        self.metadata_provider = metadata_provider or (TM1ServiceMetadataProvider(tm1) if tm1 is not None else None)
        self._cubes: Dict[str, CubeRef] = {}
        self._formulas: Dict[str, Formula] = {}

    def cube(self, cube_name: str) -> CubeRef:
        if cube_name not in self._cubes:
            self._cubes[cube_name] = CubeRef(model=self, cube_name=cube_name)
        return self._cubes[cube_name]

    def case(self, *cases: Tuple[Any, Any], default: Any) -> CaseExpression:
        normalized_cases = tuple((_ensure_expression(condition), _ensure_expression(value)) for condition, value in cases)
        return CaseExpression(cases=normalized_cases, default=_ensure_expression(default))

    def dates(self, year: Any, month: Any, day: Any) -> FunctionCallExpression:
        """Lowers to TM1's native `DATES(Year, Month, Day)` -- returns the serial date number
        for the given year/month/day. Confirmed via a worked example: `DATES(2012,12,2)`."""
        return FunctionCallExpression(
            function_name="DATES",
            args=(_ensure_expression(year), _ensure_expression(month), _ensure_expression(day)),
        )

    def day(self, date_string: Any) -> FunctionCallExpression:
        """Lowers to TM1's native `DAY(DateString)` -- extracts the day-of-month number from a
        date string in 'YY-MM-DD' or 'YYYY-MM-DD' form."""
        return FunctionCallExpression(function_name="DAY", args=(_ensure_expression(date_string),))

    def dayno(self, date_string: Any) -> FunctionCallExpression:
        """Lowers to TM1's native `DAYNO(DateString)` -- the TM1 serial date number (days since
        1960-01-01) for a date string in 'YY-MM-DD' or 'YYYY-MM-DD' form."""
        return FunctionCallExpression(function_name="DAYNO", args=(_ensure_expression(date_string),))

    def date(self, serial_number: Any, four_digit_year: Any = 0) -> FunctionCallExpression:
        """Lowers to TM1's native `DATE(SerialNumber, FourDigitYear)` -- converts a serial date
        number back to a text date. `four_digit_year` is TM1's own optional 0/1 flag (0 = 2-digit
        year, 1 = 4-digit year), defaulting to 0 to match TM1's own default."""
        return FunctionCallExpression(
            function_name="DATE",
            args=(_ensure_expression(serial_number), _ensure_expression(four_digit_year)),
        )

    def import_native_rule_text(self, cube_name: str, rule_text: str) -> "RuleIngestionReport":
        from .ingest import RuleIngestionRejection, RuleIngestionReport, parse_native_rule_text

        basic_logger.info(f"Ingesting native rule text for cube '{cube_name}' ({len(rule_text)} chars).")
        parsed_statements, rejections = parse_native_rule_text(
            cube_name=cube_name, rule_text=rule_text, metadata_provider=self.metadata_provider
        )
        report = RuleIngestionReport(cube_name=cube_name, rejected=list(rejections))
        if rejections:
            basic_logger.warning(
                f"Cube '{cube_name}' ingestion rejected {len(rejections)} statement(s): "
                f"{[r.reason for r in rejections]}"
            )

        for statement in parsed_statements:
            try:
                self.cube(cube_name)[statement.measure_name] = statement.expression
            except ValueError as error:
                basic_logger.warning(
                    f"Cube '{cube_name}' measure '{statement.measure_name}' rejected after parsing: {error}"
                )
                report.rejected.append(
                    RuleIngestionRejection(
                        statement_text=statement.expression.describe(),
                        reason=str(error),
                        measure_name=statement.measure_name,
                    )
                )
                continue
            report.ingested.append(statement.measure_name)

        basic_logger.info(
            f"Cube '{cube_name}' ingestion complete: {len(report.ingested)} ingested, "
            f"{len(report.rejected)} rejected."
        )
        return report

    def import_cube_rules_from_tm1(self, cube_name: str) -> "RuleIngestionReport":
        if self.tm1 is None:
            raise ValueError("Model.import_cube_rules_from_tm1 requires an attached TM1 service")

        basic_logger.debug(f"Fetching live rule text for cube '{cube_name}' from TM1.")
        cube = self.tm1.cubes.get(cube_name)
        rules = getattr(cube, "rules", None)
        rule_text = getattr(rules, "text", None)
        if rule_text is None:
            rule_text = str(rules) if rules is not None else ""

        return self.import_native_rule_text(cube_name=cube_name, rule_text=rule_text)

    def execute_python_backend(
        self,
        *,
        read_function: Optional[Any] = None,
        write_function: Optional[Any] = None,
        write: bool = True,
    ) -> "PythonExecutionReport":
        from . import execute as execute_module
        from .. import extractor, loader, utility

        if read_function is None:
            def read_function(tm1_service: Any, mdx: str) -> Any:
                return extractor.tm1_mdx_to_dataframe(tm1_service=tm1_service, data_mdx=mdx)

        if write_function is None:
            def write_function(tm1_service: Any, cube_name: str, dataframe: Any, cube_dims: List[str]) -> None:
                loader.dataframe_to_cube(
                    tm1_service=tm1_service,
                    dataframe=dataframe,
                    cube_name=cube_name,
                    cube_dims=cube_dims,
                    use_blob=True,
                )

        plan = self._build_plan()
        report = execute_module.PythonExecutionReport()

        accepted_by_cube: Dict[str, List[Formula]] = defaultdict(list)
        for key in plan.order:
            formula = plan.formulas[key]
            decision = self._select_backend(formula)
            if decision.backend != Backend.PYTHON:
                continue
            unsupported_reason = execute_module.unsupported_python_execution_reason(
                formula.expression, formula.target.cube_name
            )
            if unsupported_reason is not None:
                report.rejected.append(
                    execute_module.PythonExecutionRejection(target=key, reason=unsupported_reason)
                )
                continue
            accepted_by_cube[formula.target.cube_name].append(formula)

        for cube_name, formulas in accepted_by_cube.items():
            metadata = self._get_cube_metadata(cube_name)
            if metadata is None or not metadata.measure_dimension_name:
                for formula in formulas:
                    report.rejected.append(
                        execute_module.PythonExecutionRejection(
                            target=formula.target.key,
                            reason=f"cube metadata is required to build the execution MDX for '{cube_name}'",
                        )
                    )
                continue

            measure_dimension_name = metadata.measure_dimension_name
            required_measures = {formula.target.measure_name for formula in formulas}
            for formula in formulas:
                required_measures.update(ref.measure_name for ref in formula.expression.iter_measure_refs())

            mdx = utility.generate_dynamic_mdx_query_string(
                tm1_service=self.tm1,
                target_cube_name=cube_name,
                dimension_filter_mapping={measure_dimension_name: sorted(required_measures)},
                cube_dimensions_list=list(metadata.dimensions),
            )
            long_dataframe = read_function(self.tm1, mdx)
            wide_dataframe, dimension_columns = execute_module.pivot_long_to_wide(
                long_dataframe, measure_dimension_name
            )

            write_frames = []
            for formula in formulas:
                value, mask = execute_module.evaluate_formula(
                    formula.expression,
                    wide_dataframe,
                    cube_name,
                    metadata=metadata,
                    dimension_columns=dimension_columns,
                )
                wide_dataframe[formula.target.measure_name] = value
                write_frames.append(
                    execute_module.melt_wide_to_long(
                        wide_dataframe,
                        dimension_columns,
                        measure_dimension_name,
                        formula.target.measure_name,
                        value,
                        mask,
                    )
                )
                report.executed.append(formula.target.key)
                report.written_rows[formula.target.key] = int(mask.sum()) if mask is not None else len(value)

            if write and write_frames:
                import pandas as pd

                write_dataframe = pd.concat(write_frames, ignore_index=True)
                write_function(self.tm1, cube_name, write_dataframe, list(metadata.dimensions))

        return report

    def register_formula(self, target: MeasureExpression, expression: Expression, priority: int = 0) -> None:
        key = target.ref.key
        if key in self._formulas:
            raise ValueError(f"Formula target '{key}' is already registered")

        scope = "leaf"
        feeder_mode = "ranked_single"
        if isinstance(expression, ScopedExpression):
            scope = expression.scope
            feeder_mode = expression.feeder_mode
            expression = expression.inner
        if scope not in self._SUPPORTED_RULE_AREA_SCOPES:
            raise ValueError(
                f"Unsupported rule-area scope '{scope}' for target '{key}'; "
                f"currently supported scopes are {', '.join(self._SUPPORTED_RULE_AREA_SCOPES)}"
            )
        if feeder_mode not in self._SUPPORTED_FEEDER_MODES:
            raise ValueError(
                f"Unsupported feeder mode '{feeder_mode}' for target '{key}'; "
                f"currently supported modes are {', '.join(self._SUPPORTED_FEEDER_MODES)}"
            )

        self._formulas[key] = Formula(
            target=target.ref,
            expression=expression,
            scope=scope,
            feeder_mode=feeder_mode,
            priority=priority,
        )

    @property
    def formulas(self) -> List[Formula]:
        return [self._formulas[key] for key in sorted(self._formulas)]

    def validate(self) -> ValidationReport:
        return self._build_plan().report

    def explain(self, target: Optional[str] = None) -> Dict[str, Any]:
        plan = self._build_plan()
        if target is None:
            return {
                "formulas": {
                    key: self._formula_explanation(plan, key)
                    for key in sorted(plan.formulas)
                },
                "execution_order": plan.order,
                "errors": plan.report.errors,
                "warnings": plan.report.warnings,
            }

        normalized_target = self._normalize_target(target)
        if normalized_target not in plan.formulas:
            raise KeyError(f"Unknown formula target '{target}'")
        return self._formula_explanation(plan, normalized_target)

    def phase2_readiness(self, target: Optional[str] = None) -> Union[Phase2ReadinessReport, Dict[str, Any]]:
        plan = self._build_plan()
        formulas = {
            key: self._formula_phase2_readiness(plan, plan.formulas[key])
            for key in sorted(plan.formulas)
        }
        if target is None:
            return Phase2ReadinessReport(
                formulas=formulas,
                errors=list(plan.report.errors),
                warnings=list(plan.report.warnings),
            )

        normalized_target = self._normalize_target(target)
        if normalized_target not in formulas:
            raise KeyError(f"Unknown formula target '{target}'")
        return formulas[normalized_target]

    _DEPLOYMENT_LOG_DIR = ".tm1_bedrock_deployments"

    def compile(
        self,
        dry_run: bool = True,
        deployment_log_dir: Optional[str] = None,
    ) -> CompilePreview:
        basic_logger.info(f"compile() starting, dry_run={dry_run}")
        plan = self._build_plan()
        rules_by_cube: Dict[str, List[str]] = defaultdict(list)
        feeders_by_cube: Dict[str, List[str]] = defaultdict(list)
        manifest: Dict[str, Dict[str, Any]] = {}
        errors = list(plan.report.errors)
        warnings = list(plan.report.warnings)
        basic_logger.debug(f"compile() build plan resolved {len(plan.formulas)} formula(s), {len(errors)} error(s)")

        if errors:
            basic_logger.warning(
                f"compile() aborting before artifact generation, {len(errors)} build-plan error(s): {errors}"
            )
            return CompilePreview(
                rules={},
                feeders={},
                manifest={},
                errors=errors,
                warnings=warnings,
            )

        for target_key in plan.order:
            formula = plan.formulas[target_key]
            decision = self._select_backend(formula)
            readiness = self._formula_phase2_readiness(plan, formula)
            feeder_plan = (
                self._plan_feeders(plan, formula)
                if decision.backend is Backend.NATIVE
                else FeederPlan(
                    statements=(),
                    strategy="python_fallback",
                    rationale="Python-materialized formulas do not emit native TM1 feeders.",
                    deployment_cube=None,
                    preview_only=False,
                )
            )
            feeder_statements = list(feeder_plan.statements)
            rule_statement = None
            if decision.backend is Backend.NATIVE:
                rule_body = self._compile_native_expression(formula.expression, formula.target.cube_name)
                target_reference = self._compile_native_measure_reference_with_area(
                    cube_name=formula.target.cube_name,
                    measure_name=formula.target.measure_name,
                    area=formula.target.area,
                )
                rule_area_keyword = self._RULE_AREA_KEYWORDS[formula.scope]
                rule_statement = f"{target_reference} = {rule_area_keyword}: {rule_body};"

            manifest[target_key] = {
                "cube": formula.target.cube_name,
                "measure": formula.target.measure_name,
                "rule_area": formula.scope,
                "feeder_mode": formula.feeder_mode,
                "backend": decision.backend.value,
                "rationale": decision.rationale,
                "expression": formula.expression.describe(),
                "metadata_ready": readiness["metadata_ready"],
                "required_metadata": readiness["required_metadata"],
                "missing_metadata": readiness["missing_metadata"],
                "artifact": {
                    "rule_statement": rule_statement,
                    "feeder_statements": feeder_statements,
                    "feeder_strategy": feeder_plan.strategy,
                    "feeder_rationale": feeder_plan.rationale,
                    "feeder_deployment_cube": feeder_plan.deployment_cube,
                    "deployment_cube": formula.target.cube_name if decision.backend is Backend.NATIVE else None,
                    "preview_only": decision.backend is Backend.NATIVE and feeder_plan.preview_only,
                },
            }
            feeder_deployment_cubes = feeder_plan.deployment_cubes(default_cube=formula.target.cube_name)
            if len(feeder_deployment_cubes) > 1:
                manifest[target_key]["artifact"]["feeder_deployment_cubes"] = list(feeder_deployment_cubes)
            if decision.backend is not Backend.NATIVE:
                continue

            rules_by_cube[formula.target.cube_name].append(
                rule_statement
            )
            for feeder_cube_name, grouped_feeder_statements in feeder_plan.iter_deployment_statements(
                default_cube=formula.target.cube_name
            ):
                feeders_by_cube[feeder_cube_name].extend(grouped_feeder_statements)

        preview = CompilePreview(
            rules={
                cube_name: "\n".join(lines)
                for cube_name, lines in sorted(rules_by_cube.items())
            },
            feeders={
                cube_name: "\n".join(sorted(set(lines)))
                for cube_name, lines in sorted(feeders_by_cube.items())
                if lines
            },
            manifest=manifest,
            errors=errors,
            warnings=warnings,
        )
        preview.deployment = self._build_deployment_plan(preview, live=False)
        basic_logger.info(
            f"compile() produced rule artifacts for {len(preview.rules)} cube(s), "
            f"feeder artifacts for {len(preview.feeders)} cube(s)"
        )
        if dry_run:
            basic_logger.debug("compile() dry_run=True, skipping live deployment")
            return preview

        self._deploy_compile_preview(preview, deployment_log_dir=deployment_log_dir)
        return preview

    def _fetch_prior_rule_text(self, cube_name: str) -> str:
        try:
            return self.tm1.cubes.get(cube_name).rules.text or ""
        except Exception:
            return ""

    def _deploy_compile_preview(
        self,
        preview: CompilePreview,
        deployment_log_dir: Optional[str] = None,
    ) -> None:
        if self.tm1 is None:
            raise DeploymentError("Live TM1 deployment requires an attached TM1 service.")

        self._assert_deployment_ready(preview)

        deployment = self._build_deployment_plan(preview, live=True)
        deployed_cubes: List[str] = []
        cube_names_to_deploy = sorted(set(preview.rules) | set(preview.feeders))
        basic_logger.info(f"Deploying compiled rules/feeders to {len(cube_names_to_deploy)} cube(s) on live TM1.")
        try:
            for cube_name in cube_names_to_deploy:
                prior_rule_text = self._fetch_prior_rule_text(cube_name)
                rule_text = self._render_rule_artifact(
                    cube_name,
                    preview.rules.get(cube_name, ""),
                    preview.feeders.get(cube_name, ""),
                    include_feedstrings=any(
                        manifest_entry.get("rule_area") == "string"
                        for manifest_entry in preview.manifest.values()
                        if manifest_entry.get("cube") == cube_name
                    ),
                )
                deployment[cube_name]["prior_rule_text"] = prior_rule_text
                deployment[cube_name]["diff"] = "\n".join(
                    difflib.unified_diff(
                        prior_rule_text.splitlines(),
                        rule_text.splitlines(),
                        fromfile=f"{cube_name}/prior",
                        tofile=f"{cube_name}/new",
                        lineterm="",
                    )
                )

                basic_logger.debug(f"Writing rule text ({len(rule_text)} chars) to cube '{cube_name}'.")
                self.tm1.cubes.update_or_create_rules(cube_name, rule_text)
                deployed_cubes.append(cube_name)
                check_response = self.tm1.cubes.check_rules(cube_name)
                status_code = getattr(check_response, "status_code", None)
                if status_code is not None and not (200 <= status_code < 300):
                    basic_logger.warning(
                        f"TM1 rule validation failed for cube '{cube_name}' with status {status_code}, "
                        f"rolling back {len(deployed_cubes)} already-deployed cube(s)."
                    )
                    raise DeploymentError(
                        f"TM1 rule validation failed for cube '{cube_name}' with status {status_code}."
                    )
                deployment[cube_name]["deployed"] = True
                deployment[cube_name]["rule_length"] = len(rule_text)
                deployment[cube_name]["check_rules_status"] = status_code
                basic_logger.info(f"Cube '{cube_name}' rules deployed and validated (status {status_code}).")
        except DeploymentError:
            self._rollback_cubes(deployment, deployed_cubes)
            raise

        preview.deployment = deployment
        preview.deployment_manifest_path = self._persist_deployment_manifest(
            preview, deployment_log_dir=deployment_log_dir
        )
        basic_logger.info(
            f"Deployment of {len(deployed_cubes)} cube(s) complete. "
            f"Manifest written to {preview.deployment_manifest_path}."
        )

    def _rollback_cubes(self, deployment: Dict[str, Dict[str, Any]], cube_names: Iterable[str]) -> None:
        cube_names = list(cube_names)
        basic_logger.warning(f"Rolling back {len(cube_names)} cube(s) to their prior rule text.")
        for cube_name in cube_names:
            prior_rule_text = deployment.get(cube_name, {}).get("prior_rule_text", "")
            self.tm1.cubes.update_or_create_rules(cube_name, prior_rule_text)
            deployment.setdefault(cube_name, {})["deployed"] = False
            deployment[cube_name]["rolled_back"] = True
            basic_logger.debug(f"Cube '{cube_name}' rolled back to prior rule text ({len(prior_rule_text)} chars).")

    def _persist_deployment_manifest(
        self,
        preview: CompilePreview,
        deployment_log_dir: Optional[str] = None,
    ) -> str:
        log_dir = deployment_log_dir or self._DEPLOYMENT_LOG_DIR
        os.makedirs(log_dir, exist_ok=True)
        timestamp = time.strftime("%Y%m%dT%H%M%S", time.gmtime())
        manifest_path = os.path.join(log_dir, f"deployment_{timestamp}_{int(time.time() * 1000) % 1000:03d}.json")
        record = {
            "timestamp": timestamp,
            "deployment": preview.deployment,
            "manifest": preview.manifest,
        }
        with open(manifest_path, "w", encoding="utf-8") as handle:
            json.dump(record, handle, indent=2, sort_keys=True)
        basic_logger.debug(f"Deployment manifest persisted to {manifest_path}.")
        return manifest_path

    def rollback_deployment(self, manifest_path: str) -> Dict[str, Any]:
        if self.tm1 is None:
            raise DeploymentError("Rollback requires an attached TM1 service.")

        basic_logger.info(f"rollback_deployment() reading manifest from {manifest_path}.")
        with open(manifest_path, "r", encoding="utf-8") as handle:
            record = json.load(handle)

        deployment = record.get("deployment", {})
        results: Dict[str, Any] = {}
        for cube_name, cube_record in sorted(deployment.items()):
            if not cube_record.get("deployed"):
                basic_logger.debug(f"Cube '{cube_name}' was never deployed in this manifest, skipping rollback.")
                continue
            prior_rule_text = cube_record.get("prior_rule_text", "")
            self.tm1.cubes.update_or_create_rules(cube_name, prior_rule_text)
            check_response = self.tm1.cubes.check_rules(cube_name)
            status_code = getattr(check_response, "status_code", None)
            if status_code is not None and not (200 <= status_code < 300):
                basic_logger.warning(
                    f"TM1 rule validation failed while rolling back cube '{cube_name}' with status {status_code}."
                )
                raise DeploymentError(
                    f"TM1 rule validation failed while rolling back cube '{cube_name}' with status {status_code}."
                )
            results[cube_name] = {"rolled_back": True, "check_rules_status": status_code}
            basic_logger.info(f"Cube '{cube_name}' rolled back via manifest (status {status_code}).")

        basic_logger.info(f"rollback_deployment() complete, {len(results)} cube(s) rolled back.")
        return results

    def _assert_deployment_ready(self, preview: CompilePreview) -> None:
        if preview.errors:
            raise DeploymentError(
                "Cannot deploy because validation errors were found: " + "; ".join(preview.errors)
            )

        missing_metadata_by_formula = {
            target: manifest["missing_metadata"]
            for target, manifest in preview.manifest.items()
            if manifest["backend"] == Backend.NATIVE.value and manifest["missing_metadata"]
        }
        if missing_metadata_by_formula:
            details = ", ".join(
                f"{target} -> {', '.join(missing_codes)}"
                for target, missing_codes in sorted(missing_metadata_by_formula.items())
            )
            raise DeploymentError(
                "Cannot deploy native rule artifacts because required metadata is missing: " + details
            )

        preview_only_targets = sorted(
            target
            for target, manifest in preview.manifest.items()
            if manifest["backend"] == Backend.NATIVE.value and manifest["artifact"].get("preview_only")
        )
        if preview_only_targets:
            raise DeploymentError(
                "Cannot deploy native rule artifacts because preview-only cross-cube compilation "
                "still needs explicit feeder and deployment support: " + ", ".join(preview_only_targets)
            )

    @staticmethod
    def _render_rule_artifact(
        cube_name: str,
        rules_text: str,
        feeders_text: str,
        include_feedstrings: bool = False,
    ) -> str:
        sections = [
            "# Generated by TM1_bedrock_py calc Phase 2 compiler",
            f"# Cube: {cube_name}",
        ]
        if rules_text.strip():
            if include_feedstrings:
                sections.extend(["FEEDSTRINGS;", ""])
            sections.extend(
                [
                    "SKIPCHECK;",
                    "",
                    rules_text.strip(),
                ]
            )
        if feeders_text.strip():
            sections.extend(
                [
                    "",
                    "FEEDERS;",
                    feeders_text.strip(),
                ]
            )
        return "\n".join(section for section in sections if section is not None).strip() + "\n"

    @staticmethod
    def _build_deployment_plan(preview: CompilePreview, live: bool) -> Dict[str, Dict[str, Any]]:
        formulas_by_cube: Dict[str, List[str]] = defaultdict(list)
        for target_key, manifest in sorted(preview.manifest.items()):
            if manifest["backend"] == Backend.NATIVE.value:
                formulas_by_cube[manifest["cube"]].append(target_key)

        return {
            cube_name: {
                "mode": "live" if live else "dry_run",
                "deployed": False,
                "formula_targets": formulas_by_cube.get(cube_name, []),
                "rule_line_count": len([line for line in preview.rules.get(cube_name, "").splitlines() if line.strip()]),
                "feeder_count": len([line for line in preview.feeders.get(cube_name, "").splitlines() if line.strip()]),
                "artifact_preview": preview.rules.get(cube_name, ""),
                "check_rules_status": None,
            }
            for cube_name in sorted(set(preview.rules) | set(preview.feeders))
        }

    def _formula_area_overlaps(self, plan: BuildPlan, target_key: str) -> List[Dict[str, Any]]:
        """Other registered formulas sharing this target's base measure whose rule area could
        match the same cell, with how their relative order was resolved -- see
        `Model._formula_areas_overlap` and the priority-edge pass in `Model._build_plan`."""
        formula = plan.formulas[target_key]
        overlaps: List[Dict[str, Any]] = []
        for other_key, other_formula in plan.formulas.items():
            if other_key == target_key or other_formula.target.base_key != formula.target.base_key:
                continue
            if not self._formula_areas_overlap(formula.target.area, other_formula.target.area):
                continue
            if formula.priority == other_formula.priority:
                resolution = "unresolved: equal priority, fails closed at build time"
            elif formula.priority < other_formula.priority:
                resolution = "this formula is emitted first (lower priority)"
            else:
                resolution = "this formula is emitted after (higher priority)"
            overlaps.append(
                {
                    "target": other_key,
                    "this_priority": formula.priority,
                    "other_priority": other_formula.priority,
                    "resolution": resolution,
                }
            )
        return overlaps

    def _formula_explanation(self, plan: BuildPlan, target_key: str) -> Dict[str, Any]:
        formula = plan.formulas[target_key]
        upstream = self._collect_upstream_formula_keys(plan.dependencies, target_key)
        decision = self._select_backend(formula)
        readiness = self._formula_phase2_readiness(plan, formula)
        return {
            "target": target_key,
            "expression": formula.expression.describe(),
            "area": dict(formula.target.area),
            "priority": formula.priority,
            "area_overlaps": self._formula_area_overlaps(plan, target_key),
            "measure_dependencies": plan.dependencies[target_key],
            "attribute_dependencies": sorted({ref.label for ref in formula.expression.iter_attribute_refs()}),
            "upstream_formulas": upstream,
            "execution_order": [key for key in plan.order if key in upstream or key == target_key],
            "backend": decision.backend.value,
            "rationale": decision.rationale,
            "feeder_mode": formula.feeder_mode,
            "feeder_strategy": (
                self._plan_feeders(plan, formula).strategy
                if decision.backend is Backend.NATIVE
                else None
            ),
            "feeder_rationale": (
                self._plan_feeders(plan, formula).rationale
                if decision.backend is Backend.NATIVE
                else None
            ),
            "feeder_origin_candidates": (
                self._format_feeder_origin_candidates(self._plan_feeders(plan, formula))
                if decision.backend is Backend.NATIVE
                else []
            ),
            "selected_feeder_origin": (
                self._plan_feeders(plan, formula).selected_origin_key
                if decision.backend is Backend.NATIVE
                else None
            ),
            "metadata_ready": readiness["metadata_ready"],
            "required_metadata": readiness["required_metadata"],
            "missing_metadata": readiness["missing_metadata"],
            "native_eligibility": readiness["native_eligibility"],
        }

    def _formula_phase2_readiness(self, plan: BuildPlan, formula: Formula) -> Dict[str, Any]:
        decision = self._select_backend(formula)
        requirements = self._metadata_requirements_for_formula(formula)
        missing_metadata = [
            requirement.code
            for requirement in requirements
            if not requirement.satisfied
        ]
        return {
            "target": formula.key,
            "backend": decision.backend.value,
            "rationale": decision.rationale,
            "feeder_mode": formula.feeder_mode,
            "metadata_ready": all(requirement.satisfied for requirement in requirements),
            "required_metadata": [
                {
                    "code": requirement.code,
                    "cube": requirement.cube_name,
                    "description": requirement.description,
                    "detail": requirement.detail,
                    "satisfied": requirement.satisfied,
                }
                for requirement in requirements
            ],
            "missing_metadata": missing_metadata,
            "feeder_origin_candidates": (
                self._format_feeder_origin_candidates(self._plan_feeders(plan, formula))
                if decision.backend is Backend.NATIVE
                else []
            ),
            "selected_feeder_origin": (
                self._plan_feeders(plan, formula).selected_origin_key
                if decision.backend is Backend.NATIVE
                else None
            ),
            "native_eligibility": self._native_eligibility(
                plan=plan,
                formula=formula,
                decision=decision,
                missing_metadata=missing_metadata,
            ),
        }

    def _native_eligibility(
        self,
        plan: BuildPlan,
        formula: Formula,
        decision: BackendDecision,
        missing_metadata: Sequence[str],
    ) -> Dict[str, str]:
        if decision.backend is Backend.NATIVE:
            if missing_metadata:
                return {
                    "status": "metadata_incomplete",
                    "code": self._native_metadata_missing_code(formula, missing_metadata),
                    "category": "metadata",
                    "detail": self._native_metadata_missing_detail(formula, missing_metadata),
                }
            feeder_plan = self._plan_feeders(plan, formula)
            if feeder_plan.preview_only:
                preview_only_code = self._native_preview_only_code(formula, feeder_plan)
                return {
                    "status": "preview_only",
                    "code": preview_only_code,
                    "category": (
                        "metadata"
                        if preview_only_code == "native_align_target_leaf_scope_unproven"
                        else "feeder_proof"
                    ),
                    "detail": self._native_preview_only_detail(preview_only_code),
                }
            return {
                "status": "deployable",
                "code": "native_deployable",
                "category": "supported",
                "detail": "The formula is currently within the bounded native subset and is deployable.",
            }

        blocker_reason = decision.rationale.removeprefix("Falls back to Python materialization because ").strip()
        if "multi-cell aggregation" in blocker_reason:
            return {
                "status": "unsupported",
                "code": "native_non_native_by_design",
                "category": "by_design",
                "detail": "This shape is intentionally excluded from the native subset because it requires true multi-cell aggregation.",
            }
        if "must be a literal or target-cube attribute reference" in blocker_reason:
            # Shared between align(...) and the cross-cube subset of consolidated_max/min/avg/
            # count/count_unique(...) -- both route through Model._native_dimension_mapping_
            # unsupported_reason, whose value-driven-rejection message starts with the triggering
            # construct's own label (e.g. "align(...)" or "consolidated_max(...)"), so the detail
            # below names the real construct instead of hardcoding one.
            triggering_construct_match = re.match(r"^(\S+\(\.\.\.\))", blocker_reason)
            triggering_construct = (
                triggering_construct_match.group(1) if triggering_construct_match else "This mapping"
            )
            return {
                "status": "unsupported",
                "code": "native_value_driven_mapping",
                "category": "mapping_shape",
                "detail": (
                    f"{triggering_construct}'s dimension mapping is value-driven (the routing value comes "
                    "from a measure cell) rather than metadata-invertible, so it is not native-safe. This is "
                    "a confirmed permanent exclusion, not an unimplemented feature: TM1 places no constraint "
                    "on a measure cell's possible values (unlike a dimension's fixed leaf-element set), so "
                    "no compile-time metadata fact can ever bound the feeder enumeration for this shape. "
                    "Multi-hop attribute chains (ChainedAttributeExpression) are a different, bounded shape "
                    "and are native-eligible -- see native_align_reverse_mapping_unproven instead. "
                    "Recommended workaround: pre-resolve the driver value into a dimension element or "
                    "attribute upstream of this model (e.g. via a TI process or this repo's own "
                    "dimension-builder tooling), so the routing becomes element/attribute-based and "
                    "therefore native-compilable."
                ),
            }
        if "cannot infer source dimension" in blocker_reason:
            # Shared substring between align(...)'s "cannot infer source dimensions" and
            # consolidated_max/min/avg/count/count_unique(...)'s "cannot infer source dimension(s)"
            # -- both route through the same unresolved-cross-cube-dimension failure shape.
            return {
                "status": "unsupported",
                "code": "native_mapping_unresolved",
                "category": "mapping_shape",
                "detail": "The cross-cube dimension mapping leaves source dimension(s) unresolved, so the lookup is not native-safe.",
            }
        if (
            "attribute mappings must come from the target cube context" in blocker_reason
            or "which is not part of cube" in blocker_reason
            or "which does not exist in cube" in blocker_reason
        ):
            return {
                "status": "unsupported",
                "code": "native_mapping_unresolved",
                "category": "mapping_shape",
                "detail": "The cross-cube dimension mapping cannot be resolved to a metadata-safe target-cube lookup shape.",
            }
        if "multiple hierarchies" in blocker_reason:
            return {
                "status": "unsupported",
                "code": "native_hierarchy_ambiguous",
                "category": "metadata",
                "detail": "Hierarchy intent is ambiguous, so fixed-element native emission is blocked.",
            }
        if "cross-cube reference" in blocker_reason and "outside the native subset" in blocker_reason:
            return {
                "status": "unsupported",
                "code": "native_cross_cube_reference_unbounded",
                "category": "shape",
                "detail": "The formula uses a cross-cube reference outside the bounded native lookup subset.",
            }
        if "outside the native subset" in blocker_reason or "does not support" in blocker_reason:
            return {
                "status": "unsupported",
                "code": "native_shape_unsupported",
                "category": "shape",
                "detail": "The formula shape is not currently part of the bounded native subset.",
            }
        return {
            "status": "unsupported",
            "code": "native_blocked",
            "category": "shape",
            "detail": blocker_reason or "The formula is not currently native-safe.",
        }

    def _native_metadata_missing_code(
        self,
        formula: Formula,
        missing_metadata: Sequence[str],
    ) -> str:
        if (
            "native_align_target_leaf_scope_unproven" in missing_metadata
            and self._contains_align_expression(formula.expression)
        ):
            return "native_align_target_leaf_scope_unproven"
        if (
            "native_align_reverse_mapping_unproven" in missing_metadata
            and self._contains_align_expression(formula.expression)
        ):
            return "native_align_reverse_mapping_unproven"
        return "native_metadata_missing"

    def _native_metadata_missing_detail(
        self,
        formula: Formula,
        missing_metadata: Sequence[str],
    ) -> str:
        code = self._native_metadata_missing_code(formula, missing_metadata)
        if code == "native_align_target_leaf_scope_unproven":
            return (
                "Native rule lowering remains the intended path, but metadata does not yet prove "
                "the target-only broadcast leaf scope."
            )
        if code == "native_align_reverse_mapping_unproven":
            return (
                "Native rule lowering remains the intended path, but metadata does not yet prove "
                "the reverse attribute mapping from source lookup elements to target leaf elements, "
                "including any required composite multi-attribute route."
            )
        return "Native compilation remains the intended path, but required metadata is missing."

    def _native_preview_only_code(self, formula: Formula, feeder_plan: Optional[FeederPlan] = None) -> str:
        if feeder_plan is None:
            feeder_plan = self._plan_feeders(self._build_plan(), formula)
        if feeder_plan.strategy == "preview_only_ranked_origin":
            return "native_live_feeder_ranking_required"
        if self._contains_align_expression(formula.expression):
            if self._align_contains_unproven_target_leaf_scope(
                formula.expression,
                formula.target.cube_name,
            ):
                return "native_align_target_leaf_scope_unproven"
            if self._align_contains_unproven_reverse_mapping(
                formula.expression,
                formula.target.cube_name,
            ):
                return "native_align_reverse_mapping_unproven"
        return "native_feeder_plan_unproven"

    @staticmethod
    def _native_preview_only_detail(code: str) -> str:
        if code == "native_live_feeder_ranking_required":
            return (
                "Multiple safe feeder origins exist with equal dimension counts, so live TM1 nonzero "
                "data is required to choose the rarer-fill origin. No live read was available or the "
                "read failed, so deployment stays preview-only."
            )
        if code == "native_align_target_leaf_scope_unproven":
            return (
                "Rule lowering is native-safe, but feeder deployment proof is still blocked because "
                "the target-only broadcast leaf scope is not metadata-proven."
            )
        if code == "native_align_reverse_mapping_unproven":
            return (
                "Rule lowering is native-safe, but feeder deployment proof is still blocked because "
                "the reverse attribute mapping from source lookup elements to target leaf elements, "
                "including any required composite multi-attribute route, is not metadata-proven."
            )
        return "Rule lowering is native-safe, but feeder/deployment proof is still incomplete."

    def _select_backend(self, formula: Formula) -> BackendDecision:
        if formula.scope == "consolidated":
            unsupported_reason = self._native_consolidated_unsupported_reason(formula)
            if unsupported_reason is None:
                return BackendDecision(
                    backend=Backend.NATIVE,
                    rationale=(
                        "Compiles within the Phase 2 C: MVP subset: same-cube arithmetic, comparisons, "
                        "where, case, and constrained metadata-resolved cross-cube align lookups over a "
                        "metadata-proven, bounded leaf-measure expansion of the consolidated target."
                    ),
                )
            return BackendDecision(
                backend=Backend.PYTHON,
                rationale=f"Falls back to Python materialization because {unsupported_reason}",
            )

        if formula.scope == "string":
            unsupported_reason = self._native_string_unsupported_reason(formula)
            if unsupported_reason is None:
                return BackendDecision(
                    backend=Backend.NATIVE,
                    rationale=(
                        "Compiles within the Phase 2 S: MVP subset: same-cube arithmetic, "
                        "comparisons, where, case, and string-specific operations over "
                        "same-cube String-typed measures."
                    ),
                )
            return BackendDecision(
                backend=Backend.PYTHON,
                rationale=f"Falls back to Python materialization because {unsupported_reason}",
            )

        unsupported_reason = self._native_unsupported_reason(
            formula.expression,
            target_cube_name=formula.target.cube_name,
        )
        if unsupported_reason is None:
            return BackendDecision(
                backend=Backend.NATIVE,
                rationale=(
                    "Compiles within the Phase 2 MVP subset: same-cube arithmetic, comparisons, "
                    "where, case, and constrained metadata-resolved cross-cube align lookups."
                ),
            )
        return BackendDecision(
            backend=Backend.PYTHON,
            rationale=f"Falls back to Python materialization because {unsupported_reason}",
        )

    def _native_consolidated_unsupported_reason(self, formula: Formula) -> Optional[str]:
        """Gate for C: rule-area native compilation.

        Deliberately narrower than the N: subset: even constructs that would otherwise be
        metadata-safe for N: (shift, growth, ytd, rolling) are rejected outright here,
        because C: feeders are a structurally different problem (feed the consolidated
        target's leaf measures, never the consolidated element itself). Cross-cube align(...)
        is allowed only through the existing bounded N:-level feeder subsets, applied
        independently to each expanded target leaf measure.
        """
        expression_reason = self._native_unsupported_reason(formula.expression, formula.target.cube_name)
        if expression_reason is not None:
            return expression_reason

        if self._contains_disallowed_consolidated_methods(formula.expression):
            return (
                "C: rule-area compilation does not support shift(...), growth(...), ytd(), or "
                "rolling(...) in the current bounded subset; supported shapes are same-cube "
                "arithmetic/comparisons/where/case plus constrained align(...) lookups that each "
                "expanded target leaf can prove safe through the existing N:-level feeder planner"
            )

        metadata = self._get_cube_metadata(formula.target.cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return "C: rule-area compilation needs measure dimension metadata to prove the consolidated target"

        target_measure_type = self._measure_type_for(metadata, formula.target.measure_name)
        if not self._is_consolidated_measure_type(target_measure_type):
            return (
                "C: rule-area compilation requires the target measure to resolve to a "
                "Consolidated element type"
            )

        leaf_measures = self._expand_consolidated_element_to_leaves(
            metadata=metadata,
            dimension_name=metadata.measure_dimension_name,
            element_name=formula.target.measure_name,
        )
        if not leaf_measures:
            return (
                "C: rule-area compilation could not prove a bounded leaf-measure expansion for the "
                "consolidated target (no provable hierarchy child metadata)"
            )
        if len(leaf_measures) > self._CONSOLIDATED_FEEDER_LEAF_CEILING:
            return (
                f"C: rule-area compilation rejected because the consolidated target expands to "
                f"{len(leaf_measures)} leaf measures, beyond the bounded ceiling of "
                f"{self._CONSOLIDATED_FEEDER_LEAF_CEILING}"
            )
        return None

    def _contains_disallowed_consolidated_methods(self, expression: Expression) -> bool:
        return self._contains_method_names(expression, {"shift", "growth", "ytd", "rolling.sum"})

    def _contains_disallowed_string_methods(self, expression: Expression) -> bool:
        return self._contains_method_names(
            expression, {"shift", "growth", "ytd", "rolling.sum"}
        )

    def _contains_method_names(self, expression: Expression, disallowed: set) -> bool:
        """Shared recursive walk used by both the C: and S: method-shape gates.

        Each rule-area scope disallows a different method set (C: still permits align(...)
        through its bounded leaf-expansion subset; S: blocks it outright in this first MVP
        slice, same posture C: took initially), so the disallowed set is parameterized rather
        than duplicating this walk per scope.
        """
        if isinstance(expression, MethodExpression):
            if expression.method_name in disallowed:
                return True
            if self._contains_method_names(expression.base, disallowed):
                return True
            if any(self._contains_method_names(arg, disallowed) for arg in expression.args):
                return True
            return any(
                self._contains_method_names(value, disallowed)
                for value in expression.kwargs.values()
            )

        if isinstance(expression, UnaryExpression):
            return self._contains_method_names(expression.operand, disallowed)

        if isinstance(expression, (BinaryExpression, ComparisonExpression)):
            return self._contains_method_names(
                expression.left, disallowed
            ) or self._contains_method_names(expression.right, disallowed)

        if isinstance(expression, RollingExpression):
            return True

        if isinstance(expression, CaseExpression):
            for condition, value in expression.cases:
                if self._contains_method_names(condition, disallowed) or self._contains_method_names(
                    value, disallowed
                ):
                    return True
            return self._contains_method_names(expression.default, disallowed)

        return False

    @staticmethod
    def _is_consolidated_measure_type(measure_type: Any) -> bool:
        if measure_type is None:
            return False
        if isinstance(measure_type, str):
            return measure_type.strip().casefold() == "consolidated"
        return measure_type == 3

    @staticmethod
    def _is_string_measure_type(measure_type: Any) -> bool:
        if measure_type is None:
            return False
        if isinstance(measure_type, str):
            return measure_type.strip().casefold() == "string"
        return measure_type == 2

    def _native_string_unsupported_reason(self, formula: Formula) -> Optional[str]:
        """Gate for S: rule-area native compilation.

        Same-cube arithmetic/comparisons/where/case plus string-specific operations, and
        bounded cross-cube align(...) reusing the same metadata-proven feeder-origin subsets
        N: already relies on (the underlying _native_unsupported_reason call below proves
        align(...) mapping/hierarchy/reverse-mapping safety identically regardless of rule
        area -- that proof has nothing to do with the target/source measure's element type).
        Every time-intelligence method (shift/growth/ytd/rolling) stays rejected outright:
        S: has no demonstrated need for them yet and no test coverage proving the existing
        time-shift/rolling/ytd feeder builders behave correctly against a String-typed target.
        """
        expression_reason = self._native_unsupported_reason(formula.expression, formula.target.cube_name)
        if expression_reason is not None:
            return expression_reason

        if self._contains_disallowed_string_methods(formula.expression):
            return (
                "S: rule-area compilation does not support shift(...), growth(...), ytd(), or "
                "rolling(...) in the current bounded subset; supported shapes are same-cube "
                "arithmetic/comparisons/where/case plus string-specific operations "
                "(concat/upper/lower/trim/substring) over String-typed measures, plus bounded "
                "cross-cube align(...) when metadata proves the same feeder-origin safety N: "
                "already requires"
            )

        metadata = self._get_cube_metadata(formula.target.cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return "S: rule-area compilation needs measure dimension metadata to prove the string target"

        target_measure_type = self._measure_type_for(metadata, formula.target.measure_name)
        if not self._is_string_measure_type(target_measure_type):
            return "S: rule-area compilation requires the target measure to resolve to a String element type"

        type_overrides = self._measure_type_conversion_overrides(formula.expression)
        for ref in self._iter_expression_measure_refs(formula.expression, formula.target.cube_name):
            if type_overrides.get((ref.cube_name, ref.measure_name)) == "Numeric":
                continue
            ref_metadata = self._get_cube_metadata(ref.cube_name)
            ref_measure_type = self._measure_type_for(ref_metadata, ref.measure_name)
            if not self._is_string_measure_type(ref_measure_type):
                return (
                    f"S: rule-area compilation requires referenced measure '{ref.measure_name}' in "
                    f"cube '{ref.cube_name}' to resolve to a String element type in the current "
                    "bounded subset"
                )

        return None

    def _metadata_requirements_for_formula(self, formula: Formula) -> List[MetadataRequirement]:
        requirements: List[MetadataRequirement] = []
        seen: set[tuple[str, str, str]] = set()

        def add_requirement(code: str, description: str, cube_name: str, detail: str, satisfied: bool) -> None:
            key = (code, cube_name, detail)
            if key in seen:
                return
            seen.add(key)
            requirements.append(
                MetadataRequirement(
                    code=code,
                    description=description,
                    cube_name=cube_name,
                    detail=detail,
                    satisfied=satisfied,
                )
            )

        measure_refs = tuple(self._iter_expression_measure_refs(formula.expression, formula.target.cube_name))

        for cube_name in sorted({formula.target.cube_name, *[ref.cube_name for ref in measure_refs]}):
            metadata = self._get_cube_metadata(cube_name)
            add_requirement(
                code="cube_dimensions",
                description="Cube dimension order is needed for compile planning and deployment packaging.",
                cube_name=cube_name,
                detail=f"Cube '{cube_name}' dimensions",
                satisfied=metadata is not None and bool(metadata.dimensions),
            )
            add_requirement(
                code="measure_dimension",
                description="Measure dimension identity is needed to place targets and validate rule scope.",
                cube_name=cube_name,
                detail=f"Cube '{cube_name}' measure dimension",
                satisfied=metadata is not None and bool(metadata.measure_dimension_name),
            )
            add_requirement(
                code="measure_types",
                description="Measure element types are needed to validate native rule targets and feeder sources.",
                cube_name=cube_name,
                detail=f"Cube '{cube_name}' measure element types",
                satisfied=metadata is not None and bool(metadata.measure_element_types),
            )

        target_metadata = self._get_cube_metadata(formula.target.cube_name)
        target_measure_type = self._measure_type_for(target_metadata, formula.target.measure_name)
        add_requirement(
            code="measure_exists",
            description="Target measures must exist in cube metadata before native rule deployment.",
            cube_name=formula.target.cube_name,
            detail=f"Target measure '{formula.target.measure_name}'",
            satisfied=target_measure_type is not None,
        )
        if formula.scope == "consolidated":
            add_requirement(
                code="consolidated_target",
                description=(
                    "C: rule-area compilation requires the target measure to resolve to a "
                    "Consolidated element type."
                ),
                cube_name=formula.target.cube_name,
                detail=f"Target measure '{formula.target.measure_name}'",
                satisfied=self._is_consolidated_measure_type(target_measure_type),
            )
            leaf_measures = None
            if target_metadata is not None and target_metadata.measure_dimension_name:
                leaf_measures = self._expand_consolidated_element_to_leaves(
                    metadata=target_metadata,
                    dimension_name=target_metadata.measure_dimension_name,
                    element_name=formula.target.measure_name,
                )
            add_requirement(
                code="consolidated_leaf_expansion",
                description=(
                    "C: rule-area compilation requires a bounded, metadata-proven leaf-measure "
                    "expansion of the consolidated target, within the leaf-count ceiling."
                ),
                cube_name=formula.target.cube_name,
                detail=f"Target measure '{formula.target.measure_name}' leaf expansion",
                satisfied=bool(leaf_measures) and len(leaf_measures) <= self._CONSOLIDATED_FEEDER_LEAF_CEILING,
            )
        elif formula.scope == "string":
            add_requirement(
                code="string_target",
                description="S: rule-area compilation requires the target measure to resolve to a String element type.",
                cube_name=formula.target.cube_name,
                detail=f"Target measure '{formula.target.measure_name}'",
                satisfied=self._is_string_measure_type(target_measure_type),
            )
        else:
            add_requirement(
                code="numeric_measure",
                description="Native rule compilation currently requires numeric target measures.",
                cube_name=formula.target.cube_name,
                detail=f"Target measure '{formula.target.measure_name}'",
                satisfied=self._is_numeric_measure_type(target_measure_type),
            )

        type_overrides = self._measure_type_conversion_overrides(formula.expression)
        for ref in sorted(
            set(measure_refs),
            key=lambda item: (item.cube_name, item.measure_name),
        ):
            metadata = self._get_cube_metadata(ref.cube_name)
            measure_type = self._measure_type_for(metadata, ref.measure_name)
            add_requirement(
                code="measure_exists",
                description="Referenced measures must exist in cube metadata before compile planning and deployment.",
                cube_name=ref.cube_name,
                detail=f"Referenced measure '{ref.measure_name}'",
                satisfied=measure_type is not None,
            )
            override = type_overrides.get((ref.cube_name, ref.measure_name))
            if override is not None:
                # Explicitly converted via .to_string(...)/.to_number() -- the expected type is
                # the opposite of the formula's own scope, not the scope's normal requirement.
                add_requirement(
                    code="string_measure" if override == "String" else "numeric_measure",
                    description=(
                        f"'.{'to_number' if override == 'String' else 'to_string'}(...)' "
                        f"requires the wrapped measure to resolve to a {override} element type."
                    ),
                    cube_name=ref.cube_name,
                    detail=f"Referenced measure '{ref.measure_name}'",
                    satisfied=(
                        self._is_string_measure_type(measure_type)
                        if override == "String"
                        else self._is_numeric_measure_type(measure_type)
                    ),
                )
                continue
            if formula.scope == "string":
                add_requirement(
                    code="string_measure",
                    description="The current S: rule-area subset assumes String-typed referenced measures.",
                    cube_name=ref.cube_name,
                    detail=f"Referenced measure '{ref.measure_name}'",
                    satisfied=self._is_string_measure_type(measure_type),
                )
            else:
                add_requirement(
                    code="numeric_measure",
                    description="The current native rule subset assumes numeric referenced measures.",
                    cube_name=ref.cube_name,
                    detail=f"Referenced measure '{ref.measure_name}'",
                    satisfied=self._is_numeric_measure_type(measure_type),
                )

        for ref in sorted(
            {ref for ref in formula.expression.iter_attribute_refs()},
            key=lambda item: (item.cube_name, item.dimension_name, item.attribute_name),
        ):
            metadata = self._get_cube_metadata(ref.cube_name)
            available_attributes = () if metadata is None else metadata.dimension_attributes.get(ref.dimension_name, ())
            add_requirement(
                code="dimension_attribute",
                description="Referenced attributes must exist to validate alignments and lookup intent.",
                cube_name=ref.cube_name,
                detail=f"{ref.dimension_name}.{ref.attribute_name}",
                satisfied=ref.attribute_name in available_attributes,
            )

        for code, description, cube_name, detail in self._expression_specific_metadata_requirements(
            formula.expression,
            target_cube_name=formula.target.cube_name,
        ):
            add_requirement(
                code=code,
                description=description,
                cube_name=cube_name,
                detail=detail,
                satisfied=self._metadata_requirement_satisfied(formula, code),
            )

        return requirements

    def _expression_specific_metadata_requirements(
        self,
        expression: Expression,
        target_cube_name: str,
    ) -> List[Tuple[str, str, str, str]]:
        requirements: List[Tuple[str, str, str, str]] = []

        if isinstance(expression, MethodExpression):
            if expression.method_name == "align":
                requirements.append(
                    (
                        "alignment_mapping",
                        "Cross-cube alignment rules need metadata-proven dimension mapping and constrained lookup semantics.",
                        target_cube_name,
                        "align(...) mapping resolution",
                    )
                )
                requirements.append(
                    (
                        "hierarchy_resolution",
                        "Fixed element references in native rules need provably unambiguous hierarchy resolution.",
                        target_cube_name,
                        "align(...) hierarchy resolution",
                    )
                )
                if self._align_uses_attribute_mapping(expression):
                    requirements.append(
                        (
                            "native_align_reverse_mapping_unproven",
                            "Attribute-routed native align(...) needs metadata-proven reverse mapping from source lookup elements to target leaf elements, including any required composite multi-attribute route.",
                            target_cube_name,
                            "align(...) reverse attribute mapping proof",
                        )
                    )
                if self._align_has_target_only_broadcast_dimensions(expression, target_cube_name):
                    requirements.append(
                        (
                            "native_align_target_leaf_scope_unproven",
                            "Native align(...) broadcast into target-only dimensions needs an explicit, hierarchy-safe target leaf set.",
                            target_cube_name,
                            "align(...) target-only broadcast leaf scope",
                        )
                    )
            elif expression.method_name in {"shift", "ytd", "growth", "rolling.sum"}:
                requirements.append(
                    (
                        "time_mapping",
                        "Time-intelligence lowering needs explicit time-dimension metadata and mapping semantics.",
                        target_cube_name,
                        expression.method_name,
                    )
                )
            elif expression.method_name == "growth":
                requirements.append(
                    (
                        "growth_baseline",
                        "Growth lowering needs a same-cube baseline measure reference or explicit baseline measure name.",
                        target_cube_name,
                        "growth(vs=...)",
                    )
                )

            requirements.extend(
                self._expression_specific_metadata_requirements(expression.base, target_cube_name)
            )
            for arg in expression.args:
                requirements.extend(
                    self._expression_specific_metadata_requirements(arg, target_cube_name)
                )
            for value in expression.kwargs.values():
                requirements.extend(
                    self._expression_specific_metadata_requirements(value, target_cube_name)
                )

        elif isinstance(expression, RollingExpression):
            requirements.append(
                (
                    "time_mapping",
                    "Rolling windows need explicit time-dimension metadata and ordering semantics.",
                    target_cube_name,
                    "rolling(...)",
                )
            )
            requirements.extend(
                self._expression_specific_metadata_requirements(expression.base, target_cube_name)
            )
            for value in expression.kwargs.values():
                requirements.extend(
                    self._expression_specific_metadata_requirements(value, target_cube_name)
                )

        elif isinstance(expression, UnaryExpression):
            requirements.extend(
                self._expression_specific_metadata_requirements(expression.operand, target_cube_name)
            )

        elif isinstance(expression, (BinaryExpression, ComparisonExpression)):
            requirements.extend(
                self._expression_specific_metadata_requirements(expression.left, target_cube_name)
            )
            requirements.extend(
                self._expression_specific_metadata_requirements(expression.right, target_cube_name)
            )

        elif isinstance(expression, CaseExpression):
            for condition, value in expression.cases:
                requirements.extend(
                    self._expression_specific_metadata_requirements(condition, target_cube_name)
                )
                requirements.extend(
                    self._expression_specific_metadata_requirements(value, target_cube_name)
                )
            requirements.extend(
                self._expression_specific_metadata_requirements(expression.default, target_cube_name)
            )

        return requirements

    def _metadata_requirement_satisfied(self, formula: Formula, code: str) -> bool:
        if code in {"alignment_mapping", "hierarchy_resolution", "growth_baseline"}:
            return self._native_unsupported_reason(
                formula.expression,
                target_cube_name=formula.target.cube_name,
            ) is None
        if code == "native_align_reverse_mapping_unproven":
            if self._formula_can_use_same_cube_cross_cube_fallback(formula):
                return True
            return not self._align_contains_unproven_reverse_mapping(
                formula.expression,
                formula.target.cube_name,
            )
        if code == "native_align_target_leaf_scope_unproven":
            if self._formula_can_use_same_cube_cross_cube_fallback(formula):
                return True
            return not self._align_contains_unproven_target_leaf_scope(
                formula.expression,
                formula.target.cube_name,
            )
        return True

    def _formula_can_use_same_cube_cross_cube_fallback(self, formula: Formula) -> bool:
        align_expression_count = sum(1 for _ in self._iter_align_expressions(formula.expression))
        if align_expression_count > 1:
            return False
        return any(
            ref.cube_name == formula.target.cube_name
            for ref in self._iter_expression_measure_refs(
                formula.expression,
                formula.target.cube_name,
            )
        )

    def _get_cube_metadata(self, cube_name: str) -> Any:
        if self.metadata_provider is None:
            return None
        return self.metadata_provider.get_cube_metadata(cube_name)

    def refresh_metadata_cache(self) -> None:
        """Drops any cached cube metadata so the next lookup re-queries TM1.

        `TM1ServiceMetadataProvider.get_cube_metadata` caches per cube name for the lifetime
        of the provider, since cube/dimension structure doesn't change mid-compile. Call this
        if the underlying schema was altered (e.g. via `dimension_builder`) after this `Model`
        was constructed and before compiling again against the same live server.
        """
        clear_cache = getattr(self.metadata_provider, "clear_cache", None)
        if callable(clear_cache):
            basic_logger.debug("refresh_metadata_cache() clearing cached cube metadata.")
            clear_cache()

    @staticmethod
    def _measure_type_for(metadata: Any, measure_name: str) -> Any:
        if metadata is None:
            return None
        for current_measure_name, current_measure_type in metadata.measure_element_types.items():
            if str(current_measure_name).strip().casefold() == str(measure_name).strip().casefold():
                return current_measure_type
        return None

    @staticmethod
    def _is_numeric_measure_type(measure_type: Any) -> bool:
        if measure_type is None:
            return False
        if isinstance(measure_type, str):
            return measure_type.strip().casefold() == "numeric"
        return measure_type == 1

    def _native_unsupported_reason(self, expression: Expression, target_cube_name: str) -> Optional[str]:
        if isinstance(expression, LiteralExpression):
            return None

        if isinstance(expression, MeasureExpression):
            if expression.ref.cube_name != target_cube_name:
                return f"cross-cube reference '{expression.ref.label}' is outside the native subset"
            return None

        if isinstance(expression, AttributeExpression):
            return f"attribute reference '{expression.ref.label}' is outside the native subset"

        if isinstance(expression, ChainedAttributeExpression):
            return f"chained attribute reference '{expression.ref.label}' is outside the native subset"

        if isinstance(expression, DimensionFunctionExpression):
            if expression.ref.cube_name != target_cube_name:
                return f"dimension-function reference '{expression.ref.label}' is outside the native subset (cross-cube)"
            return None

        if isinstance(expression, FunctionCallExpression):
            for arg in expression.args:
                arg_reason = self._native_unsupported_reason(arg, target_cube_name)
                if arg_reason is not None:
                    return arg_reason
            return None

        if isinstance(expression, UnaryExpression):
            return self._native_unsupported_reason(expression.operand, target_cube_name)

        if isinstance(expression, (BinaryExpression, ComparisonExpression)):
            left_reason = self._native_unsupported_reason(expression.left, target_cube_name)
            if left_reason is not None:
                return left_reason
            return self._native_unsupported_reason(expression.right, target_cube_name)

        if isinstance(expression, MethodExpression):
            if expression.method_name == "align":
                return self._native_align_unsupported_reason(expression, target_cube_name)
            if expression.method_name == "growth":
                return self._native_growth_unsupported_reason(expression, target_cube_name)
            if expression.method_name == "shift":
                return self._native_shift_unsupported_reason(expression, target_cube_name)
            if expression.method_name == "rolling.sum":
                return self._native_rolling_unsupported_reason(expression, target_cube_name)
            if expression.method_name == "ytd":
                return self._native_ytd_unsupported_reason(expression, target_cube_name)
            if expression.method_name in self._CONSOLIDATED_AGGREGATE_FUNCTIONS:
                return self._native_consolidated_aggregate_unsupported_reason(expression, target_cube_name)

            base_reason = self._native_unsupported_reason(expression.base, target_cube_name)
            if base_reason is not None:
                return base_reason
            for arg in expression.args:
                arg_reason = self._native_unsupported_reason(arg, target_cube_name)
                if arg_reason is not None:
                    return arg_reason
            for value in expression.kwargs.values():
                value_reason = self._native_unsupported_reason(value, target_cube_name)
                if value_reason is not None:
                    return value_reason
            if expression.method_name == "where":
                if len(expression.args) != 1:
                    return "where requires exactly one condition"
                return None
            if expression.method_name in (
                "abs",
                "round",
                "mod",
                "int_part",
                "upper",
                "lower",
                "trim",
                "substring",
                "delete",
                "scan",
                "length",
                "char_code",
                "to_char",
                "exp",
                "ln",
                "log10",
                "sqrt",
                "sin",
                "cos",
                "tan",
                "asin",
                "acos",
                "atan",
                "insert",
            ):
                return None
            if expression.method_name in ("to_string", "to_number"):
                if not isinstance(expression.base, MeasureExpression):
                    return (
                        f"'{expression.method_name}(...)' is only supported directly on a "
                        "measure reference in the current bounded subset"
                    )
                return None
            return f"method '{expression.method_name}' is outside the native subset"

        if isinstance(expression, RollingExpression):
            return (
                "rolling(...) requires summing or aggregating a variable-size window of cells, which "
                "is a multi-cell aggregation that does not reduce to a single bounded DB(...)/ATTRS(...) "
                "lookup; it stays on the Python materialization backend by design"
            )

        if isinstance(expression, CaseExpression):
            for condition, value in expression.cases:
                condition_reason = self._native_unsupported_reason(condition, target_cube_name)
                if condition_reason is not None:
                    return condition_reason
                value_reason = self._native_unsupported_reason(value, target_cube_name)
                if value_reason is not None:
                    return value_reason
            return self._native_unsupported_reason(expression.default, target_cube_name)

        return f"expression type '{type(expression).__name__}' is not supported yet"

    def _compile_native_expression(self, expression: Expression, target_cube_name: str) -> str:
        if isinstance(expression, LiteralExpression):
            if isinstance(expression.value, str):
                return f"'{expression.value}'"
            return repr(expression.value)

        if isinstance(expression, MeasureExpression):
            if expression.ref.cube_name != target_cube_name:
                raise ValueError(f"Cross-cube reference '{expression.ref.label}' cannot compile natively in Phase 2 MVP")
            return self._compile_native_measure_reference(
                cube_name=target_cube_name,
                measure_name=expression.ref.measure_name,
            )

        if isinstance(expression, DimensionFunctionExpression):
            return self._compile_dimension_function_expression(expression, target_cube_name)

        if isinstance(expression, FunctionCallExpression):
            args = ", ".join(
                self._compile_native_expression(arg, target_cube_name) for arg in expression.args
            )
            return f"{expression.function_name}({args})"

        if isinstance(expression, UnaryExpression):
            return f"({expression.operator}{self._compile_native_expression(expression.operand, target_cube_name)})"

        if isinstance(expression, ComparisonExpression):
            return self._compile_native_binary_expression(expression, target_cube_name)

        if isinstance(expression, BinaryExpression):
            return self._compile_native_binary_expression(expression, target_cube_name)

        if isinstance(expression, MethodExpression):
            if expression.method_name == "where":
                return self._compile_native_where_expression(expression, target_cube_name)
            if expression.method_name == "align":
                return self._compile_native_align_expression(expression, target_cube_name)
            if expression.method_name == "growth":
                return self._compile_native_growth_expression(expression, target_cube_name)
            if expression.method_name == "shift":
                return self._compile_native_shift_expression(expression, target_cube_name)
            if expression.method_name == "rolling.sum":
                return self._compile_native_rolling_expression(expression, target_cube_name)
            if expression.method_name == "ytd":
                return self._compile_native_ytd_expression(expression, target_cube_name)
            if expression.method_name in self._CONSOLIDATED_AGGREGATE_FUNCTIONS:
                return self._compile_native_consolidated_aggregate_expression(expression, target_cube_name)
            if expression.method_name == "abs":
                return f"ABS({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "round":
                ndigits = self._compile_native_expression(expression.args[0], target_cube_name)
                return f"ROUND({self._compile_native_expression(expression.base, target_cube_name)}, {ndigits})"
            if expression.method_name == "mod":
                divisor = self._compile_native_expression(expression.args[0], target_cube_name)
                return f"MOD({self._compile_native_expression(expression.base, target_cube_name)}, {divisor})"
            if expression.method_name == "int_part":
                return f"INT({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "upper":
                return f"UPPER({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "lower":
                return f"LOWER({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "trim":
                return f"TRIM({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "substring":
                start = self._compile_native_expression(expression.args[0], target_cube_name)
                length = self._compile_native_expression(expression.args[1], target_cube_name)
                return f"SUBST({self._compile_native_expression(expression.base, target_cube_name)}, {start}, {length})"
            if expression.method_name == "to_string":
                length = self._compile_native_expression(expression.args[0], target_cube_name)
                decimals = self._compile_native_expression(expression.args[1], target_cube_name)
                return f"STR({self._compile_native_expression(expression.base, target_cube_name)}, {length}, {decimals})"
            if expression.method_name == "to_number":
                return f"NUMBR({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "delete":
                start = self._compile_native_expression(expression.args[0], target_cube_name)
                length = self._compile_native_expression(expression.args[1], target_cube_name)
                return f"DELET({self._compile_native_expression(expression.base, target_cube_name)}, {start}, {length})"
            if expression.method_name == "scan":
                substring = self._compile_native_expression(expression.args[0], target_cube_name)
                return f"SCAN({substring}, {self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "length":
                return f"LONG({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "char_code":
                position = self._compile_native_expression(expression.args[0], target_cube_name)
                return f"CODE({self._compile_native_expression(expression.base, target_cube_name)}, {position})"
            if expression.method_name == "to_char":
                return f"CHAR({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name in self._SINGLE_ARG_MATH_FUNCTIONS:
                function_name = self._SINGLE_ARG_MATH_FUNCTIONS[expression.method_name]
                return f"{function_name}({self._compile_native_expression(expression.base, target_cube_name)})"
            if expression.method_name == "insert":
                text = self._compile_native_expression(expression.args[0], target_cube_name)
                position = self._compile_native_expression(expression.args[1], target_cube_name)
                return f"INSRT({text}, {self._compile_native_expression(expression.base, target_cube_name)}, {position})"
            raise ValueError(f"Method '{expression.method_name}' is not supported by the native compiler")

        if isinstance(expression, CaseExpression):
            return self._compile_native_case_expression(expression, target_cube_name)

        raise ValueError(f"Expression type '{type(expression).__name__}' is not supported by the native compiler")

    def _compile_dimension_function_expression(
        self, expression: DimensionFunctionExpression, target_cube_name: str
    ) -> str:
        ref = expression.ref
        if ref.element_name is None:
            element_coordinate = f"!{ref.dimension_name}"
        else:
            element_coordinate = self._compile_dimension_element_coordinate(
                cube_name=ref.cube_name,
                dimension_name=ref.dimension_name,
                element_name=ref.element_name,
                explicit_hierarchy_name=None,
            )
        dimension_literal = f"'{ref.dimension_name}'"

        if ref.function_name == "ellev":
            return f"ELLEV({dimension_literal}, {element_coordinate})"
        if ref.function_name == "elparn":
            return f"ELPARN({dimension_literal}, {element_coordinate})"
        if ref.function_name == "elcompn":
            return f"ELCOMPN({dimension_literal}, {element_coordinate})"
        if ref.function_name == "elcomp":
            position = ref.args[0]
            return f"ELCOMP({dimension_literal}, {element_coordinate}, {position})"
        if ref.function_name == "eliscomp":
            parent_coordinate = self._compile_dimension_element_coordinate(
                cube_name=ref.cube_name,
                dimension_name=ref.dimension_name,
                element_name=ref.args[0],
                explicit_hierarchy_name=None,
            )
            return f"ELISCOMP({dimension_literal}, {element_coordinate}, {parent_coordinate})"
        if ref.function_name == "elpar":
            index = ref.args[0]
            return f"ELPAR({dimension_literal}, {element_coordinate}, {index})"
        if ref.function_name == "elispar":
            child_coordinate = self._compile_dimension_element_coordinate(
                cube_name=ref.cube_name,
                dimension_name=ref.dimension_name,
                element_name=ref.args[0],
                explicit_hierarchy_name=None,
            )
            return f"ELISPAR({dimension_literal}, {element_coordinate}, {child_coordinate})"
        if ref.function_name == "elisanc":
            descendant_coordinate = self._compile_dimension_element_coordinate(
                cube_name=ref.cube_name,
                dimension_name=ref.dimension_name,
                element_name=ref.args[0],
                explicit_hierarchy_name=None,
            )
            return f"ELISANC({dimension_literal}, {element_coordinate}, {descendant_coordinate})"
        if ref.function_name == "elweight":
            parent_coordinate = self._compile_dimension_element_coordinate(
                cube_name=ref.cube_name,
                dimension_name=ref.dimension_name,
                element_name=ref.args[0],
                explicit_hierarchy_name=None,
            )
            return f"ELWEIGHT({dimension_literal}, {parent_coordinate}, {element_coordinate})"
        raise ValueError(f"dimension function '{ref.function_name}' is not supported by the native compiler")

    def _compile_native_binary_expression(self, expression: BinaryExpression, target_cube_name: str) -> str:
        left = self._compile_native_expression(expression.left, target_cube_name)
        right = self._compile_native_expression(expression.right, target_cube_name)
        if expression.operator == "!=":
            operator = "<>"
        elif expression.operator == "==":
            operator = "="
        else:
            operator = expression.operator
        return f"({left} {operator} {right})"

    def _compile_native_measure_reference(self, cube_name: str, measure_name: str) -> str:
        metadata = self._get_cube_metadata(cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return f"['{measure_name}']"

        hierarchy_name, ambiguity_reason = self._resolve_dimension_hierarchy(
            cube_name=cube_name,
            dimension_name=metadata.measure_dimension_name,
            explicit_hierarchy_name=None,
        )
        if hierarchy_name is None:
            if ambiguity_reason is not None:
                raise ValueError(ambiguity_reason)
            return f"['{measure_name}']"

        return (
            f"['{metadata.measure_dimension_name}':'{hierarchy_name}':'{measure_name}']"
        )

    def _compile_native_measure_reference_with_area(
        self, cube_name: str, measure_name: str, area: Tuple[Tuple[str, str], ...]
    ) -> str:
        """Builds the rule-statement LHS for an area-scoped target (`CubeRef.area(...)`), e.g.
        `[Measure-coordinate, 'US']` for `Region="US"`. Mirrors TM1's own rule-LHS syntax: one
        coordinate per fixed dimension, every other dimension left as an implicit passthrough.
        Requires metadata (unlike the whole-measure case) because every fixed element must be
        validated against a real dimension of the cube and hierarchy-resolved the same way every
        other fixed coordinate in this compiler is -- no silent guessing for area targets.
        """
        if not area:
            return self._compile_native_measure_reference(cube_name, measure_name)

        metadata = self._get_cube_metadata(cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            raise ValueError(
                f"Cannot compile area-scoped rule target for '{cube_name}'.'{measure_name}' "
                "without cube metadata to validate and hierarchy-resolve the fixed dimension(s)"
            )

        area_by_dimension = dict(area)
        unknown_dimensions = sorted(set(area_by_dimension) - set(metadata.dimensions))
        if unknown_dimensions:
            raise ValueError(
                f"Area-scoped rule target for '{cube_name}'.'{measure_name}' references "
                f"unknown dimension(s) of that cube: {', '.join(unknown_dimensions)}"
            )
        if metadata.measure_dimension_name in area_by_dimension:
            raise ValueError(
                f"Area-scoped rule target for '{cube_name}'.'{measure_name}' must not fix the "
                "measure dimension itself; register a separate measure target instead"
            )

        coordinates: List[str] = [
            self._compile_dimension_element_coordinate(
                cube_name=cube_name,
                dimension_name=metadata.measure_dimension_name,
                element_name=measure_name,
                explicit_hierarchy_name=None,
            )
        ]
        for dimension_name in metadata.dimensions:
            if dimension_name == metadata.measure_dimension_name or dimension_name not in area_by_dimension:
                continue
            coordinates.append(
                self._compile_dimension_element_coordinate(
                    cube_name=cube_name,
                    dimension_name=dimension_name,
                    element_name=area_by_dimension[dimension_name],
                    explicit_hierarchy_name=None,
                )
            )
        return f"[{', '.join(coordinates)}]"

    def _compile_native_where_expression(self, expression: MethodExpression, target_cube_name: str) -> str:
        value = self._compile_native_expression(expression.base, target_cube_name)
        condition = self._compile_native_expression(expression.args[0], target_cube_name)
        return f"IF({condition}, {value}, STET)"

    def _compile_native_growth_expression(self, expression: MethodExpression, target_cube_name: str) -> str:
        unsupported_reason = self._native_growth_unsupported_reason(expression, target_cube_name)
        if unsupported_reason is not None:
            raise ValueError(f"growth(...) cannot compile natively: {unsupported_reason}")

        base = self._compile_native_expression(expression.base, target_cube_name)
        baseline_ref = self._resolve_growth_baseline_ref(expression, target_cube_name)
        assert baseline_ref is not None
        baseline = self._compile_native_measure_reference(
            cube_name=baseline_ref.cube_name,
            measure_name=baseline_ref.measure_name,
        )
        return f"IF(({baseline} <> 0), (({base} - {baseline}) / {baseline}), STET)"

    def _compile_native_align_expression(self, expression: MethodExpression, target_cube_name: str) -> str:
        unsupported_reason = self._native_align_unsupported_reason(expression, target_cube_name)
        if unsupported_reason is not None:
            raise ValueError(f"align(...) cannot compile natively: {unsupported_reason}")

        source_measure = expression.base.ref
        source_metadata = self._get_cube_metadata(source_measure.cube_name)
        target_metadata = self._get_cube_metadata(target_cube_name)
        assert source_metadata is not None
        assert target_metadata is not None

        coordinates: List[str] = []
        for dimension_name in source_metadata.dimensions:
            if dimension_name == source_metadata.measure_dimension_name:
                coordinates.append(
                    self._compile_dimension_element_coordinate(
                        cube_name=source_measure.cube_name,
                        dimension_name=dimension_name,
                        element_name=source_measure.measure_name,
                        explicit_hierarchy_name=None,
                    )
                )
                continue

            if dimension_name in expression.kwargs:
                coordinates.append(
                    self._compile_native_lookup_coordinate(
                        cube_name=source_measure.cube_name,
                        dimension_name=dimension_name,
                        expression=expression.kwargs[dimension_name],
                    )
                )
                continue

            if dimension_name in target_metadata.dimensions:
                coordinates.append(f"!{dimension_name}")
                continue

            raise ValueError(
                f"align(...) could not resolve source dimension '{dimension_name}' "
                f"for cube '{source_measure.cube_name}'."
            )

        return f"DB('{source_measure.cube_name}', {', '.join(coordinates)})"

    def _native_growth_unsupported_reason(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[str]:
        if expression.args:
            return "growth(...) does not support positional arguments in the native subset"
        if set(expression.kwargs) != {"vs"}:
            return "growth(...) requires exactly one 'vs' keyword argument in the native subset"

        base_reason = self._native_unsupported_reason(expression.base, target_cube_name)
        if base_reason is not None:
            return base_reason

        baseline_ref = self._resolve_growth_baseline_ref(expression, target_cube_name)
        if baseline_ref is None:
            return (
                "growth(...) baseline must be a same-cube measure reference or a target-cube "
                "measure name string in the native subset"
            )
        if baseline_ref.cube_name != target_cube_name:
            return "growth(...) baseline must come from the target cube in the native subset"
        return None

    def _native_shift_unsupported_reason(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[str]:
        if expression.args:
            return "shift(...) does not support positional arguments in the native subset"
        if len(expression.kwargs) != 1:
            return "shift(...) requires exactly one dimension keyword argument in the native subset"
        if not isinstance(expression.base, MeasureExpression):
            return "shift(...) requires a same-cube measure base in the native subset"
        if expression.base.ref.cube_name != target_cube_name:
            return "shift(...) base measure must come from the target cube in the native subset"

        dimension_name, mapping_expression = next(iter(expression.kwargs.items()))
        if not isinstance(mapping_expression, LiteralExpression):
            return (
                "shift(...) requires a literal attribute-name string or integer offset for the "
                "shift dimension"
            )

        metadata = self._get_cube_metadata(target_cube_name)
        if metadata is None:
            return "shift(...) needs cube metadata to resolve the shift dimension"
        if dimension_name not in metadata.dimensions:
            return f"shift(...) dimension '{dimension_name}' is not part of cube '{target_cube_name}'"
        if dimension_name == metadata.measure_dimension_name:
            return "shift(...) cannot target the measure dimension"

        mapping_value = mapping_expression.value

        if isinstance(mapping_value, str):
            attribute_name = mapping_value
            available_attributes = metadata.dimension_attributes.get(dimension_name, ())
            if attribute_name not in available_attributes:
                return (
                    f"shift(...) attribute '{attribute_name}' is not available for dimension "
                    f"'{dimension_name}' in cube '{target_cube_name}'"
                )

            hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
                cube_name=target_cube_name,
                dimension_name=dimension_name,
                explicit_hierarchy_name=None,
            )
            if hierarchy_reason is not None:
                return hierarchy_reason

            return None

        if isinstance(mapping_value, int) and not isinstance(mapping_value, bool):
            leaf_elements = metadata.dimension_leaf_elements.get(dimension_name, ())
            if not leaf_elements:
                return (
                    f"shift(...) numeric offset for dimension '{dimension_name}' needs leaf-element "
                    "metadata to prove the elements are numeric"
                )
            if any(not self._is_numeric_element_name(element_name) for element_name in leaf_elements):
                return (
                    f"shift(...) numeric offset requires dimension '{dimension_name}' leaf elements "
                    "to be numeric strings in the native subset"
                )
            return None

        return "shift(...) mapping value must be a literal attribute-name string or integer offset"

    @staticmethod
    def _is_numeric_element_name(element_name: Any) -> bool:
        text = str(element_name).strip()
        if text.startswith("-"):
            text = text[1:]
        return bool(text) and text.isdigit()

    @staticmethod
    def _resolve_shift_spec(
        expression: MethodExpression,
    ) -> Optional[Tuple[str, str, Union[str, int]]]:
        if len(expression.kwargs) != 1:
            return None
        dimension_name, mapping_expression = next(iter(expression.kwargs.items()))
        if not isinstance(mapping_expression, LiteralExpression):
            return None
        mapping_value = mapping_expression.value
        if isinstance(mapping_value, str):
            return dimension_name, "attribute", mapping_value
        if isinstance(mapping_value, int) and not isinstance(mapping_value, bool):
            return dimension_name, "numeric_offset", mapping_value
        return None

    def _compile_native_shift_expression(self, expression: MethodExpression, target_cube_name: str) -> str:
        unsupported_reason = self._native_shift_unsupported_reason(expression, target_cube_name)
        if unsupported_reason is not None:
            raise ValueError(f"shift(...) cannot compile natively: {unsupported_reason}")

        resolved = self._resolve_shift_spec(expression)
        assert resolved is not None
        dimension_name, mode, mapping_value = resolved
        assert isinstance(expression.base, MeasureExpression)
        source_measure = expression.base.ref

        metadata = self._get_cube_metadata(target_cube_name)
        assert metadata is not None

        coordinates: List[str] = []
        for current_dimension_name in metadata.dimensions:
            if current_dimension_name == metadata.measure_dimension_name:
                coordinates.append(
                    self._compile_dimension_element_coordinate(
                        cube_name=target_cube_name,
                        dimension_name=current_dimension_name,
                        element_name=source_measure.measure_name,
                        explicit_hierarchy_name=None,
                    )
                )
            elif current_dimension_name == dimension_name:
                if mode == "attribute":
                    coordinates.append(
                        f"ATTRS('{dimension_name}', !{dimension_name}, '{mapping_value}')"
                    )
                else:
                    coordinates.append(f"STR(NUMBR(!{dimension_name}) + ({mapping_value}))")
            else:
                coordinates.append(f"!{current_dimension_name}")

        return f"DB('{target_cube_name}', {', '.join(coordinates)})"

    def _native_rolling_unsupported_reason(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[str]:
        if expression.args:
            return "rolling(...).sum() does not support positional arguments in the native subset"
        if len(expression.kwargs) != 1:
            return "rolling(...).sum() requires exactly one dimension keyword argument in the native subset"
        if not isinstance(expression.base, MeasureExpression):
            return "rolling(...).sum() requires a same-cube measure base in the native subset"
        if expression.base.ref.cube_name != target_cube_name:
            return "rolling(...).sum() base measure must come from the target cube in the native subset"

        resolved = self._resolve_rolling_spec(expression)
        if resolved is None:
            return "rolling(...).sum() requires a literal positive integer window for the rolling dimension"
        dimension_name, _window = resolved

        metadata = self._get_cube_metadata(target_cube_name)
        if metadata is None:
            return "rolling(...).sum() needs cube metadata to resolve the rolling dimension"
        if dimension_name not in metadata.dimensions:
            return f"rolling(...).sum() dimension '{dimension_name}' is not part of cube '{target_cube_name}'"
        if dimension_name == metadata.measure_dimension_name:
            return "rolling(...).sum() cannot target the measure dimension"

        leaf_elements = metadata.dimension_leaf_elements.get(dimension_name, ())
        if not leaf_elements:
            return (
                f"rolling(...).sum() for dimension '{dimension_name}' needs leaf-element metadata "
                "to prove the elements are numeric, the same bounding fact shift(...)'s numeric-"
                "offset form relies on"
            )
        if any(not self._is_numeric_element_name(element_name) for element_name in leaf_elements):
            return (
                f"rolling(...).sum() requires dimension '{dimension_name}' leaf elements to be "
                "numeric strings in the native subset, because the window is unrolled at compile "
                "time into a bounded sum of numeric-offset shift(...) terms"
            )
        return None

    @staticmethod
    def _resolve_rolling_spec(expression: MethodExpression) -> Optional[Tuple[str, int]]:
        if len(expression.kwargs) != 1:
            return None
        dimension_name, window_expression = next(iter(expression.kwargs.items()))
        if not isinstance(window_expression, LiteralExpression):
            return None
        window_value = window_expression.value
        if not isinstance(window_value, int) or isinstance(window_value, bool) or window_value < 1:
            return None
        return dimension_name, window_value

    def _compile_native_rolling_expression(self, expression: MethodExpression, target_cube_name: str) -> str:
        """Loop-unrolls rolling(dimension=window).sum() into a bounded sum of shift(...) terms.

        The window is known and finite at compile time (metadata proves the dimension's leaf set),
        so `rolling(dim=window).sum()` lowers to `shift(dim=0) + shift(dim=-1) + ... +
        shift(dim=-(window-1))`, reusing the existing numeric-offset shift(...) lowering unchanged
        per term. No boundary clamp is applied for a current element near the start of the
        dimension's known range: a synthesized offset coordinate that doesn't name an existing
        element is left to TM1's own DB(...) behavior (it evaluates to 0), exactly the same
        assumption shift(...)'s numeric-offset form already makes for a single term.
        """
        unsupported_reason = self._native_rolling_unsupported_reason(expression, target_cube_name)
        if unsupported_reason is not None:
            raise ValueError(f"rolling(...).sum() cannot compile natively: {unsupported_reason}")

        resolved = self._resolve_rolling_spec(expression)
        assert resolved is not None
        dimension_name, window = resolved

        terms = [
            self._compile_native_shift_expression(
                MethodExpression(
                    "shift",
                    expression.base,
                    kwargs={dimension_name: LiteralExpression(-offset)},
                ),
                target_cube_name,
            )
            for offset in range(window)
        ]
        return " + ".join(terms)

    @staticmethod
    def _resolve_ytd_spec(expression: MethodExpression) -> Optional[Tuple[str, str]]:
        if set(expression.kwargs) != {"dimension", "period_number_attribute"}:
            return None
        dimension_expression = expression.kwargs.get("dimension")
        attribute_expression = expression.kwargs.get("period_number_attribute")
        if not isinstance(dimension_expression, LiteralExpression) or not isinstance(
            dimension_expression.value, str
        ):
            return None
        if not isinstance(attribute_expression, LiteralExpression) or not isinstance(
            attribute_expression.value, str
        ):
            return None
        return dimension_expression.value, attribute_expression.value

    def _native_ytd_unsupported_reason(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[str]:
        if expression.args:
            return "ytd(...) does not support positional arguments in the native subset"
        if not isinstance(expression.base, MeasureExpression):
            return "ytd(...) requires a same-cube measure base in the native subset"
        if expression.base.ref.cube_name != target_cube_name:
            return "ytd(...) base measure must come from the target cube in the native subset"

        resolved = self._resolve_ytd_spec(expression)
        if resolved is None:
            return (
                "ytd(...) requires explicit literal string 'dimension' and 'period_number_attribute' "
                "keyword arguments in the native subset"
            )
        dimension_name, period_attribute = resolved

        metadata = self._get_cube_metadata(target_cube_name)
        if metadata is None:
            return "ytd(...) needs cube metadata to resolve the time dimension"
        if dimension_name not in metadata.dimensions:
            return f"ytd(...) dimension '{dimension_name}' is not part of cube '{target_cube_name}'"
        if dimension_name == metadata.measure_dimension_name:
            return "ytd(...) cannot target the measure dimension"

        leaf_elements = metadata.dimension_leaf_elements.get(dimension_name, ())
        if not leaf_elements:
            return (
                f"ytd(...) for dimension '{dimension_name}' needs leaf-element metadata to prove "
                "the elements are numeric, the same bounding fact shift(...)'s numeric-offset form "
                "relies on"
            )
        if any(not self._is_numeric_element_name(element_name) for element_name in leaf_elements):
            return (
                f"ytd(...) requires dimension '{dimension_name}' leaf elements to be numeric strings "
                "in the native subset, because the year-to-date window is unrolled at compile time "
                "into a bounded sum of numeric-offset shift(...) terms"
            )

        available_attributes = metadata.dimension_attributes.get(dimension_name, ())
        if period_attribute not in available_attributes:
            return (
                f"ytd(...) period-number attribute '{period_attribute}' is not available for "
                f"dimension '{dimension_name}' in cube '{target_cube_name}'"
            )

        attribute_values = metadata.dimension_attribute_values.get(dimension_name, {})
        for element_name in leaf_elements:
            raw_value = str(attribute_values.get(element_name, {}).get(period_attribute, "")).strip()
            if not raw_value.lstrip("-").isdigit() or int(raw_value) < 1:
                return (
                    f"ytd(...) period-number attribute '{period_attribute}' must resolve to a "
                    f"positive integer for every leaf element of dimension '{dimension_name}', to "
                    "bound the per-element unrolled term count"
                )

        hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
            cube_name=target_cube_name,
            dimension_name=dimension_name,
            explicit_hierarchy_name=None,
        )
        if hierarchy_reason is not None:
            return hierarchy_reason

        return None

    @staticmethod
    def _resolve_consolidated_aggregate_flag(expression: MethodExpression) -> Optional[int]:
        flag_expression = expression.kwargs.get("__flag__")
        if not isinstance(flag_expression, LiteralExpression) or not isinstance(flag_expression.value, int):
            return None
        return flag_expression.value

    def _native_consolidated_aggregate_unsupported_reason(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[str]:
        """Gate for ConsolidatedMax/Min/Avg/Count/CountUnique -- the non-additive consolidation
        override family. See dev/07_handover_status.md's "non-sum consolidation overrides" entry
        for the confirmed TM1 function signature (flag, cube name, one literal/passthrough
        coordinate per cube dimension in order, including the measure dimension) and why no new
        feeder statements are needed for this shape: per IBM/Cubewise documentation, these
        functions read directly off the aggregated measure's own existing leaf data the same way
        TM1's native structural (sum) consolidation does -- a consolidation *read*, not a
        rule-derived `DB(...)`-style lookup. That distinction, not the cube boundary, is what
        determines whether a feeder is needed, so it holds the same way for a foreign 'CubeName'
        argument as it does for the target's own cube; see
        `dev/19_ordering_consolidation_and_indirect_db_plan.md` Work Item 2 for the full reasoning.

        Cross-cube is therefore allowed (closed 2026-06-25) for the bounded subset where every
        source-cube dimension other than the measure dimension resolves one of three ways, reusing
        exactly the same per-dimension mapping proof `align(...)` uses:
        - a same-named dimension in the target cube (implicit passthrough), or
        - an explicit literal/element/target-cube-attribute `dimension_overrides` kwarg, or
        - fails closed (a named reason) for anything else, including a value-driven mapping.
        """
        if expression.args:
            return (
                f"{expression.method_name}(...) does not support positional arguments in the "
                "native subset; use flag=... and dimension=literal_element keyword arguments"
            )
        if not isinstance(expression.base, MeasureExpression):
            return f"{expression.method_name}(...) requires a measure lookup base in the native subset"

        source_cube_name = expression.base.ref.cube_name
        is_cross_cube = source_cube_name != target_cube_name

        flag = self._resolve_consolidated_aggregate_flag(expression)
        if flag is None:
            return f"{expression.method_name}(...) requires a literal integer 'flag' argument"
        if flag not in (0, 1, 2, 3):
            return (
                f"{expression.method_name}(...) 'flag' must be 0 (plain), 1 (consolidation-weighted), "
                "2 (ignore zeros), or 3 (weighted and ignore zeros)"
            )

        metadata = self._get_cube_metadata(source_cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return f"{expression.method_name}(...) needs measure dimension metadata to resolve the cube layout"

        base_measure_type = self._measure_type_for(metadata, expression.base.ref.measure_name)
        if not self._is_numeric_measure_type(base_measure_type):
            return (
                f"{expression.method_name}(...) requires the aggregated measure "
                f"'{expression.base.ref.measure_name}' to resolve to a Numeric element type"
            )

        target_metadata: Optional[CubeMetadata] = None
        target_dimension_names: set = set()
        if is_cross_cube:
            target_metadata = self._get_cube_metadata(target_cube_name)
            if target_metadata is None or not target_metadata.dimensions:
                return (
                    f"{expression.method_name}(...) needs target cube metadata to resolve a "
                    "cross-cube dimension mapping"
                )
            target_dimension_names = set(target_metadata.dimensions)

        for dimension_name, override_expression in expression.kwargs.items():
            if dimension_name == "__flag__":
                continue
            if dimension_name not in metadata.dimensions:
                return (
                    f"{expression.method_name}(...) dimension override '{dimension_name}' is not "
                    f"part of cube '{source_cube_name}'"
                )
            if dimension_name == metadata.measure_dimension_name:
                return f"{expression.method_name}(...) cannot override the measure dimension"
            if not is_cross_cube:
                if not isinstance(override_expression, LiteralExpression) or not isinstance(
                    override_expression.value, str
                ):
                    return (
                        f"{expression.method_name}(...) dimension override '{dimension_name}' must be a "
                        "literal element name string in the native subset"
                    )
                hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
                    cube_name=source_cube_name,
                    dimension_name=dimension_name,
                    explicit_hierarchy_name=None,
                )
                if hierarchy_reason is not None:
                    return hierarchy_reason
                continue
            mapping_reason = self._native_dimension_mapping_unsupported_reason(
                method_label=f"{expression.method_name}(...)",
                source_cube_name=source_cube_name,
                source_dimension_name=dimension_name,
                mapped_expression=override_expression,
                target_cube_name=target_cube_name,
                target_dimension_names=target_dimension_names,
            )
            if mapping_reason is not None:
                return mapping_reason

        if is_cross_cube:
            unresolved_dimensions = [
                dimension_name
                for dimension_name in metadata.dimensions
                if dimension_name != metadata.measure_dimension_name
                and dimension_name not in expression.kwargs
                and dimension_name not in target_dimension_names
            ]
            if unresolved_dimensions:
                return (
                    f"{expression.method_name}(...) cannot infer source dimension(s) "
                    + ", ".join(f"'{dimension_name}'" for dimension_name in unresolved_dimensions)
                    + " for the cross-cube mapping; pass an explicit dimension_overrides keyword for each"
                )

        return None

    def _compile_native_consolidated_aggregate_expression(
        self, expression: MethodExpression, target_cube_name: str
    ) -> str:
        unsupported_reason = self._native_consolidated_aggregate_unsupported_reason(expression, target_cube_name)
        if unsupported_reason is not None:
            raise ValueError(f"{expression.method_name}(...) cannot compile natively: {unsupported_reason}")

        function_name = self._CONSOLIDATED_AGGREGATE_FUNCTIONS[expression.method_name]
        flag = self._resolve_consolidated_aggregate_flag(expression)
        base_measure = expression.base.ref
        source_cube_name = base_measure.cube_name
        metadata = self._get_cube_metadata(source_cube_name)
        assert metadata is not None

        coordinates: List[str] = [str(flag), f"'{source_cube_name}'"]
        for dimension_name in metadata.dimensions:
            if dimension_name == metadata.measure_dimension_name:
                coordinates.append(
                    self._compile_dimension_element_coordinate(
                        cube_name=source_cube_name,
                        dimension_name=dimension_name,
                        element_name=base_measure.measure_name,
                        explicit_hierarchy_name=None,
                    )
                )
                continue
            if dimension_name in expression.kwargs:
                coordinates.append(
                    self._compile_native_lookup_coordinate(
                        cube_name=source_cube_name,
                        dimension_name=dimension_name,
                        expression=expression.kwargs[dimension_name],
                    )
                )
                continue
            coordinates.append(f"!{dimension_name}")

        return f"{function_name}({', '.join(coordinates)})"

    def _compile_native_ytd_expression(self, expression: MethodExpression, target_cube_name: str) -> str:
        """Loop-unrolls ytd(dimension=..., period_number_attribute=...) into a bounded, gated sum.

        Unlike rolling(...).sum(), the term count varies per current element (the current period's
        position within its year), not a fixed window. The period-number attribute (e.g. "Month
        Number", 1-12) provides the bounding fact: for an element at position `p`, exactly `p` terms
        (offsets 0 through -(p-1)) belong in the sum. Since a single native rule must hold for every
        element of `!dimension` at once, this compiles to a fixed maximal number of terms (the
        attribute's known maximum across all leaf elements), each gated by
        `IF(NUMBR(ATTRS(...)) > k, shift(dim=-k), 0)` so only the terms within the current element's
        own year actually contribute.
        """
        unsupported_reason = self._native_ytd_unsupported_reason(expression, target_cube_name)
        if unsupported_reason is not None:
            raise ValueError(f"ytd(...) cannot compile natively: {unsupported_reason}")

        resolved = self._resolve_ytd_spec(expression)
        assert resolved is not None
        dimension_name, period_attribute = resolved

        metadata = self._get_cube_metadata(target_cube_name)
        assert metadata is not None
        leaf_elements = metadata.dimension_leaf_elements.get(dimension_name, ())
        attribute_values = metadata.dimension_attribute_values.get(dimension_name, {})
        max_terms = max(
            int(str(attribute_values[element_name][period_attribute]).strip())
            for element_name in leaf_elements
        )

        terms: List[str] = []
        for offset in range(max_terms):
            shift_term = self._compile_native_shift_expression(
                MethodExpression(
                    "shift",
                    expression.base,
                    kwargs={dimension_name: LiteralExpression(-offset)},
                ),
                target_cube_name,
            )
            condition = (
                f"(NUMBR(ATTRS('{dimension_name}', !{dimension_name}, '{period_attribute}')) > {offset})"
            )
            terms.append(f"IF({condition}, {shift_term}, 0)")
        return " + ".join(terms)

    @staticmethod
    def _resolve_growth_baseline_ref(
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[MeasureRef]:
        baseline = expression.kwargs.get("vs")
        if isinstance(baseline, MeasureExpression):
            return baseline.ref
        if isinstance(baseline, LiteralExpression) and isinstance(baseline.value, str):
            return MeasureRef(cube_name=target_cube_name, measure_name=baseline.value)
        return None

    def _compile_native_lookup_coordinate(
        self,
        cube_name: str,
        dimension_name: str,
        expression: Expression,
    ) -> str:
        if isinstance(expression, LiteralExpression):
            if isinstance(expression.value, str):
                return self._compile_dimension_element_coordinate(
                    cube_name=cube_name,
                    dimension_name=dimension_name,
                    element_name=expression.value,
                    explicit_hierarchy_name=None,
                )
            return repr(expression.value)

        if isinstance(expression, AttributeExpression):
            return (
                f"ATTRS('{expression.ref.dimension_name}', "
                f"!{expression.ref.dimension_name}, "
                f"'{expression.ref.attribute_name}')"
            )

        if isinstance(expression, ChainedAttributeExpression):
            inner = (
                f"ATTRS('{expression.ref.first_dimension_name}', "
                f"!{expression.ref.first_dimension_name}, "
                f"'{expression.ref.first_attribute_name}')"
            )
            return (
                f"ATTRS('{expression.ref.second_dimension_name}', {inner}, "
                f"'{expression.ref.second_attribute_name}')"
            )

        if isinstance(expression, ElementExpression):
            return self._compile_dimension_element_coordinate(
                cube_name=expression.ref.cube_name,
                dimension_name=dimension_name,
                element_name=expression.ref.element_name,
                explicit_hierarchy_name=expression.ref.hierarchy_name,
            )

        raise ValueError(
            f"align(...) mapping for source dimension '{dimension_name}' must be a literal "
            "or a target-cube attribute reference."
        )

    def _compile_dimension_element_coordinate(
        self,
        cube_name: Optional[str],
        dimension_name: str,
        element_name: str,
        explicit_hierarchy_name: Optional[str],
    ) -> str:
        hierarchy_name, ambiguity_reason = self._resolve_dimension_hierarchy(
            cube_name=cube_name,
            dimension_name=dimension_name,
            explicit_hierarchy_name=explicit_hierarchy_name,
        )
        if hierarchy_name is None:
            if ambiguity_reason is not None:
                raise ValueError(ambiguity_reason)
            return f"'{element_name}'"

        return f"'{dimension_name}':'{hierarchy_name}':'{element_name}'"

    def _compile_native_case_expression(self, expression: CaseExpression, target_cube_name: str) -> str:
        compiled = self._compile_native_expression(expression.default, target_cube_name)
        for condition, value in reversed(expression.cases):
            compiled_condition = self._compile_native_expression(condition, target_cube_name)
            compiled_value = self._compile_native_expression(value, target_cube_name)
            compiled = f"IF({compiled_condition}, {compiled_value}, {compiled})"
        return compiled

    def _contains_align_expression(self, expression: Expression) -> bool:
        if isinstance(expression, MethodExpression):
            if expression.method_name == "align":
                return True
            return self._contains_align_expression(expression.base) or any(
                self._contains_align_expression(arg) for arg in expression.args
            ) or any(self._contains_align_expression(value) for value in expression.kwargs.values())

        if isinstance(expression, UnaryExpression):
            return self._contains_align_expression(expression.operand)

        if isinstance(expression, (BinaryExpression, ComparisonExpression)):
            return self._contains_align_expression(expression.left) or self._contains_align_expression(expression.right)

        if isinstance(expression, CaseExpression):
            return any(
                self._contains_align_expression(condition) or self._contains_align_expression(value)
                for condition, value in expression.cases
            ) or self._contains_align_expression(expression.default)

        if isinstance(expression, RollingExpression):
            return self._contains_align_expression(expression.base) or any(
                self._contains_align_expression(value) for value in expression.kwargs.values()
            )

        return False

    def _plan_feeders(self, plan: BuildPlan, formula: Formula, allow_origin_ranking: bool = True) -> FeederPlan:
        if formula.scope == "consolidated":
            if self._contains_align_expression(formula.expression):
                consolidated_cross_cube_plan = self._build_consolidated_leaf_cross_cube_feeders(
                    plan, formula
                )
                if consolidated_cross_cube_plan is not None:
                    return consolidated_cross_cube_plan
                return FeederPlan(
                    statements=(),
                    strategy="preview_only_consolidated_cross_cube",
                    rationale=(
                        "C: rule-area native preview exists, but Phase 2 cannot yet prove that every "
                        "leaf measure in the consolidated target independently resolves to a safe "
                        "N:-level cross-cube feeder plan."
                    ),
                    deployment_cube=formula.target.cube_name,
                    preview_only=True,
                )
            consolidated_plan = self._build_consolidated_target_leaf_feeders(plan, formula)
            if consolidated_plan is not None:
                return consolidated_plan
            return FeederPlan(
                statements=(),
                strategy="preview_only_consolidated",
                rationale=(
                    "C: rule-area native preview exists, but Phase 2 cannot yet prove a bounded "
                    "leaf-measure feeder expansion for this consolidated target."
                ),
                deployment_cube=formula.target.cube_name,
                preview_only=True,
            )

        target_reference = self._compile_native_measure_reference(
            cube_name=formula.target.cube_name,
            measure_name=formula.target.measure_name,
        )
        same_cube_source_refs = tuple(
            sorted(
                self._resolve_simple_feeder_source_refs(plan, formula),
                key=lambda item: item.key,
            )
        )
        statements = tuple(
            f"{self._compile_native_measure_reference(ref.cube_name, ref.measure_name)} => {target_reference};"
            for ref in same_cube_source_refs
        )

        if isinstance(formula.expression, MethodExpression) and formula.expression.method_name == "shift":
            shift_plan = self._build_same_cube_shift_feeders(formula)
            if shift_plan is not None:
                return shift_plan
            return FeederPlan(
                statements=(),
                strategy="preview_only_same_cube_shift",
                rationale=(
                    "Same-cube time-shift native preview exists, but Phase 2 cannot yet prove a safe "
                    "reverse-attribute feeder mapping from source elements to shifted target elements."
                ),
                deployment_cube=formula.target.cube_name,
                preview_only=True,
            )

        if isinstance(formula.expression, MethodExpression) and formula.expression.method_name == "rolling.sum":
            rolling_plan = self._build_same_cube_rolling_feeders(formula)
            if rolling_plan is not None:
                return rolling_plan
            return FeederPlan(
                statements=(),
                strategy="preview_only_same_cube_rolling",
                rationale=(
                    "Same-cube rolling-window native preview exists, but Phase 2 cannot yet prove a "
                    "safe reverse-mapping feeder plan for every unrolled offset term in the window."
                ),
                deployment_cube=formula.target.cube_name,
                preview_only=True,
            )

        if isinstance(formula.expression, MethodExpression) and formula.expression.method_name == "ytd":
            ytd_plan = self._build_same_cube_ytd_feeders(formula)
            if ytd_plan is not None:
                return ytd_plan
            return FeederPlan(
                statements=(),
                strategy="preview_only_same_cube_ytd",
                rationale=(
                    "Same-cube year-to-date native preview exists, but Phase 2 cannot yet prove a "
                    "safe reverse-mapping feeder plan for every unrolled, gated offset term."
                ),
                deployment_cube=formula.target.cube_name,
                preview_only=True,
            )

        if (
            isinstance(formula.expression, MethodExpression)
            and formula.expression.method_name in self._CONSOLIDATED_AGGREGATE_FUNCTIONS
        ):
            # No new feeder statements are needed for ConsolidatedMax/Min/Avg/Count/CountUnique,
            # same-cube or cross-cube: per IBM/Cubewise documentation these functions "work
            # directly off the leaf data in the same way as normal consolidation and use the
            # [existing] feeder information" -- a consolidation *read*, not a rule-derived
            # `DB(...)`-style lookup. That distinction (not which cube the literal 'CubeName'
            # argument names) is what determines whether a feeder is needed, so a foreign cube
            # argument doesn't change this: the function still reads that other cube's leaf data
            # the same way TM1's native sum-consolidation reads its own, never via the
            # FEEDERS-gated rule-evaluation mechanism that genuine cross-cube DB(...) lookups (and
            # align(...)) require. See dev/19_ordering_consolidation_and_indirect_db_plan.md Work
            # Item 2. The aggregated measure is also typically the formula's own target measure (a
            # non-additive override of its own default consolidation), which the self-reference
            # cycle guard in `_resolve_simple_feeder_source_refs` already excludes from
            # `same_cube_source_refs` -- self-feeding a measure to itself is invalid in TM1.
            is_cross_cube_aggregate = (
                isinstance(formula.expression.base, MeasureExpression)
                and formula.expression.base.ref.cube_name != formula.target.cube_name
            )
            return FeederPlan(
                statements=statements,
                strategy=(
                    "consolidated_aggregate_cross_cube_mapped"
                    if is_cross_cube_aggregate
                    else "consolidated_aggregate_self_consolidating"
                ),
                rationale=(
                    "ConsolidatedMax/Min/Avg/Count/CountUnique read the aggregated measure's own "
                    "leaf data the same way native sum-consolidation does -- a structural "
                    "consolidation read, not a rule-derived DB(...)-style lookup -- so no "
                    "additional feeder statements are required for the aggregation itself, "
                    "whether the aggregated measure is same-cube or an explicitly cube-mapped "
                    "cross-cube source; any other genuine same-cube driver measures referenced "
                    "elsewhere in the formula are still fed normally."
                ),
                deployment_cube=formula.target.cube_name,
                preview_only=False,
            )

        if self._contains_align_expression(formula.expression):
            align_expressions = tuple(self._iter_align_expressions(formula.expression))
            align_expression_count = len(align_expressions)
            if same_cube_source_refs and align_expression_count == 1 and isinstance(formula.expression, BinaryExpression):
                branch_formula = Formula(
                    target=formula.target,
                    expression=align_expressions[0],
                    scope=formula.scope,
                    feeder_mode=formula.feeder_mode,
                )
                direct_cross_cube_plan = self._build_direct_cross_cube_feeders(branch_formula)
                if direct_cross_cube_plan is not None:
                    return self._merge_same_cube_driver_feeders(
                        formula,
                        direct_cross_cube_plan,
                        formula.target.cube_name,
                        statements,
                        same_cube_source_refs,
                        allow_origin_ranking=allow_origin_ranking,
                    )
                attribute_routed_cross_cube_plan = self._build_attribute_routed_cross_cube_feeders(branch_formula)
                if attribute_routed_cross_cube_plan is not None:
                    return self._merge_same_cube_driver_feeders(
                        formula,
                        attribute_routed_cross_cube_plan,
                        formula.target.cube_name,
                        statements,
                        same_cube_source_refs,
                        allow_origin_ranking=allow_origin_ranking,
                    )
            conditional_cross_cube_plan = self._build_conditional_cross_cube_feeders(formula)
            if conditional_cross_cube_plan is not None:
                if formula.feeder_mode == "ranked_single" and not same_cube_source_refs and len(conditional_cross_cube_plan.origin_candidates) > 1:
                    return self._rank_feeder_origin_candidates(
                        target_cube_name=formula.target.cube_name,
                        candidates=conditional_cross_cube_plan.origin_candidates,
                    )
                return self._merge_same_cube_driver_feeders(
                    formula,
                    conditional_cross_cube_plan,
                    formula.target.cube_name,
                    statements,
                    same_cube_source_refs,
                    allow_origin_ranking=formula.feeder_mode == "ranked_single",
                )
            if same_cube_source_refs and align_expression_count <= 1:
                return FeederPlan(
                    statements=statements,
                    strategy="cross_cube_local_driver",
                    rationale=(
                        "Cross-cube native compilation is allowed because the lookup is driven by "
                        "same-cube sparse source measures that can feed the target directly, while "
                        "the external lookup cube remains read-only in the feeder plan."
                    ),
                    deployment_cube=formula.target.cube_name,
                    preview_only=False,
                )
            direct_cross_cube_plan = self._build_direct_cross_cube_feeders(formula)
            if direct_cross_cube_plan is not None:
                return self._merge_same_cube_driver_feeders(
                    formula,
                    direct_cross_cube_plan,
                    formula.target.cube_name,
                    statements,
                    same_cube_source_refs,
                    allow_origin_ranking=allow_origin_ranking,
                )
            attribute_routed_cross_cube_plan = self._build_attribute_routed_cross_cube_feeders(formula)
            if attribute_routed_cross_cube_plan is not None:
                return self._merge_same_cube_driver_feeders(
                    formula,
                    attribute_routed_cross_cube_plan,
                    formula.target.cube_name,
                    statements,
                    same_cube_source_refs,
                    allow_origin_ranking=allow_origin_ranking,
                )
            rationale = (
                "Cross-cube native preview exists, but Phase 2 cannot yet prove a safe feeder "
                "origin, invertible routing path, and leaf-level target scope for this lookup."
            )
            unresolved_reasons = self._describe_unresolved_cross_cube_feeder_elements(formula)
            if unresolved_reasons:
                rationale += " " + " ".join(unresolved_reasons)
            return FeederPlan(
                statements=(),
                strategy="preview_only_cross_cube",
                rationale=rationale,
                deployment_cube=formula.target.cube_name,
                preview_only=True,
            )

        same_cube_candidates = self._build_same_cube_origin_candidates(
            target_cube_name=formula.target.cube_name,
            same_cube_source_refs=same_cube_source_refs,
            target_measure_name=formula.target.measure_name,
        )
        if formula.feeder_mode == "ranked_single" and len(same_cube_candidates) > 1:
            return self._rank_feeder_origin_candidates(
                target_cube_name=formula.target.cube_name,
                candidates=same_cube_candidates,
            )

        return FeederPlan(
            statements=statements,
            strategy="same_cube_traceback",
            rationale=(
                "Same-cube feeder planning traces rule-calculated intermediates back to their original "
                "same-cube sparse source measures and feeds the final target directly."
            ),
            deployment_cube=formula.target.cube_name,
            preview_only=False,
            origin_candidates=same_cube_candidates,
        )

    def _merge_same_cube_driver_feeders(
        self,
        formula: Formula,
        plan: FeederPlan,
        target_cube_name: str,
        local_driver_statements: tuple[str, ...],
        same_cube_source_refs: tuple[MeasureRef, ...],
        allow_origin_ranking: bool = True,
    ) -> FeederPlan:
        if not local_driver_statements:
            return plan

        local_candidates = self._build_same_cube_origin_candidates(
            target_cube_name=target_cube_name,
            same_cube_source_refs=same_cube_source_refs,
            target_measure_name=formula.target.measure_name,
        )
        all_candidates = (*local_candidates, *plan.origin_candidates)
        if formula.feeder_mode == "ranked_single" and allow_origin_ranking and len(all_candidates) > 1:
            return self._rank_feeder_origin_candidates(
                target_cube_name=target_cube_name,
                candidates=all_candidates,
            )

        deployment_statements_by_cube: Dict[str, set[str]] = defaultdict(set)
        for cube_name, cube_statements in plan.iter_deployment_statements(default_cube=target_cube_name):
            deployment_statements_by_cube[cube_name].update(cube_statements)
        deployment_statements_by_cube[target_cube_name].update(local_driver_statements)

        merged_statements = tuple(
            sorted(
                statement
                for grouped_statements in deployment_statements_by_cube.values()
                for statement in grouped_statements
            )
        )
        merged_deployment_statements = tuple(
            (cube_name, tuple(sorted(grouped_statements)))
            for cube_name, grouped_statements in sorted(deployment_statements_by_cube.items())
        )
        merged_deployment_cube = (
            merged_deployment_statements[0][0] if len(merged_deployment_statements) == 1 else None
        )

        return FeederPlan(
            statements=merged_statements,
            strategy=plan.strategy,
            rationale=(
                plan.rationale
                + " Same-cube measures that also genuinely contribute to the formula's value "
                "(value-composing drivers, zero-gating multipliers, or branch selectors) are fed "
                "directly into the target as well, alongside the cross-cube origin."
            ),
            deployment_cube=merged_deployment_cube,
            deployment_statements=merged_deployment_statements,
            preview_only=plan.preview_only,
            origin_candidates=all_candidates,
        )

    def _build_same_cube_origin_candidates(
        self,
        target_cube_name: str,
        same_cube_source_refs: tuple[MeasureRef, ...],
        target_measure_name: str,
    ) -> tuple[FeederOriginCandidate, ...]:
        metadata = self._get_cube_metadata(target_cube_name)
        target_reference = self._compile_native_measure_reference(
            cube_name=target_cube_name,
            measure_name=target_measure_name,
        )
        return tuple(
            FeederOriginCandidate(
                cube_name=target_cube_name,
                measure_names=(ref.measure_name,),
                fixed_coordinates=(),
                dimension_count=len(metadata.dimensions) if metadata is not None else 0,
                fixed_coordinate_count=0,
                same_cube=True,
                description="Same-cube base driver candidate",
                candidate_kind="same_cube",
                statements=(
                    f"{self._compile_native_measure_reference(ref.cube_name, ref.measure_name)} => {target_reference};",
                ),
                deployment_cube=target_cube_name,
                deployment_statements=(
                    (
                        target_cube_name,
                        (
                            f"{self._compile_native_measure_reference(ref.cube_name, ref.measure_name)} => {target_reference};",
                        ),
                    ),
                ),
            )
            for ref in same_cube_source_refs
        )

    def _rank_feeder_origin_candidates(
        self,
        target_cube_name: str,
        candidates: Sequence[FeederOriginCandidate],
    ) -> FeederPlan:
        candidate_origins = tuple(candidates)
        winner = candidate_origins[0]
        winner_nonzero_count: Optional[int] = None

        for challenger in candidate_origins[1:]:
            if winner.dimension_count > challenger.dimension_count:
                continue
            if challenger.dimension_count > winner.dimension_count:
                winner = challenger
                winner_nonzero_count = None
                continue

            current_nonzero = self._count_candidate_origin_nonzero_cells(winner)
            challenger_nonzero = self._count_candidate_origin_nonzero_cells(challenger)
            if current_nonzero is None or challenger_nonzero is None:
                loser = challenger if winner is candidate_origins[0] else winner
                return FeederPlan(
                    statements=(),
                    strategy="preview_only_ranked_origin",
                    rationale=(
                        "Multiple safe feeder origins exist with equal dimension counts, but live TM1 "
                        "nonzero data was unavailable to choose the rarer-fill origin, so deployment "
                        "stays preview-only."
                    ),
                    deployment_cube=target_cube_name,
                    preview_only=True,
                    origin_candidates=candidate_origins,
                )
            winner_nonzero_count = current_nonzero
            if challenger_nonzero < current_nonzero:
                winner = challenger
                winner_nonzero_count = challenger_nonzero
                continue
            if current_nonzero < challenger_nonzero:
                continue
            if challenger.fixed_coordinate_count > winner.fixed_coordinate_count:
                winner = challenger
                winner_nonzero_count = challenger_nonzero
                continue
            if winner.fixed_coordinate_count > challenger.fixed_coordinate_count:
                continue
            if challenger.same_cube and not winner.same_cube:
                winner = challenger
                winner_nonzero_count = challenger_nonzero
                continue
            if winner.same_cube and not challenger.same_cube:
                continue
            if challenger.cube_name.casefold() < winner.cube_name.casefold():
                winner = challenger
                winner_nonzero_count = challenger_nonzero

        loser_candidates = [candidate for candidate in candidate_origins if candidate.key != winner.key]
        loser = loser_candidates[0] if loser_candidates else winner
        reason = "higher dimension count"
        loser_nonzero_count: Optional[int] = None
        if winner.dimension_count == loser.dimension_count:
            winner_nonzero_count = self._count_candidate_origin_nonzero_cells(winner)
            loser_nonzero_count = self._count_candidate_origin_nonzero_cells(loser)
            if winner_nonzero_count is None or loser_nonzero_count is None:
                return FeederPlan(
                    statements=(),
                    strategy="preview_only_ranked_origin",
                    rationale=(
                        "Multiple safe feeder origins exist with equal dimension counts, but live TM1 "
                        "nonzero data was unavailable to choose the rarer-fill origin, so deployment "
                        "stays preview-only."
                    ),
                    deployment_cube=target_cube_name,
                    preview_only=True,
                    origin_candidates=candidate_origins,
                )
            if winner_nonzero_count != loser_nonzero_count:
                reason = "rarer fill probability"
            elif winner.fixed_coordinate_count != loser.fixed_coordinate_count:
                reason = "more fixed/current coordinates"
            elif winner.same_cube != loser.same_cube:
                reason = "same-cube preference"
            else:
                reason = "deterministic lexical fallback"

        return self._build_ranked_origin_result(
            winner=winner,
            loser=loser,
            reason=reason,
            candidate_origins=candidate_origins,
            winner_nonzero_count=winner_nonzero_count,
            loser_nonzero_count=loser_nonzero_count,
        )

    def _build_ranked_origin_result(
        self,
        winner: FeederOriginCandidate,
        loser: FeederOriginCandidate,
        reason: str,
        candidate_origins: tuple[FeederOriginCandidate, ...],
        winner_nonzero_count: Optional[int] = None,
        loser_nonzero_count: Optional[int] = None,
    ) -> FeederPlan:
        deployment_statements = (
            winner.deployment_statements
            or (
                ((winner.deployment_cube, winner.statements),)
                if winner.deployment_cube is not None and winner.statements
                else ()
            )
        )
        statements = winner.statements or tuple(
            sorted(
                statement
                for _cube_name, cube_statements in deployment_statements
                for statement in cube_statements
            )
        )
        deployment_cube = winner.deployment_cube
        strategy = (
            "ranked_same_cube_origin"
            if winner.same_cube
            else f"ranked_{winner.candidate_kind}_origin"
        )

        count_detail = ""
        if winner_nonzero_count is not None and loser_nonzero_count is not None:
            count_detail = (
                f" Winner nonzero count={winner_nonzero_count}; loser nonzero count={loser_nonzero_count}."
            )
        rationale = (
            f"Feeder origin ranking selected '{winner.cube_name}' over '{loser.cube_name}' by {reason}, "
            f"following the original rule order (higher dimension count first; equal-dimension ties by rarer fill)."
            f"{count_detail}"
        )
        if winner.fixed_coordinate_count:
            rationale += " The winning origin qualifies as a simpler feeder surface because it uses fixed/current coordinates."

        return FeederPlan(
            statements=statements,
            strategy=strategy,
            rationale=rationale,
            deployment_cube=deployment_cube,
            deployment_statements=deployment_statements,
            preview_only=False,
            origin_candidates=candidate_origins,
            selected_origin_key=winner.key,
        )

    def _count_candidate_origin_nonzero_cells(self, candidate: FeederOriginCandidate) -> Optional[int]:
        if self.tm1 is None:
            return None
        metadata = self._get_cube_metadata(candidate.cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return None
        filter_mapping = {
            metadata.measure_dimension_name: list(candidate.measure_names),
            **{
                dimension_name: list(element_names)
                for dimension_name, element_names in candidate.fixed_coordinates
            },
        }
        try:
            mdx = utility.generate_dynamic_mdx_query_string(
                tm1_service=self.tm1,
                target_cube_name=candidate.cube_name,
                dimension_filter_mapping=filter_mapping,
                cube_dimensions_list=list(metadata.dimensions),
            )
            dataframe = extractor.tm1_mdx_to_dataframe(
                tm1_service=self.tm1,
                data_mdx=mdx,
                cube_dimensions=list(metadata.dimensions),
                default_returned_value_type=float,
                use_blob=False,
            )
        except Exception:
            return None
        if "Value" not in dataframe.columns:
            return None
        numeric_values = pd.to_numeric(dataframe["Value"], errors="coerce")
        if numeric_values.notna().any():
            return int((numeric_values.fillna(0) != 0).sum())
        return int(dataframe["Value"].astype(str).str.strip().ne("").sum())

    @staticmethod
    def _format_feeder_origin_candidates(plan: FeederPlan) -> List[Dict[str, Any]]:
        return [
            {
                "key": candidate.key,
                "cube": candidate.cube_name,
                "measures": list(candidate.measure_names),
                "dimension_count": candidate.dimension_count,
                "fixed_coordinate_count": candidate.fixed_coordinate_count,
                "same_cube": candidate.same_cube,
                "candidate_kind": candidate.candidate_kind,
                "simple_feeder": candidate.fixed_coordinate_count > 0,
                "fixed_coordinates": {
                    dimension_name: list(element_names)
                    for dimension_name, element_names in candidate.fixed_coordinates
                },
                "description": candidate.description,
            }
            for candidate in plan.origin_candidates
        ]

    def _build_conditional_cross_cube_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        align_expressions = tuple(self._iter_align_expressions(formula.expression))
        if not align_expressions:
            return None
        if len(align_expressions) == 1 and isinstance(formula.expression, MethodExpression) and formula.expression.method_name == "align":
            return None

        branch_plans: List[FeederPlan] = []
        for align_expression in align_expressions:
            branch_formula = Formula(
                target=formula.target,
                expression=align_expression,
                scope=formula.scope,
                feeder_mode=formula.feeder_mode,
            )
            direct_plan = self._build_direct_cross_cube_feeders(branch_formula)
            if direct_plan is not None:
                branch_plans.append(direct_plan)
                continue
            attribute_plan = self._build_attribute_routed_cross_cube_feeders(branch_formula)
            if attribute_plan is not None:
                branch_plans.append(attribute_plan)
                continue
            return None

        deployment_statements_by_cube: Dict[str, set[str]] = defaultdict(set)
        for plan in branch_plans:
            for cube_name, statements in plan.iter_deployment_statements():
                deployment_statements_by_cube[cube_name].update(statements)

        statements = tuple(
            sorted(
                statement
                for grouped_statements in deployment_statements_by_cube.values()
                for statement in grouped_statements
            )
        )
        if not statements:
            return None

        deployment_statements = tuple(
            (cube_name, tuple(sorted(grouped_statements)))
            for cube_name, grouped_statements in sorted(deployment_statements_by_cube.items())
        )

        deployment_cube = deployment_statements[0][0] if len(deployment_statements) == 1 else None

        return FeederPlan(
            statements=statements,
            strategy="conditional_cross_cube_branches",
            rationale=(
                "Cross-cube feeder deployment is allowed because each native align branch has a bounded, "
                "metadata-proven feeder plan, so the combined branch routing can be represented as an "
                "explicit union of safe feeder statements across the required source cubes."
            ),
            deployment_cube=deployment_cube,
            deployment_statements=deployment_statements,
            preview_only=False,
            origin_candidates=tuple(
                candidate
                for branch_plan in branch_plans
                for candidate in branch_plan.origin_candidates
            ),
        )

    def _build_direct_cross_cube_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        expression = formula.expression
        if not isinstance(expression, MethodExpression) or expression.method_name != "align":
            return None
        if not isinstance(expression.base, MeasureExpression):
            return None

        source_ref = expression.base.ref
        source_metadata = self._get_cube_metadata(source_ref.cube_name)
        target_metadata = self._get_cube_metadata(formula.target.cube_name)
        if source_metadata is None or target_metadata is None:
            return None

        source_dims = tuple(source_metadata.dimensions)
        target_dims = tuple(target_metadata.dimensions)
        if not source_dims or not target_dims:
            return None

        source_measure_dim = source_metadata.measure_dimension_name
        target_measure_dim = target_metadata.measure_dimension_name
        if source_measure_dim is None or target_measure_dim is None:
            return None

        broadcast_target_dimensions: List[str] = []
        for dimension_name in target_dims:
            if dimension_name == target_measure_dim:
                continue
            if dimension_name in expression.kwargs:
                return None
            if dimension_name not in source_dims:
                leaf_elements = tuple(target_metadata.dimension_leaf_elements.get(dimension_name, ()))
                if not leaf_elements:
                    return None
                broadcast_target_dimensions.append(dimension_name)

        for dimension_name in source_dims:
            if dimension_name == source_measure_dim:
                continue
            mapping = expression.kwargs.get(dimension_name)
            if dimension_name in target_dims:
                if mapping is not None:
                    return None
                continue
            if mapping is None:
                return None
            if not isinstance(mapping, (LiteralExpression, ElementExpression)):
                return None

        source_reference = self._compile_native_measure_reference(
            cube_name=source_ref.cube_name,
            measure_name=source_ref.measure_name,
        )
        target_statements = self._build_target_db_statements(
            source_cube_name=source_ref.cube_name,
            target_ref=formula.target,
            fixed_target_elements={},
            broadcast_target_dimensions=broadcast_target_dimensions,
        )
        if not target_statements:
            return None
        fixed_origin_options: List[List[tuple[str, Optional[str], str]]] = []
        for dimension_name in sorted(
            dimension_name
            for dimension_name in source_dims
            if dimension_name != source_measure_dim
            and dimension_name not in target_dims
        ):
            expanded_elements = self._expand_feeder_origin_elements(
                cube_name=source_ref.cube_name,
                dimension_name=dimension_name,
                expression=expression.kwargs[dimension_name],
            )
            if expanded_elements is None:
                return None
            fixed_origin_options.append(
                [
                    (dimension_name, explicit_hierarchy_name, element_name)
                    for explicit_hierarchy_name, element_name in expanded_elements
                ]
            )

        statements: List[str] = []
        fixed_coordinate_groups = tuple(
            (
                dimension_name,
                tuple(
                    element_name
                    for _explicit_hierarchy_name, element_name in self._expand_feeder_origin_elements(
                        cube_name=source_ref.cube_name,
                        dimension_name=dimension_name,
                        expression=expression.kwargs[dimension_name],
                    ) or ()
                ),
            )
            for dimension_name in sorted(
                dimension_name
                for dimension_name in source_dims
                if dimension_name != source_measure_dim
                and dimension_name not in target_dims
            )
        )
        for fixed_origin_combo in product(*fixed_origin_options) if fixed_origin_options else [()]:
            fixed_origin_by_dimension = {
                dimension_name: (explicit_hierarchy_name, element_name)
                for dimension_name, explicit_hierarchy_name, element_name in fixed_origin_combo
            }
            lhs_parts: List[str] = []
            for dimension_name in source_dims:
                if dimension_name == source_measure_dim:
                    lhs_parts.append(source_reference[1:-1] if source_reference.startswith("[") else source_reference)
                elif dimension_name in fixed_origin_by_dimension:
                    explicit_hierarchy_name, element_name = fixed_origin_by_dimension[dimension_name]
                    lhs_parts.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=source_ref.cube_name,
                            dimension_name=dimension_name,
                            element_name=element_name,
                            explicit_hierarchy_name=explicit_hierarchy_name,
                        )
                    )
                else:
                    lhs_parts.append(f"!{dimension_name}")
            statements.append(f"[{', '.join(lhs_parts)}] => {', '.join(target_statements)};")

        return FeederPlan(
            statements=tuple(statements),
            strategy="cross_cube_source_lookup",
            rationale=(
                "Cross-cube feeder deployment is allowed because the lookup is direct and invertible: "
                "shared target dimensions map one-to-one from the source cube, and any extra source "
                "dimensions are fixed to explicit leaf-safe elements or metadata-proven leaf expansions, "
                "while any target-only dimensions are expanded to explicit target leaf sets when metadata proves them."
            ),
            deployment_cube=source_ref.cube_name,
            preview_only=False,
            origin_candidates=(
                FeederOriginCandidate(
                    cube_name=source_ref.cube_name,
                    measure_names=(source_ref.measure_name,),
                    fixed_coordinates=tuple(
                        (dimension_name, element_names)
                        for dimension_name, element_names in fixed_coordinate_groups
                        if element_names
                    ),
                    dimension_count=len(source_dims),
                    fixed_coordinate_count=sum(1 for _dimension_name, element_names in fixed_coordinate_groups if element_names),
                    same_cube=source_ref.cube_name == formula.target.cube_name,
                    description="Cross-cube direct lookup candidate",
                    candidate_kind="cross_cube_direct",
                    statements=tuple(statements),
                    deployment_cube=source_ref.cube_name,
                    deployment_statements=((source_ref.cube_name, tuple(statements)),),
                ),
            ),
        )

    def _build_attribute_routed_cross_cube_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        expression = formula.expression
        if not isinstance(expression, MethodExpression) or expression.method_name != "align":
            return None
        if not isinstance(expression.base, MeasureExpression):
            return None

        source_ref = expression.base.ref
        source_metadata = self._get_cube_metadata(source_ref.cube_name)
        target_metadata = self._get_cube_metadata(formula.target.cube_name)
        if source_metadata is None or target_metadata is None:
            return None

        source_measure_dim = source_metadata.measure_dimension_name
        target_measure_dim = target_metadata.measure_dimension_name
        if source_measure_dim is None or target_measure_dim is None:
            return None

        attribute_mappings: Dict[str, Expression] = {}
        fixed_source_values: Dict[str, Expression] = {}
        passthrough_dims: List[str] = []

        for dimension_name in source_metadata.dimensions:
            if dimension_name == source_measure_dim:
                continue

            mapping = expression.kwargs.get(dimension_name)
            if isinstance(mapping, (AttributeExpression, ChainedAttributeExpression)):
                attribute_mappings[dimension_name] = mapping
                continue
            if mapping is not None:
                fixed_source_values[dimension_name] = mapping
                continue
            if dimension_name in target_metadata.dimensions:
                passthrough_dims.append(dimension_name)
                continue
            return None

        if not attribute_mappings:
            return None

        attribute_target_dimensions: set[str] = set()
        for mapping in attribute_mappings.values():
            attribute_target_dimensions.add(self._align_mapping_attribute_home_dimension(mapping))
            if isinstance(mapping, ChainedAttributeExpression):
                attribute_target_dimensions.add(mapping.ref.second_dimension_name)
        broadcast_target_dimensions = [
            dimension_name
            for dimension_name in target_metadata.dimensions
            if dimension_name != target_measure_dim
            and dimension_name not in source_metadata.dimensions
            and dimension_name not in attribute_target_dimensions
        ]

        attribute_mapping_groups = self._group_align_attribute_mappings_by_target_dimension(expression)
        target_candidates_by_dimension_group = self._build_align_attribute_reverse_mapping_groups(
            expression=expression,
            source_metadata=source_metadata,
            target_metadata=target_metadata,
            target_cube_name=formula.target.cube_name,
        )
        if target_candidates_by_dimension_group is None:
            return None

        for fixed_dimension_name, fixed_expression in fixed_source_values.items():
            if self._expand_feeder_origin_elements(
                cube_name=source_ref.cube_name,
                dimension_name=fixed_dimension_name,
                expression=fixed_expression,
            ) is None:
                return None

        source_combo_dimensions = tuple(sorted(attribute_mappings))
        source_combo_elements = [
            tuple(source_metadata.dimension_leaf_elements.get(dimension_name, ()))
            for dimension_name in source_combo_dimensions
        ]
        if any(not elements for elements in source_combo_elements):
            return None

        feeder_statements: List[str] = []
        for source_combo in product(*source_combo_elements):
            source_element_by_dimension = dict(zip(source_combo_dimensions, source_combo))
            target_element_groups: List[tuple[str, tuple[str, ...]]] = []
            for target_dimension_name, mappings in sorted(attribute_mapping_groups.items()):
                source_key = tuple(
                    source_element_by_dimension[source_dimension_name]
                    for source_dimension_name, _mapping in mappings
                )
                target_elements = target_candidates_by_dimension_group[target_dimension_name].get(
                    source_key,
                    (),
                )
                if not target_elements:
                    target_element_groups = []
                    break
                target_element_groups.append((target_dimension_name, target_elements))
            if not target_element_groups:
                continue

            target_coordinate_options = [
                [(dimension_name, element_name) for element_name in element_names]
                for dimension_name, element_names in target_element_groups
            ]

            target_db_targets: List[str] = []
            for target_combo in product(*target_coordinate_options):
                target_elements_by_dimension = dict(target_combo)
                built_targets = self._build_target_db_statements(
                    source_cube_name=source_ref.cube_name,
                    target_ref=formula.target,
                    fixed_target_elements=target_elements_by_dimension,
                    broadcast_target_dimensions=broadcast_target_dimensions,
                )
                if not built_targets:
                    return None
                target_db_targets.extend(built_targets)

            if not target_db_targets:
                continue

            source_reference = self._compile_native_measure_reference(
                cube_name=source_ref.cube_name,
                measure_name=source_ref.measure_name,
            )
            lhs_coordinate_parts: List[str] = []
            for dimension_name in source_metadata.dimensions:
                if dimension_name == source_measure_dim:
                    lhs_coordinate_parts.append(source_reference[1:-1] if source_reference.startswith("[") else source_reference)
                elif dimension_name in source_element_by_dimension:
                    lhs_coordinate_parts.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=source_ref.cube_name,
                            dimension_name=dimension_name,
                            element_name=source_element_by_dimension[dimension_name],
                            explicit_hierarchy_name=None,
                        )
                    )
                elif dimension_name in fixed_source_values:
                    expanded_elements = self._expand_feeder_origin_elements(
                        cube_name=source_ref.cube_name,
                        dimension_name=dimension_name,
                        expression=fixed_source_values[dimension_name],
                    )
                    if expanded_elements is None or len(expanded_elements) != 1:
                        return None
                    explicit_hierarchy_name, element_name = expanded_elements[0]
                    lhs_coordinate_parts.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=source_ref.cube_name,
                            dimension_name=dimension_name,
                            element_name=element_name,
                            explicit_hierarchy_name=explicit_hierarchy_name,
                        )
                    )
                elif dimension_name in passthrough_dims:
                    lhs_coordinate_parts.append(f"!{dimension_name}")
                else:
                    return None

            feeder_statements.append(
                f"[{', '.join(lhs_coordinate_parts)}] => {', '.join(target_db_targets)};"
            )

        if not feeder_statements:
            return None

        fixed_coordinate_groups = tuple(
            (
                dimension_name,
                tuple(
                    element_name
                    for _explicit_hierarchy_name, element_name in (
                        self._expand_feeder_origin_elements(
                            cube_name=source_ref.cube_name,
                            dimension_name=dimension_name,
                            expression=fixed_expression,
                        ) or ()
                    )
                ),
            )
            for dimension_name, fixed_expression in sorted(fixed_source_values.items())
        )

        return FeederPlan(
            statements=tuple(feeder_statements),
            strategy="cross_cube_attribute_lookup",
            rationale=(
                "Cross-cube feeder deployment is allowed because metadata proves a bounded reverse mapping "
                "from source lookup elements to target leaf elements through explicit target-dimension "
                "attribute values, including composite multi-attribute routes when applicable, and any "
                "target-only dimensions are expanded to explicit target leaf sets when metadata proves them."
            ),
            deployment_cube=source_ref.cube_name,
            preview_only=False,
            origin_candidates=(
                FeederOriginCandidate(
                    cube_name=source_ref.cube_name,
                    measure_names=(source_ref.measure_name,),
                    fixed_coordinates=tuple(
                        (dimension_name, element_names)
                        for dimension_name, element_names in fixed_coordinate_groups
                        if element_names
                    ),
                    dimension_count=len(source_metadata.dimensions),
                    fixed_coordinate_count=sum(1 for _dimension_name, element_names in fixed_coordinate_groups if element_names),
                    same_cube=source_ref.cube_name == formula.target.cube_name,
                    description="Cross-cube attribute-routed lookup candidate",
                    candidate_kind="cross_cube_attribute",
                    statements=tuple(feeder_statements),
                    deployment_cube=source_ref.cube_name,
                    deployment_statements=((source_ref.cube_name, tuple(feeder_statements)),),
                ),
            ),
        )

    @staticmethod
    def _align_mapping_attribute_home_dimension(mapped_expression: Expression) -> Optional[str]:
        if isinstance(mapped_expression, AttributeExpression):
            return mapped_expression.ref.dimension_name
        if isinstance(mapped_expression, ChainedAttributeExpression):
            return mapped_expression.ref.first_dimension_name
        return None

    @staticmethod
    def _group_align_attribute_mappings_by_target_dimension(
        expression: MethodExpression,
    ) -> Dict[str, List[tuple[str, Expression]]]:
        grouped_mappings: Dict[str, List[tuple[str, Expression]]] = defaultdict(list)
        for source_dimension_name, mapped_expression in sorted(expression.kwargs.items()):
            home_dimension_name = Model._align_mapping_attribute_home_dimension(mapped_expression)
            if home_dimension_name is None:
                continue
            grouped_mappings[home_dimension_name].append(
                (source_dimension_name, mapped_expression)
            )
        return dict(grouped_mappings)

    def _resolve_align_attribute_mapping_value(
        self,
        mapping: Expression,
        target_metadata: CubeMetadata,
        attribute_values: Dict[str, Dict[str, Any]],
        target_element_name: str,
    ) -> Optional[str]:
        if isinstance(mapping, AttributeExpression):
            return str(
                attribute_values.get(target_element_name, {}).get(mapping.ref.attribute_name, "")
            ).strip()

        if isinstance(mapping, ChainedAttributeExpression):
            intermediate_value = str(
                attribute_values.get(target_element_name, {}).get(mapping.ref.first_attribute_name, "")
            ).strip()
            intermediate_leaf_elements = tuple(
                target_metadata.dimension_leaf_elements.get(mapping.ref.second_dimension_name, ())
            )
            canonical_intermediate_element = next(
                (
                    leaf_element_name
                    for leaf_element_name in intermediate_leaf_elements
                    if str(leaf_element_name).strip() == intermediate_value
                ),
                None,
            )
            if canonical_intermediate_element is None:
                return None
            second_hop_attribute_values = target_metadata.dimension_attribute_values.get(
                mapping.ref.second_dimension_name, {}
            )
            final_value = second_hop_attribute_values.get(canonical_intermediate_element, {}).get(
                mapping.ref.second_attribute_name
            )
            if final_value is None:
                return None
            return str(final_value).strip()

        return None

    def _build_align_attribute_reverse_mapping_groups(
        self,
        expression: MethodExpression,
        source_metadata: CubeMetadata,
        target_metadata: CubeMetadata,
        target_cube_name: str,
    ) -> Optional[Dict[str, Dict[tuple[str, ...], tuple[str, ...]]]]:
        attribute_mapping_groups = self._group_align_attribute_mappings_by_target_dimension(expression)
        reverse_mapping_groups: Dict[str, Dict[tuple[str, ...], tuple[str, ...]]] = {}

        for target_dimension_name, mappings in sorted(attribute_mapping_groups.items()):
            leaf_elements = tuple(target_metadata.dimension_leaf_elements.get(target_dimension_name, ()))
            attribute_values = target_metadata.dimension_attribute_values.get(target_dimension_name, {})
            if not leaf_elements:
                return None

            source_leaf_elements_by_dimension: List[tuple[str, ...]] = []
            allowed_source_elements_by_dimension: Dict[str, Dict[str, str]] = {}
            for source_dimension_name, mapping in mappings:
                if mapping.ref.cube_name != target_cube_name:
                    return None
                source_leaf_elements = tuple(
                    source_metadata.dimension_leaf_elements.get(source_dimension_name, ())
                )
                if not source_leaf_elements:
                    return None
                source_leaf_elements_by_dimension.append(source_leaf_elements)
                allowed_source_elements_by_dimension[source_dimension_name] = {
                    str(source_element_name).strip(): source_element_name
                    for source_element_name in source_leaf_elements
                }

            grouped_targets: Dict[tuple[str, ...], List[str]] = defaultdict(list)
            for target_element_name in leaf_elements:
                source_key: List[str] = []
                for source_dimension_name, mapping in mappings:
                    mapped_source_element = self._resolve_align_attribute_mapping_value(
                        mapping=mapping,
                        target_metadata=target_metadata,
                        attribute_values=attribute_values,
                        target_element_name=target_element_name,
                    )
                    canonical_source_element = (
                        allowed_source_elements_by_dimension[source_dimension_name].get(mapped_source_element)
                        if mapped_source_element is not None
                        else None
                    )
                    if canonical_source_element is None:
                        return None
                    source_key.append(canonical_source_element)
                grouped_targets[tuple(source_key)].append(target_element_name)

            reverse_mapping_groups[target_dimension_name] = {
                source_combo: tuple(grouped_targets.get(source_combo, ()))
                for source_combo in product(*source_leaf_elements_by_dimension)
            }

        return reverse_mapping_groups

    def _build_same_cube_shift_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        expression = formula.expression
        if not isinstance(expression, MethodExpression) or expression.method_name != "shift":
            return None
        if self._native_shift_unsupported_reason(expression, formula.target.cube_name) is not None:
            return None

        resolved = self._resolve_shift_spec(expression)
        assert resolved is not None
        dimension_name, mode, mapping_value = resolved
        assert isinstance(expression.base, MeasureExpression)
        source_measure = expression.base.ref
        cube_name = formula.target.cube_name

        metadata = self._get_cube_metadata(cube_name)
        if metadata is None:
            return None

        leaf_elements = tuple(metadata.dimension_leaf_elements.get(dimension_name, ()))
        if not leaf_elements:
            return None

        matched_target_elements_by_source: Dict[str, List[str]] = defaultdict(list)
        if mode == "attribute":
            attribute_values = metadata.dimension_attribute_values.get(dimension_name, {})
            for element_name in leaf_elements:
                predecessor = str(attribute_values.get(element_name, {}).get(mapping_value, "")).strip()
                if predecessor:
                    matched_target_elements_by_source[predecessor].append(element_name)
            strategy = "same_cube_attribute_shift"
            rationale = (
                "Same-cube time-shift feeder deployment is allowed because metadata proves a bounded "
                "reverse mapping from source elements to shifted target elements through the shift "
                "dimension's attribute."
            )
        else:
            offset = mapping_value
            allowed_source_elements = {str(element_name).strip() for element_name in leaf_elements}
            for element_name in leaf_elements:
                predecessor_value = int(str(element_name).strip()) + offset
                predecessor_key = str(predecessor_value)
                if predecessor_key in allowed_source_elements:
                    matched_target_elements_by_source[predecessor_key].append(element_name)
            strategy = "same_cube_numeric_shift"
            rationale = (
                "Same-cube time-shift feeder deployment is allowed because metadata proves the shift "
                "dimension's leaf elements are numeric, so the reverse mapping from source elements to "
                "shifted target elements can be derived through bounded integer arithmetic."
            )

        if not matched_target_elements_by_source:
            return None

        source_reference = self._compile_native_measure_reference(cube_name, source_measure.measure_name)
        statements: List[str] = []
        for source_element_name in sorted(matched_target_elements_by_source):
            target_db_targets: List[str] = []
            for target_element_name in matched_target_elements_by_source[source_element_name]:
                target_db_targets.extend(
                    self._build_target_db_statements(
                        source_cube_name=cube_name,
                        target_ref=formula.target,
                        fixed_target_elements={dimension_name: target_element_name},
                        broadcast_target_dimensions=[],
                    )
                )
            if not target_db_targets:
                continue

            lhs_parts: List[str] = []
            for current_dimension_name in metadata.dimensions:
                if current_dimension_name == metadata.measure_dimension_name:
                    lhs_parts.append(source_reference[1:-1] if source_reference.startswith("[") else source_reference)
                elif current_dimension_name == dimension_name:
                    lhs_parts.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=cube_name,
                            dimension_name=dimension_name,
                            element_name=source_element_name,
                            explicit_hierarchy_name=None,
                        )
                    )
                else:
                    lhs_parts.append(f"!{current_dimension_name}")
            statements.append(f"[{', '.join(lhs_parts)}] => {', '.join(target_db_targets)};")

        if not statements:
            return None

        return FeederPlan(
            statements=tuple(statements),
            strategy=strategy,
            rationale=rationale,
            deployment_cube=cube_name,
            preview_only=False,
        )

    def _build_same_cube_rolling_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        """Unions the bounded shift(...) feeder plan for every unrolled term in the window.

        All-or-nothing, same discipline as every other bounded expansion in this project: if any
        unrolled offset term cannot independently prove a safe reverse-mapping feeder plan, the
        whole rolling(...).sum() formula stays preview-only rather than partially feeding.
        """
        expression = formula.expression
        if not isinstance(expression, MethodExpression) or expression.method_name != "rolling.sum":
            return None
        if self._native_rolling_unsupported_reason(expression, formula.target.cube_name) is not None:
            return None

        resolved = self._resolve_rolling_spec(expression)
        assert resolved is not None
        dimension_name, window = resolved

        combined_statements: set = set()
        for offset in range(window):
            synthetic_formula = Formula(
                target=formula.target,
                expression=MethodExpression(
                    "shift",
                    expression.base,
                    kwargs={dimension_name: LiteralExpression(-offset)},
                ),
                scope=formula.scope,
                feeder_mode=formula.feeder_mode,
            )
            term_plan = self._build_same_cube_shift_feeders(synthetic_formula)
            if term_plan is None:
                return None
            combined_statements.update(term_plan.statements)

        if not combined_statements:
            return None

        return FeederPlan(
            statements=tuple(sorted(combined_statements)),
            strategy="same_cube_rolling_window",
            rationale=(
                "Same-cube rolling-window feeder deployment is allowed because the window is "
                "unrolled at compile time into a bounded sum of numeric-offset shift(...) terms, "
                "each of which independently proves a safe reverse-mapping feeder through the "
                "rolling dimension's numeric leaf elements; the union of all per-term feeders "
                "covers the whole window."
            ),
            deployment_cube=formula.target.cube_name,
            preview_only=False,
        )

    def _build_same_cube_ytd_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        """Unions a gated per-term feeder plan for every unrolled offset in the year-to-date sum.

        Mirrors `_build_same_cube_shift_feeders`'s numeric-offset branch, but each term `k` only
        contributes a feeder for a target element whose period-number attribute value is `> k`,
        matching exactly the runtime gate `_compile_native_ytd_expression` emits in the rule itself.
        All-or-nothing per term, same discipline as every other bounded expansion in this project.
        """
        expression = formula.expression
        if not isinstance(expression, MethodExpression) or expression.method_name != "ytd":
            return None
        if self._native_ytd_unsupported_reason(expression, formula.target.cube_name) is not None:
            return None

        resolved = self._resolve_ytd_spec(expression)
        assert resolved is not None
        dimension_name, period_attribute = resolved
        assert isinstance(expression.base, MeasureExpression)
        source_measure = expression.base.ref
        cube_name = formula.target.cube_name

        metadata = self._get_cube_metadata(cube_name)
        if metadata is None:
            return None

        leaf_elements = tuple(metadata.dimension_leaf_elements.get(dimension_name, ()))
        if not leaf_elements:
            return None

        attribute_values = metadata.dimension_attribute_values.get(dimension_name, {})
        period_value_by_element: Dict[str, int] = {}
        for element_name in leaf_elements:
            raw_value = str(attribute_values.get(element_name, {}).get(period_attribute, "")).strip()
            if not raw_value.lstrip("-").isdigit():
                return None
            period_value_by_element[element_name] = int(raw_value)

        max_terms = max(period_value_by_element.values())
        if max_terms < 1:
            return None

        source_reference = self._compile_native_measure_reference(cube_name, source_measure.measure_name)
        combined_statements: set = set()
        allowed_source_elements = {str(element_name).strip() for element_name in leaf_elements}

        for offset in range(max_terms):
            matched_target_elements_by_source: Dict[str, List[str]] = defaultdict(list)
            for element_name in leaf_elements:
                if period_value_by_element[element_name] <= offset:
                    continue
                predecessor_value = int(str(element_name).strip()) - offset
                predecessor_key = str(predecessor_value)
                if predecessor_key in allowed_source_elements:
                    matched_target_elements_by_source[predecessor_key].append(element_name)

            for source_element_name in sorted(matched_target_elements_by_source):
                target_db_targets: List[str] = []
                for target_element_name in matched_target_elements_by_source[source_element_name]:
                    target_db_targets.extend(
                        self._build_target_db_statements(
                            source_cube_name=cube_name,
                            target_ref=formula.target,
                            fixed_target_elements={dimension_name: target_element_name},
                            broadcast_target_dimensions=[],
                        )
                    )
                if not target_db_targets:
                    continue

                lhs_parts: List[str] = []
                for current_dimension_name in metadata.dimensions:
                    if current_dimension_name == metadata.measure_dimension_name:
                        lhs_parts.append(
                            source_reference[1:-1] if source_reference.startswith("[") else source_reference
                        )
                    elif current_dimension_name == dimension_name:
                        lhs_parts.append(
                            self._compile_dimension_element_coordinate(
                                cube_name=cube_name,
                                dimension_name=dimension_name,
                                element_name=source_element_name,
                                explicit_hierarchy_name=None,
                            )
                        )
                    else:
                        lhs_parts.append(f"!{current_dimension_name}")
                combined_statements.add(f"[{', '.join(lhs_parts)}] => {', '.join(target_db_targets)};")

        if not combined_statements:
            return None

        return FeederPlan(
            statements=tuple(sorted(combined_statements)),
            strategy="same_cube_ytd_window",
            rationale=(
                "Same-cube year-to-date feeder deployment is allowed because the variable-size "
                "window is unrolled at compile time into a bounded, gated sum of numeric-offset "
                "shift(...) terms, each of which independently proves a safe reverse-mapping feeder "
                "restricted to the target elements where that term's period-number gate is active."
            ),
            deployment_cube=cube_name,
            preview_only=False,
        )

    def _build_consolidated_target_leaf_feeders(self, plan: BuildPlan, formula: Formula) -> Optional[FeederPlan]:
        """Feeder strategy for C: targets: never feed the consolidated element itself.

        Instead, expand the target measure (a Consolidated element on the measure dimension)
        to its bounded leaf-measure set and emit one same-cube feeder statement per
        (driver measure, leaf measure) pair, exactly mirroring the project's own rule:
        "do not write feeders for C type elements ... determine what exact N elements to
        write multiple feeders for, instead of over-feeding."
        """
        metadata = self._get_cube_metadata(formula.target.cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return None

        leaf_measures = self._expand_consolidated_element_to_leaves(
            metadata=metadata,
            dimension_name=metadata.measure_dimension_name,
            element_name=formula.target.measure_name,
        )
        if not leaf_measures or len(leaf_measures) > self._CONSOLIDATED_FEEDER_LEAF_CEILING:
            return None

        same_cube_source_refs = tuple(
            sorted(
                self._resolve_simple_feeder_source_refs(plan, formula),
                key=lambda item: item.key,
            )
        )

        same_cube_candidates = tuple(
            FeederOriginCandidate(
                cube_name=formula.target.cube_name,
                measure_names=(ref.measure_name,),
                fixed_coordinates=(),
                dimension_count=len(metadata.dimensions),
                fixed_coordinate_count=0,
                same_cube=True,
                description="Same-cube consolidated leaf feeder candidate",
                candidate_kind="same_cube_consolidated",
                statements=tuple(
                    f"{self._compile_native_measure_reference(ref.cube_name, ref.measure_name)} => "
                    f"{self._compile_native_measure_reference(formula.target.cube_name, leaf_measure_name)};"
                    for leaf_measure_name in leaf_measures
                ),
                deployment_cube=formula.target.cube_name,
                deployment_statements=(
                    (
                        formula.target.cube_name,
                        tuple(
                            f"{self._compile_native_measure_reference(ref.cube_name, ref.measure_name)} => "
                            f"{self._compile_native_measure_reference(formula.target.cube_name, leaf_measure_name)};"
                            for leaf_measure_name in leaf_measures
                        ),
                    ),
                ),
            )
            for ref in same_cube_source_refs
        )
        if formula.feeder_mode == "ranked_single" and len(same_cube_candidates) > 1:
            ranked_plan = self._rank_feeder_origin_candidates(
                target_cube_name=formula.target.cube_name,
                candidates=same_cube_candidates,
            )
            return FeederPlan(
                statements=ranked_plan.statements,
                strategy="consolidated_target_leaf_feeders",
                rationale=ranked_plan.rationale,
                deployment_cube=ranked_plan.deployment_cube,
                deployment_statements=ranked_plan.deployment_statements,
                preview_only=ranked_plan.preview_only,
                origin_candidates=ranked_plan.origin_candidates,
                selected_origin_key=ranked_plan.selected_origin_key,
            )

        statements = tuple(
            sorted(
                {
                    f"{self._compile_native_measure_reference(ref.cube_name, ref.measure_name)} => "
                    f"{self._compile_native_measure_reference(formula.target.cube_name, leaf_measure_name)};"
                    for leaf_measure_name in leaf_measures
                    for ref in same_cube_source_refs
                }
            )
        )

        return FeederPlan(
            statements=statements,
            strategy="consolidated_target_leaf_feeders",
            rationale=(
                "C: rule-area feeder deployment is allowed because the consolidated target measure "
                "expands to a bounded, metadata-proven set of leaf measures; feeders are emitted "
                "for each leaf measure individually, never for the consolidated element itself."
            ),
            deployment_cube=formula.target.cube_name,
            preview_only=False,
            origin_candidates=same_cube_candidates,
        )

    def _build_consolidated_leaf_cross_cube_feeders(
        self,
        plan: BuildPlan,
        formula: Formula,
    ) -> Optional[FeederPlan]:
        """Apply the bounded N:-level cross-cube feeder planner to each expanded target leaf.

        This keeps C: cross-cube support deliberately narrow: the consolidated target is first
        expanded to a bounded set of leaf measures, then each leaf is treated like an ordinary
        N: target and must independently resolve through the existing safe feeder subsets.
        """
        metadata = self._get_cube_metadata(formula.target.cube_name)
        if metadata is None or not metadata.measure_dimension_name:
            return None

        leaf_measures = self._expand_consolidated_element_to_leaves(
            metadata=metadata,
            dimension_name=metadata.measure_dimension_name,
            element_name=formula.target.measure_name,
        )
        if not leaf_measures or len(leaf_measures) > self._CONSOLIDATED_FEEDER_LEAF_CEILING:
            return None

        deployment_statements_by_cube: Dict[str, set[str]] = defaultdict(set)
        for leaf_measure_name in leaf_measures:
            leaf_formula = Formula(
                target=MeasureRef(
                    cube_name=formula.target.cube_name,
                    measure_name=leaf_measure_name,
                ),
                expression=formula.expression,
                scope="leaf",
                feeder_mode=formula.feeder_mode,
            )
            leaf_plan = self._plan_feeders(
                plan,
                leaf_formula,
                allow_origin_ranking=formula.feeder_mode == "ranked_single",
            )
            if leaf_plan.preview_only:
                return None
            for cube_name, cube_statements in leaf_plan.iter_deployment_statements(
                default_cube=formula.target.cube_name
            ):
                deployment_statements_by_cube[cube_name].update(cube_statements)

        deployment_statements_by_cube = {
            cube_name: self._merge_db_target_feeder_statements(cube_statements)
            for cube_name, cube_statements in deployment_statements_by_cube.items()
        }

        statements = tuple(
            sorted(
                statement
                for grouped_statements in deployment_statements_by_cube.values()
                for statement in grouped_statements
            )
        )
        if not statements:
            return None

        deployment_statements = tuple(
            (cube_name, tuple(sorted(grouped_statements)))
            for cube_name, grouped_statements in sorted(deployment_statements_by_cube.items())
        )
        deployment_cube = deployment_statements[0][0] if len(deployment_statements) == 1 else None

        return FeederPlan(
            statements=statements,
            strategy="consolidated_target_leaf_cross_cube",
            rationale=(
                "C: cross-cube feeder deployment is allowed because the consolidated target expands "
                "to a bounded, metadata-proven set of leaf measures, and each leaf measure "
                "independently resolves through an existing safe N:-level cross-cube feeder plan; "
                "the deployed feeders are the explicit union of those leaf-level plans."
            ),
            deployment_cube=deployment_cube,
            deployment_statements=deployment_statements,
            preview_only=False,
        )

    @staticmethod
    def _merge_db_target_feeder_statements(statements: Iterable[str]) -> set[str]:
        grouped_targets_by_lhs: Dict[str, set[str]] = defaultdict(set)
        passthrough: set[str] = set()

        for statement in statements:
            if " => " not in statement or not statement.endswith(";"):
                passthrough.add(statement)
                continue
            lhs, rhs_with_semicolon = statement[:-1].split(" => ", 1)
            rhs = rhs_with_semicolon.strip()
            if not rhs.startswith("DB("):
                passthrough.add(statement)
                continue
            grouped_targets_by_lhs[lhs].add(rhs)

        merged = {
            f"{lhs} => {', '.join(sorted(targets))};"
            for lhs, targets in grouped_targets_by_lhs.items()
        }
        return merged | passthrough

    def _build_target_db_statements(
        self,
        source_cube_name: str,
        target_ref: MeasureRef,
        fixed_target_elements: Mapping[str, str],
        broadcast_target_dimensions: Sequence[str],
    ) -> List[str]:
        target_metadata = self._get_cube_metadata(target_ref.cube_name)
        assert target_metadata is not None
        assert target_metadata.measure_dimension_name is not None

        broadcast_dimensions = tuple(sorted(set(broadcast_target_dimensions)))
        if any(
            self._dimension_hierarchy_ambiguity_reason(
                cube_name=target_ref.cube_name,
                dimension_name=dimension_name,
                explicit_hierarchy_name=None,
            )
            is not None
            for dimension_name in broadcast_dimensions
        ):
            return []
        broadcast_options = [
            tuple(target_metadata.dimension_leaf_elements.get(dimension_name, ()))
            for dimension_name in broadcast_dimensions
        ]
        if any(not options for options in broadcast_options):
            return []

        statements: List[str] = []
        for broadcast_combo in product(*broadcast_options) if broadcast_options else [()]:
            broadcast_elements_by_dimension = dict(zip(broadcast_dimensions, broadcast_combo))
            target_coordinates: List[str] = []
            for dimension_name in target_metadata.dimensions:
                if dimension_name == target_metadata.measure_dimension_name:
                    target_coordinates.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=target_ref.cube_name,
                            dimension_name=dimension_name,
                            element_name=target_ref.measure_name,
                            explicit_hierarchy_name=None,
                        )
                    )
                elif dimension_name in fixed_target_elements:
                    target_coordinates.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=target_ref.cube_name,
                            dimension_name=dimension_name,
                            element_name=fixed_target_elements[dimension_name],
                            explicit_hierarchy_name=None,
                        )
                    )
                elif dimension_name in broadcast_elements_by_dimension:
                    target_coordinates.append(
                        self._compile_dimension_element_coordinate(
                            cube_name=target_ref.cube_name,
                            dimension_name=dimension_name,
                            element_name=broadcast_elements_by_dimension[dimension_name],
                            explicit_hierarchy_name=None,
                        )
                    )
                elif dimension_name in (self._get_cube_metadata(source_cube_name).dimensions if self._get_cube_metadata(source_cube_name) is not None else ()):
                    target_coordinates.append(f"!{dimension_name}")
                else:
                    return []
            statements.append(f"DB('{target_ref.cube_name}', {', '.join(target_coordinates)})")

        return statements

    def _expand_feeder_origin_elements(
        self,
        cube_name: str,
        dimension_name: str,
        expression: Expression,
    ) -> Optional[tuple[tuple[Optional[str], str], ...]]:
        if isinstance(expression, LiteralExpression) and isinstance(expression.value, str):
            return self._resolve_leaf_safe_element_names(
                cube_name=cube_name,
                dimension_name=dimension_name,
                element_name=str(expression.value),
                explicit_hierarchy_name=None,
            )
        if isinstance(expression, ElementExpression):
            return self._resolve_leaf_safe_element_names(
                cube_name=expression.ref.cube_name,
                dimension_name=dimension_name,
                element_name=expression.ref.element_name,
                explicit_hierarchy_name=expression.ref.hierarchy_name,
            )
        return None

    def _resolve_leaf_safe_element_names(
        self,
        cube_name: str,
        dimension_name: str,
        element_name: str,
        explicit_hierarchy_name: Optional[str],
    ) -> Optional[tuple[tuple[Optional[str], str], ...]]:
        metadata = self._get_cube_metadata(cube_name)
        if metadata is None:
            return None

        leaf_elements = tuple(metadata.dimension_leaf_elements.get(dimension_name, ()))
        if element_name in leaf_elements:
            return ((explicit_hierarchy_name, element_name),)

        expansions = metadata.dimension_element_leaf_expansions.get(dimension_name, {})
        expanded_leaf_elements = tuple(expansions.get(element_name, ()))
        if expanded_leaf_elements:
            return tuple((explicit_hierarchy_name, leaf_element_name) for leaf_element_name in expanded_leaf_elements)

        structural_leaf_elements = self._expand_consolidated_element_to_leaves(
            metadata=metadata,
            dimension_name=dimension_name,
            element_name=element_name,
            explicit_hierarchy_name=explicit_hierarchy_name,
        )
        if structural_leaf_elements:
            return tuple(
                (explicit_hierarchy_name, leaf_element_name) for leaf_element_name in structural_leaf_elements
            )

        return None

    def _expand_consolidated_element_to_leaves(
        self,
        metadata: Any,
        dimension_name: str,
        element_name: str,
        explicit_hierarchy_name: Optional[str] = None,
        _visited: frozenset[tuple[Optional[str], str]] = frozenset(),
    ) -> Optional[tuple[str, ...]]:
        leaf_elements = tuple(metadata.dimension_leaf_elements.get(dimension_name, ()))
        if element_name in leaf_elements:
            return (element_name,)

        visited_key = (explicit_hierarchy_name, element_name)
        if visited_key in _visited:
            return None

        hierarchy_children = metadata.dimension_children_by_hierarchy.get(dimension_name, {})
        hierarchy_order: List[Optional[str]] = []
        if explicit_hierarchy_name is not None:
            # Caller pinned a hierarchy: resolve only against that hierarchy. Falling through to
            # other hierarchies here would silently resolve a different, unrequested hierarchy's
            # subtree -- exactly the "guess instead of fail closed" outcome this project's safety
            # discipline forbids elsewhere (see _dimension_hierarchy_ambiguity_reason).
            hierarchy_order.append(explicit_hierarchy_name)
        else:
            default_hierarchy_name = metadata.default_hierarchies.get(dimension_name)
            if default_hierarchy_name is not None and default_hierarchy_name not in hierarchy_order:
                hierarchy_order.append(default_hierarchy_name)
            for hierarchy_name in metadata.dimension_hierarchies.get(dimension_name, ()):
                if hierarchy_name not in hierarchy_order:
                    hierarchy_order.append(hierarchy_name)
            if None not in hierarchy_order:
                hierarchy_order.append(None)

        visited = _visited | {visited_key}
        for hierarchy_name in hierarchy_order:
            if hierarchy_name is None:
                children = tuple(metadata.dimension_children.get(dimension_name, {}).get(element_name, ()))
            else:
                children = tuple(hierarchy_children.get(hierarchy_name, {}).get(element_name, ()))
            if not children:
                continue

            collected_leaf_elements: List[str] = []
            for child_name in children:
                child_leaf_elements = self._expand_consolidated_element_to_leaves(
                    metadata=metadata,
                    dimension_name=dimension_name,
                    element_name=child_name,
                    explicit_hierarchy_name=hierarchy_name,
                    _visited=visited,
                )
                if child_leaf_elements is None:
                    collected_leaf_elements = []
                    break
                collected_leaf_elements.extend(child_leaf_elements)
            if collected_leaf_elements:
                return tuple(collected_leaf_elements)

        return None

    def _consolidated_feeder_rejection_reason(
        self,
        cube_name: str,
        dimension_name: str,
        element_name: str,
        explicit_hierarchy_name: Optional[str] = None,
    ) -> Optional[str]:
        resolved = self._resolve_leaf_safe_element_names(
            cube_name=cube_name,
            dimension_name=dimension_name,
            element_name=element_name,
            explicit_hierarchy_name=explicit_hierarchy_name,
        )
        if resolved is not None:
            return None

        metadata = self._get_cube_metadata(cube_name)
        element_type = None
        if metadata is not None:
            element_type = metadata.dimension_element_types.get(dimension_name, {}).get(element_name)

        if element_type == "Consolidated":
            return (
                f"element '{element_name}' on dimension '{dimension_name}' in cube '{cube_name}' is "
                "consolidated and has no provable leaf expansion (no explicit mapping and no hierarchy "
                "child metadata)"
            )

        return (
            f"element '{element_name}' on dimension '{dimension_name}' in cube '{cube_name}' has unknown "
            "element type and no provable leaf expansion"
        )

    def _iter_align_expressions(self, expression: Expression) -> Iterator[MethodExpression]:
        if isinstance(expression, MethodExpression):
            if expression.method_name == "align":
                yield expression
            yield from self._iter_align_expressions(expression.base)
            for arg in expression.args:
                yield from self._iter_align_expressions(arg)
            for value in expression.kwargs.values():
                yield from self._iter_align_expressions(value)
            return

        if isinstance(expression, UnaryExpression):
            yield from self._iter_align_expressions(expression.operand)
            return

        if isinstance(expression, (BinaryExpression, ComparisonExpression)):
            yield from self._iter_align_expressions(expression.left)
            yield from self._iter_align_expressions(expression.right)
            return

        if isinstance(expression, RollingExpression):
            yield from self._iter_align_expressions(expression.base)
            for value in expression.kwargs.values():
                yield from self._iter_align_expressions(value)
            return

        if isinstance(expression, CaseExpression):
            for condition, value in expression.cases:
                yield from self._iter_align_expressions(condition)
                yield from self._iter_align_expressions(value)
            yield from self._iter_align_expressions(expression.default)

    def _describe_unresolved_cross_cube_feeder_elements(self, formula: Formula) -> List[str]:
        reasons: List[str] = []
        seen: set[str] = set()
        for align_expression in self._iter_align_expressions(formula.expression):
            if not isinstance(align_expression.base, MeasureExpression):
                continue
            source_cube_name = align_expression.base.ref.cube_name
            for dimension_name, mapping in align_expression.kwargs.items():
                if isinstance(mapping, LiteralExpression) and isinstance(mapping.value, str):
                    element_name = mapping.value
                    explicit_hierarchy_name = None
                elif isinstance(mapping, ElementExpression):
                    element_name = mapping.ref.element_name
                    explicit_hierarchy_name = mapping.ref.hierarchy_name
                else:
                    continue

                reason = self._consolidated_feeder_rejection_reason(
                    cube_name=source_cube_name,
                    dimension_name=dimension_name,
                    element_name=element_name,
                    explicit_hierarchy_name=explicit_hierarchy_name,
                )
                if reason is not None and reason not in seen:
                    seen.add(reason)
                    reasons.append(reason)

            source_metadata = self._get_cube_metadata(source_cube_name)
            target_metadata = self._get_cube_metadata(formula.target.cube_name)
            if source_metadata is None or target_metadata is None:
                continue
            source_dimension_names = set(source_metadata.dimensions)
            for dimension_name in target_metadata.dimensions:
                if dimension_name == target_metadata.measure_dimension_name:
                    continue
                if dimension_name in source_dimension_names:
                    continue
                if dimension_name in align_expression.kwargs:
                    continue
                reason = self._dimension_hierarchy_ambiguity_reason(
                    cube_name=formula.target.cube_name,
                    dimension_name=dimension_name,
                    explicit_hierarchy_name=None,
                )
                if reason is not None and reason not in seen:
                    seen.add(reason)
                    reasons.append(reason)

        return reasons

    def _native_dimension_mapping_unsupported_reason(
        self,
        method_label: str,
        source_cube_name: str,
        source_dimension_name: str,
        mapped_expression: Expression,
        target_cube_name: str,
        target_dimension_names: set,
    ) -> Optional[str]:
        """Shared per-dimension mapping validation for any cross-cube lookup that resolves a
        source-cube dimension's coordinate to a literal, a target-cube attribute (single-hop or
        chained), or an explicit element reference -- used by both `align(...)` and the cross-cube
        subset of `consolidated_max/min/avg/count/count_unique(...)`. Value-driven mappings (a
        measure cell's own contents) are rejected the same way in both cases: no metadata field
        anywhere enumerates a generic measure cell's possible values, so no compile-time bounding
        fact can exist for that shape.
        """
        if isinstance(mapped_expression, AttributeExpression):
            if mapped_expression.ref.cube_name != target_cube_name:
                return f"{method_label} attribute mappings must come from the target cube context"
            if mapped_expression.ref.dimension_name not in target_dimension_names:
                return (
                    f"{method_label} attribute mapping uses target dimension "
                    f"'{mapped_expression.ref.dimension_name}', which is not part of cube '{target_cube_name}'"
                )
            return self._dimension_hierarchy_ambiguity_reason(
                cube_name=target_cube_name,
                dimension_name=mapped_expression.ref.dimension_name,
                explicit_hierarchy_name=None,
            )
        if isinstance(mapped_expression, LiteralExpression):
            return self._dimension_hierarchy_ambiguity_reason(
                cube_name=source_cube_name,
                dimension_name=source_dimension_name,
                explicit_hierarchy_name=None,
            )
        if isinstance(mapped_expression, ElementExpression):
            if mapped_expression.ref.dimension_name != source_dimension_name:
                return (
                    f"{method_label} element mapping for source dimension '{source_dimension_name}' "
                    f"must target the same dimension, not '{mapped_expression.ref.dimension_name}'"
                )
            return self._dimension_hierarchy_ambiguity_reason(
                cube_name=source_cube_name,
                dimension_name=source_dimension_name,
                explicit_hierarchy_name=mapped_expression.ref.hierarchy_name,
            )
        if isinstance(mapped_expression, ChainedAttributeExpression):
            if mapped_expression.ref.cube_name != target_cube_name:
                return f"{method_label} attribute mappings must come from the target cube context"
            if mapped_expression.ref.first_dimension_name not in target_dimension_names:
                return (
                    f"{method_label} attribute mapping uses target dimension "
                    f"'{mapped_expression.ref.first_dimension_name}', which is not part of cube '{target_cube_name}'"
                )
            # second_dimension_name is the intermediate "via" dimension for the attribute chase. Unlike
            # first_dimension_name, it is never used as a feeder coordinate axis -- it's purely a metadata
            # lookup table -- so it does not need to be one of this cube's own dimensions.
            first_hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
                cube_name=target_cube_name,
                dimension_name=mapped_expression.ref.first_dimension_name,
                explicit_hierarchy_name=None,
            )
            if first_hierarchy_reason is not None:
                return first_hierarchy_reason
            return self._dimension_hierarchy_ambiguity_reason(
                cube_name=target_cube_name,
                dimension_name=mapped_expression.ref.second_dimension_name,
                explicit_hierarchy_name=None,
            )
        if isinstance(mapped_expression, (MeasureExpression, MethodExpression, CaseExpression)):
            return (
                f"{method_label} mapping for source dimension '{source_dimension_name}' must be a literal "
                "or target-cube attribute reference; this mapping's value comes from a measure cell, "
                "whose possible contents are not enumerable from cube/dimension metadata -- TM1 places "
                "no constraint on a String/Numeric cell's value, unlike a dimension's fixed leaf-element "
                "set, so no compile-time feeder enumeration is possible for this shape"
            )
        return (
            f"{method_label} mapping for source dimension '{source_dimension_name}' must be a literal "
            "or target-cube attribute reference"
        )

    def _native_align_unsupported_reason(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> Optional[str]:
        if expression.args:
            return "align(...) does not support positional arguments in the native subset"
        if not isinstance(expression.base, MeasureExpression):
            return "align(...) requires a measure lookup source in the native subset"
        if expression.base.ref.cube_name == target_cube_name:
            return "align(...) only supports cross-cube lookups in the native subset"

        source_metadata = self._get_cube_metadata(expression.base.ref.cube_name)
        target_metadata = self._get_cube_metadata(target_cube_name)
        if source_metadata is None or target_metadata is None:
            return "align(...) needs source and target cube metadata to resolve dimension mapping"
        if not source_metadata.dimensions or not target_metadata.dimensions:
            return "align(...) needs source and target cube dimensions to resolve lookup shape"
        if not source_metadata.measure_dimension_name:
            return f"align(...) needs the measure dimension for cube '{expression.base.ref.cube_name}'"

        measure_hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
            cube_name=expression.base.ref.cube_name,
            dimension_name=source_metadata.measure_dimension_name,
            explicit_hierarchy_name=None,
        )
        if measure_hierarchy_reason is not None:
            return measure_hierarchy_reason

        source_dimension_names = set(source_metadata.dimensions)
        target_dimension_names = set(target_metadata.dimensions)

        for source_dimension_name, mapped_expression in expression.kwargs.items():
            if source_dimension_name not in source_dimension_names:
                return (
                    f"align(...) maps source dimension '{source_dimension_name}', "
                    f"which does not exist in cube '{expression.base.ref.cube_name}'"
                )
            mapping_reason = self._native_dimension_mapping_unsupported_reason(
                method_label="align(...)",
                source_cube_name=expression.base.ref.cube_name,
                source_dimension_name=source_dimension_name,
                mapped_expression=mapped_expression,
                target_cube_name=target_cube_name,
                target_dimension_names=target_dimension_names,
            )
            if mapping_reason is not None:
                return mapping_reason

        unresolved_dimensions = [
            dimension_name
            for dimension_name in source_metadata.dimensions
            if dimension_name != source_metadata.measure_dimension_name
            and dimension_name not in expression.kwargs
            and dimension_name not in target_dimension_names
        ]
        if unresolved_dimensions:
            return (
                "align(...) cannot infer source dimensions "
                + ", ".join(f"'{dimension_name}'" for dimension_name in unresolved_dimensions)
            )

        return None

    @staticmethod
    def _align_uses_attribute_mapping(expression: MethodExpression) -> bool:
        return any(
            isinstance(mapped_expression, (AttributeExpression, ChainedAttributeExpression))
            for mapped_expression in expression.kwargs.values()
        )

    def _align_has_target_only_broadcast_dimensions(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> bool:
        if expression.method_name != "align" or not isinstance(expression.base, MeasureExpression):
            return False
        source_metadata = self._get_cube_metadata(expression.base.ref.cube_name)
        target_metadata = self._get_cube_metadata(target_cube_name)
        if source_metadata is None or target_metadata is None:
            return False
        source_dimension_names = set(source_metadata.dimensions)
        attribute_target_dimensions: set[str] = set()
        for mapped_expression in expression.kwargs.values():
            if isinstance(mapped_expression, AttributeExpression):
                attribute_target_dimensions.add(mapped_expression.ref.dimension_name)
            elif isinstance(mapped_expression, ChainedAttributeExpression):
                attribute_target_dimensions.add(mapped_expression.ref.first_dimension_name)
                attribute_target_dimensions.add(mapped_expression.ref.second_dimension_name)
        return any(
            dimension_name != target_metadata.measure_dimension_name
            and dimension_name not in source_dimension_names
            and dimension_name not in attribute_target_dimensions
            for dimension_name in target_metadata.dimensions
        )

    def _align_reverse_mapping_proven(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> bool:
        if expression.method_name != "align" or not isinstance(expression.base, MeasureExpression):
            return True

        source_metadata = self._get_cube_metadata(expression.base.ref.cube_name)
        target_metadata = self._get_cube_metadata(target_cube_name)
        if source_metadata is None or target_metadata is None:
            return False

        attribute_mappings = {
            source_dimension_name: mapped_expression
            for source_dimension_name, mapped_expression in expression.kwargs.items()
            if isinstance(mapped_expression, (AttributeExpression, ChainedAttributeExpression))
        }
        if not attribute_mappings:
            return True

        return (
            self._build_align_attribute_reverse_mapping_groups(
                expression=expression,
                source_metadata=source_metadata,
                target_metadata=target_metadata,
                target_cube_name=target_cube_name,
            )
            is not None
        )

    def _align_target_leaf_scope_proven(
        self,
        expression: MethodExpression,
        target_cube_name: str,
    ) -> bool:
        if not self._align_has_target_only_broadcast_dimensions(expression, target_cube_name):
            return True
        if not isinstance(expression.base, MeasureExpression):
            return False
        source_metadata = self._get_cube_metadata(expression.base.ref.cube_name)
        target_metadata = self._get_cube_metadata(target_cube_name)
        if source_metadata is None or target_metadata is None:
            return False
        source_dimension_names = set(source_metadata.dimensions)
        for dimension_name in target_metadata.dimensions:
            if dimension_name == target_metadata.measure_dimension_name:
                continue
            if dimension_name in source_dimension_names:
                continue
            if (
                self._dimension_hierarchy_ambiguity_reason(
                    cube_name=target_cube_name,
                    dimension_name=dimension_name,
                    explicit_hierarchy_name=None,
                )
                is not None
            ):
                return False
            if not tuple(target_metadata.dimension_leaf_elements.get(dimension_name, ())):
                return False
        return True

    def _align_contains_unproven_reverse_mapping(
        self,
        expression: Expression,
        target_cube_name: str,
    ) -> bool:
        return any(
            not self._align_reverse_mapping_proven(align_expression, target_cube_name)
            for align_expression in self._iter_align_expressions(expression)
            if self._align_uses_attribute_mapping(align_expression)
        )

    def _align_contains_unproven_target_leaf_scope(
        self,
        expression: Expression,
        target_cube_name: str,
    ) -> bool:
        return any(
            not self._align_target_leaf_scope_proven(align_expression, target_cube_name)
            for align_expression in self._iter_align_expressions(expression)
            if self._align_has_target_only_broadcast_dimensions(align_expression, target_cube_name)
        )

    def _resolve_dimension_hierarchy(
        self,
        cube_name: Optional[str],
        dimension_name: str,
        explicit_hierarchy_name: Optional[str],
    ) -> Tuple[Optional[str], Optional[str]]:
        metadata = self._get_cube_metadata(cube_name) if cube_name is not None else None
        if metadata is None:
            return None, None

        known_hierarchies = tuple(metadata.dimension_hierarchies.get(dimension_name, ()))
        default_hierarchy_name = metadata.default_hierarchies.get(dimension_name)

        if explicit_hierarchy_name is not None:
            if known_hierarchies and explicit_hierarchy_name not in known_hierarchies:
                return None, (
                    f"Hierarchy '{explicit_hierarchy_name}' is not available for dimension "
                    f"'{dimension_name}' in cube '{cube_name}'."
                )
            return explicit_hierarchy_name, None

        if default_hierarchy_name:
            return default_hierarchy_name, None

        if len(known_hierarchies) == 1:
            return known_hierarchies[0], None

        return None, None

    def _dimension_hierarchy_ambiguity_reason(
        self,
        cube_name: str,
        dimension_name: str,
        explicit_hierarchy_name: Optional[str],
    ) -> Optional[str]:
        metadata = self._get_cube_metadata(cube_name)
        if metadata is None:
            return f"dimension hierarchy metadata is missing for cube '{cube_name}'"

        known_hierarchies = tuple(metadata.dimension_hierarchies.get(dimension_name, ()))
        default_hierarchy_name = metadata.default_hierarchies.get(dimension_name)

        if explicit_hierarchy_name is not None:
            if known_hierarchies and explicit_hierarchy_name not in known_hierarchies:
                return (
                    f"dimension '{dimension_name}' in cube '{cube_name}' does not contain "
                    f"hierarchy '{explicit_hierarchy_name}'"
                )
            return None

        if default_hierarchy_name:
            return None
        if len(known_hierarchies) <= 1:
            return None

        return (
            f"dimension '{dimension_name}' in cube '{cube_name}' has multiple hierarchies and "
            "no default hierarchy metadata, so fixed-element rule emission would be ambiguous"
        )

    def _resolve_simple_feeder_source_refs(
        self,
        plan: BuildPlan,
        formula: Formula,
    ) -> set[MeasureRef]:
        resolved: set[MeasureRef] = set()
        visited_keys: set[str] = set()

        def visit_measure_ref(ref: MeasureRef) -> None:
            if ref.cube_name != formula.target.cube_name:
                return
            if ref.key in visited_keys:
                # Cycle guard: a formula that references its own target (e.g. a non-additive
                # consolidation override reading the same measure's own leaf data, or any other
                # self-referencing same-cube formula) would otherwise recurse into the identical
                # expression tree forever, since plan.formulas.get(ref.key) resolves back to this
                # same formula. Already-visited refs are skipped rather than re-traced.
                return
            visited_keys.add(ref.key)

            nested_formula = plan.formulas.get(ref.key)
            if nested_formula is None:
                resolved.add(ref)
                return

            for nested_ref in self._iter_expression_measure_refs(
                nested_formula.expression,
                nested_formula.target.cube_name,
            ):
                visit_measure_ref(nested_ref)

        for ref in self._iter_expression_measure_refs(formula.expression, formula.target.cube_name):
            visit_measure_ref(ref)

        return resolved

    @staticmethod
    def _formula_areas_overlap(area_a: Tuple[Tuple[str, str], ...], area_b: Tuple[Tuple[str, str], ...]) -> bool:
        """Two area-scoped targets on the same measure overlap if every dimension fixed by both
        agrees on the element, and any dimension fixed by only one side is an unconstrained
        wildcard on the other -- i.e. there is at least one real cell both areas could match.
        A dimension neither side fixes is a wildcard on both sides and never disqualifies overlap.
        """
        dict_a = dict(area_a)
        dict_b = dict(area_b)
        for dimension_name in set(dict_a) & set(dict_b):
            if dict_a[dimension_name] != dict_b[dimension_name]:
                return False
        return True

    def _build_plan(self) -> BuildPlan:
        formulas = dict(sorted(self._formulas.items()))
        basic_logger.debug(f"_build_plan() resolving dependency order for {len(formulas)} formula(s).")
        dependencies: Dict[str, List[str]] = {}
        graph: Dict[str, List[str]] = defaultdict(list)
        in_degree: Dict[str, int] = {key: 0 for key in formulas}
        warnings: List[str] = []
        errors: List[str] = []

        # Multiple area-scoped formulas (CubeRef.area(...)) can share the same base "cube:measure"
        # identity. A measure *reference* inside another formula's expression never carries an
        # area of its own, so it always resolves to the bare base-measure key -- this mapping lets
        # such a reference depend on every area-variant registered for that measure, not just an
        # (impossible) exact string match against an area-suffixed key.
        base_measure_keys: Dict[str, List[str]] = defaultdict(list)
        for key, formula in formulas.items():
            base_measure_keys[formula.target.base_key].append(key)

        for key, formula in formulas.items():
            measure_deps = sorted(
                {
                    ref.key
                    for ref in self._iter_expression_measure_refs(
                        formula.expression,
                        formula.target.cube_name,
                    )
                }
            )
            dependencies[key] = measure_deps
            for dep_key in measure_deps:
                if dep_key == key:
                    # A formula referencing its own target (e.g. a non-additive consolidation
                    # override reading the same measure's own leaf data via
                    # ConsolidatedMax/Min/Avg/Count/CountUnique) is not a real build-order
                    # dependency -- there is no other coordinate to place "before" or "after"
                    # itself in this measure-level graph, and TM1 evaluates the rule directly
                    # without needing a Python-side execution order for it. Recording a
                    # self-edge here would make the topological sort see a cycle that isn't
                    # real at the cell level, so it is excluded rather than reported.
                    continue
                matched_keys = [
                    matched_key for matched_key in base_measure_keys.get(dep_key, []) if matched_key != key
                ]
                if matched_keys:
                    for matched_key in matched_keys:
                        graph[matched_key].append(key)
                        in_degree[key] += 1
                elif dep_key.split(":", 1)[0] == formula.target.cube_name:
                    warnings.append(
                        f"Formula '{key}' references '{dep_key}', which is treated as an external input because no formula is registered for it."
                    )

        # Rule-area overlap: two formulas on the same measure whose areas could both match the
        # same real cell must have an explicit, distinct precedence (Formula.priority), since
        # TM1 applies the first matching rule statement in file order for overlapping areas, and
        # this project never wants that order to fall out of an incidental alphabetical
        # tie-break. Fail closed instead of guessing whenever precedence is ambiguous.
        for base_key, group_keys in sorted(base_measure_keys.items()):
            if len(group_keys) < 2:
                continue
            for index_a in range(len(group_keys)):
                for index_b in range(index_a + 1, len(group_keys)):
                    key_a, key_b = group_keys[index_a], group_keys[index_b]
                    formula_a, formula_b = formulas[key_a], formulas[key_b]
                    if not self._formula_areas_overlap(formula_a.target.area, formula_b.target.area):
                        continue
                    if formula_a.priority == formula_b.priority:
                        errors.append(
                            f"Formulas '{key_a}' and '{key_b}' have overlapping rule areas on the "
                            f"same measure '{base_key}' with no distinct priority; set a lower "
                            "'priority' on whichever one must take precedence (CubeRef.area(...)"
                            ".set(expression, priority=...))."
                        )
                        continue
                    lower_key, higher_key = (
                        (key_a, key_b) if formula_a.priority < formula_b.priority else (key_b, key_a)
                    )
                    if higher_key not in graph[lower_key]:
                        graph[lower_key].append(higher_key)
                        in_degree[higher_key] += 1

        for dep_key in graph:
            graph[dep_key].sort()

        queue = deque(sorted(key for key, degree in in_degree.items() if degree == 0))
        order: List[str] = []

        while queue:
            current = queue.popleft()
            order.append(current)
            for downstream in graph.get(current, []):
                in_degree[downstream] -= 1
                if in_degree[downstream] == 0:
                    queue.append(downstream)

        if len(order) != len(formulas):
            cycle_nodes = sorted(key for key, degree in in_degree.items() if degree > 0)
            basic_logger.warning(f"_build_plan() detected a cyclic dependency among: {cycle_nodes}")
            errors.append(
                "Cyclic formula dependencies detected for targets: " + ", ".join(cycle_nodes)
            )

        if not errors:
            for key in sorted(formulas):
                formula = formulas[key]
                decision = self._select_backend(formula)
                readiness = self._formula_phase2_readiness(
                    BuildPlan(
                        formulas=formulas,
                        graph=dict(graph),
                        dependencies=dependencies,
                        order=order,
                        report=ValidationReport(errors=errors, warnings=[], execution_order=order),
                    ),
                    formula,
                )
                if decision.backend is Backend.PYTHON:
                    warnings.append(
                        f"Formula '{key}' will use Python materialization backend. {decision.rationale}"
                    )
                    continue

                if readiness["missing_metadata"]:
                    warnings.append(
                        f"Formula '{key}' is native-targeted but not deployment-ready because metadata is missing: "
                        + ", ".join(readiness["missing_metadata"])
                    )

                feeder_plan = self._plan_feeders(
                    BuildPlan(
                        formulas=formulas,
                        graph=dict(graph),
                        dependencies=dependencies,
                        order=order,
                        report=ValidationReport(errors=errors, warnings=[], execution_order=order),
                    ),
                    formula,
                )
                if feeder_plan.preview_only:
                    warnings.append(
                        f"Formula '{key}' compiles to native preview but live deployment stays blocked until "
                        "a safe cross-cube feeder plan is proven."
                    )

        basic_logger.debug(
            f"_build_plan() resolved order for {len(order)} formula(s), "
            f"{len(errors)} error(s), {len(set(warnings))} warning(s)."
        )
        return BuildPlan(
            formulas=formulas,
            graph=dict(graph),
            dependencies=dependencies,
            order=order,
            report=ValidationReport(errors=errors, warnings=sorted(set(warnings)), execution_order=order),
        )

    def _iter_expression_measure_refs(
        self,
        expression: Expression,
        target_cube_name: str,
    ) -> Iterator[MeasureRef]:
        if isinstance(expression, MeasureExpression):
            yield expression.ref
            return

        if isinstance(expression, (LiteralExpression, AttributeExpression, ChainedAttributeExpression, ElementExpression)):
            return

        if isinstance(expression, UnaryExpression):
            yield from self._iter_expression_measure_refs(expression.operand, target_cube_name)
            return

        if isinstance(expression, (BinaryExpression, ComparisonExpression)):
            yield from self._iter_expression_measure_refs(expression.left, target_cube_name)
            yield from self._iter_expression_measure_refs(expression.right, target_cube_name)
            return

        if isinstance(expression, MethodExpression):
            yield from self._iter_expression_measure_refs(expression.base, target_cube_name)
            for arg in expression.args:
                yield from self._iter_expression_measure_refs(arg, target_cube_name)
            for value in expression.kwargs.values():
                yield from self._iter_expression_measure_refs(value, target_cube_name)
            if expression.method_name == "growth":
                baseline_ref = self._resolve_growth_baseline_ref(expression, target_cube_name)
                if baseline_ref is not None:
                    yield baseline_ref
            return

        if isinstance(expression, RollingExpression):
            yield from self._iter_expression_measure_refs(expression.base, target_cube_name)
            for value in expression.kwargs.values():
                yield from self._iter_expression_measure_refs(value, target_cube_name)
            return

        if isinstance(expression, CaseExpression):
            for condition, value in expression.cases:
                yield from self._iter_expression_measure_refs(condition, target_cube_name)
                yield from self._iter_expression_measure_refs(value, target_cube_name)
            yield from self._iter_expression_measure_refs(expression.default, target_cube_name)

    def _measure_type_conversion_overrides(self, expression: Expression) -> Dict[Tuple[str, str], str]:
        """Finds every `measure_ref.to_string(...)`/`measure_ref.to_number()` in ``expression``.

        Returns a ``{(cube_name, measure_name): expected_type}`` map naming the *actual*
        element type the wrapped measure is expected to have (e.g. `to_number()` only makes
        sense applied to a String measure, so its expected type is "String"). Callers use this
        to skip the normal same-type-as-target-scope check for exactly the references that are
        explicitly converted -- this is what lets an `S:` formula reference a Numeric measure
        (via `.to_string(...)`) or an `N:` formula reference a String measure (via
        `.to_number()`) without the type-checking gate rejecting the cross-type reference.
        Bounded MVP: only recognizes the conversion method applied directly to a measure
        reference, matching `_native_unsupported_reason`'s structural gate for these methods.
        """
        overrides: Dict[Tuple[str, str], str] = {}

        def walk(node: Expression) -> None:
            if isinstance(node, MethodExpression):
                if node.method_name == "to_number" and isinstance(node.base, MeasureExpression):
                    overrides[(node.base.ref.cube_name, node.base.ref.measure_name)] = "String"
                elif node.method_name == "to_string" and isinstance(node.base, MeasureExpression):
                    overrides[(node.base.ref.cube_name, node.base.ref.measure_name)] = "Numeric"
                walk(node.base)
                for arg in node.args:
                    walk(arg)
                for value in node.kwargs.values():
                    walk(value)
                return
            if isinstance(node, UnaryExpression):
                walk(node.operand)
                return
            if isinstance(node, (BinaryExpression, ComparisonExpression)):
                walk(node.left)
                walk(node.right)
                return
            if isinstance(node, RollingExpression):
                walk(node.base)
                for value in node.kwargs.values():
                    walk(value)
                return
            if isinstance(node, CaseExpression):
                for condition, value in node.cases:
                    walk(condition)
                    walk(value)
                walk(node.default)
                return

        walk(expression)
        return overrides

    @staticmethod
    def _collect_upstream_formula_keys(dependencies: Mapping[str, Sequence[str]], target_key: str) -> List[str]:
        upstream: set[str] = set()
        stack = list(dependencies.get(target_key, ()))
        while stack:
            current = stack.pop()
            if current in upstream:
                continue
            upstream.add(current)
            stack.extend(dependencies.get(current, ()))
        return sorted(upstream)

    @staticmethod
    def _normalize_target(target: str) -> str:
        text = str(target).strip()
        if ":" in text:
            return text
        if "." in text:
            cube_name, measure_name = text.split(".", 1)
            return f"{cube_name}:{measure_name}"
        raise ValueError("Target must look like 'Cube:Measure' or 'Cube.Measure'")
