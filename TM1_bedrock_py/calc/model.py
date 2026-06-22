from __future__ import annotations

from collections import defaultdict, deque
from dataclasses import dataclass, field
from enum import Enum
from itertools import product
from typing import Any, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, Tuple, Union

from .metadata import MetadataProvider, TM1ServiceMetadataProvider


def _ensure_expression(value: Any) -> "Expression":
    if isinstance(value, Expression):
        return value
    return LiteralExpression(value=value)


@dataclass(frozen=True)
class MeasureRef:
    cube_name: str
    measure_name: str

    @property
    def key(self) -> str:
        return f"{self.cube_name}:{self.measure_name}"

    @property
    def label(self) -> str:
        return f"{self.cube_name}.{self.measure_name}"


@dataclass(frozen=True)
class AttributeRef:
    cube_name: str
    dimension_name: str
    attribute_name: str

    @property
    def label(self) -> str:
        return f"{self.cube_name}.{self.dimension_name}.attribute({self.attribute_name})"


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

    def ytd(self) -> "Expression":
        return MethodExpression("ytd", self)

    def growth(self, **kwargs: Any) -> "Expression":
        return MethodExpression("growth", self, kwargs={key: _ensure_expression(value) for key, value in kwargs.items()})

    def rolling(self, **kwargs: Any) -> "RollingExpression":
        return RollingExpression(self, kwargs={key: _ensure_expression(value) for key, value in kwargs.items()})

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        return iter(())

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        return iter(())

    def iter_element_refs(self) -> Iterator[ElementRef]:
        return iter(())

    def describe(self) -> str:
        raise NotImplementedError


@dataclass(frozen=True)
class LiteralExpression(Expression):
    value: Any

    def describe(self) -> str:
        return repr(self.value)


@dataclass(frozen=True)
class MeasureExpression(Expression):
    ref: MeasureRef

    def iter_measure_refs(self) -> Iterator[MeasureRef]:
        yield self.ref

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True)
class AttributeExpression(Expression):
    ref: AttributeRef

    def iter_attribute_refs(self) -> Iterator[AttributeRef]:
        yield self.ref

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True)
class ElementExpression(Expression):
    ref: ElementRef

    def iter_element_refs(self) -> Iterator[ElementRef]:
        yield self.ref

    def describe(self) -> str:
        return self.ref.label


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class ComparisonExpression(BinaryExpression):
    pass


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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


@dataclass(frozen=True)
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


@dataclass(frozen=True)
class Formula:
    target: MeasureRef
    expression: Expression

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

    def element(self, element_name: str, hierarchy: Optional[str] = None) -> ElementExpression:
        return ElementExpression(
            ref=ElementRef(
                cube_name=self._cube_name,
                dimension_name=self._dimension_name,
                element_name=element_name,
                hierarchy_name=hierarchy,
            )
        )


class CubeRef:
    def __init__(self, model: "Model", cube_name: str):
        self._model = model
        self._cube_name = cube_name

    def __getitem__(self, measure_name: str) -> MeasureExpression:
        return MeasureExpression(ref=MeasureRef(cube_name=self._cube_name, measure_name=measure_name))

    def __setitem__(self, measure_name: str, expression: Any) -> None:
        self._model.register_formula(self[measure_name], _ensure_expression(expression))

    def __getattr__(self, name: str) -> DimensionProxy:
        if name.startswith("_"):
            raise AttributeError(name)
        return DimensionProxy(self._cube_name, name)


class Model:
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

    def register_formula(self, target: MeasureExpression, expression: Expression) -> None:
        key = target.ref.key
        if key in self._formulas:
            raise ValueError(f"Formula target '{key}' is already registered")
        self._formulas[key] = Formula(target=target.ref, expression=expression)

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
            key: self._formula_phase2_readiness(plan.formulas[key])
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

    def compile(self, dry_run: bool = True) -> CompilePreview:
        plan = self._build_plan()
        rules_by_cube: Dict[str, List[str]] = defaultdict(list)
        feeders_by_cube: Dict[str, List[str]] = defaultdict(list)
        manifest: Dict[str, Dict[str, Any]] = {}
        errors = list(plan.report.errors)
        warnings = list(plan.report.warnings)

        if errors:
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
            readiness = self._formula_phase2_readiness(formula)
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
                target_reference = self._compile_native_measure_reference(
                    cube_name=formula.target.cube_name,
                    measure_name=formula.target.measure_name,
                )
                rule_statement = f"{target_reference} = N: {rule_body};"

            manifest[target_key] = {
                "cube": formula.target.cube_name,
                "measure": formula.target.measure_name,
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
        if dry_run:
            return preview

        self._deploy_compile_preview(preview)
        return preview

    def _deploy_compile_preview(self, preview: CompilePreview) -> None:
        if self.tm1 is None:
            raise DeploymentError("Live TM1 deployment requires an attached TM1 service.")

        self._assert_deployment_ready(preview)

        deployment = self._build_deployment_plan(preview, live=True)
        for cube_name in sorted(set(preview.rules) | set(preview.feeders)):
            rule_text = self._render_rule_artifact(
                cube_name,
                preview.rules.get(cube_name, ""),
                preview.feeders.get(cube_name, ""),
            )
            self.tm1.cubes.update_or_create_rules(cube_name, rule_text)
            check_response = self.tm1.cubes.check_rules(cube_name)
            status_code = getattr(check_response, "status_code", None)
            if status_code is not None and not (200 <= status_code < 300):
                raise DeploymentError(
                    f"TM1 rule validation failed for cube '{cube_name}' with status {status_code}."
                )
            deployment[cube_name]["deployed"] = True
            deployment[cube_name]["rule_length"] = len(rule_text)
            deployment[cube_name]["check_rules_status"] = status_code

        preview.deployment = deployment

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
    def _render_rule_artifact(cube_name: str, rules_text: str, feeders_text: str) -> str:
        sections = [
            "# Generated by TM1_bedrock_py calc Phase 2 compiler",
            f"# Cube: {cube_name}",
        ]
        if rules_text.strip():
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

    def _formula_explanation(self, plan: BuildPlan, target_key: str) -> Dict[str, Any]:
        formula = plan.formulas[target_key]
        upstream = self._collect_upstream_formula_keys(plan.dependencies, target_key)
        decision = self._select_backend(formula)
        readiness = self._formula_phase2_readiness(formula)
        return {
            "target": target_key,
            "expression": formula.expression.describe(),
            "measure_dependencies": plan.dependencies[target_key],
            "attribute_dependencies": sorted({ref.label for ref in formula.expression.iter_attribute_refs()}),
            "upstream_formulas": upstream,
            "execution_order": [key for key in plan.order if key in upstream or key == target_key],
            "backend": decision.backend.value,
            "rationale": decision.rationale,
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
            "metadata_ready": readiness["metadata_ready"],
            "required_metadata": readiness["required_metadata"],
            "missing_metadata": readiness["missing_metadata"],
        }

    def _formula_phase2_readiness(self, formula: Formula) -> Dict[str, Any]:
        decision = self._select_backend(formula)
        requirements = self._metadata_requirements_for_formula(formula)
        return {
            "target": formula.key,
            "backend": decision.backend.value,
            "rationale": decision.rationale,
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
            "missing_metadata": [
                requirement.code
                for requirement in requirements
                if not requirement.satisfied
            ],
        }

    def _select_backend(self, formula: Formula) -> BackendDecision:
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
        add_requirement(
            code="numeric_measure",
            description="Native rule compilation currently requires numeric target measures.",
            cube_name=formula.target.cube_name,
            detail=f"Target measure '{formula.target.measure_name}'",
            satisfied=self._is_numeric_measure_type(target_measure_type),
        )

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
                satisfied=(
                    code not in {"alignment_mapping", "hierarchy_resolution", "growth_baseline"}
                    or self._native_unsupported_reason(
                        formula.expression,
                        target_cube_name=formula.target.cube_name,
                    ) is None
                ),
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

    def _get_cube_metadata(self, cube_name: str) -> Any:
        if self.metadata_provider is None:
            return None
        return self.metadata_provider.get_cube_metadata(cube_name)

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
            if expression.method_name == "ytd":
                return (
                    "ytd(...) requires summing a variable number of prior-period cells, which is a "
                    "multi-cell aggregation that does not reduce to a single bounded DB(...)/ATTRS(...) "
                    "lookup; it stays on the Python materialization backend by design"
                )

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
            raise ValueError(f"Method '{expression.method_name}' is not supported by the native compiler")

        if isinstance(expression, CaseExpression):
            return self._compile_native_case_expression(expression, target_cube_name)

        raise ValueError(f"Expression type '{type(expression).__name__}' is not supported by the native compiler")

    def _compile_native_binary_expression(self, expression: BinaryExpression, target_cube_name: str) -> str:
        left = self._compile_native_expression(expression.left, target_cube_name)
        right = self._compile_native_expression(expression.right, target_cube_name)
        operator = "<>" if expression.operator == "!=" else expression.operator
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

    def _plan_feeders(self, plan: BuildPlan, formula: Formula) -> FeederPlan:
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

        if self._contains_align_expression(formula.expression):
            conditional_cross_cube_plan = self._build_conditional_cross_cube_feeders(formula)
            if conditional_cross_cube_plan is not None:
                return self._merge_same_cube_driver_feeders(
                    conditional_cross_cube_plan, formula.target.cube_name, statements
                )
            align_expression_count = sum(1 for _ in self._iter_align_expressions(formula.expression))
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
                    direct_cross_cube_plan, formula.target.cube_name, statements
                )
            attribute_routed_cross_cube_plan = self._build_attribute_routed_cross_cube_feeders(formula)
            if attribute_routed_cross_cube_plan is not None:
                return self._merge_same_cube_driver_feeders(
                    attribute_routed_cross_cube_plan, formula.target.cube_name, statements
                )
            return FeederPlan(
                statements=(),
                strategy="preview_only_cross_cube",
                rationale=(
                    "Cross-cube native preview exists, but Phase 2 cannot yet prove a safe feeder "
                    "origin, invertible routing path, and leaf-level target scope for this lookup."
                ),
                deployment_cube=formula.target.cube_name,
                preview_only=True,
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
        )

    def _merge_same_cube_driver_feeders(
        self,
        plan: FeederPlan,
        target_cube_name: str,
        local_driver_statements: tuple[str, ...],
    ) -> FeederPlan:
        if not local_driver_statements:
            return plan

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
        )

    def _build_conditional_cross_cube_feeders(self, formula: Formula) -> Optional[FeederPlan]:
        align_expressions = tuple(self._iter_align_expressions(formula.expression))
        if not align_expressions:
            return None
        if len(align_expressions) == 1 and isinstance(formula.expression, MethodExpression) and formula.expression.method_name == "align":
            return None

        branch_plans: List[FeederPlan] = []
        for align_expression in align_expressions:
            branch_formula = Formula(target=formula.target, expression=align_expression)
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

        attribute_mappings: Dict[str, AttributeExpression] = {}
        fixed_source_values: Dict[str, Expression] = {}
        passthrough_dims: List[str] = []

        for dimension_name in source_metadata.dimensions:
            if dimension_name == source_measure_dim:
                continue

            mapping = expression.kwargs.get(dimension_name)
            if isinstance(mapping, AttributeExpression):
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

        target_candidates_by_source_dim: Dict[str, Dict[str, tuple[str, ...]]] = {}
        for source_dimension_name, mapping in sorted(attribute_mappings.items()):
            if mapping.ref.cube_name != formula.target.cube_name:
                return None

            target_dimension_name = mapping.ref.dimension_name
            leaf_elements = tuple(target_metadata.dimension_leaf_elements.get(target_dimension_name, ()))
            attribute_values = target_metadata.dimension_attribute_values.get(target_dimension_name, {})
            source_leaf_elements = tuple(source_metadata.dimension_leaf_elements.get(source_dimension_name, ()))
            if not leaf_elements or not source_leaf_elements:
                return None

            candidates: Dict[str, tuple[str, ...]] = {}
            for source_element_name in source_leaf_elements:
                matched_target_elements = tuple(
                    target_element_name
                    for target_element_name in leaf_elements
                    if str(
                        attribute_values.get(target_element_name, {}).get(mapping.ref.attribute_name, "")
                    ).strip()
                    == str(source_element_name).strip()
                )
                candidates[source_element_name] = matched_target_elements
            target_candidates_by_source_dim[source_dimension_name] = candidates

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
            for source_dimension_name, mapping in sorted(attribute_mappings.items()):
                source_element_name = source_element_by_dimension[source_dimension_name]
                target_elements = target_candidates_by_source_dim[source_dimension_name][source_element_name]
                if not target_elements:
                    target_element_groups = []
                    break
                target_element_groups.append((mapping.ref.dimension_name, target_elements))
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
                    broadcast_target_dimensions=[],
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

        return FeederPlan(
            statements=tuple(feeder_statements),
            strategy="cross_cube_attribute_lookup",
            rationale=(
                "Cross-cube feeder deployment is allowed because metadata proves a bounded reverse mapping "
                "from source lookup elements to target leaf elements through explicit attribute values."
            ),
            deployment_cube=source_ref.cube_name,
            preview_only=False,
        )

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
            for element_name in leaf_elements:
                predecessor_value = int(str(element_name).strip()) + offset
                matched_target_elements_by_source[str(predecessor_value)].append(element_name)
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

        return None

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
            if isinstance(mapped_expression, AttributeExpression):
                if mapped_expression.ref.cube_name != target_cube_name:
                    return "align(...) attribute mappings must come from the target cube context"
                if mapped_expression.ref.dimension_name not in target_dimension_names:
                    return (
                        f"align(...) attribute mapping uses target dimension "
                        f"'{mapped_expression.ref.dimension_name}', which is not part of cube '{target_cube_name}'"
                    )
                attribute_hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
                    cube_name=target_cube_name,
                    dimension_name=mapped_expression.ref.dimension_name,
                    explicit_hierarchy_name=None,
                )
                if attribute_hierarchy_reason is not None:
                    return attribute_hierarchy_reason
                continue
            if isinstance(mapped_expression, LiteralExpression):
                hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
                    cube_name=expression.base.ref.cube_name,
                    dimension_name=source_dimension_name,
                    explicit_hierarchy_name=None,
                )
                if hierarchy_reason is not None:
                    return hierarchy_reason
                continue
            if isinstance(mapped_expression, ElementExpression):
                if mapped_expression.ref.dimension_name != source_dimension_name:
                    return (
                        f"align(...) element mapping for source dimension '{source_dimension_name}' "
                        f"must target the same dimension, not '{mapped_expression.ref.dimension_name}'"
                    )
                hierarchy_reason = self._dimension_hierarchy_ambiguity_reason(
                    cube_name=expression.base.ref.cube_name,
                    dimension_name=source_dimension_name,
                    explicit_hierarchy_name=mapped_expression.ref.hierarchy_name,
                )
                if hierarchy_reason is not None:
                    return hierarchy_reason
                continue
            return (
                f"align(...) mapping for source dimension '{source_dimension_name}' must be a literal "
                "or target-cube attribute reference"
            )

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

        def visit_measure_ref(ref: MeasureRef) -> None:
            if ref.cube_name != formula.target.cube_name:
                return

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

    def _build_plan(self) -> BuildPlan:
        formulas = dict(sorted(self._formulas.items()))
        dependencies: Dict[str, List[str]] = {}
        graph: Dict[str, List[str]] = defaultdict(list)
        in_degree: Dict[str, int] = {key: 0 for key in formulas}
        warnings: List[str] = []

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
                if dep_key in formulas:
                    graph[dep_key].append(key)
                    in_degree[key] += 1
                elif dep_key.split(":", 1)[0] == formula.target.cube_name:
                    warnings.append(
                        f"Formula '{key}' references '{dep_key}', which is treated as an external input because no formula is registered for it."
                    )

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

        errors: List[str] = []
        if len(order) != len(formulas):
            cycle_nodes = sorted(key for key, degree in in_degree.items() if degree > 0)
            errors.append(
                "Cyclic formula dependencies detected for targets: " + ", ".join(cycle_nodes)
            )

        if not errors:
            for key in sorted(formulas):
                formula = formulas[key]
                decision = self._select_backend(formula)
                readiness = self._formula_phase2_readiness(formula)
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

        if isinstance(expression, (LiteralExpression, AttributeExpression, ElementExpression)):
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
