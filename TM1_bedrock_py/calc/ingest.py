from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple

from .model import (
    AttributeExpression,
    AttributeRef,
    BinaryExpression,
    CaseExpression,
    ComparisonExpression,
    Expression,
    LiteralExpression,
    MeasureExpression,
    MeasureRef,
    MethodExpression,
    UnaryExpression,
)

if TYPE_CHECKING:
    from .metadata import MetadataProvider


class UnsupportedRuleIngestionError(ValueError):
    """Raised for genuine misuse of the ingestion API, not for unsupported rule syntax.

    Unsupported rule syntax is reported through ``RuleIngestionRejection`` instead, so
    one unrecognized statement does not abort ingestion of every other statement in the
    same rule text.
    """


class _RuleParseError(Exception):
    pass


_STET = object()

_COMPARE_OPS = {"<>": "!=", "=": "==", ">": ">", ">=": ">=", "<": "<", "<=": "<="}
_UNSUPPORTED_FUNCTIONS = {"ATTRS", "STR", "NUMBR"}

_TOKEN_PATTERN = re.compile(
    r"""
    (?P<NUMBER>\d+\.\d+|\d+)
  | (?P<STRING>'[^']*')
  | (?P<BRACKET>\[[^\]]*\])
  | (?P<IDENT>[A-Za-z_][A-Za-z_0-9]*)
  | (?P<OP><>|>=|<=|[+\-*/<>=(),!:])
  | (?P<SPACE>\s+)
    """,
    re.VERBOSE,
)


@dataclass(frozen=True)
class RuleIngestionRejection:
    statement_text: str
    reason: str
    measure_name: Optional[str] = None


@dataclass(frozen=True)
class ParsedRuleStatement:
    measure_name: str
    scope: str
    expression: Expression


@dataclass
class RuleIngestionReport:
    cube_name: str
    ingested: List[str] = field(default_factory=list)
    rejected: List[RuleIngestionRejection] = field(default_factory=list)


def _strip_comments(rule_text: str) -> str:
    lines = []
    for line in rule_text.splitlines():
        hash_index = line.find("#")
        lines.append(line if hash_index < 0 else line[:hash_index])
    return "\n".join(lines)


def _split_statements(rule_text: str) -> List[str]:
    statements: List[str] = []
    buffer: List[str] = []
    depth = 0
    in_quote = False
    for character in rule_text:
        if in_quote:
            buffer.append(character)
            if character == "'":
                in_quote = False
            continue
        if character == "'":
            in_quote = True
            buffer.append(character)
            continue
        if character in "([":
            depth += 1
            buffer.append(character)
            continue
        if character in ")]":
            depth -= 1
            buffer.append(character)
            continue
        if character == ";" and depth == 0:
            statements.append("".join(buffer))
            buffer = []
            continue
        buffer.append(character)
    tail = "".join(buffer).strip()
    if tail:
        statements.append(tail)
    return statements


_STATEMENT_PATTERN = re.compile(
    r"^\[(?P<target>[^\]]*)\]\s*=\s*(?P<scope>[A-Za-z]+)\s*:\s*(?P<rhs>.*)$",
    re.DOTALL,
)


def _extract_measure_name(bracket_content: str) -> str:
    quoted = re.findall(r"'([^']+)'", bracket_content)
    if quoted:
        return quoted[-1]
    return bracket_content.strip()


def _tokenize(rhs_text: str) -> List[Tuple[str, str]]:
    tokens: List[Tuple[str, str]] = []
    position = 0
    length = len(rhs_text)
    while position < length:
        match = _TOKEN_PATTERN.match(rhs_text, position)
        if not match:
            raise _RuleParseError(f"unrecognized character at position {position}: '{rhs_text[position]}'")
        kind = match.lastgroup
        text = match.group()
        position = match.end()
        if kind == "SPACE":
            continue
        tokens.append((kind, text))
    return tokens


class _ExpressionParser:
    def __init__(
        self,
        tokens: List[Tuple[str, str]],
        cube_name: str,
        metadata_provider: Optional["MetadataProvider"] = None,
    ):
        self._tokens = tokens
        self._position = 0
        self._cube_name = cube_name
        self._metadata_provider = metadata_provider

    def parse(self) -> Expression:
        expression = self._parse_comparison()
        if self._position != len(self._tokens):
            remaining = " ".join(text for _, text in self._tokens[self._position :])
            raise _RuleParseError(f"trailing tokens after expression: {remaining}")
        return expression

    def _peek(self) -> Optional[Tuple[str, str]]:
        if self._position < len(self._tokens):
            return self._tokens[self._position]
        return None

    def _peek_text(self, offset: int = 0) -> Optional[str]:
        index = self._position + offset
        if index < len(self._tokens):
            return self._tokens[index][1]
        return None

    def _advance(self) -> Tuple[str, str]:
        token = self._tokens[self._position]
        self._position += 1
        return token

    def _expect_text(self, expected: str) -> None:
        token = self._peek()
        if token is None or token[1].upper() != expected.upper():
            raise _RuleParseError(f"expected '{expected}' but found '{token[1] if token else 'end of expression'}'")
        self._advance()

    def _parse_comparison(self) -> Expression:
        left = self._parse_additive()
        next_text = self._peek_text()
        if next_text is not None and next_text in _COMPARE_OPS:
            self._advance()
            right = self._parse_additive()
            return ComparisonExpression(_COMPARE_OPS[next_text], left, right)
        return left

    def _parse_additive(self) -> Expression:
        left = self._parse_multiplicative()
        while self._peek_text() in ("+", "-"):
            operator = self._advance()[1]
            right = self._parse_multiplicative()
            left = BinaryExpression(operator, left, right)
        return left

    def _parse_multiplicative(self) -> Expression:
        left = self._parse_unary()
        while self._peek_text() in ("*", "/"):
            operator = self._advance()[1]
            right = self._parse_unary()
            left = BinaryExpression(operator, left, right)
        return left

    def _parse_unary(self) -> Expression:
        if self._peek_text() == "-":
            self._advance()
            return UnaryExpression("-", self._parse_unary())
        return self._parse_atom()

    def _parse_atom(self) -> Expression:
        token = self._peek()
        if token is None:
            raise _RuleParseError("unexpected end of expression")
        kind, text = token

        if kind == "NUMBER":
            self._advance()
            return LiteralExpression(value=float(text) if "." in text else int(text))

        if kind == "STRING":
            self._advance()
            return LiteralExpression(value=text[1:-1])

        if kind == "BRACKET":
            self._advance()
            measure_name = _extract_measure_name(text[1:-1])
            return MeasureExpression(ref=MeasureRef(cube_name=self._cube_name, measure_name=measure_name))

        if kind == "OP" and text == "(":
            self._advance()
            inner = self._parse_comparison()
            self._expect_text(")")
            return inner

        if kind == "IDENT":
            upper_text = text.upper()
            if upper_text == "IF":
                return self._parse_if_call()
            if upper_text == "DB":
                return self._parse_db_call()
            if upper_text in _UNSUPPORTED_FUNCTIONS:
                raise _RuleParseError(f"'{upper_text}(...)' is not yet supported by rule ingestion")
            raise _RuleParseError(f"unrecognized identifier '{text}'")

        raise _RuleParseError(f"unexpected token '{text}'")

    def _parse_db_call(self) -> Expression:
        if self._metadata_provider is None:
            raise _RuleParseError(
                "DB(...) cross-cube ingestion requires a metadata provider to resolve the "
                "source cube's dimension order; none was supplied"
            )

        self._expect_text("DB")
        self._expect_text("(")
        source_cube_token = self._peek()
        if source_cube_token is None or source_cube_token[0] != "STRING":
            raise _RuleParseError("DB(...) must start with a quoted source cube name")
        self._advance()
        source_cube_name = source_cube_token[1][1:-1]

        coordinates: List[Any] = []
        while True:
            self._expect_text(",")
            coordinates.append(self._parse_db_coordinate())
            if self._peek_text() == ")":
                break
        self._expect_text(")")

        source_metadata = self._metadata_provider.get_cube_metadata(source_cube_name)
        if source_metadata is None:
            raise _RuleParseError(
                f"DB(...) references source cube '{source_cube_name}' which has no available metadata"
            )

        return _build_db_align_or_shift_expression(
            target_cube_name=self._cube_name,
            source_cube_name=source_cube_name,
            source_metadata=source_metadata,
            coordinates=coordinates,
        )

    def _parse_db_coordinate(self) -> Any:
        token = self._peek()
        if token is None:
            raise _RuleParseError("unexpected end of DB(...) coordinate list")
        kind, text = token

        if kind == "NUMBER":
            self._advance()
            return ("literal", float(text) if "." in text else int(text))

        if kind == "STRING":
            value = text[1:-1]
            self._advance()
            while self._peek_text() == ":":
                self._advance()
                next_token = self._peek()
                if next_token is None or next_token[0] != "STRING":
                    raise _RuleParseError("expected a quoted segment after ':' in a DB(...) coordinate")
                value = next_token[1][1:-1]
                self._advance()
            return ("literal", value)

        if kind == "OP" and text == "!":
            self._advance()
            ident_token = self._peek()
            if ident_token is None or ident_token[0] != "IDENT":
                raise _RuleParseError("expected a dimension name after '!' in a DB(...) coordinate")
            self._advance()
            return ("passthrough", ident_token[1])

        if kind == "IDENT" and text.upper() == "ATTRS":
            return self._parse_attrs_coordinate()

        if kind == "IDENT" and text.upper() == "STR":
            return self._parse_numeric_shift_coordinate()

        raise _RuleParseError(f"unsupported DB(...) coordinate shape near '{text}'")

    def _parse_attrs_coordinate(self) -> Any:
        self._expect_text("ATTRS")
        self._expect_text("(")
        dim_token = self._peek()
        if dim_token is None or dim_token[0] != "STRING":
            raise _RuleParseError("ATTRS(...) first argument must be a quoted dimension name")
        self._advance()
        dimension_name = dim_token[1][1:-1]
        self._expect_text(",")
        self._expect_text("!")
        current_element_token = self._peek()
        if current_element_token is None or current_element_token[0] != "IDENT":
            raise _RuleParseError("ATTRS(...) second argument must be a current-element reference (!Dimension)")
        self._advance()
        self._expect_text(",")
        attribute_token = self._peek()
        if attribute_token is None or attribute_token[0] != "STRING":
            raise _RuleParseError("ATTRS(...) third argument must be a quoted attribute name")
        self._advance()
        attribute_name = attribute_token[1][1:-1]
        self._expect_text(")")
        return ("attrs", dimension_name, attribute_name)

    def _parse_numeric_shift_coordinate(self) -> Any:
        self._expect_text("STR")
        self._expect_text("(")
        self._expect_text("NUMBR")
        self._expect_text("(")
        self._expect_text("!")
        dim_token = self._peek()
        if dim_token is None or dim_token[0] != "IDENT":
            raise _RuleParseError("STR(NUMBR(...)) expects a current-element reference (!Dimension)")
        self._advance()
        dimension_name = dim_token[1]
        self._expect_text(")")

        sign_text = self._peek_text()
        if sign_text not in ("+", "-"):
            raise _RuleParseError("STR(NUMBR(...) ...) expects a '+' or '-' offset")
        self._advance()

        self._expect_text("(")
        negative = False
        if self._peek_text() == "-":
            negative = True
            self._advance()
        number_token = self._peek()
        if number_token is None or number_token[0] != "NUMBER":
            raise _RuleParseError("STR(NUMBR(...) ...) offset must be a numeric literal")
        self._advance()
        offset_value = int(number_token[1])
        if negative:
            offset_value = -offset_value
        if sign_text == "-":
            offset_value = -offset_value
        self._expect_text(")")
        self._expect_text(")")
        return ("numeric_shift", dimension_name, offset_value)

    def _parse_if_call(self) -> Expression:
        cases, default = self._parse_if_chain()
        if default is _STET:
            if len(cases) != 1:
                raise _RuleParseError("STET is only supported as the final branch of a single-condition IF(...)")
            condition, value = cases[0]
            return MethodExpression("where", value, args=(condition,))
        return CaseExpression(cases=tuple(cases), default=default)

    def _parse_if_chain(self) -> Tuple[List[Tuple[Expression, Expression]], Any]:
        self._expect_text("IF")
        self._expect_text("(")
        condition = self._parse_comparison()
        self._expect_text(",")
        value = self._parse_comparison()
        self._expect_text(",")

        if self._peek_text() is not None and self._peek_text().upper() == "STET":
            self._advance()
            self._expect_text(")")
            return [(condition, value)], _STET

        if self._peek_text() is not None and self._peek_text().upper() == "IF" and self._peek_text(1) == "(":
            nested_cases, nested_default = self._parse_if_chain()
            self._expect_text(")")
            return [(condition, value)] + nested_cases, nested_default

        default = self._parse_comparison()
        self._expect_text(")")
        return [(condition, value)], default


def _build_db_align_or_shift_expression(
    target_cube_name: str,
    source_cube_name: str,
    source_metadata: Any,
    coordinates: List[Any],
) -> Expression:
    source_dimensions = tuple(source_metadata.dimensions)
    measure_dimension_name = source_metadata.measure_dimension_name

    if len(coordinates) != len(source_dimensions):
        raise _RuleParseError(
            f"DB(...) coordinate count ({len(coordinates)}) does not match the number of "
            f"dimensions ({len(source_dimensions)}) in source cube '{source_cube_name}'"
        )

    source_measure_name: Optional[str] = None
    coordinate_by_dimension: Dict[str, Any] = {}
    for dimension_name, coordinate in zip(source_dimensions, coordinates):
        if dimension_name == measure_dimension_name:
            if coordinate[0] != "literal" or not isinstance(coordinate[1], str):
                raise _RuleParseError(
                    f"DB(...) measure-dimension coordinate for '{dimension_name}' must be a literal "
                    "measure name"
                )
            source_measure_name = coordinate[1]
            continue
        coordinate_by_dimension[dimension_name] = coordinate

    if source_measure_name is None:
        raise _RuleParseError(
            f"could not resolve a source measure name from DB(...) for cube '{source_cube_name}'"
        )

    non_measure_dimensions = [name for name in source_dimensions if name != measure_dimension_name]
    shift_candidates = [
        (dimension_name, coordinate)
        for dimension_name, coordinate in coordinate_by_dimension.items()
        if coordinate[0] in ("attrs", "numeric_shift")
    ]
    all_others_passthrough = all(
        coordinate_by_dimension[name][0] == "passthrough"
        for name in non_measure_dimensions
        if name not in {dimension_name for dimension_name, _ in shift_candidates}
    )

    if source_cube_name == target_cube_name and not shift_candidates and all_others_passthrough:
        return MeasureExpression(ref=MeasureRef(cube_name=target_cube_name, measure_name=source_measure_name))

    if source_cube_name == target_cube_name and len(shift_candidates) == 1 and all_others_passthrough:
        shift_dimension_name, shift_coordinate = shift_candidates[0]
        base = MeasureExpression(ref=MeasureRef(cube_name=target_cube_name, measure_name=source_measure_name))
        if shift_coordinate[0] == "attrs":
            _, attrs_dimension_name, attribute_name = shift_coordinate
            if attrs_dimension_name == shift_dimension_name:
                return base.shift(**{shift_dimension_name: attribute_name})
        else:
            _, _, offset_value = shift_coordinate
            return base.shift(**{shift_dimension_name: offset_value})

    kwargs: Dict[str, Expression] = {}
    for dimension_name in non_measure_dimensions:
        coordinate = coordinate_by_dimension[dimension_name]
        kind = coordinate[0]
        if kind == "passthrough":
            continue
        if kind == "literal":
            kwargs[dimension_name] = LiteralExpression(value=coordinate[1])
            continue
        if kind == "attrs":
            _, attrs_dimension_name, attribute_name = coordinate
            kwargs[dimension_name] = AttributeExpression(
                ref=AttributeRef(
                    cube_name=target_cube_name,
                    dimension_name=attrs_dimension_name,
                    attribute_name=attribute_name,
                )
            )
            continue
        raise _RuleParseError(
            f"DB(...) coordinate for dimension '{dimension_name}' has a shape that cannot be "
            "reconstructed into align(...)"
        )

    base = MeasureExpression(ref=MeasureRef(cube_name=source_cube_name, measure_name=source_measure_name))
    return base.align(**kwargs)


def _parse_expression(
    cube_name: str, rhs_text: str, metadata_provider: Optional["MetadataProvider"] = None
) -> Expression:
    tokens = _tokenize(rhs_text)
    parser = _ExpressionParser(tokens, cube_name=cube_name, metadata_provider=metadata_provider)
    return parser.parse()


def parse_native_rule_text(
    cube_name: str, rule_text: str, metadata_provider: Optional["MetadataProvider"] = None
) -> Tuple[List[ParsedRuleStatement], List[RuleIngestionRejection]]:
    if not cube_name:
        raise UnsupportedRuleIngestionError("cube_name is required for rule ingestion")

    parsed: List[ParsedRuleStatement] = []
    rejected: List[RuleIngestionRejection] = []

    cleaned = _strip_comments(rule_text)
    for raw_statement in _split_statements(cleaned):
        statement_text = raw_statement.strip()
        if not statement_text:
            continue
        if statement_text.upper() == "SKIPCHECK":
            continue
        if statement_text.upper() == "FEEDERS":
            break

        match = _STATEMENT_PATTERN.match(statement_text)
        if not match:
            rejected.append(
                RuleIngestionRejection(
                    statement_text=statement_text,
                    reason="statement does not match the expected \"['Measure'] = SCOPE: expr\" shape",
                )
            )
            continue

        measure_name = _extract_measure_name(match.group("target"))
        scope = match.group("scope").upper()
        if scope not in ("N", "C"):
            rejected.append(
                RuleIngestionRejection(
                    statement_text=statement_text,
                    reason=f"rule-area scope '{scope}:' is not supported by the current DSL (supported: N, C)",
                    measure_name=measure_name,
                )
            )
            continue

        try:
            expression = _parse_expression(cube_name, match.group("rhs"), metadata_provider=metadata_provider)
        except _RuleParseError as error:
            rejected.append(
                RuleIngestionRejection(
                    statement_text=statement_text,
                    reason=str(error),
                    measure_name=measure_name,
                )
            )
            continue

        if scope == "C":
            expression = expression.native(scope="consolidated")

        parsed.append(ParsedRuleStatement(measure_name=measure_name, scope=scope, expression=expression))

    return parsed, rejected
