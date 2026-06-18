from __future__ import annotations

import contextvars
import functools
import inspect
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from pandas import DataFrame

from TM1_bedrock_py import basic_logger


_operation_depth = contextvars.ContextVar("tm1_bedrock_operation_depth", default=0)
_SENSITIVE_PARTS = ("password", "secret", "token", "credential", "api_key", "apikey")
_DEFAULT_CONTEXT_FIELDS = (
    "dimension_name",
    "hierarchy_name",
    "source_dimension_name",
    "source_hierarchy_name",
    "target_dimension_name",
    "target_hierarchy_name",
    "cube_name",
    "target_cube_name",
    "source_cube_name",
    "build_mode",
    "build_strategy",
    "output_mode",
    "output_format",
    "target_destinations",
    "sql_table_name",
    "target_table_name",
    "sql_table_for_count",
    "source_csv_file_path",
    "target_csv_file_name",
    "max_workers",
    "slice_size",
    "data_mdx",
    "sql_query",
    "sql_query_template",
)


def _safe_value(name: str, value: Any) -> str:
    normalized_name = name.casefold()
    if any(part in normalized_name for part in _SENSITIVE_PARTS):
        return "<redacted>"
    if "mdx" in normalized_name or "query" in normalized_name:
        if isinstance(value, str):
            return f"<query text, {len(value)} characters>"
        if isinstance(value, (list, tuple, set, dict)):
            return f"<query collection, {len(value)} items>"
        return f"<{type(value).__name__}>"
    if value is None or isinstance(value, (bool, int, float)):
        return repr(value)
    if isinstance(value, str):
        if len(value) > 160:
            return repr(value[:157] + "...")
        return repr(value)
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, DataFrame):
        return f"<DataFrame rows={len(value)}, columns={list(value.columns)}>"
    if isinstance(value, (list, tuple, set)):
        if len(value) <= 8 and all(isinstance(item, (str, int, float, bool)) for item in value):
            return repr(value)
        return f"<{type(value).__name__} items={len(value)}>"
    if isinstance(value, dict):
        return f"<dict keys={list(value)[:8]}, items={len(value)}>"
    if callable(value):
        return f"<callable {getattr(value, '__name__', type(value).__name__)}>"
    return f"<{type(value).__name__}>"


def _collect_context(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
    context_fields: Iterable[str],
) -> dict[str, str]:
    try:
        bound = inspect.signature(function).bind_partial(*args, **kwargs)
        arguments = bound.arguments
    except (TypeError, ValueError):
        arguments = kwargs

    context = {}
    for field in context_fields:
        if field in arguments and arguments[field] is not None:
            context[field] = _safe_value(field, arguments[field])

    forwarded_kwargs = arguments.get("kwargs") or arguments.get("writer_kwargs") or arguments.get("builder_kwargs")
    if isinstance(forwarded_kwargs, dict):
        for field in context_fields:
            if field not in context and forwarded_kwargs.get(field) is not None:
                context[field] = _safe_value(field, forwarded_kwargs[field])
    return context


def _collect_values_to_redact(
    function: Callable[..., Any],
    args: tuple[Any, ...],
    kwargs: dict[str, Any],
) -> dict[str, str]:
    try:
        arguments = inspect.signature(function).bind_partial(*args, **kwargs).arguments
    except (TypeError, ValueError):
        arguments = kwargs

    values = {}

    def collect_strings(value: Any, replacement_name: str) -> None:
        if isinstance(value, str):
            if value:
                values[value] = replacement_name.format(length=len(value))
            return
        if isinstance(value, dict):
            for nested_value in value.values():
                collect_strings(nested_value, replacement_name)
            return
        if isinstance(value, (list, tuple, set)):
            for nested_value in value:
                collect_strings(nested_value, replacement_name)

    for name, value in arguments.items():
        normalized_name = name.casefold()
        if any(part in normalized_name for part in _SENSITIVE_PARTS):
            collect_strings(value, "<redacted>")
        elif "mdx" in normalized_name or "query" in normalized_name:
            collect_strings(value, "<query text, {length} characters>")

    for forwarded_name in ("kwargs", "writer_kwargs", "builder_kwargs", "dim_builder_kwargs"):
        forwarded = arguments.get(forwarded_name)
        if not isinstance(forwarded, dict):
            continue
        for name, value in forwarded.items():
            normalized_name = name.casefold()
            if any(part in normalized_name for part in _SENSITIVE_PARTS):
                collect_strings(value, "<redacted>")
            elif "mdx" in normalized_name or "query" in normalized_name:
                collect_strings(value, "<query text, {length} characters>")
    return values


def _format_exception(exception: Exception) -> str:
    original_message = getattr(exception, "_tm1_bedrock_original_message", str(exception))
    operation_path = " -> ".join(getattr(exception, "_tm1_bedrock_operations", []))
    context = getattr(exception, "_tm1_bedrock_context", {})

    lines = [
        "TM1 Bedrock operation failed",
        f"Operation: {operation_path}",
    ]
    if context:
        lines.append("Context:")
        lines.extend(f"  {name}: {value}" for name, value in context.items())
    lines.extend(
        [
            f"Cause: {type(exception).__name__}: {original_message}",
            "Inspect the chained traceback for the originating call.",
        ]
    )
    return "\n".join(lines)


def redact_exception_values(
    exception: BaseException,
    values_to_redact: dict[str, str],
) -> None:
    """Scrub known sensitive values from an exception and its chained causes."""
    visited: set[int] = set()

    def redact(current: Optional[BaseException]) -> None:
        if current is None or id(current) in visited:
            return
        visited.add(id(current))

        original_message = getattr(current, "_tm1_bedrock_original_message", None)
        if isinstance(original_message, str):
            for raw_value, replacement in values_to_redact.items():
                if raw_value:
                    original_message = original_message.replace(raw_value, replacement)
            current._tm1_bedrock_original_message = original_message

        if current.args:
            redacted_args = []
            for argument in current.args:
                if isinstance(argument, str):
                    for raw_value, replacement in values_to_redact.items():
                        if raw_value:
                            argument = argument.replace(raw_value, replacement)
                redacted_args.append(argument)
            try:
                current.args = tuple(redacted_args)
            except (AttributeError, TypeError):
                pass

        redact(current.__cause__)
        redact(current.__context__)

    redact(exception)


def _attach_context(
    exception: Exception,
    operation: str,
    context: dict[str, str],
    values_to_redact: dict[str, str],
) -> str:
    if not hasattr(exception, "_tm1_bedrock_original_message"):
        exception._tm1_bedrock_original_message = str(exception)
        exception._tm1_bedrock_operations = []
        exception._tm1_bedrock_context = {}

    redact_exception_values(exception, values_to_redact)

    operations = exception._tm1_bedrock_operations
    if operation not in operations:
        operations.insert(0, operation)
    exception._tm1_bedrock_context = {**context, **exception._tm1_bedrock_context}

    formatted_message = _format_exception(exception)
    remaining_args = exception.args[1:] if exception.args else ()
    exception.args = (formatted_message, *remaining_args)
    return formatted_message


def _log_outermost_failure(exception: Exception, formatted_message: str) -> None:
    basic_logger.error(
        formatted_message,
        exc_info=True,
        extra={
            "operation": " -> ".join(exception._tm1_bedrock_operations),
            "error_type": type(exception).__name__,
            "error_context": exception._tm1_bedrock_context,
        },
    )


def public_operation(
    operation: Optional[str] = None,
    context_fields: Optional[Iterable[str]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Add safe, structured context to failures from a public wrapper."""

    selected_fields = tuple(context_fields or _DEFAULT_CONTEXT_FIELDS)

    def decorator(function: Callable[..., Any]) -> Callable[..., Any]:
        operation_name = operation or function.__name__

        if inspect.iscoroutinefunction(function):
            @functools.wraps(function)
            async def async_wrapper(*args: Any, **kwargs: Any) -> Any:
                depth = _operation_depth.get()
                token = _operation_depth.set(depth + 1)
                try:
                    return await function(*args, **kwargs)
                except Exception as exception:
                    context = _collect_context(function, args, kwargs, selected_fields)
                    values_to_redact = _collect_values_to_redact(function, args, kwargs)
                    message = _attach_context(
                        exception, operation_name, context, values_to_redact
                    )
                    if depth == 0:
                        _log_outermost_failure(exception, message)
                    raise
                finally:
                    _operation_depth.reset(token)

            async_wrapper._tm1_bedrock_public_operation = True
            return async_wrapper

        @functools.wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            depth = _operation_depth.get()
            token = _operation_depth.set(depth + 1)
            try:
                return function(*args, **kwargs)
            except Exception as exception:
                context = _collect_context(function, args, kwargs, selected_fields)
                values_to_redact = _collect_values_to_redact(function, args, kwargs)
                message = _attach_context(
                    exception, operation_name, context, values_to_redact
                )
                if depth == 0:
                    _log_outermost_failure(exception, message)
                raise
            finally:
                _operation_depth.reset(token)

        wrapper._tm1_bedrock_public_operation = True
        return wrapper

    return decorator
