"""AI-facing capability manifest for tm1_bedrock_py.

Provides a machine-readable catalog of all bedrock functions with
their signatures, parameter schemas, operation types, and dependencies.
An AI entity can call `get_bedrock_capabilities()` to discover what the
library can do without reading source code or trial-and-error.

Usage:
    >>> from TM1_bedrock_py.capabilities import get_bedrock_capabilities
    >>> caps = get_bedrock_capabilities()
    >>> caps["functions"][0]
    {"name": "cube_builder", "category": "builder", ...}
"""

from __future__ import annotations

import inspect
import sys
from dataclasses import asdict, dataclass, field
from typing import Any, Callable, get_type_hints


# ---------------------------------------------------------------------------
# Metadata registry — manually maintained for semantic tags that can't be
# auto-inferred from function signatures alone.
# ---------------------------------------------------------------------------

@dataclass
class FunctionMeta:
    """Semantic metadata for a bedrock function."""
    name: str
    category: str           # builder | copier | modifier | loader | exporter | input
    operation: str          # read | write | both
    summary: str            # one-line description
    touches: list[str]      # what TM1 objects are affected: dimension, cube, hierarchy
    produces: str           # what it creates/modifies: dimension, cube, hierarchy, data, none
    key_params: list[str]   # most important parameter names for planning


# Manually curated metadata — updated when functions are added/changed.
# This is the only part that needs maintenance; signatures/docs are introspected.
_FUNCTION_META: dict[str, FunctionMeta] = {
    "cube_builder": FunctionMeta(
        name="cube_builder",
        category="builder",
        operation="write",
        summary="Build a new cube from a dimension map or clone an existing cube structure.",
        touches=["cube", "dimension"],
        produces="cube",
        key_params=["build_mode", "cube_dimension_create_map", "copy_source_cubes"],
    ),
    "dimension_builder": FunctionMeta(
        name="dimension_builder",
        category="builder",
        operation="write",
        summary="Build a dimension from a DataFrame using parent_child, indented_levels, or filled_levels format.",
        touches=["dimension"],
        produces="dimension",
        key_params=["dimension_name", "input_format", "build_strategy"],
    ),
    "hierarchy_builder": FunctionMeta(
        name="hierarchy_builder",
        category="builder",
        operation="write",
        summary="Build or rebuild a hierarchy within an existing dimension.",
        touches=["dimension", "hierarchy"],
        produces="hierarchy",
        key_params=["dimension_name", "hierarchy_name", "input_format"],
    ),
    "hierarchy_build_from_attributes": FunctionMeta(
        name="hierarchy_build_from_attributes",
        category="builder",
        operation="write",
        summary="Create a new hierarchy by grouping elements by attribute values.",
        touches=["dimension", "hierarchy"],
        produces="hierarchy",
        key_params=["dimension_name", "attributes", "new_hierarchy_name"],
    ),
    "dimension_copy": FunctionMeta(
        name="dimension_copy",
        category="copier",
        operation="write",
        summary="Copy a dimension (and optionally its hierarchies) to a new name, possibly on another TM1 instance.",
        touches=["dimension"],
        produces="dimension",
        key_params=["source_dimension_name", "target_dimension_name", "hierarchy_rename_map"],
    ),
    "hierarchy_copy": FunctionMeta(
        name="hierarchy_copy",
        category="copier",
        operation="write",
        summary="Copy a single hierarchy to another dimension, possibly on another TM1 instance.",
        touches=["hierarchy", "dimension"],
        produces="hierarchy",
        key_params=["source_dimension_name", "source_hierarchy_name", "target_dimension_name"],
    ),
    "dimension_modify": FunctionMeta(
        name="dimension_modify",
        category="modifier",
        operation="write",
        summary="Apply a custom modification function to a dimension's elements/edges via TM1py native views.",
        touches=["dimension"],
        produces="dimension",
        key_params=["dimension_name", "modify_function"],
    ),
    "hierarchy_modify": FunctionMeta(
        name="hierarchy_modify",
        category="modifier",
        operation="write",
        summary="Apply a custom modification function to a single hierarchy's elements/edges.",
        touches=["hierarchy"],
        produces="hierarchy",
        key_params=["dimension_name", "hierarchy_name", "modify_function"],
    ),
    "dimension_export": FunctionMeta(
        name="dimension_export",
        category="exporter",
        operation="read",
        summary="Export a dimension's structure to a DataFrame in the requested format.",
        touches=["dimension"],
        produces="data",
        key_params=["dimension_name", "output_format"],
    ),
    "data_copy_intercube": FunctionMeta(
        name="data_copy_intercube",
        category="loader",
        operation="write",
        summary="Copy data between cubes on the same or different TM1 instances with optional mapping pipeline.",
        touches=["cube"],
        produces="data",
        key_params=["target_cube_name", "data_mdx", "mapping_steps"],
    ),
    "data_copy": FunctionMeta(
        name="data_copy",
        category="loader",
        operation="write",
        summary="Load data into a TM1 cube from MDX, SQL, or CSV sources with optional mapping pipeline.",
        touches=["cube"],
        produces="data",
        key_params=["data_mdx", "sql_engine", "csv_function", "mapping_steps"],
    ),
    "input_handler": FunctionMeta(
        name="input_handler",
        category="input",
        operation="write",
        summary="Execute a calculation pipeline (sumif, countif, if, formula, etc.) and write the result to a TM1 cube cell.",
        touches=["cube"],
        produces="data",
        key_params=["input_value", "target_cube_name", "domain_coordinates", "calculation_steps"],
    ),
    "load_sql_data_to_tm1_cube": FunctionMeta(
        name="load_sql_data_to_tm1_cube",
        category="loader",
        operation="write",
        summary="Execute a SQL query and load the result set into a TM1 cube.",
        touches=["cube"],
        produces="data",
        key_params=["target_cube_name", "sql_query"],
    ),
    "load_tm1_cube_to_sql_table": FunctionMeta(
        name="load_tm1_cube_to_sql_table",
        category="exporter",
        operation="read",
        summary="Extract data from a TM1 cube and write it to a SQL table.",
        touches=["cube"],
        produces="data",
        key_params=["target_table_name", "data_mdx"],
    ),
    "load_csv_data_to_tm1_cube": FunctionMeta(
        name="load_csv_data_to_tm1_cube",
        category="loader",
        operation="write",
        summary="Load data from a CSV file into a TM1 cube.",
        touches=["cube"],
        produces="data",
        key_params=["target_cube_name", "source_csv_file_path"],
    ),
    "load_tm1_cube_to_csv_file": FunctionMeta(
        name="load_tm1_cube_to_csv_file",
        category="exporter",
        operation="read",
        summary="Export TM1 cube data to a CSV file.",
        touches=["cube"],
        produces="data",
        key_params=["data_mdx"],
    ),
}


# ---------------------------------------------------------------------------
# Introspection — auto-extracts signatures from the bedrock module
# ---------------------------------------------------------------------------

def _get_bedrock_module():
    """Import the bedrock module, handling any import errors gracefully."""
    try:
        from TM1_bedrock_py import bedrock
    except Exception:
        # If full import fails (e.g., missing TM1py), try a lightweight import
        import importlib.util
        from pathlib import Path
        import TM1_bedrock_py as pkg
        bedrock_path = Path(pkg.__path__[0]) / "bedrock.py"
        spec = importlib.util.spec_from_file_location("bedrock", bedrock_path)
        bedrock = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(bedrock)
    return bedrock


def _extract_param_schema(func: Callable) -> list[dict[str, Any]]:
    """Extract parameter names, types, and defaults from a function signature."""
    params = []
    try:
        sig = inspect.signature(func)
        hints = get_type_hints(func) if hasattr(func, '__annotations__') else {}
    except (ValueError, TypeError):
        return params

    for name, param in sig.parameters.items():
        if name == 'self':
            continue
        p = {
            "name": name,
            "type": str(hints.get(name, "Any")).replace("typing.", "").replace("Optional[", "Optional["),
            "required": param.default is inspect.Parameter.empty,
        }
        if param.default is not inspect.Parameter.empty:
            p["default"] = repr(param.default)
        params.append(p)
    return params


def _extract_doc_summary(func: Callable) -> str:
    """Extract the first line of the docstring."""
    doc = inspect.getdoc(func)
    if not doc:
        return ""
    return doc.split("\n")[0].strip()


def _classify_func(func_name: str, func: Callable) -> dict[str, Any]:
    """Combine introspected signature with curated metadata."""
    meta = _FUNCTION_META.get(func_name)
    if meta is None:
        # Unmapped function — use basic introspection only
        return {
            "name": func_name,
            "category": "unknown",
            "operation": "unknown",
            "summary": _extract_doc_summary(func),
            "touches": [],
            "produces": "unknown",
            "parameters": _extract_param_schema(func),
            "key_params": [],
        }

    return {
        "name": meta.name,
        "category": meta.category,
        "operation": meta.operation,
        "summary": meta.summary or _extract_doc_summary(func),
        "touches": meta.touches,
        "produces": meta.produces,
        "parameters": _extract_param_schema(func),
        "key_params": meta.key_params,
    }


def _get_top_level_functions(module) -> list[tuple[str, Callable]]:
    """Get all top-level functions from a module (exclude nested/inner functions)."""
    import ast
    try:
        src = inspect.getsource(module)
    except (OSError, TypeError):
        # Fallback: inspect all members
        return [
            (name, obj)
            for name, obj in inspect.getmembers(module, inspect.isfunction)
            if not name.startswith("_") and obj.__module__ == getattr(module, '__name__', '')
        ]

    tree = ast.parse(src)
    top_level_names = {
        node.name for node in ast.walk(tree)
        if isinstance(node, ast.FunctionDef) and node.col_offset == 0
    }
    result = []
    for name in top_level_names:
        obj = getattr(module, name, None)
        if obj and callable(obj) and not name.startswith("_"):
            result.append((name, obj))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_bedrock_capabilities() -> dict[str, Any]:
    """Return a machine-readable catalog of all bedrock functions.

    Each function entry includes:
        name       — function name
        category   — builder | copier | modifier | loader | exporter | input
        operation  — read | write | both
        summary    — one-line description
        touches    — TM1 objects affected (dimension, cube, hierarchy)
        produces   — what it creates/modifies
        parameters — list of {name, type, required, default?}
        key_params — most important parameters for planning

    Returns:
        dict with keys:
            library       — "tm1_bedrock_py"
            total_functions — count
            categories     — list of unique categories
            functions      — list of function capability dicts
    """
    bedrock = _get_bedrock_module()
    top_level = _get_top_level_functions(bedrock)

    functions = []
    for name, func in top_level:
        # Skip decorator/closure helper functions defined inside other functions
        try:
            src_file = inspect.getfile(func)
            if "bedrock" not in src_file:
                continue
        except TypeError:
            continue
        cap = _classify_func(name, func)
        functions.append(cap)

    # Sort by category then name
    functions.sort(key=lambda f: (f["category"], f["name"]))

    categories = sorted(set(f["category"] for f in functions))

    return {
        "library": "tm1_bedrock_py",
        "total_functions": len(functions),
        "categories": categories,
        "functions": functions,
    }


def get_function_details(function_name: str) -> dict[str, Any] | None:
    """Get detailed capability info for a specific function by name."""
    caps = get_bedrock_capabilities()
    for func in caps["functions"]:
        if func["name"] == function_name:
            return func
    return None


def list_functions_by_category(category: str) -> list[dict[str, Any]]:
    """Filter capabilities by category (builders, copiers, loaders, etc.)."""
    caps = get_bedrock_capabilities()
    return [f for f in caps["functions"] if f["category"] == category]


def list_functions_by_operation(operation: str) -> list[dict[str, Any]]:
    """Filter capabilities by operation type (read, write)."""
    caps = get_bedrock_capabilities()
    return [f for f in caps["functions"] if f["operation"] == operation]


def get_operation_plan_skeleton(function_name: str) -> dict[str, Any] | None:
    """Return a skeleton plan for a function — what an AI would fill in.

    Includes required vs optional parameters, type hints, and a description.
    An AI entity can use this to plan an operation before executing it.
    """
    details = get_function_details(function_name)
    if not details:
        return None

    required = [p for p in details["parameters"] if p["required"]]
    optional = [p for p in details["parameters"] if not p["required"]]

    return {
        "function": function_name,
        "category": details["category"],
        "summary": details["summary"],
        "touches": details["touches"],
        "produces": details["produces"],
        "required_parameters": required,
        "optional_parameters": optional,
        "steps": [
            f"1. Prepare inputs: {', '.join(p['name'] for p in required)}",
            f"2. Call {function_name} with the prepared parameters",
            "3. Verify the result (check return value, row counts, TM1 state)",
        ],
    }
