"""AI-facing TM1 state snapshot.

Provides a lightweight, read-only snapshot of the current TM1 server
state: cubes, dimensions, element counts, and hierarchy structure.
An AI entity can call this to understand "what's on this server" without
making dozens of individual API calls.

All functions are read-only — they never modify TM1 state.

Usage:
    >>> from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot
    >>> state = get_tm1_state_snapshot(tm1_service)
    >>> state["cubes"]["Sales"]["dimensions"]
    ["Product", "Region", "Month", "Measure"]
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field, asdict
from typing import Any


@dataclass
class CubeInfo:
    name: str
    dimensions: list[str]
    has_rules: bool = False


@dataclass
class DimensionInfo:
    name: str
    hierarchy_count: int = 0
    element_count: int = 0
    hierarchy_names: list[str] = field(default_factory=list)


@dataclass
class Tm1StateSnapshot:
    """Machine-readable snapshot of a TM1 server's current state."""
    server_name: str = ""
    cube_count: int = 0
    dimension_count: int = 0
    cubes: dict[str, CubeInfo] = field(default_factory=dict)
    dimensions: dict[str, DimensionInfo] = field(default_factory=dict)
    snapshot_timestamp: str = ""
    snapshot_duration_seconds: float = 0.0
    warnings: list[str] = field(default_factory=list)


def _safe_api_call(fn, *args, **kwargs):
    """Call a TM1py API method, returning None on any error."""
    try:
        return fn(*args, **kwargs)
    except Exception as e:
        return None


def get_tm1_state_snapshot(
    tm1_service: Any,
    include_element_counts: bool = True,
    include_rules: bool = False,
    max_dimension_elements: int = 100_000,
) -> dict[str, Any]:
    """Return a compact snapshot of the TM1 server's current state.

    Queries cube and dimension metadata from the TM1 server via TM1py.
    All calls are read-only and non-mutating.

    Args:
        tm1_service: A TM1py TM1Service instance.
        include_element_counts: Query element counts per dimension (adds ~1 call per dimension).
        include_rules: Check whether cubes have rules (adds 1 call per cube).
        max_dimension_elements: Skip element count for dimensions larger than this.

    Returns:
        dict with keys:
            server_name, cube_count, dimension_count,
            cubes — dict of cube_name -> {name, dimensions, has_rules?}
            dimensions — dict of dim_name -> {name, hierarchy_count, element_count?, hierarchy_names}
            snapshot_timestamp, snapshot_duration_seconds, warnings
    """
    start = time.monotonic()
    warnings: list[str] = []
    snapshot = Tm1StateSnapshot()

    # --- Server name ---
    try:
        # TM1py stores the base URL; extract server name from it
        base_url = getattr(tm1_service, '_url', '') or getattr(tm1_service, 'base_url', '')
        if base_url:
            # URL pattern: http(s)://host:port/
            from urllib.parse import urlparse
            parsed = urlparse(base_url)
            snapshot.server_name = parsed.hostname or "unknown"
        else:
            snapshot.server_name = "unknown"
    except Exception:
        snapshot.server_name = "unknown"

    # --- Cubes ---
    try:
        cube_names = _safe_api_call(tm1_service.cubes.get_all_names)
        if cube_names:
            snapshot.cube_count = len(cube_names)
            for cn in sorted(cube_names):
                dims = _safe_api_call(tm1_service.cubes.get_dimension_names, cn)
                if dims is None:
                    dims = []
                has_rules = False
                if include_rules:
                    cube_obj = _safe_api_call(tm1_service.cubes.get, cn)
                    if cube_obj:
                        has_rules = bool(getattr(cube_obj, 'rules', ''))
                snapshot.cubes[cn] = CubeInfo(
                    name=cn,
                    dimensions=sorted(dims) if dims else [],
                    has_rules=has_rules,
                )
        else:
            warnings.append("Could not retrieve cube list from TM1 server")
    except Exception as e:
        warnings.append(f"Cube retrieval failed: {e}")

    # --- Dimensions ---
    try:
        dim_names = _safe_api_call(tm1_service.dimensions.get_all_names)
        if dim_names:
            snapshot.dimension_count = len(dim_names)
            for dn in sorted(dim_names):
                hier_names = _safe_api_call(tm1_service.dimensions.hierarchies.get_all_names, dn)
                if hier_names is None:
                    hier_names = []
                element_count = 0
                if include_element_counts and hier_names:
                    try:
                        # Get element count from the first (leaves) hierarchy
                        count = _safe_api_call(
                            tm1_service.dimensions.hierarchies.elements.get_number_of_elements,
                            dn, hier_names[0]
                        )
                        if count is not None and count <= max_dimension_elements:
                            element_count = count
                        elif count is not None:
                            element_count = -1  # Too large, skip
                    except Exception:
                        element_count = 0
                snapshot.dimensions[dn] = DimensionInfo(
                    name=dn,
                    hierarchy_count=len(hier_names),
                    element_count=element_count,
                    hierarchy_names=hier_names,
                )
        else:
            warnings.append("Could not retrieve dimension list from TM1 server")
    except Exception as e:
        warnings.append(f"Dimension retrieval failed: {e}")

    snapshot.snapshot_timestamp = _now_iso()
    snapshot.snapshot_duration_seconds = round(time.monotonic() - start, 3)
    snapshot.warnings = warnings

    return {
        "server_name": snapshot.server_name,
        "cube_count": snapshot.cube_count,
        "dimension_count": snapshot.dimension_count,
        "cubes": {name: asdict(info) for name, info in snapshot.cubes.items()},
        "dimensions": {name: asdict(info) for name, info in snapshot.dimensions.items()},
        "snapshot_timestamp": snapshot.snapshot_timestamp,
        "snapshot_duration_seconds": snapshot.snapshot_duration_seconds,
        "warnings": snapshot.warnings,
    }


def get_tm1_lightweight_state(tm1_service: Any) -> dict[str, Any]:
    """Even lighter snapshot — just cube/dimension names, no element counts.

    Suitable for quick "what's there" checks. Makes 2 API calls total.
    """
    return get_tm1_state_snapshot(
        tm1_service,
        include_element_counts=False,
        include_rules=False,
    )


def list_cubes(tm1_service: Any) -> dict[str, list[str]]:
    """Return a dict mapping each cube name to its dimension names.

    Makes 1 + N API calls (1 for cube list, N for each cube's dimensions).
    """
    snapshot = get_tm1_state_snapshot(
        tm1_service,
        include_element_counts=False,
        include_rules=False,
    )
    return {name: info["dimensions"] for name, info in snapshot["cubes"].items()}


def list_dimensions(tm1_service: Any) -> dict[str, dict[str, Any]]:
    """Return a dict mapping each dimension name to its hierarchy names and element count."""
    snapshot = get_tm1_state_snapshot(
        tm1_service,
        include_element_counts=True,
        include_rules=False,
    )
    return {
        name: {
            "hierarchy_count": info["hierarchy_count"],
            "element_count": info["element_count"],
            "hierarchy_names": info["hierarchy_names"],
        }
        for name, info in snapshot["dimensions"].items()
    }


def _now_iso() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()
