"""Centralized diagnostic logging for tm1_bedrock_py operations.

Captures structured, machine-readable execution records for every
TM1 data operation. Designed to enable later auto-analysis and
auto-correction from collected edge-case logs.

Usage:
    from TM1_bedrock_py.diagnostics import DiagnosticLogger

    diag = DiagnosticLogger("data_copy_intercube")
    diag.record_start(params={"target_cube": cube_name})
    # ... do work ...
    diag.record_stage("extract_complete", {"rows": len(df)})
    diag.record_stage("transform_complete", {"rows": len(df)})
    diag.record_end(success=True, summary={"loaded_rows": loaded_count})

Each record is a JSON-serializable dict written to the diagnostics log.
"""

from __future__ import annotations

import json
import os
import sys
import threading
import time
import traceback
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

# Default log directory — can be overridden via env var
_DEFAULT_LOG_DIR = os.environ.get(
    "TM1_BEDROCK_DIAGNOSTICS_DIR",
    os.path.join(os.path.expanduser("~"), ".tm1_bedrock_py", "diagnostics"),
)

# Max records per session before rotation
_MAX_RECORDS = int(os.environ.get("TM1_BEDROCK_DIAGNOSTICS_MAX", "10000"))
_DIAG_LOGGER = None


@dataclass
class ExecutionRecord:
    """A single diagnostic record for one function call."""

    function: str
    timestamp_start: str = ""
    timestamp_end: str = ""
    duration_seconds: float = 0.0
    params: dict[str, Any] = field(default_factory=dict)
    stages: list[dict[str, Any]] = field(default_factory=list)
    summary: dict[str, Any] = field(default_factory=dict)
    success: bool = False
    error: str = ""
    error_traceback: str = ""
    warnings: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "function": self.function,
            "timestamp_start": self.timestamp_start,
            "timestamp_end": self.timestamp_end,
            "duration_seconds": round(self.duration_seconds, 4),
            "params": self.params,
            "stages": self.stages,
            "summary": self.summary,
            "success": self.success,
            "error": self.error,
            "error_traceback": self.error_traceback,
            "warnings": self.warnings,
            "metadata": self.metadata,
        }


class DiagnosticLogger:
    """Thread-safe diagnostic logger for TM1 operations.

    Collects structured execution metadata (timing, row counts, errors)
    and writes JSON records to disk for later analysis.

    Usage pattern:
        diag = DiagnosticLogger("data_copy_intercube")
        diag.record_start(params={"target_cube": "Sales"})
        try:
            # ... perform operation ...
            diag.record_stage("extract", {"rows": 1500})
            diag.record_end(success=True, summary={"loaded": 1500})
        except Exception as e:
            diag.record_error(e)
            diag.record_end(success=False)
            raise
    """

    def __init__(self, function_name: str):
        self._record = ExecutionRecord(function=function_name)
        self._start_time: float | None = None
        self._lock = threading.Lock()

    def record_start(self, params: dict[str, Any] | None = None) -> None:
        """Record the start of an operation."""
        self._start_time = time.monotonic()
        self._record.timestamp_start = datetime.now(timezone.utc).isoformat()
        if params:
            # Sanitize params: exclude tm1_service, passwords, etc.
            safe = {}
            for k, v in params.items():
                if k in ("tm1_service", "target_tm1_service", "password", "sql_connection",
                         "sql_engine"):
                    safe[k] = "<service>"
                elif isinstance(v, (str, int, float, bool, list, dict, type(None))):
                    safe[k] = v
                else:
                    safe[k] = str(type(v).__name__)
            self._record.params = safe

    def record_stage(self, stage_name: str, details: dict[str, Any] | None = None) -> None:
        """Record a stage completion within an operation (extract, transform, load, etc.)."""
        elapsed = time.monotonic() - self._start_time if self._start_time else 0.0
        stage = {
            "stage": stage_name,
            "elapsed_seconds": round(elapsed, 4),
        }
        if details:
            stage.update(details)
        self._record.stages.append(stage)

    def record_warning(self, message: str) -> None:
        """Record a non-fatal warning."""
        self._record.warnings.append(message)

    def record_error(self, exc: Exception | None = None) -> None:
        """Record an error with full traceback."""
        self._record.error = str(exc) if exc else ""
        if exc:
            self._record.error_traceback = "".join(
                traceback.format_exception(type(exc), exc, exc.__traceback__)
            )[-2000:]  # Cap at 2000 chars

    def record_end(self, success: bool, summary: dict[str, Any] | None = None) -> None:
        """Record the end of an operation and write the record."""
        elapsed = time.monotonic() - self._start_time if self._start_time else 0.0
        self._record.timestamp_end = datetime.now(timezone.utc).isoformat()
        self._record.duration_seconds = elapsed
        self._record.success = success
        if summary:
            self._record.summary = summary
        self._write()

    def _write(self) -> None:
        """Write the record to disk (thread-safe)."""
        global _DIAG_LOGGER
        with self._lock:
            try:
                # Re-read env var to support runtime overrides (tests)
                log_dir = Path(os.environ.get("TM1_BEDROCK_DIAGNOSTICS_DIR", _DEFAULT_LOG_DIR))
                log_dir.mkdir(parents=True, exist_ok=True)

                date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
                log_file = log_dir / f"diagnostics_{date_str}.jsonl"

                record_json = json.dumps(self._record.to_dict(), ensure_ascii=False)
                with open(log_file, "a", encoding="utf-8") as f:
                    f.write(record_json + "\n")

            except Exception as exc:
                # Never let diagnostics logging crash the main operation
                print(f"[diagnostics] Failed to write record: {exc}", file=sys.stderr)


def get_diagnostics_summary(days: int = 7) -> dict[str, Any]:
    """Read recent diagnostic records and produce a summary.

    Returns counts by function, success/failure rates, and recent errors.
    """
    summary: dict[str, Any] = {
        "total_records": 0,
        "by_function": defaultdict(lambda: {"total": 0, "success": 0, "failure": 0}),
        "recent_errors": [],
        "avg_duration_by_function": defaultdict(list),
    }

    log_dir = Path(_DEFAULT_LOG_DIR)
    if not log_dir.exists():
        return summary

    from datetime import timedelta
    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    for log_file in sorted(log_dir.glob("diagnostics_*.jsonl"), reverse=True):
        try:
            with open(log_file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError:
                        continue

                    ts = record.get("timestamp_start", "")
                    try:
                        rec_time = datetime.fromisoformat(ts)
                        if rec_time.replace(tzinfo=timezone.utc) < cutoff:
                            continue
                    except (ValueError, TypeError):
                        pass

                    summary["total_records"] += 1
                    func = record.get("function", "unknown")
                    summary["by_function"][func]["total"] += 1
                    if record.get("success"):
                        summary["by_function"][func]["success"] += 1
                    else:
                        summary["by_function"][func]["failure"] += 1
                        if record.get("error"):
                            summary["recent_errors"].append({
                                "function": func,
                                "error": record["error"],
                                "timestamp": ts,
                            })

                    dur = record.get("duration_seconds", 0)
                    if dur:
                        summary["avg_duration_by_function"][func].append(dur)

        except Exception:
            continue

    # Compute averages
    avg_durations = {}
    for func, durations in summary["avg_duration_by_function"].items():
        if durations:
            avg_durations[func] = round(sum(durations) / len(durations), 4)
    summary["avg_duration_by_function"] = avg_durations

    # Keep only recent 20 errors
    summary["recent_errors"] = summary["recent_errors"][-20:]

    # Convert defaultdict to regular dict
    summary["by_function"] = dict(summary["by_function"])
    for func_stats in summary["by_function"].values():
        func_stats["success_rate"] = round(
            func_stats["success"] / max(func_stats["total"], 1) * 100, 1
        )

    return summary


# ---------------------------------------------------------------------------
# Convenience: context manager for automatic start/end recording
# ---------------------------------------------------------------------------


class DiagnosticContext:
    """Context manager that automatically records start and end.

    Usage:
        with DiagnosticContext("data_copy_intercube", params={"cube": "Sales"}) as diag:
            # do work
            diag.record_stage("extract", {"rows": 1500})
            # on success: record_end(success=True)
            # on exception: record_error + record_end(success=False)
    """

    def __init__(self, function_name: str, params: dict[str, Any] | None = None):
        self.diag = DiagnosticLogger(function_name)
        self._params = params

    def __enter__(self):
        self.diag.record_start(self._params)
        return self.diag

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.diag.record_error(exc_val)
            self.diag.record_end(success=False)
        else:
            self.diag.record_end(success=True)
        return False  # Don't suppress exceptions
