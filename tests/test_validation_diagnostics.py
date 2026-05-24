"""Unit tests for validation, diagnostics, and guard clauses.

Tests that can run offline without a TM1 server.
"""

import json
import os
import tempfile
import unittest
from pathlib import Path

import pandas as pd

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from TM1_bedrock_py.validation import (
    validate_dimension_builder_input,
    validate_data_copy_input,
    validate_cube_builder_input,
    validate_input_handler_input,
    validate_async_executor_input,
)
from TM1_bedrock_py.diagnostics import (
    DiagnosticLogger,
    DiagnosticContext,
    get_diagnostics_summary,
)


class TestValidationGuardClauses(unittest.TestCase):
    """Test that validation functions catch common input errors."""

    # --- dimension_builder ---

    def test_dimension_builder_missing_name(self):
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(dimension_name="", input_format="indented_levels")
        self.assertIn("dimension_name", str(ctx.exception))

    def test_dimension_builder_invalid_format(self):
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(
                dimension_name="TestDim", input_format="csv"
            )
        self.assertIn("input_format", str(ctx.exception))

    def test_dimension_builder_no_data_source(self):
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(
                dimension_name="TestDim", input_format="indented_levels"
            )
        self.assertIn("raw_input_df or input_datasource", str(ctx.exception))

    def test_dimension_builder_empty_df(self):
        empty_df = pd.DataFrame()
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(
                dimension_name="TestDim",
                input_format="indented_levels",
                raw_input_df=empty_df,
            )
        self.assertIn("no columns", str(ctx.exception))

    def test_dimension_builder_missing_level_columns(self):
        df = pd.DataFrame({"A": [1, 2], "B": [3, 4]})
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(
                dimension_name="TestDim",
                input_format="indented_levels",
                raw_input_df=df,
                level_columns=["A", "C"],
            )
        self.assertIn("level_columns", str(ctx.exception))
        self.assertIn("C", str(ctx.exception))

    def test_dimension_builder_invalid_strategy(self):
        df = pd.DataFrame({"A": [1, 2]})
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(
                dimension_name="TestDim",
                input_format="indented_levels",
                raw_input_df=df,
                build_strategy="delete",
            )
        self.assertIn("build_strategy", str(ctx.exception))

    def test_dimension_builder_valid_input_passes(self):
        df = pd.DataFrame({"Level1": ["A", "B"], "Level2": ["C", "D"]})
        # Should not raise
        validate_dimension_builder_input(
            dimension_name="TestDim",
            input_format="indented_levels",
            raw_input_df=df,
            level_columns=["Level1", "Level2"],
            build_strategy="rebuild",
        )

    def test_dimension_builder_zero_rows(self):
        df = pd.DataFrame(columns=["A", "B"])
        with self.assertRaises(ValueError) as ctx:
            validate_dimension_builder_input(
                dimension_name="TestDim",
                input_format="indented_levels",
                raw_input_df=df,
            )
        self.assertIn("no rows", str(ctx.exception))

    # --- data_copy ---

    def test_data_copy_missing_target(self):
        with self.assertRaises(ValueError) as ctx:
            validate_data_copy_input(target_cube_name="")
        self.assertIn("target_cube_name", str(ctx.exception))

    def test_data_copy_no_source(self):
        with self.assertRaises(ValueError) as ctx:
            validate_data_copy_input(target_cube_name="Sales")
        self.assertIn("data source", str(ctx.exception))

    def test_data_copy_invalid_mapping_steps(self):
        with self.assertRaises(ValueError) as ctx:
            validate_data_copy_input(
                target_cube_name="Sales", data_mdx="SELECT ...", mapping_steps="not_a_list"
            )
        self.assertIn("mapping_steps must be a list", str(ctx.exception))

    def test_data_copy_mapping_step_missing_method(self):
        with self.assertRaises(ValueError) as ctx:
            validate_data_copy_input(
                target_cube_name="Sales",
                data_mdx="SELECT ...",
                mapping_steps=[{"no_method_here": True}],
            )
        self.assertIn("method", str(ctx.exception))

    def test_data_copy_valid_passes(self):
        # Should not raise
        validate_data_copy_input(
            target_cube_name="Sales",
            data_mdx="SELECT ... FROM [Sales]",
        )

    # --- cube_builder ---

    def test_cube_builder_empty_map(self):
        with self.assertRaises(ValueError) as ctx:
            validate_cube_builder_input(cube_dimension_create_map={})
        self.assertIn("cube_dimension_create_map", str(ctx.exception))

    def test_cube_builder_copy_missing_source(self):
        with self.assertRaises(ValueError) as ctx:
            validate_cube_builder_input(
                build_mode="copy_from_source", copy_source_cubes=["TestCube"]
            )
        self.assertIn("copy_source_tm1_service", str(ctx.exception))

    def test_cube_builder_valid_passes(self):
        # Should not raise
        validate_cube_builder_input(
            cube_dimension_create_map={"SalesCube": ["Dim1", "Dim2"]}
        )

    # --- input_handler ---

    def test_input_handler_missing_target(self):
        with self.assertRaises(ValueError) as ctx:
            validate_input_handler_input(input_value=100, target_cube_name="")
        self.assertIn("target_cube_name", str(ctx.exception))

    def test_input_handler_no_domain(self):
        with self.assertRaises(ValueError) as ctx:
            validate_input_handler_input(
                input_value=100, target_cube_name="Sales"
            )
        self.assertIn("domain_coordinates or domain_mdx", str(ctx.exception))

    def test_input_handler_invalid_value_type(self):
        with self.assertRaises(ValueError) as ctx:
            validate_input_handler_input(
                input_value="not_a_number",
                target_cube_name="Sales",
                domain_mdx="SELECT ...",
            )
        self.assertIn("input_value must be numeric", str(ctx.exception))

    def test_input_handler_unknown_method(self):
        with self.assertRaises(ValueError) as ctx:
            validate_input_handler_input(
                input_value=100,
                target_cube_name="Sales",
                domain_mdx="SELECT ...",
                calculation_steps=[{"method": "nuclear_fusion"}],
            )
        self.assertIn("nuclear_fusion", str(ctx.exception))

    def test_input_handler_valid_passes(self):
        validate_input_handler_input(
            input_value=100.0,
            target_cube_name="Sales",
            domain_mdx="SELECT ...",
            calculation_steps=[
                {"name": "count", "method": "count"},
                {"name": "result", "method": "formula", "formula": "Input / count"},
            ],
        )

    # --- async_executor ---

    def test_async_empty_param_list(self):
        with self.assertRaises(ValueError) as ctx:
            validate_async_executor_input(param_set_mdx_list=[])
        self.assertIn("param_set_mdx_list", str(ctx.exception))

    def test_async_invalid_workers(self):
        with self.assertRaises(ValueError) as ctx:
            validate_async_executor_input(
                param_set_mdx_list=["MDX1"], max_workers=0
            )
        self.assertIn("max_workers", str(ctx.exception))

    def test_async_valid_passes(self):
        validate_async_executor_input(
            param_set_mdx_list=["{[Dim].[Element]}"], max_workers=4
        )


class TestDiagnosticLogger(unittest.TestCase):
    """Test the centralized diagnostic logging system."""

    def setUp(self):
        # Override log dir to temp for testing
        self.temp_dir = tempfile.mkdtemp()
        self.orig_env = os.environ.get("TM1_BEDROCK_DIAGNOSTICS_DIR")
        os.environ["TM1_BEDROCK_DIAGNOSTICS_DIR"] = self.temp_dir

    def tearDown(self):
        if self.orig_env:
            os.environ["TM1_BEDROCK_DIAGNOSTICS_DIR"] = self.orig_env
        else:
            os.environ.pop("TM1_BEDROCK_DIAGNOSTICS_DIR", None)
        import shutil

        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_basic_record_workflow(self):
        diag = DiagnosticLogger("data_copy_intercube")
        diag.record_start(params={"target_cube": "Sales", "data_mdx": "SELECT ..."})
        diag.record_stage("extract", {"rows": 1000})
        diag.record_stage("transform", {"rows": 950})
        diag.record_end(success=True, summary={"loaded_rows": 950})

        # Verify file was written
        files = list(Path(self.temp_dir).glob("diagnostics_*.jsonl"))
        self.assertEqual(len(files), 1)

        record = json.loads(files[0].read_text().strip())
        self.assertEqual(record["function"], "data_copy_intercube")
        self.assertEqual(record["success"], True)
        self.assertEqual(record["summary"]["loaded_rows"], 950)
        self.assertEqual(len(record["stages"]), 2)
        self.assertGreaterEqual(record["duration_seconds"], 0)
        # Verify service params are sanitized
        self.assertIn("target_cube", record["params"])
        self.assertEqual(record["params"]["target_cube"], "Sales")

    def test_error_recording(self):
        diag = DiagnosticLogger("dimension_builder")
        diag.record_start(params={"dimension_name": "TestDim"})
        try:
            raise ValueError("Simulated failure: empty DataFrame")
        except ValueError as e:
            diag.record_error(e)
        diag.record_end(success=False)

        files = list(Path(self.temp_dir).glob("diagnostics_*.jsonl"))
        self.assertEqual(len(files), 1)
        record = json.loads(files[0].read_text().strip())
        self.assertEqual(record["success"], False)
        self.assertIn("Simulated failure", record["error"])
        self.assertNotEqual(record["error_traceback"], "")

    def test_context_manager_success(self):
        with DiagnosticContext("test_func", params={"cube": "Test"}) as diag:
            diag.record_stage("processing", {"items": 42})

        files = list(Path(self.temp_dir).glob("diagnostics_*.jsonl"))
        self.assertEqual(len(files), 1)
        record = json.loads(files[0].read_text().strip())
        self.assertEqual(record["success"], True)
        self.assertEqual(record["function"], "test_func")

    def test_context_manager_exception(self):
        try:
            with DiagnosticContext("failing_func", params={"cube": "Test"}):
                raise RuntimeError("intentional test error")
        except RuntimeError:
            pass

        files = list(Path(self.temp_dir).glob("diagnostics_*.jsonl"))
        self.assertEqual(len(files), 1)
        record = json.loads(files[0].read_text().strip())
        self.assertEqual(record["success"], False)
        self.assertIn("intentional test error", record["error"])

    def test_param_sanitization(self):
        diag = DiagnosticLogger("test_func")
        # Simulate a service-like object
        diag.record_start(
            params={
                "tm1_service": "secret_connection",
                "password": "secret123",
                "sql_engine": "sql_alchemy_engine",
                "cube_name": "Sales",
                "max_workers": 8,
            }
        )
        diag.record_end(success=True)

        files = list(Path(self.temp_dir).glob("diagnostics_*.jsonl"))
        record = json.loads(files[0].read_text().strip())
        params = record["params"]
        self.assertEqual(params.get("tm1_service"), "<service>")
        self.assertEqual(params.get("password"), "<service>")
        self.assertEqual(params.get("cube_name"), "Sales")
        self.assertEqual(params.get("max_workers"), 8)

    def test_multiple_records(self):
        for i in range(5):
            with DiagnosticContext(f"test_{i}", params={"iteration": i}) as diag:
                diag.record_stage("work", {"result": i * 10})

        files = list(Path(self.temp_dir).glob("diagnostics_*.jsonl"))
        self.assertEqual(len(files), 1)  # Same day = same file
        records = [json.loads(line) for line in files[0].read_text().strip().split("\n")]
        self.assertEqual(len(records), 5)
        self.assertTrue(all(r["success"] for r in records))

    def test_diagnostics_did_not_crash_on_write_failure(self):
        # Point to a non-writable path
        os.environ["TM1_BEDROCK_DIAGNOSTICS_DIR"] = "/root/forbidden"
        diag = DiagnosticLogger("resilient_func")
        diag.record_start()
        diag.record_end(success=True)
        # Should not raise — diagnostics failures must never crash main operations
        self.assertTrue(True)  # Reached this line = no crash


if __name__ == "__main__":
    unittest.main()
