"""Unit tests for AI capabilities: capability manifest and state snapshot.

Tests that can run offline without a TM1 server.
"""

import json
import unittest
from typing import Any
from unittest.mock import MagicMock, patch

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))


class TestCapabilityManifest(unittest.TestCase):
    """Test the function capability manifest system."""

    def test_get_bedrock_capabilities_returns_valid_structure(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        self.assertEqual(caps["library"], "tm1_bedrock_py")
        self.assertGreater(caps["total_functions"], 0)
        self.assertIn("categories", caps)
        self.assertIn("functions", caps)
        self.assertIsInstance(caps["functions"], list)

    def test_every_function_has_required_fields(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        required_fields = {"name", "category", "operation", "summary", "touches",
                           "produces", "parameters", "key_params"}

        for func in caps["functions"]:
            self.assertTrue(
                required_fields.issubset(func.keys()),
                f"Function {func.get('name', '?')} missing fields: "
                f"{required_fields - set(func.keys())}"
            )

    def test_all_categories_are_valid(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        valid_categories = {"builder", "copier", "modifier", "loader", "exporter", "input", "unknown"}
        for func in caps["functions"]:
            self.assertIn(func["category"], valid_categories,
                          f"Function {func['name']} has invalid category: {func['category']}")

    def test_core_functions_present(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        names = {f["name"] for f in caps["functions"]}
        expected = {
            "cube_builder", "dimension_builder", "hierarchy_builder",
            "data_copy_intercube", "data_copy", "input_handler",
            "dimension_copy", "hierarchy_copy",
            "load_sql_data_to_tm1_cube", "load_tm1_cube_to_sql_table",
        }
        missing = expected - names
        self.assertFalse(missing, f"Missing core functions: {missing}")

    def test_builders_have_write_operation(self):
        from TM1_bedrock_py.capabilities import list_functions_by_category

        builders = list_functions_by_category("builder")
        for func in builders:
            self.assertEqual(func["operation"], "write",
                             f"Builder {func['name']} should be 'write', got '{func['operation']}'")

    def test_exporters_have_read_operation(self):
        from TM1_bedrock_py.capabilities import list_functions_by_category

        exporters = list_functions_by_category("exporter")
        for func in exporters:
            self.assertEqual(func["operation"], "read",
                             f"Exporter {func['name']} should be 'read', got '{func['operation']}'")

    def test_list_by_operation_returns_correct_functions(self):
        from TM1_bedrock_py.capabilities import list_functions_by_operation

        read_funcs = list_functions_by_operation("read")
        write_funcs = list_functions_by_operation("write")
        self.assertGreater(len(read_funcs), 0, "Should have at least one read function")
        self.assertGreater(len(write_funcs), 0, "Should have at least one write function")

    def test_get_function_details_returns_valid_function(self):
        from TM1_bedrock_py.capabilities import get_function_details

        details = get_function_details("cube_builder")
        self.assertIsNotNone(details, "cube_builder should be in the manifest")
        self.assertEqual(details["name"], "cube_builder")
        self.assertEqual(details["category"], "builder")
        self.assertIn("parameters", details)
        self.assertGreater(len(details["parameters"]), 0)

    def test_get_function_details_returns_none_for_unknown(self):
        from TM1_bedrock_py.capabilities import get_function_details

        details = get_function_details("definitely_not_a_function_xyz")
        self.assertIsNone(details)

    def test_get_operation_plan_skeleton_returns_valid_structure(self):
        from TM1_bedrock_py.capabilities import get_operation_plan_skeleton

        plan = get_operation_plan_skeleton("dimension_builder")
        self.assertIsNotNone(plan)
        self.assertIn("required_parameters", plan)
        self.assertIn("optional_parameters", plan)
        self.assertIn("steps", plan)
        self.assertEqual(plan["function"], "dimension_builder")
        # dimension_builder has at least dimension_name as required
        req_names = [p["name"] for p in plan["required_parameters"]]
        self.assertIn("dimension_name", req_names)

    def test_parameters_have_name_type_required(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        for func in caps["functions"]:
            for param in func["parameters"]:
                self.assertIn("name", param)
                self.assertIn("type", param)
                self.assertIn("required", param)
                self.assertIsInstance(param["required"], bool)

    def test_manifest_is_json_serializable(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        # Should not raise
        json_str = json.dumps(caps, indent=2)
        self.assertIsInstance(json_str, str)
        # Round-trip
        parsed = json.loads(json_str)
        self.assertEqual(parsed["library"], "tm1_bedrock_py")

    def test_filter_by_category_returns_only_matching(self):
        from TM1_bedrock_py.capabilities import list_functions_by_category

        loaders = list_functions_by_category("loader")
        for func in loaders:
            self.assertEqual(func["category"], "loader",
                             f"Expected all loaders, got {func['name']} with category={func['category']}")

    def test_no_duplicate_function_names(self):
        from TM1_bedrock_py.capabilities import get_bedrock_capabilities

        caps = get_bedrock_capabilities()
        names = [f["name"] for f in caps["functions"]]
        self.assertEqual(len(names), len(set(names)),
                         f"Duplicate function names found: {[n for n in names if names.count(n) > 1]}")


class TestStateSnapshot(unittest.TestCase):
    """Test the TM1 state snapshot functions with a mocked TM1py service."""

    def setUp(self):
        self.mock_tm1 = MagicMock()
        # Configure basic cube/dimension responses
        self.mock_tm1.cubes.get_all_names.return_value = ["Sales", "Costs", "ExchangeRates"]
        self.mock_tm1.cubes.get_dimension_names.side_effect = lambda cn: {
            "Sales": ["Product", "Region", "Month", "SalesMeasure"],
            "Costs": ["CostCenter", "Region", "Month", "CostMeasure"],
            "ExchangeRates": ["Currency", "Month", "ExchangeMeasure"],
        }.get(cn, [])

        self.mock_tm1.dimensions.get_all_names.return_value = [
            "Product", "Region", "Month", "SalesMeasure", "CostMeasure",
            "ExchangeMeasure", "CostCenter", "Currency",
        ]
        self.mock_tm1.dimensions.hierarchies.get_all_names.return_value = ["Default"]
        self.mock_tm1.dimensions.hierarchies.elements.get_number_of_elements.return_value = 150

        self.mock_tm1._url = "https://tm1-server.example.com:12354/"

    def test_get_tm1_state_snapshot_returns_valid_structure(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertIn("server_name", state)
        self.assertIn("cube_count", state)
        self.assertIn("dimension_count", state)
        self.assertIn("cubes", state)
        self.assertIn("dimensions", state)
        self.assertIn("snapshot_timestamp", state)
        self.assertIn("snapshot_duration_seconds", state)

    def test_snapshot_server_name_extracted(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertEqual(state["server_name"], "tm1-server.example.com")

    def test_snapshot_cube_count_correct(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertEqual(state["cube_count"], 3)
        self.assertEqual(state["dimension_count"], 8)

    def test_snapshot_cubes_have_dimensions(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertIn("Sales", state["cubes"])
        self.assertIn("dimensions", state["cubes"]["Sales"])
        self.assertEqual(len(state["cubes"]["Sales"]["dimensions"]), 4)

    def test_snapshot_dimensions_have_hierarchy_info(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertIn("Product", state["dimensions"])
        self.assertIn("hierarchy_count", state["dimensions"]["Product"])
        self.assertIn("hierarchy_names", state["dimensions"]["Product"])

    def test_snapshot_handles_missing_cubes_gracefully(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        self.mock_tm1.cubes.get_all_names.return_value = None
        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertEqual(state["cube_count"], 0)
        self.assertGreater(len(state["warnings"]), 0)

    def test_snapshot_handles_missing_dimensions_gracefully(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        self.mock_tm1.dimensions.get_all_names.return_value = None
        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertEqual(state["dimension_count"], 0)
        self.assertGreater(len(state["warnings"]), 0)

    def test_snapshot_handles_api_errors_gracefully(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        # Cube retrieval raises, dimension retrieval works
        self.mock_tm1.cubes.get_all_names.side_effect = ConnectionError("Server unreachable")
        state = get_tm1_state_snapshot(self.mock_tm1)
        self.assertIn("warnings", state)
        # Should not crash
        self.assertIsInstance(state["cubes"], dict)
        self.assertIsInstance(state["dimensions"], dict)

    def test_lightweight_snapshot_calls_fewer_apis(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_lightweight_state

        state = get_tm1_lightweight_state(self.mock_tm1)
        self.assertEqual(state["cube_count"], 3)
        # Element counts should not have been called
        for dim_info in state["dimensions"].values():
            self.assertEqual(dim_info["element_count"], 0)

    def test_list_cubes_returns_flat_dict(self):
        from TM1_bedrock_py.state_snapshot import list_cubes

        result = list_cubes(self.mock_tm1)
        self.assertIsInstance(result, dict)
        self.assertIn("Sales", result)
        self.assertIsInstance(result["Sales"], list)
        self.assertIn("Product", result["Sales"])

    def test_list_dimensions_returns_structured_dict(self):
        from TM1_bedrock_py.state_snapshot import list_dimensions

        result = list_dimensions(self.mock_tm1)
        self.assertIsInstance(result, dict)
        self.assertIn("Product", result)
        self.assertIn("hierarchy_count", result["Product"])
        self.assertIn("element_count", result["Product"])

    def test_snapshot_is_json_serializable(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        state = get_tm1_state_snapshot(self.mock_tm1)
        json_str = json.dumps(state, indent=2)
        parsed = json.loads(json_str)
        self.assertEqual(parsed["server_name"], "tm1-server.example.com")
        self.assertEqual(parsed["cube_count"], 3)

    def test_snapshot_skip_large_dimensions(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        self.mock_tm1.dimensions.hierarchies.elements.get_number_of_elements.return_value = 200_000
        state = get_tm1_state_snapshot(self.mock_tm1, max_dimension_elements=100_000)
        # Element count should be -1 (skipped as too large)
        for dim_info in state["dimensions"].values():
            self.assertEqual(dim_info["element_count"], -1)

    def test_snapshot_server_name_unknown_when_no_url(self):
        from TM1_bedrock_py.state_snapshot import get_tm1_state_snapshot

        mock = MagicMock()
        mock.cubes.get_all_names.return_value = ["TestCube"]
        mock.cubes.get_dimension_names.return_value = ["Dim1"]
        mock.dimensions.get_all_names.return_value = ["Dim1"]
        mock.dimensions.hierarchies.get_all_names.return_value = ["Default"]
        mock.dimensions.hierarchies.elements.get_number_of_elements.return_value = 10
        # No _url attribute
        del mock._url

        state = get_tm1_state_snapshot(mock)
        self.assertEqual(state["server_name"], "unknown")


if __name__ == "__main__":
    unittest.main()
