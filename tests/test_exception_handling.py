import asyncio
import inspect

import pandas as pd
import pytest

from TM1_bedrock_py import bedrock, exception_handling, transformer, validation, basic_logger
from TM1_bedrock_py.dimension_builder.exceptions import SchemaValidationError
from tests.config import mock_tm1_service
from tests.mock_tm1_service import UnregisteredTM1QueryError


def test_every_bedrock_wrapper_has_exception_context():
    public_wrappers = {
        name: function
        for name, function in vars(bedrock).items()
        if inspect.isfunction(function) and function.__module__ == bedrock.__name__
    }

    assert public_wrappers
    assert all(
        getattr(function, "_tm1_bedrock_public_operation", False)
        for function in public_wrappers.values()
    )


def test_public_operation_preserves_exception_type_and_traceback():
    @exception_handling.public_operation(context_fields=("cube_name",))
    def failing_wrapper(cube_name):
        raise ValueError("source data is invalid")

    with pytest.raises(ValueError) as excinfo:
        failing_wrapper("Planning")

    message = str(excinfo.value)
    assert "TM1 Bedrock operation failed" in message
    assert "Operation: failing_wrapper" in message
    assert "cube_name: 'Planning'" in message
    assert "Cause: ValueError: source data is invalid" in message
    assert excinfo.traceback[-1].name == "failing_wrapper"


def test_public_operation_redacts_sensitive_context():
    @exception_handling.public_operation(context_fields=("username", "password"))
    def failing_wrapper(username, password):
        raise RuntimeError("authentication failed")

    with pytest.raises(RuntimeError) as excinfo:
        failing_wrapper("admin", "super-secret")

    message = str(excinfo.value)
    assert "username: 'admin'" in message
    assert "password: <redacted>" in message
    assert "super-secret" not in message


def test_nested_public_operations_report_full_path_once(caplog):
    @exception_handling.public_operation()
    def inner_operation():
        raise SchemaValidationError("bad hierarchy")

    @exception_handling.public_operation()
    def outer_operation():
        inner_operation()

    with pytest.raises(SchemaValidationError) as excinfo:
        outer_operation()

    assert "Operation: outer_operation -> inner_operation" in str(excinfo.value)
    failure_logs = [
        record for record in caplog.records
        if record.getMessage().startswith("TM1 Bedrock operation failed")
    ]
    assert len(failure_logs) == 1


def test_query_text_is_summarized_not_exposed():
    secret_query = "SELECT {[Password].[Password].[do-not-log]} ON 0 FROM [Secrets]"

    @exception_handling.public_operation(context_fields=("data_mdx",))
    def failing_wrapper(data_mdx):
        raise ValueError("query failed")

    with pytest.raises(ValueError) as excinfo:
        failing_wrapper(secret_query)

    message = str(excinfo.value)
    assert f"<query text, {len(secret_query)} characters>" in message
    assert "do-not-log" not in message


def test_query_text_is_redacted_from_chained_causes():
    secret_query = "SELECT * FROM Payroll WHERE token = 'do-not-log'"
    cause = OSError(f"database rejected: {secret_query}")
    failure = RuntimeError("worker failed")
    failure.__cause__ = cause

    exception_handling.redact_exception_values(
        failure,
        {secret_query: f"<query text, {len(secret_query)} characters>"},
    )

    assert secret_query not in str(cause)
    assert "<query text" in str(cause)


def test_query_collection_items_are_redacted():
    first_query = "SELECT {[Version].[Actual]} ON 0 FROM [Sales]"
    second_query = "SELECT {[Version].[Budget]} ON 0 FROM [Sales]"

    @exception_handling.public_operation(context_fields=("data_mdx_list",))
    def failing_wrapper(data_mdx_list):
        raise ValueError(f"second query failed: {data_mdx_list[1]}")

    with pytest.raises(ValueError) as excinfo:
        failing_wrapper([first_query, second_query])

    message = str(excinfo.value)
    assert first_query not in message
    assert second_query not in message
    assert "query collection, 2 items" in message
    assert "<query text" in message


def test_async_public_operation_preserves_exception_type():
    @exception_handling.public_operation(context_fields=("max_workers",))
    async def failing_executor(max_workers):
        raise OSError("worker pool unavailable")

    with pytest.raises(OSError) as excinfo:
        asyncio.run(failing_executor(max_workers=4))

    assert "Operation: failing_executor" in str(excinfo.value)
    assert "max_workers: 4" in str(excinfo.value)


def test_data_copy_failure_contains_safe_wrapper_context(mock_tm1_service):
    mock_tm1_service.add_dimension("Version", {"Actual": "String"})
    mock_tm1_service.add_dimension("Measure", {"Amount": "Numeric"})
    mock_tm1_service.add_cube("Planning", ["Version", "Measure"])
    mdx = "SELECT {[Version].[Version].[Actual]} ON 0 FROM [Planning]"

    with pytest.raises(UnregisteredTM1QueryError) as excinfo:
        bedrock.data_copy(tm1_service=mock_tm1_service, data_mdx=mdx)

    message = str(excinfo.value)
    assert "Operation: data_copy" in message
    assert "Cause: UnregisteredTM1QueryError" in message
    assert mdx not in message


def test_dimension_builder_keeps_existing_validation_exception_type(
    monkeypatch, mock_tm1_service
):
    edges = pd.DataFrame(
        {
            "Parent": ["A"],
            "Child": ["A"],
            "Weight": [1.0],
            "Dimension": ["Product"],
            "Hierarchy": ["Product"],
        }
    )
    elements = pd.DataFrame(
        {
            "ElementName": ["A"],
            "ElementType": ["Consolidated"],
            "Dimension": ["Product"],
            "Hierarchy": ["Product"],
        }
    )

    monkeypatch.setattr(
        "TM1_bedrock_py.bedrock.apply.resolve_schema",
        lambda **_kwargs: (_ for _ in ()).throw(
            SchemaValidationError("inconsistent element types")
        ),
    )

    with pytest.raises(SchemaValidationError) as excinfo:
        bedrock.dimension_builder(
            dimension_name="Product",
            input_format="parent_child",
            build_strategy="rebuild",
            tm1_service=mock_tm1_service,
            override_input_edges_df=edges,
            override_input_elements_df=elements,
        )

    assert "Operation: dimension_builder" in str(excinfo.value)
    assert "Cause: SchemaValidationError: inconsistent element types" in str(
        excinfo.value
    )


def test_mapping_configuration_error_identifies_invalid_field():
    with pytest.raises(ValueError) as excinfo:
        validation.validate_mapping_configuration(
            [{"method": "replace"}],
            basic_logger,
        )

    message = str(excinfo.value)
    assert "mapping_steps configuration is invalid" in message
    assert "mapping" in message


def test_map_and_replace_error_explains_missing_join_columns():
    with pytest.raises(ValueError) as excinfo:
        transformer.dataframe_map_and_replace(
            data_df=pd.DataFrame({"Product": ["A"], "Value": [1]}),
            mapping_df=pd.DataFrame({"Account": ["Revenue"]}),
            mapped_dimensions={"Product": "Account"},
        )

    message = str(excinfo.value)
    assert "requires at least one shared join column" in message
    assert "Product" in message
    assert "Account" in message
