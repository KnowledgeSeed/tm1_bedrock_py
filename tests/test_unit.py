import os

import numpy as np
import pandas as pd
import parametrize_from_file
import pytest
from TM1py.Exceptions import TM1pyRestException
from pandas.core.frame import DataFrame
from sqlalchemy import text, create_engine
from sqlalchemy.engine import Engine
from sqlalchemy.exc import OperationalError

from TM1_bedrock_py import bedrock, extractor, transformer, utility, loader, input as bedrock_input
from TM1_bedrock_py.context_metadata import ContextMetadata
from tests.config import tm1_connection_factory, sql_engine_factory

EXCEPTION_MAP = {
    "ValueError": ValueError,
    "TypeError": TypeError,
    "TM1pyRestException": TM1pyRestException,
    "OperationalError": OperationalError,
    "IndexError": IndexError,
    "KeyError": KeyError
}


def test_tm1_connection(tm1_connection_factory):
    with tm1_connection_factory("tm1srv") as conn:
        server_name = conn.server.get_server_name()
        print("Connection to TM1 established! Your server name is: {}".format(server_name))


# ------------------------------------------------------------------------------------------------------------
# Utility: MDX query parsing functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_get_cube_name_from_mdx(mdx_query):
    cube_name = utility.get_cube_name_from_mdx(mdx_query)
    assert isinstance(cube_name, str)


@parametrize_from_file
def test_mdx_filter_to_dictionary(mdx_query):
    dimensions = utility.mdx_filter_to_dictionary(mdx_query)
    if mdx_query:
        for dim in dimensions:
            for elem in dim:
                assert isinstance(elem, str)
    else:
        assert dimensions == {}


@parametrize_from_file
def test_get_kwargs_dict_from_set_mdx_list_success(set_mdx_list, expected_kwargs):
    """
    Tests successful extraction of kwargs from various valid MDX lists.
    """
    kwargs = utility.get_kwargs_dict_from_set_mdx_list(set_mdx_list)
    assert kwargs == expected_kwargs


"""
# Test focusing on filtering, edge cases, and empty results
@parametrize_from_file
def test_get_kwargs_dict_from_set_mdx_list_filtering(set_mdx_list, expected_exception):

    Tests filtering of invalid/non-matching strings and edge cases.

    kwargs = utility.__get_kwargs_dict_from_set_mdx_list(set_mdx_list)
    assert kwargs == expected_exception
"""


@parametrize_from_file
def test_get_dimensions_from_set_mdx_list_success(mdx_sets, expected_dimensions):
    """
    Tests successful extraction of first dimension names from MDX strings.
    """
    result = utility.get_dimensions_from_set_mdx_list(mdx_sets)
    assert result == expected_dimensions


@parametrize_from_file
def test__get_kwargs_dict_from_set_mdx_list_fail(mdx_expressions, expected_exception):
    """
    Tests if the function raises a ValueError if at least one expression does not match.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type):
        utility.get_kwargs_dict_from_set_mdx_list(mdx_expressions)


@parametrize_from_file
def test_get_dimensions_from_set_mdx_list_failure(mdx_sets, expected_exception, expected_message_part):
    """
    Tests type errors for invalid input.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.get_dimensions_from_set_mdx_list(mdx_sets)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_generate_cartesian_product_success(list_of_lists, expected_product):
    """
    Tests successful generation of Cartesian products.
    """
    expected_tuples = [tuple(item) for item in expected_product]
    result = utility.generate_cartesian_product(list_of_lists)
    assert result == expected_tuples


@parametrize_from_file
def test_generate_cartesian_product_failure(list_of_lists, expected_exception, expected_message_part):
    """
    Tests failing scenarios due to invalid input types.
    """
    exception_type = eval(expected_exception)
    with pytest.raises(exception_type) as excinfo:
        utility.generate_cartesian_product(list_of_lists)
    assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_generate_element_lists_from_set_mdx_list_success(tm1_connection_factory, set_mdx_list, expected_result):
    """
    Tests successful extraction of element lists using a fake TM1 service.
    """
    with tm1_connection_factory("tm1srv") as conn:
        result = utility.generate_element_lists_from_set_mdx_list(conn, set_mdx_list)
        assert result == expected_result


@parametrize_from_file
def test_generate_element_lists_from_set_mdx_list_failure(
        tm1_connection_factory, use_none_service, set_mdx_list, expected_exception, expected_message_part):
    """
    Tests failing scenarios for element list extraction using fake or invalid service/inputs.
    """
    with tm1_connection_factory("tm1srv") as conn:
        if use_none_service:
            test_service = None
        else:
            test_service = conn

        exception_type = eval(expected_exception)
        with pytest.raises(exception_type) as excinfo:
            utility.generate_element_lists_from_set_mdx_list(test_service, set_mdx_list)

        assert expected_message_part in str(excinfo.value)


@parametrize_from_file
def test_extract_mdx_components(input_mdx, expected_set_mdx_list):
    output_set_mdx_list = utility.extract_mdx_components(mdx=input_mdx)
    assert output_set_mdx_list == expected_set_mdx_list


@parametrize_from_file
def test_add_nonempty_to_mdx_all_modes(input_mdx, expected_mdx):
    output_mdx = utility.add_non_empty_to_mdx(input_mdx)
    assert "".join(output_mdx.split()) == "".join(expected_mdx.split())


@parametrize_from_file
def test_all_leaves_identifiers_to_dataframe(tm1_connection_factory, dimname, expected):
    with tm1_connection_factory("tm1srv") as conn:
        df = utility.all_leaves_identifiers_to_dataframe(conn, dimname, dimname)
        live_expected_df = pd.DataFrame({
            dimname: sorted(conn.elements.get_all_leaf_element_identifiers(dimname, dimname))
        })

        print(df)
        print(live_expected_df)

        pd.testing.assert_frame_equal(df, live_expected_df)


@parametrize_from_file
def test_rename_columns_with_reference(input_df, input_list, expected_df):
    input_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    output_df = transformer.rename_columns_by_reference(dataframe=input_df, column_names=input_list)
    pd.testing.assert_frame_equal(output_df, expected_df)


@parametrize_from_file
def test_normalize_dict_strings(input_dict, expected_dict):
    output_dict = utility.normalize_structure_strings(input_dict)
    assert output_dict == expected_dict


@parametrize_from_file
def test_normalize_dataframe_strings(input_df, expected_df):
    output_df = pd.DataFrame(input_df)
    expected_df = pd.DataFrame(expected_df)
    utility.normalize_dataframe_strings(output_df)
    pd.testing.assert_frame_equal(output_df, expected_df)


def test_create_sql_engine_invalid_connection_type():
    with pytest.raises(ValueError) as excinfo:
        utility.create_sql_engine(connection_type="unsupported")

    assert "Unsupported connection_type" in str(excinfo.value)


def test_dataframe_to_sql_rejects_unknown_writer():
    with pytest.raises(ValueError, match="Unsupported sql_function"):
        loader.dataframe_to_sql(sql_function="unknown", dataframe=pd.DataFrame({"A": [1]}))


def test_clear_table_rejects_unknown_clear_function():
    with pytest.raises(ValueError, match="Unsupported clear_function"):
        loader.clear_table(clear_function="unknown")


def test_clear_table_requires_table_name_or_delete_statement():
    with pytest.raises(ValueError, match="Either 'table_name' or 'delete_statement'"):
        loader.clear_table(clear_function="pyodbc", database_engine_or_connection=object())


def test_clear_table_rolls_back_failed_database_operation():
    class BrokenCursor:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def execute(self, _statement):
            raise OSError("write failed")

    class Connection:
        def __init__(self):
            self.rollback_called = False

        def cursor(self):
            return BrokenCursor()

        def rollback(self):
            self.rollback_called = True

    connection = Connection()
    with pytest.raises(OSError, match="write failed"):
        loader.clear_table(
            clear_function="pyodbc",
            database_engine_or_connection=connection,
            table_name="Target",
            delete_statement=None,
        )

    assert connection.rollback_called is True


def test_clear_table_supports_cursor_without_context_manager():
    import sqlite3

    connection = sqlite3.connect(":memory:")
    connection.execute("CREATE TABLE Target (Value INTEGER)")
    connection.execute("INSERT INTO Target VALUES (1)")
    connection.commit()

    loader.clear_table(
        database_engine_or_connection=connection,
        delete_statement="DELETE FROM Target",
    )

    assert connection.execute("SELECT COUNT(*) FROM Target").fetchone()[0] == 0
    connection.close()


def test_clear_table_sqlalchemy_default_is_backend_agnostic():
    engine = create_engine("sqlite://", echo=False)
    pd.DataFrame({"Value": [1, 2]}).to_sql("Target", engine, index=False, if_exists="replace")

    loader.clear_table(
        clear_function="sqlalchemy",
        database_engine_or_connection=engine,
        table_name="Target",
    )

    with engine.connect() as connection:
        assert connection.execute(text("SELECT COUNT(*) FROM Target")).fetchone()[0] == 0


def test_clear_table_preserves_primary_error_when_cleanup_also_fails():
    class BrokenCursor:
        def execute(self, _statement):
            raise OSError("primary write failure")

        def close(self):
            raise RuntimeError("cursor close failure")

    class Connection:
        def cursor(self):
            return BrokenCursor()

        def rollback(self):
            raise RuntimeError("rollback failure")

    with pytest.raises(OSError, match="primary write failure"):
        loader.clear_table(
            clear_function="pyodbc",
            database_engine_or_connection=Connection(),
            table_name="Target",
            delete_statement=None,
        )


def test_get_sql_table_count_propagates_connection_failure():
    class BrokenEngine:
        def connect(self):
            raise OSError("database unavailable")

    with pytest.raises(RuntimeError, match="Failed to get row count"):
        extractor._get_sql_table_count(BrokenEngine(), "SourceTable")


def test_async_worker_errors_are_propagated():
    worker_error = ValueError("worker failed")

    with pytest.raises(RuntimeError, match=r"indexes: 1"):
        utility.raise_async_worker_errors([None, worker_error], "Test operation")


@pytest.mark.parametrize(
    ("kwargs", "expected_message"),
    [
        ({"if_exists": "invalid"}, "Unsupported if_exists strategy"),
        ({"chunksize": 0}, "'chunksize' must be greater than zero"),
    ],
)
def test_sql_api_writer_rejects_invalid_structural_inputs(kwargs, expected_message):
    with pytest.raises(ValueError, match=expected_message):
        loader.__write_dataframe_sql_api(
            dataframe=pd.DataFrame({"A": [1]}),
            table_name="Target",
            database_engine_or_connection=object(),
            table_exists_query="SELECT 1",
            **kwargs,
        )


def test_sql_api_writer_rejects_dataframe_without_columns():
    with pytest.raises(ValueError, match="without columns"):
        loader.__write_dataframe_sql_api(
            dataframe=pd.DataFrame(index=[0]),
            table_name="Target",
            database_engine_or_connection=object(),
            table_exists_query="SELECT 1",
        )


def test_context_metadata_rejects_empty_sql_result(monkeypatch):
    monkeypatch.setattr(
        extractor,
        "sql_to_dataframe",
        lambda **_kwargs: pd.DataFrame(),
    )
    context = ContextMetadata(sql_engine=object())

    with pytest.raises(ValueError, match="returned no values"):
        context.add_parameter_from_sql("CurrentPeriod", "SELECT Period")


def test_context_metadata_rejects_tm1_result_without_value(monkeypatch):
    monkeypatch.setattr(
        extractor,
        "tm1_mdx_to_dataframe",
        lambda **_kwargs: pd.DataFrame({"Period": ["2026"]}),
    )
    context = ContextMetadata(tm1_service=object())

    with pytest.raises(ValueError, match="has no 'Value' column"):
        context.add_parameter_from_tm1("CurrentPeriod", "SELECT ...")


def test_context_metadata_reads_first_tm1_value_by_position(monkeypatch):
    monkeypatch.setattr(
        extractor,
        "tm1_mdx_to_dataframe",
        lambda **_kwargs: pd.DataFrame({"Value": ["2026"]}, index=[5]),
    )
    context = ContextMetadata(tm1_service=object())

    context.add_parameter_from_tm1("CurrentPeriod", "SELECT ...")

    assert context.get_value("CurrentPeriod") == "2026"


def test_context_metadata_rejects_empty_init_yaml(tmp_path):
    source = tmp_path / "empty_context.yaml"
    source.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="must contain a mapping"):
        ContextMetadata(path_to_init_yaml=str(source))


def test_context_metadata_rejects_non_mapping_parameter_config(tmp_path):
    source = tmp_path / "invalid_context.yaml"
    source.write_text("CurrentPeriod: 202601\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must be a mapping"):
        ContextMetadata(path_to_init_yaml=str(source))


def test_context_metadata_requires_one_value_source(tmp_path):
    source = tmp_path / "missing_value_source.yaml"
    source.write_text("CurrentPeriod:\n  type: dimension_element\n", encoding="utf-8")

    with pytest.raises(ValueError, match="must define one of"):
        ContextMetadata(path_to_init_yaml=str(source))


def test_build_input_domain_drops_value_column(monkeypatch):
    class DummyMetadata:
        def get_cube_dims(self):
            return ["Dim1"]

        def get_filter_dict(self):
            return {"Version": "Actual"}

    monkeypatch.setattr(utility.TM1CubeObjectMetadata, "collect", lambda **_kwargs: DummyMetadata())
    monkeypatch.setattr(
        extractor,
        "tm1_mdx_to_dataframe",
        lambda **_kwargs: pd.DataFrame({"Dim1": ["A"], "Value": [1]})
    )

    domain_df = extractor.build_input_domain(
        tm1_service=object(),
        domain_mdx="SELECT {[Dim1].[A]} ON 0 FROM [Cube]"
    )

    assert "Value" not in domain_df.columns


def test_build_input_domain_from_coordinates_expands_leaf_combinations():
    class ElementsService:
        def __init__(self, responses):
            self._responses = responses

        def execute_set_mdx(self, mdx):
            return self._responses[mdx]

    class TM1Stub:
        def __init__(self, responses):
            self.elements = ElementsService(responses)

    first_mdx = "{Tm1FilterByLevel({Tm1DrillDownMember({[Version].[Actual]},ALL,RECURSIVE)},0)}"
    second_mdx = "{Tm1FilterByLevel({Tm1DrillDownMember({[Period].[FY24].[Q1]},ALL,RECURSIVE)},0)}"
    tm1 = TM1Stub(
        {
            first_mdx: [[{"Name": "Actual"}]],
            second_mdx: [[{"Name": "Jan"}], [{"Name": "Feb"}]],
        }
    )

    domain_df = extractor.build_input_domain(
        tm1_service=tm1,
        domain_coords={"Version": "Actual", "Period": "FY24:Q1"},
    )

    expected_df = pd.DataFrame(
        {
            "Version": ["Actual", "Actual"],
            "Period": ["FY24:Jan", "FY24:Feb"],
        },
        dtype="string",
    )
    pd.testing.assert_frame_equal(domain_df, expected_df)


def test_sql_to_dataframe_rejects_missing_query_and_table_name():
    with pytest.raises(ValueError, match="Either 'table_name' or 'sql_query'"):
        extractor.sql_to_dataframe(engine=object())


def test_sql_to_dataframe_supports_dbapi_cursor_connections():
    class Cursor:
        description = [("Id",), ("Name",)]

        def execute(self, sql_query):
            self.sql_query = sql_query

        def fetchall(self):
            return [(1, "A"), (2, "B")]

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

    class Connection:
        def cursor(self):
            return Cursor()

    dataframe = extractor.sql_to_dataframe(
        engine=Connection(),
        sql_query="SELECT Id, Name FROM Example",
    )

    expected = pd.DataFrame({"Id": [1, 2], "Name": ["A", "B"]})
    pd.testing.assert_frame_equal(dataframe, expected)


def test_sql_to_dataframe_supports_cursor_without_context_manager():
    class Cursor:
        description = [("Id",)]

        def __init__(self):
            self.closed = False

        def execute(self, _sql_query):
            return None

        def fetchall(self):
            return [(1,)]

        def close(self):
            self.closed = True

    class Connection:
        def __init__(self):
            self.db_cursor = Cursor()

        def cursor(self):
            return self.db_cursor

    connection = Connection()
    dataframe = extractor.sql_to_dataframe(
        engine=connection,
        sql_query="SELECT Id FROM Source",
    )

    pd.testing.assert_frame_equal(dataframe, pd.DataFrame({"Id": [1]}))
    assert connection.db_cursor.closed is True


def test_native_view_cleanup_does_not_mask_extraction_failure(monkeypatch):
    class TemporaryObject:
        def __init__(self, **_kwargs):
            pass

        def add_elements(self, **_kwargs):
            pass

        def add_column(self, **_kwargs):
            pass

        def add_row(self, **_kwargs):
            pass

    class Service:
        class Subsets:
            def create(self, **_kwargs):
                pass

            def delete(self, **_kwargs):
                raise RuntimeError("cleanup failed")

        class Views:
            def create(self, **_kwargs):
                pass

            def delete(self, **_kwargs):
                raise RuntimeError("cleanup failed")

        class Cells:
            def execute_view_dataframe(self, **_kwargs):
                raise OSError("primary extraction failure")

        subsets = Subsets()
        views = Views()
        cells = Cells()

    monkeypatch.setattr(extractor, "NativeView", TemporaryObject)
    monkeypatch.setattr(extractor, "Subset", TemporaryObject)
    monkeypatch.setattr(utility, "get_cube_name_from_mdx", lambda **_kwargs: "Cube")
    monkeypatch.setattr(utility, "extract_mdx_components", lambda **_kwargs: ["set"])
    monkeypatch.setattr(utility, "get_dimensions_from_set_mdx_list", lambda **_kwargs: ["Dim"])
    monkeypatch.setattr(utility, "generate_element_lists_from_set_mdx_list", lambda **_kwargs: [["A"]])

    with pytest.raises(OSError, match="primary extraction failure"):
        extractor.__tm1_mdx_to_native_view_to_dataframe(
            tm1_service=Service(),
            data_mdx="SELECT FROM [Cube]",
        )


# ------------------------------------------------------------------------------------------------------------
# Utility: Cube metadata collection using input MDXs and/or other cubes
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_cube_name_success(tm1_connection_factory, cube_name):
    """Collects metadata based on cube name and checks if the method's output is a Metadata object"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            print(metadata.to_dict())
            assert isinstance(metadata, utility.TM1CubeObjectMetadata)
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_cube_name_fail(tm1_connection_factory, cube_name, exception):
    """Runs collect_metadata based with bad cube name and checks if the method's output is a Metadata object."""
    with tm1_connection_factory("tm1srv") as conn:
        with pytest.raises(EXCEPTION_MAP[exception]):
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            print(metadata.to_dict())
            assert isinstance(metadata, utility.TM1CubeObjectMetadata)


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_mdx_name_success(tm1_connection_factory, data_mdx):
    """Collects metadata based on MDX and checks if the method's output is a Metadata object"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            assert isinstance(
                utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx),
                utility.TM1CubeObjectMetadata
            )
        except TM1pyRestException as e:
            pytest.fail(f"Cube not found based on MDX: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_based_on_mdx_name_fail(tm1_connection_factory, data_mdx, exception):
    """Runs collect_metadata with bad input for MDX and checks if the method's output is a Metadata object."""
    with tm1_connection_factory("tm1srv") as conn:
        with pytest.raises(EXCEPTION_MAP[exception]):
            assert isinstance(
                utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx),
                utility.TM1CubeObjectMetadata
            )


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_cube_dimensions_not_empty(tm1_connection_factory, cube_name):
    """Collects metadata and verifies that cube dimensions are not empty."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            cube_dims = metadata.get_cube_dims()
            assert cube_dims != 0
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_cube_dimensions_match_dimensions(
        tm1_connection_factory, cube_name, expected_dimensions
):
    """Collects metadata and verifies that cube dimensions match the expected dimensions."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, cube_name=cube_name)
            cube_dims = metadata.get_cube_dims()
            assert cube_dims == expected_dimensions
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_filter_dict_not_empty(tm1_connection_factory, data_mdx):
    """Collects metadata and verifies that filter dimensions are not empty."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx)
            filter_dims = metadata.get_filter_dict()
            assert filter_dims
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


@parametrize_from_file
def test_tm1_cube_object_metadata_collect_filter_dict_match(tm1_connection_factory, data_mdx, expected_filter_dict):
    """Collects metadata and verifies that filter dimensions are not empty."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            metadata = utility.TM1CubeObjectMetadata.collect(tm1_service=conn, mdx=data_mdx)
            filter_dict = metadata.get_filter_dict()
            assert filter_dict == expected_filter_dict
        except TM1pyRestException as e:
            pytest.fail(f"Cube name not found: {e}")


def test_tm1_cube_object_metadata_requires_mdx_or_cube_name():
    with pytest.raises(ValueError, match="either an MDX or a cube name"):
        utility.TM1CubeObjectMetadata.collect(tm1_service=object())


# ------------------------------------------------------------------------------------------------------------
# Main: MDX query to normalized pandas dataframe functions
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_success(tm1_connection_factory, data_mdx):
    """Run MDX to dataframe function and verifies that the output is a DataFrame object."""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            assert isinstance(df, DataFrame)
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_mdx_to_dataframe_execute_query_fail(tm1_connection_factory, data_mdx):
    """Run MDX to dataframe function with bad input. Raises error."""
    with tm1_connection_factory("tm1srv") as conn:
        with pytest.raises((TM1pyRestException, ValueError)):
            assert isinstance(
                extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx),
                DataFrame
            )


@parametrize_from_file
def test_normalize_dataframe_is_dataframe_true(tm1_connection_factory, data_mdx):
    """Run normalize dataframe function and check for if output is dataframe"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            df = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=conn, mdx=data_mdx)
            assert isinstance(df, DataFrame)
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_number_of_dimensions_success(tm1_connection_factory, data_mdx, expected_dimensions):
    """Run normalize dataframe function and check if the output has the correct number of dimensions"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            df = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=conn, mdx=data_mdx)
            df.keys()
            assert len(df.keys()) == expected_dimensions
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


@parametrize_from_file
def test_normalize_dataframe_match_dimensions_success(tm1_connection_factory, data_mdx, expected_dimensions):
    """Runs normalize dataframe function and validates that the output's dimension keys match the expected"""
    with tm1_connection_factory("tm1srv") as conn:
        try:
            df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            df = transformer.normalize_dataframe_for_testing(dataframe=df, tm1_service=conn, mdx=data_mdx)
            keys = [key for key in df.keys()]
            assert expected_dimensions == keys
        except Exception as e:
            pytest.fail(f"MDX query execution failed: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: dataframe transform utility functions
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_dataframe_filter_inplace(dataframe, filter_condition, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_filter_inplace(dataframe=df, filter_condition=filter_condition)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_filter(dataframe, filter_condition, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    filtered_df = transformer.dataframe_filter(dataframe=df, filter_condition=filter_condition)

    pd.testing.assert_frame_equal(filtered_df, expected_df)


@parametrize_from_file
def test_dataframe_drop_column(dataframe, column_list, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_drop_column(dataframe=df, column_list=column_list)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_redimension_scale_down(dataframe, filter_condition, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_drop_filtered_column(dataframe=df, filter_condition=filter_condition)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_relabel(dataframe, columns, expected_columns):
    df = pd.DataFrame(dataframe)
    transformer.dataframe_relabel(dataframe=df, columns=columns)
    new_columns = list(map(str, df.columns))
    assert new_columns == expected_columns


@parametrize_from_file
def test_dataframe_add_column_assign_value(dataframe, column_values, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_add_column_assign_value(dataframe=df, column_value=column_values)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_redimension_and_transform(
        dataframe, source_dim_mapping, related_dimensions, target_dim_mapping, expected_dataframe
):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_redimension_and_transform(
        df, source_dim_mapping, related_dimensions, target_dim_mapping
    )

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_reorder_dimensions(dataframe, cube_cols, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_reorder_dimensions(dataframe=df, cube_dimensions=cube_cols)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_value_scale(dataframe, expected_dataframe):
    def add_one(x): return x + 1

    def multiply_by_two(x): return x * 2

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    transformer.dataframe_value_scale(df, add_one)
    transformer.dataframe_value_scale(df, multiply_by_two)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_itemskip_elements(source, check1, check2, defaults, expected):
    utility.set_logging_level("DEBUG")
    df = pd.DataFrame(source)
    check_dfs = {'a': pd.DataFrame(check1), 'b': pd.DataFrame(check2)}
    expected_df = pd.DataFrame(expected)
    transformer.dataframe_itemskip_elements(dataframe=df, check_dfs=check_dfs, fallback_elements=defaults)
    print(df)
    print(expected_df)
    pd.testing.assert_frame_equal(df, expected_df)


def test_dataframe_itemskip_elements_case_and_space_insensitive_with_fallback():
    df = pd.DataFrame({
        "versionrep": [" actual ", "budget", "Fcst"],
        "measure": [" x ", "Y", "z"],
        "value": [10, 20, 30]
    })
    check_dfs = {
        "versionrep": pd.DataFrame({"versionrep": ["Actual", "Fcst", "Fallback"]}),
        "measure": pd.DataFrame({"measure": ["X", "Y", "Z"]})
    }
    expected_df = pd.DataFrame({
        "versionrep": [" actual ", "fallback", "Fcst"],
        "measure": [" x ", "Y", "z"],
        "value": [10, 20, 30]
    })

    transformer.dataframe_itemskip_elements(
        dataframe=df,
        check_dfs=check_dfs,
        fallback_elements={"versionrep": "Fallback"}
    )

    pd.testing.assert_frame_equal(df, expected_df)


def test_dataframe_itemskip_elements_case_and_space_insensitive_return_dropped_rows():
    df = pd.DataFrame({
        "versionrep": [" actual ", "budget", None, "Fcst"],
        "measure": [" x ", "bad", "Y", "z"],
        "value": [10, 20, 30, 40]
    })
    check_dfs = {
        "versionrep": pd.DataFrame({"versionrep": ["Actual", "Fcst"]}),
        "measure": pd.DataFrame({"measure": ["X", "Y", "Z"]})
    }
    expected_df = pd.DataFrame({
        "versionrep": [" actual ", "Fcst"],
        "measure": [" x ", "z"],
        "value": [10, 40]
    })
    expected_dropped = pd.DataFrame({
        "versionrep": [None, "budget", "budget"],
        "measure": ["Y", "bad", "bad"],
        "value": [30, 20, 20],
        "Itemskip:NaN:versionrep": [1.0, np.nan, np.nan],
        "Itemskip:NaN:measure": [np.nan, np.nan, np.nan],
        "Itemskip:NaN:value": [np.nan, np.nan, np.nan],
        "Itemskip:Missing:versionrep": [np.nan, 1.0, np.nan],
        "Itemskip:Missing:measure": [np.nan, np.nan, 1.0]
    })

    dropped = transformer.dataframe_itemskip_elements(
        dataframe=df,
        check_dfs=check_dfs,
        return_dropped_rows=True
    )

    pd.testing.assert_frame_equal(df, expected_df)
    pd.testing.assert_frame_equal(dropped.reset_index(drop=True), expected_dropped)


@parametrize_from_file
def test_dataframe_validate_datatypes(measuredim, measuretypes, dataframe, expected):
    input_dataframe = pd.DataFrame(dataframe)
    expected_dataframe = pd.DataFrame(expected)
    transformer.dataframe_cast_value_by_measure_type(
        dataframe=input_dataframe,
        measure_dimension_name=measuredim,
        measure_element_types=measuretypes
    )
    pd.testing.assert_frame_equal(input_dataframe, expected_dataframe)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for dataframe remapping and copy functions
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_dataframe_find_and_replace_success(dataframe, mapping, expected_dataframe):
    """Remaps elements based on literal mapping, without dimension manipulation and checks for successful execution"""

    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_find_and_replace(dataframe=df, mapping=mapping)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_find_and_replace_fail(dataframe, mapping, expected_dataframe):
    """Tries to remap elements based on literal mapping, without dimension manipulation with bad input. Raises error."""
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    with pytest.raises(AssertionError):
        df = transformer.dataframe_find_and_replace(dataframe=df, mapping=mapping)
        pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_map_and_replace_success(dataframe, mapping_dataframe, mapping_dimensions, expected_dataframe):
    df = pd.DataFrame(dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_map_and_replace(
        data_df=df,
        mapping_df=mapping_df,
        mapped_dimensions=mapping_dimensions)

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_map_and_join_success(dataframe, joined_cols, mapping_dataframe, expected_dataframe):
    df = pd.DataFrame(dataframe)
    mapping_df = pd.DataFrame(mapping_dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_map_and_join(
        data_df=df,
        mapping_df=mapping_df,
        joined_columns=joined_cols
    )

    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_dataframe_execute_mappings_replace_success(dataframe, mapping_steps, expected_dataframe):
    df = pd.DataFrame(dataframe)
    expected_df = pd.DataFrame(expected_dataframe)
    df = transformer.dataframe_execute_mappings(
        data_df=df,
        mapping_steps=mapping_steps,
    )

    pd.testing.assert_frame_equal(df, expected_df)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for sql connections and I/O processes
# ------------------------------------------------------------------------------------------------------------

def create_mock_sql_database(expected, table_name) -> tuple[Engine, DataFrame]:
    engine = create_engine('sqlite://', echo=False)
    expected_dataframe = pd.DataFrame(expected)
    expected_dataframe.to_sql(name=table_name, con=engine, index=False)
    return engine, expected_dataframe


def test_mssql_database_connection(sql_engine_factory):
    sql_engine = create_engine('sqlite://', echo=False)
    # assert sql_engine.closed is False
    assert str(sql_engine.url.get_backend_name()) == "sqlite", f"Wrong backend: {sql_engine.url.get_backend_name()}"


def test_mssql_server_responds_to_query(sql_engine_factory):
    sql_engine = create_engine('sqlite://', echo=False)
    with sql_engine.connect() as connection:
        result = connection.execute(text("SELECT 1"))
        response = result.fetchone()
    assert response == (1,)


@parametrize_from_file
def test_mssql_extract_full_table(sql_engine_factory, table_name, expected):
    sql_engine, expected_df = create_mock_sql_database(expected, table_name)

    df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_table_columns(sql_engine_factory, table_name, columns, expected):
    sql_engine, expected_df = create_mock_sql_database(expected, table_name)

    df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name, table_columns=columns)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_query(sql_engine_factory, query, expected):
    table_name = "Unit Test Table"
    sql_engine, expected_df = create_mock_sql_database(expected, table_name)

    df = extractor.sql_to_dataframe(engine=sql_engine, sql_query=query)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_extract_query_with_chunksize(sql_engine_factory, query, expected, chunksize):
    table_name = "Unit Test Table"
    sql_engine, expected_df = create_mock_sql_database(expected, table_name)

    df = extractor.sql_to_dataframe(engine=sql_engine, sql_query=query, chunksize=chunksize)
    pd.testing.assert_frame_equal(df, expected_df)


def test_sql_to_dataframe_chunked_table_read_supports_empty_table():
    table_name = "EmptyTable"
    sql_engine = create_engine("sqlite://", echo=False)
    pd.DataFrame(columns=["Version", "Value"]).to_sql(
        name=table_name, con=sql_engine, index=False, if_exists="replace"
    )

    df = extractor.sql_to_dataframe(engine=sql_engine, table_name=table_name, chunksize=10)

    assert list(df.columns) == ["Version", "Value"]
    assert df.empty


@parametrize_from_file
def test_sql_normalize_relabel(dataframe, expected, column_mapping):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, column_mapping=column_mapping)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_sql_normalize_drop(dataframe, expected, drop):
    df = pd.DataFrame(dataframe)
    transformer.normalize_table_source_dataframe(dataframe=df, columns_to_drop=drop)
    expected_df = pd.DataFrame(expected)
    pd.testing.assert_frame_equal(df, expected_df)


@parametrize_from_file
def test_mssql_loader_replace(sql_engine_factory, dataframe, if_exists, table_name):
    sql_engine, df = create_mock_sql_database(dataframe, table_name)
    loader.dataframe_to_sql(
        dataframe=df, engine=sql_engine, table_name=table_name, if_exists=if_exists, index=False
    )


def test_dataframe_to_sql_default_creates_missing_table():
    sql_engine = create_engine("sqlite://", echo=False)
    df = pd.DataFrame({"Version": ["Actual"], "Value": [1.0]})

    loader.dataframe_to_sql(
        dataframe=df,
        engine=sql_engine,
        table_name="new_target",
        if_exists="append",
        index=False,
    )

    result = pd.read_sql_table("new_target", sql_engine)
    pd.testing.assert_frame_equal(result, df)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for csv I/O processes
# ------------------------------------------------------------------------------------------------------------

@parametrize_from_file
def test_dataframe_casting_for_csv_file(dataframe, cube_dims):
    """ Tests utility.cast_coordinates_to_str() function. """
    df = pd.DataFrame(dataframe)
    for dim_col in cube_dims:
        if dim_col in df.columns:
            df[dim_col] = df[dim_col].astype(str)
            assert df[dim_col].apply(lambda v: isinstance(v, str)).all()


@parametrize_from_file
def test_dataframe_to_csv(data_dataframe, expected_dataframe):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    csv_file_name = "sample_data.csv"
    output_dir = "./dataframe_to_csv"
    csv_file_path = f"{output_dir}/{csv_file_name}"
    try:
        data_dataframe["Price"] = [float(x) for x in data_dataframe["Price"]]
        data_dataframe["Quantity"] = [float(x) for x in data_dataframe["Quantity"]]

        expected_dataframe["Price"] = [float(x) for x in expected_dataframe["Price"]]
        expected_dataframe["Quantity"] = [float(x) for x in expected_dataframe["Quantity"]]

        data_df = pd.DataFrame(data_dataframe)
        dtype_mapping = data_df.dtypes.apply(lambda x: x.name).to_dict()

        loader.dataframe_to_csv(dataframe=data_df, csv_file_name=csv_file_name, decimal=".")
        df = extractor.csv_to_dataframe(
            csv_file_path=csv_file_path,
            decimal=".",
            dtype=dtype_mapping,
            keep_default_na=False,
            na_values=["NULL", "Nan"]
        )

        expected_df = pd.DataFrame(expected_dataframe)

        pd.testing.assert_frame_equal(df, expected_df)

    finally:
        if os.path.exists(csv_file_path):
            os.remove(csv_file_path)
            os.rmdir(output_dir)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx(tm1_connection_factory, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    with tm1_connection_factory("tm1srv") as conn:
        csv_file_name = "sample_data.csv"
        try:
            expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            dtype_mapping = expected_df.dtypes.apply(lambda x: x.name).to_dict()

            expected_df = transformer.normalize_dataframe_for_testing(dataframe=expected_df, tm1_service=conn, mdx=data_mdx)

            loader.dataframe_to_csv(
                dataframe=expected_df, csv_file_name=csv_file_name, csv_output_dir="./", decimal=".", mode="a"
            )
            df = extractor.csv_to_dataframe(csv_file_path=f"./{csv_file_name}", decimal=".", dtype=dtype_mapping)
            pd.testing.assert_frame_equal(df, expected_df)

        finally:
            if os.path.exists(csv_file_name):
                os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx_with_param_optimisation(tm1_connection_factory, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        Deletes CSV file after assertion.
    """
    with tm1_connection_factory("tm1srv") as conn:
        csv_file_name = "sample_data.csv"
        try:
            expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)
            dtype_mapping = expected_df.dtypes.apply(lambda x: x.name).to_dict()

            loader.dataframe_to_csv(
                dataframe=expected_df, csv_file_name=csv_file_name, csv_output_dir="./", decimal="."
            )
            df = extractor.csv_to_dataframe(
                csv_file_path=f"./{csv_file_name}",
                decimal=".",
                dtype=dtype_mapping,
                chunksize=204
            )

            pd.testing.assert_frame_equal(df, expected_df)

        finally:
            if os.path.exists(csv_file_name):
                os.remove(csv_file_name)


@parametrize_from_file
def test_dataframe_to_csv_build_dataframe_form_mdx_fail(tm1_connection_factory, data_mdx):
    """
        Loads data from DataFrame file to a CSV file then does the reverse.
        Checks if the DataFrame stayed the same after the operations.
        As the original data types are not passed to the function, the types differ.
        Expected to fail.
        Deletes CSV file after assertion.
    """
    with tm1_connection_factory("tm1srv") as conn:
        csv_file_name = "sample_data.csv"
        with pytest.raises(AssertionError):
            try:
                expected_df = extractor.tm1_mdx_to_dataframe(tm1_service=conn, data_mdx=data_mdx)

                loader.dataframe_to_csv(
                    dataframe=expected_df, csv_file_name=csv_file_name, csv_output_dir="./", decimal="."
                )
                df = extractor.csv_to_dataframe(csv_file_path=f"./{csv_file_name}", decimal=".")

                pd.testing.assert_frame_equal(df, expected_df)

            finally:
                if os.path.exists(csv_file_name):
                    os.remove(csv_file_name)


# ------------------------------------------------------------------------------------------------------------
# Main: tests for airflow executor common functions
# ------------------------------------------------------------------------------------------------------------

@pytest.mark.skip
@parametrize_from_file
def test_generate_mapping_queries_for_slice(kwargs, ms, sm, expected_ms, expected_sm):
    try:
        from TM1_bedrock_py.airflow_executor import common as airflow_common
        output_ms, output_sm = airflow_common.generate_mapping_queries_for_slice(
            expand_kwargs=kwargs,
            mapping_steps=ms,
            shared_mapping=sm
        )
        assert (output_ms, output_sm) == (expected_ms, expected_sm)
    except ModuleNotFoundError as e:
        print(f"Airflow executor sub-modul packages were not installed: {e}")


# ------------------------------------------------------------------------------------------------------------
# Main: tests for input.py modul
# ------------------------------------------------------------------------------------------------------------


@parametrize_from_file
def test_assign_constant(dataframe, value, expected, name="Value"):
    df = pd.DataFrame(dataframe)
    expected = pd.DataFrame(expected)

    result = transformer.assign_constant(value=value, name=name, dataframe=df.copy())

    pd.testing.assert_frame_equal(result, expected)


@parametrize_from_file
def test_sum_cells(dataframe, name, column_to_sum, expected):
    df = pd.DataFrame(dataframe)
    expected = pd.DataFrame(expected)

    result = transformer.sum_cells(dataframe=df.copy(), name=name, column_to_sum=column_to_sum)

    pd.testing.assert_frame_equal(result, expected)


@parametrize_from_file
def test_sum_if_cells(dataframe, name, column_to_sum, group_dimension_list, expected):
    df = pd.DataFrame(dataframe)
    expected = pd.DataFrame(expected)

    result = transformer.sum_group_cells(dataframe=df.copy(), name=name, column_to_sum=column_to_sum,
                                         group_column_list=group_dimension_list)

    pd.testing.assert_frame_equal(result, expected)


@parametrize_from_file
def test_count_cells(dataframe, name, expected):
    df = pd.DataFrame(dataframe)
    expected = pd.DataFrame(expected)

    result = transformer.count_cells(dataframe=df.copy(), name=name)

    pd.testing.assert_frame_equal(result, expected)


@parametrize_from_file
def test_count_if_cells(dataframe, name, group_dimension_list, expected):
    df = pd.DataFrame(dataframe)
    expected = pd.DataFrame(expected)

    result = transformer.count_group_cells(dataframe=df.copy(), name=name, group_column_list=group_dimension_list)

    pd.testing.assert_frame_equal(result, expected)


@parametrize_from_file
def test_assign_data(dataframe, calc_df, expected, name):
    df = pd.DataFrame(dataframe)
    assign_df = pd.DataFrame(calc_df)
    expected = pd.DataFrame(expected)

    result = transformer.assign_data(dataframe=df.copy(), calc_df=assign_df.copy(), name=name)

    pd.testing.assert_frame_equal(result, expected)
