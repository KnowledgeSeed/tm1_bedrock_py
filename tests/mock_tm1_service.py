from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass
from threading import RLock
from typing import Any, Callable, Iterable, Optional, Union

import pandas as pd
from TM1py.Objects import Cube, Dimension, Element, Hierarchy


class UnregisteredTM1QueryError(KeyError):
    """Raised when a test executes a query without defining its response."""


@dataclass(frozen=True)
class TM1Call:
    service: str
    method: str
    args: tuple[Any, ...]
    kwargs: dict[str, Any]


def _query_key(query: str) -> str:
    return " ".join(str(query).split())


def _name_key(name: str) -> str:
    return "".join(str(name).split()).casefold()


class _BaseService:
    def __init__(self, tm1: "MockTM1Service"):
        self._tm1 = tm1

    def _record(self, method: str, *args: Any, **kwargs: Any) -> None:
        self._tm1.calls.append(TM1Call(self.__class__.__name__, method, args, kwargs))


class MockServerService(_BaseService):
    def get_server_name(self, **kwargs: Any) -> str:
        self._record("get_server_name", **kwargs)
        return self._tm1.server_name


class MockCubeService(_BaseService):
    def create(self, cube: Cube, **kwargs: Any) -> None:
        self._record("create", cube, **kwargs)
        self._tm1._cubes[_name_key(cube.name)] = deepcopy(cube)
        self._tm1._cube_data.setdefault(_name_key(cube.name), pd.DataFrame())

    def delete(self, cube_name: str, **kwargs: Any) -> None:
        self._record("delete", cube_name, **kwargs)
        key = _name_key(cube_name)
        self._tm1._cubes.pop(key, None)
        self._tm1._cube_data.pop(key, None)

    def exists(self, cube_name: str, **kwargs: Any) -> bool:
        self._record("exists", cube_name, **kwargs)
        return _name_key(cube_name) in self._tm1._cubes

    def get_dimension_names(
        self, cube_name: str, skip_sandbox_dimension: bool = True, **kwargs: Any
    ) -> list[str]:
        self._record("get_dimension_names", cube_name, skip_sandbox_dimension, **kwargs)
        return list(self._tm1._get_cube(cube_name).dimensions)


class MockDimensionAttributeService(_BaseService):
    def get_all(self, dimension_name: str, **kwargs: Any) -> list[Any]:
        self._record("get_all", dimension_name, **kwargs)
        attributes: dict[str, Any] = {}
        for hierarchy in self._tm1._get_dimension(dimension_name).hierarchies:
            for attribute in hierarchy.element_attributes:
                attributes[_name_key(attribute.name)] = attribute
        return list(attributes.values())


class MockDimensionService(_BaseService):
    def __init__(self, tm1: "MockTM1Service"):
        super().__init__(tm1)
        self.attributes = MockDimensionAttributeService(tm1)

    def get(self, dimension_name: str, **kwargs: Any) -> Dimension:
        self._record("get", dimension_name, **kwargs)
        return deepcopy(self._tm1._get_dimension(dimension_name))

    def update_or_create(self, dimension: Dimension, **kwargs: Any) -> None:
        self._record("update_or_create", dimension, **kwargs)
        with self._tm1._lock:
            self._tm1._dimensions[_name_key(dimension.name)] = deepcopy(dimension)

    def exists(self, dimension_name: str, **kwargs: Any) -> bool:
        self._record("exists", dimension_name, **kwargs)
        return _name_key(dimension_name) in self._tm1._dimensions

    def get_all_names(self, skip_control_dims: bool = False, **kwargs: Any) -> list[str]:
        self._record("get_all_names", skip_control_dims, **kwargs)
        names = [dimension.name for dimension in self._tm1._dimensions.values()]
        if skip_control_dims:
            names = [name for name in names if not name.startswith("}")]
        return names


class MockHierarchyService(_BaseService):
    def get(self, dimension_name: str, hierarchy_name: str, **kwargs: Any) -> Hierarchy:
        self._record("get", dimension_name, hierarchy_name, **kwargs)
        return deepcopy(self._tm1._get_hierarchy(dimension_name, hierarchy_name))

    def get_all_names(self, dimension_name: str, **kwargs: Any) -> list[str]:
        self._record("get_all_names", dimension_name, **kwargs)
        return list(self._tm1._get_dimension(dimension_name).hierarchy_names)

    def update_or_create(self, hierarchy: Hierarchy, **kwargs: Any) -> None:
        self._record("update_or_create", hierarchy, **kwargs)
        dimension_key = _name_key(hierarchy.dimension_name)
        dimension = self._tm1._dimensions.get(dimension_key, Dimension(hierarchy.dimension_name))
        remaining = [
            current
            for current in dimension.hierarchies
            if _name_key(current.name) != _name_key(hierarchy.name)
        ]
        self._tm1._dimensions[dimension_key] = Dimension(
            dimension.name, remaining + [deepcopy(hierarchy)]
        )

    def exists(self, dimension_name: str, hierarchy_name: str, **kwargs: Any) -> bool:
        self._record("exists", dimension_name, hierarchy_name, **kwargs)
        try:
            self._tm1._get_hierarchy(dimension_name, hierarchy_name)
            return True
        except KeyError:
            return False

    def _implement_hierarchy_sort_order(
        self,
        dimension_name: str,
        hierarchy_name: str,
        hierarchy_sort_order: tuple[int, int, int, int],
        **kwargs: Any,
    ) -> None:
        self._record(
            "_implement_hierarchy_sort_order",
            dimension_name,
            hierarchy_name,
            hierarchy_sort_order,
            **kwargs,
        )
        self._tm1.hierarchy_sort_orders[
            (_name_key(dimension_name), _name_key(hierarchy_name))
        ] = hierarchy_sort_order


class MockElementService(_BaseService):
    def exists(
        self, dimension_name: str, hierarchy_name: str, element_name: str, **kwargs: Any
    ) -> bool:
        self._record("exists", dimension_name, hierarchy_name, element_name, **kwargs)
        hierarchy = self._tm1._get_hierarchy(dimension_name, hierarchy_name)
        return any(_name_key(name) == _name_key(element_name) for name in hierarchy.elements)

    def delete(
        self, dimension_name: str, hierarchy_name: str, element_name: str, **kwargs: Any
    ) -> None:
        self._record("delete", dimension_name, hierarchy_name, element_name, **kwargs)
        hierarchy = self._tm1._get_hierarchy(dimension_name, hierarchy_name)
        matching_name = next(
            (name for name in hierarchy.elements if _name_key(name) == _name_key(element_name)),
            None,
        )
        if matching_name is not None:
            del hierarchy.elements[matching_name]

    def get_elements(
        self, dimension_name: str, hierarchy_name: str, **kwargs: Any
    ) -> list[Element]:
        self._record("get_elements", dimension_name, hierarchy_name, **kwargs)
        return [
            deepcopy(element)
            for element in self._tm1._get_hierarchy(dimension_name, hierarchy_name).elements.values()
        ]

    def get_elements_dataframe(
        self,
        dimension_name: str,
        hierarchy_name: str,
        skip_consolidations: bool = True,
        attribute_suffix: bool = False,
        element_type_column: str = "Type",
        **kwargs: Any,
    ) -> pd.DataFrame:
        self._record(
            "get_elements_dataframe",
            dimension_name,
            hierarchy_name,
            skip_consolidations,
            attribute_suffix,
            element_type_column,
            **kwargs,
        )
        hierarchy = self._tm1._get_hierarchy(dimension_name, hierarchy_name)
        rows = []
        for element in hierarchy.elements.values():
            if skip_consolidations and element.element_type == Element.Types.CONSOLIDATED:
                continue
            row = {
                dimension_name: element.name,
                element_type_column: str(element.element_type),
            }
            rows.append(row)
        return pd.DataFrame(rows)

    def execute_set_mdx(self, mdx: str, top_records: Optional[int] = None, **kwargs: Any) -> list:
        self._record("execute_set_mdx", mdx, top_records, **kwargs)
        result = self._tm1._resolve_query(self._tm1._set_mdx_results, mdx, "set MDX")
        if top_records is not None:
            result = result[:top_records]
        return deepcopy(result)

    def get_all_leaf_element_identifiers(
        self, dimension_name: str, hierarchy_name: str, **kwargs: Any
    ) -> set[str]:
        self._record("get_all_leaf_element_identifiers", dimension_name, hierarchy_name, **kwargs)
        hierarchy = self._tm1._get_hierarchy(dimension_name, hierarchy_name)
        return {
            element.name
            for element in hierarchy.elements.values()
            if element.element_type != Element.Types.CONSOLIDATED
        }

    def get_element_types(
        self,
        dimension_name: str,
        hierarchy_name: str,
        skip_consolidations: bool = False,
        **kwargs: Any,
    ) -> dict[str, int]:
        self._record(
            "get_element_types",
            dimension_name,
            hierarchy_name,
            skip_consolidations,
            **kwargs,
        )
        hierarchy = self._tm1._get_hierarchy(dimension_name, hierarchy_name)
        return {
            element.name: element.element_type.value
            for element in hierarchy.elements.values()
            if not skip_consolidations or element.element_type != Element.Types.CONSOLIDATED
        }


class MockCellService(_BaseService):
    def execute_mdx_dataframe(self, mdx: str, **kwargs: Any) -> pd.DataFrame:
        self._record("execute_mdx_dataframe", mdx, **kwargs)
        result = self._tm1._resolve_query(self._tm1._mdx_results, mdx, "MDX")
        dataframe = result.copy(deep=True)
        dtype = kwargs.get("dtype")
        if dtype:
            for column, column_type in dtype.items():
                if column in dataframe.columns:
                    dataframe[column] = dataframe[column].astype(column_type)
        return dataframe

    def execute_mdx_dataframe_async(
        self, mdx_list: list[str], **kwargs: Any
    ) -> pd.DataFrame:
        self._record("execute_mdx_dataframe_async", mdx_list, **kwargs)
        return pd.concat(
            [self.execute_mdx_dataframe(mdx, **kwargs) for mdx in mdx_list],
            ignore_index=True,
        )

    def execute_view_dataframe(
        self, cube_name: str, view_name: str, **kwargs: Any
    ) -> pd.DataFrame:
        self._record("execute_view_dataframe", cube_name, view_name, **kwargs)
        key = (_name_key(cube_name), _name_key(view_name))
        if key not in self._tm1._view_results:
            raise UnregisteredTM1QueryError(
                f"No view result registered for cube '{cube_name}', view '{view_name}'."
            )
        return self._tm1._view_results[key].copy(deep=True)

    def write_dataframe(
        self,
        cube_name: str,
        data: pd.DataFrame,
        dimensions: Optional[Iterable[str]] = None,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        **kwargs: Any,
    ) -> None:
        self._record(
            "write_dataframe",
            cube_name,
            data,
            dimensions,
            increment,
            sum_numeric_duplicates,
            **kwargs,
        )
        cube_dimensions = list(dimensions or self._tm1._get_cube(cube_name).dimensions)
        expected_columns = cube_dimensions + ["Value"]
        if list(data.columns) != expected_columns:
            raise ValueError(
                f"DataFrame columns {list(data.columns)} do not match cube shape {expected_columns}."
            )

        key = _name_key(cube_name)
        incoming = data.copy(deep=True)
        with self._tm1._lock:
            existing = self._tm1._cube_data.get(key, pd.DataFrame(columns=expected_columns))
            combined = pd.concat([existing, incoming], ignore_index=True)

            if increment:
                numeric_values = pd.to_numeric(combined["Value"], errors="raise")
                combined = combined.assign(Value=numeric_values).groupby(
                    cube_dimensions, as_index=False, dropna=False
                )["Value"].sum()
            elif sum_numeric_duplicates and combined.duplicated(cube_dimensions).any():
                numeric_values = pd.to_numeric(combined["Value"], errors="coerce")
                if numeric_values.notna().all():
                    combined = combined.assign(Value=numeric_values).groupby(
                        cube_dimensions, as_index=False, dropna=False
                    )["Value"].sum()
                else:
                    combined = combined.drop_duplicates(cube_dimensions, keep="last")
            else:
                combined = combined.drop_duplicates(cube_dimensions, keep="last")

            self._tm1._cube_data[key] = combined.reset_index(drop=True)

    def write_dataframe_async(self, **kwargs: Any) -> None:
        self._record("write_dataframe_async", **kwargs)
        self.write_dataframe(**kwargs)

    def clear(self, cube: str, **kwargs: Any) -> None:
        self._record("clear", cube, **kwargs)
        key = _name_key(cube)
        with self._tm1._lock:
            current = self._tm1._cube_data.get(key, pd.DataFrame())
            if current.empty or not kwargs:
                self._tm1._cube_data[key] = current.iloc[0:0].copy()
                return

            mask = pd.Series(True, index=current.index)
            for dimension_name, set_mdx in kwargs.items():
                if dimension_name not in current.columns:
                    continue
                records = self._tm1._resolve_query(
                    self._tm1._set_mdx_results, set_mdx, "set MDX"
                )
                elements = {record[0]["Name"] for record in records}
                mask &= current[dimension_name].isin(elements)
            self._tm1._cube_data[key] = current.loc[~mask].reset_index(drop=True)

    def create_cellset(self, mdx: str, **kwargs: Any) -> str:
        self._record("create_cellset", mdx, **kwargs)
        self._tm1._resolve_query(self._tm1._mdx_results, mdx, "MDX")
        return f"mock-cellset-{len(self._tm1.calls)}"


class MockObjectService(_BaseService):
    def __init__(self, tm1: "MockTM1Service", object_type: str):
        super().__init__(tm1)
        self._object_type = object_type

    def create(self, **kwargs: Any) -> None:
        self._record("create", **kwargs)
        obj = next(iter(kwargs.values()))
        self._tm1._temporary_objects[self._object_type].add(_name_key(obj.name))

    def delete(self, **kwargs: Any) -> None:
        self._record("delete", **kwargs)
        name = kwargs.get("view_name") or kwargs.get("subset_name")
        self._tm1._temporary_objects[self._object_type].discard(_name_key(name))


class MockRestService(_BaseService):
    def POST(self, url: str, data: Any = None, **kwargs: Any) -> Any:
        self._record("POST", url, data, **kwargs)
        handler = self._tm1.rest_post_handlers.get(url)
        if handler is None:
            raise NotImplementedError(f"No mock REST POST handler registered for '{url}'.")
        return handler(data=data, **kwargs)


class MockTM1Service:
    """Stateful, TM1py-shaped test double for wrapper and integration tests."""

    def __init__(self, server_name: str = "mock-tm1"):
        self.server_name = server_name
        self.calls: list[TM1Call] = []
        self.hierarchy_sort_orders: dict[tuple[str, str], tuple[int, int, int, int]] = {}
        self.rest_post_handlers: dict[str, Callable[..., Any]] = {}
        self._dimensions: dict[str, Dimension] = {}
        self._cubes: dict[str, Cube] = {}
        self._cube_data: dict[str, pd.DataFrame] = {}
        self._mdx_results: dict[str, Union[pd.DataFrame, Exception]] = {}
        self._set_mdx_results: dict[str, Union[list, Exception]] = {}
        self._view_results: dict[tuple[str, str], pd.DataFrame] = {}
        self._temporary_objects = {"views": set(), "subsets": set()}
        self._lock = RLock()
        self.connected = True

        self.server = MockServerService(self)
        self.cubes = MockCubeService(self)
        self.dimensions = MockDimensionService(self)
        self.hierarchies = MockHierarchyService(self)
        self.elements = MockElementService(self)
        self.cells = MockCellService(self)
        self.views = MockObjectService(self, "views")
        self.subsets = MockObjectService(self, "subsets")
        self._tm1_rest = MockRestService(self)

    def add_dimension(
        self,
        name: str,
        elements: Union[dict[str, Union[str, Element.Types]], Iterable[Element]],
        hierarchy_name: Optional[str] = None,
        edges: Optional[dict[tuple[str, str], float]] = None,
    ) -> Dimension:
        hierarchy_name = hierarchy_name or name
        element_objects = (
            [Element(element_name, element_type) for element_name, element_type in elements.items()]
            if isinstance(elements, dict)
            else list(elements)
        )
        dimension = Dimension(
            name,
            [Hierarchy(hierarchy_name, name, elements=element_objects, edges=edges)],
        )
        self.dimensions.update_or_create(dimension)
        return dimension

    def add_cube(
        self,
        name: str,
        dimensions: Iterable[str],
        data: Optional[pd.DataFrame] = None,
    ) -> Cube:
        cube = Cube(name, list(dimensions))
        self.cubes.create(cube)
        if data is not None:
            self._cube_data[_name_key(name)] = data.copy(deep=True)
        return cube

    def register_mdx(
        self, mdx: str, result: Union[pd.DataFrame, Exception]
    ) -> None:
        self._mdx_results[_query_key(mdx)] = result

    def register_set_mdx(
        self, mdx: str, elements: Union[Iterable[str], Exception]
    ) -> None:
        self._set_mdx_results[_query_key(mdx)] = (
            elements
            if isinstance(elements, Exception)
            else [[{"Name": element_name}] for element_name in elements]
        )

    def register_view(
        self, cube_name: str, view_name: str, result: pd.DataFrame
    ) -> None:
        self._view_results[(_name_key(cube_name), _name_key(view_name))] = result.copy(deep=True)

    def cube_data(self, cube_name: str) -> pd.DataFrame:
        return self._cube_data.get(_name_key(cube_name), pd.DataFrame()).copy(deep=True)

    def re_connect(self) -> None:
        self.calls.append(TM1Call("MockTM1Service", "re_connect", (), {}))
        self.connected = True

    def logout(self) -> None:
        self.calls.append(TM1Call("MockTM1Service", "logout", (), {}))
        self.connected = False

    def __enter__(self) -> "MockTM1Service":
        return self

    def __exit__(self, *_args: Any) -> None:
        self.logout()

    def _get_cube(self, cube_name: str) -> Cube:
        try:
            return self._cubes[_name_key(cube_name)]
        except KeyError as exc:
            raise KeyError(f"Cube '{cube_name}' does not exist in MockTM1Service.") from exc

    def _get_dimension(self, dimension_name: str) -> Dimension:
        try:
            return self._dimensions[_name_key(dimension_name)]
        except KeyError as exc:
            raise KeyError(
                f"Dimension '{dimension_name}' does not exist in MockTM1Service."
            ) from exc

    def _get_hierarchy(self, dimension_name: str, hierarchy_name: str) -> Hierarchy:
        dimension = self._get_dimension(dimension_name)
        for hierarchy in dimension.hierarchies:
            if _name_key(hierarchy.name) == _name_key(hierarchy_name):
                return hierarchy
        raise KeyError(
            f"Hierarchy '{hierarchy_name}' does not exist in dimension '{dimension_name}'."
        )

    @staticmethod
    def _resolve_query(
        registry: dict[str, Any], query: str, query_type: str
    ) -> Any:
        key = _query_key(query)
        if key not in registry:
            raise UnregisteredTM1QueryError(
                f"No {query_type} response registered for: {query}"
            )
        result = registry[key]
        if isinstance(result, Exception):
            raise result
        return result
