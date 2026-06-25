from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence

from TM1_bedrock_py import basic_logger, utility


@dataclass(frozen=True)
class CubeMetadata:
    cube_name: str
    dimensions: tuple[str, ...]
    measure_dimension_name: Optional[str] = None
    default_hierarchies: Mapping[str, str] = field(default_factory=dict)
    measure_element_types: Mapping[str, int] = field(default_factory=dict)
    dimension_attributes: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    dimension_hierarchies: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    dimension_leaf_elements: Mapping[str, tuple[str, ...]] = field(default_factory=dict)
    dimension_attribute_values: Mapping[str, Mapping[str, Mapping[str, Any]]] = field(default_factory=dict)
    dimension_element_leaf_expansions: Mapping[str, Mapping[str, tuple[str, ...]]] = field(default_factory=dict)
    dimension_element_types: Mapping[str, Mapping[str, str]] = field(default_factory=dict)
    dimension_children: Mapping[str, Mapping[str, tuple[str, ...]]] = field(default_factory=dict)
    dimension_children_by_hierarchy: Mapping[str, Mapping[str, Mapping[str, tuple[str, ...]]]] = field(
        default_factory=dict
    )


class MetadataProvider(Protocol):
    def get_cube_metadata(self, cube_name: str) -> Optional[CubeMetadata]:
        ...


class StaticMetadataProvider:
    def __init__(self, cubes: Mapping[str, CubeMetadata]):
        self._cubes = {
            self._normalize_cube_name(cube_name): metadata
            for cube_name, metadata in cubes.items()
        }

    def get_cube_metadata(self, cube_name: str) -> Optional[CubeMetadata]:
        return self._cubes.get(self._normalize_cube_name(cube_name))

    @staticmethod
    def _normalize_cube_name(cube_name: str) -> str:
        return str(cube_name).strip().casefold()


_ELEMENT_TYPE_NAMES_BY_CODE = {1: "Numeric", 2: "String", 3: "Consolidated"}


def _normalize_element_type(raw_type: Any) -> Optional[str]:
    if raw_type is None:
        return None
    if isinstance(raw_type, str):
        text = raw_type.strip()
        return text if text else None
    return _ELEMENT_TYPE_NAMES_BY_CODE.get(raw_type)


class TM1ServiceMetadataProvider:
    def __init__(self, tm1_service: Any):
        self._tm1_service = tm1_service
        self._cache: dict[str, Optional["CubeMetadata"]] = {}

    def clear_cache(self) -> None:
        self._cache.clear()

    def _collect_dimension_element_types(self, dimension_name: str, hierarchy_name: str) -> Mapping[str, str]:
        try:
            raw_types = self._tm1_service.elements.get_element_types(
                dimension_name=dimension_name, hierarchy_name=hierarchy_name
            )
        except Exception:
            return {}
        return {
            element_name: normalized_type
            for element_name, raw_type in dict(raw_types).items()
            for normalized_type in (_normalize_element_type(raw_type),)
            if normalized_type is not None
        }

    def _collect_dimension_leaf_elements(self, dimension_name: str, hierarchy_name: str) -> tuple[str, ...]:
        try:
            leaf_names = self._tm1_service.elements.get_leaf_element_names(
                dimension_name=dimension_name, hierarchy_name=hierarchy_name
            )
        except Exception:
            return ()
        return tuple(leaf_names)

    def _collect_dimension_children(self, dimension_name: str, hierarchy_name: str) -> Mapping[str, tuple[str, ...]]:
        try:
            edges = self._tm1_service.elements.get_edges(
                dimension_name=dimension_name, hierarchy_name=hierarchy_name
            )
        except Exception:
            return {}
        children_by_parent: dict[str, list[str]] = {}
        for parent_name, child_name in dict(edges).keys():
            children_by_parent.setdefault(parent_name, []).append(child_name)
        return {
            parent_name: tuple(child_names)
            for parent_name, child_names in children_by_parent.items()
        }

    def _collect_dimension_attribute_values(
        self,
        dimension_name: str,
        hierarchy_name: str,
        attribute_names: Sequence[str],
    ) -> Mapping[str, Mapping[str, Any]]:
        values_by_element: dict[str, dict[str, Any]] = {}
        for attribute_name in attribute_names:
            try:
                attribute_values = self._tm1_service.elements.get_attribute_of_elements(
                    dimension_name=dimension_name,
                    hierarchy_name=hierarchy_name,
                    attribute=attribute_name,
                    elements=None,
                    exclude_empty_cells=False,
                )
            except Exception:
                continue
            for element_name, value in dict(attribute_values).items():
                values_by_element.setdefault(element_name, {})[attribute_name] = value
        return values_by_element

    def get_cube_metadata(self, cube_name: str) -> Optional[CubeMetadata]:
        cache_key = str(cube_name).strip().casefold()
        if cache_key in self._cache:
            basic_logger.debug(f"Cube metadata cache hit for '{cube_name}'.")
            return self._cache[cache_key]
        basic_logger.info(f"Cube metadata cache miss for '{cube_name}', collecting from TM1.")
        result = self._collect_cube_metadata(cube_name)
        self._cache[cache_key] = result
        return result

    def _collect_cube_metadata(self, cube_name: str) -> Optional[CubeMetadata]:
        try:
            raw_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=self._tm1_service,
                cube_name=cube_name,
                collect_measure_types=True,
            )
        except Exception as error:
            basic_logger.warning(f"Failed to collect cube metadata for '{cube_name}': {error}")
            return None

        dimensions = tuple(raw_metadata.get_cube_dims() or ())
        basic_logger.debug(f"Cube '{cube_name}' has {len(dimensions)} dimension(s): {dimensions}")
        measure_dimension_name = dimensions[-1] if dimensions else None
        default_hierarchies = {
            dimension_name: utility.get_default_hierarchy(self._tm1_service, dimension_name)
            for dimension_name in dimensions
        }
        dimension_attributes = {
            dimension_name: tuple(
                sorted(
                    {
                        attribute.name
                        for attribute in self._tm1_service.elements.get_element_attributes(
                            dimension_name=dimension_name,
                            hierarchy_name=default_hierarchies.get(dimension_name) or dimension_name,
                        )
                    }
                )
            )
            for dimension_name in dimensions
        }
        dimension_hierarchies = {
            dimension_name: tuple(
                sorted(self._tm1_service.hierarchies.get_all_names(dimension_name))
            )
            for dimension_name in dimensions
        }
        measure_element_types = dict(raw_metadata.get_measure_element_types() or {})

        dimension_leaf_elements = {}
        dimension_element_types = {}
        dimension_children = {}
        dimension_children_by_hierarchy = {}
        dimension_attribute_values = {}
        for dimension_name in dimensions:
            basic_logger.debug(f"Cube '{cube_name}': collecting dimension metadata for '{dimension_name}'.")
            hierarchy_names = tuple(dimension_hierarchies.get(dimension_name, ()))
            hierarchy_name = default_hierarchies.get(dimension_name) or dimension_name
            leaf_elements = self._collect_dimension_leaf_elements(dimension_name, hierarchy_name)
            if leaf_elements:
                dimension_leaf_elements[dimension_name] = leaf_elements
            element_types = self._collect_dimension_element_types(dimension_name, hierarchy_name)
            if element_types:
                dimension_element_types[dimension_name] = element_types
            hierarchy_children = {}
            for current_hierarchy_name in hierarchy_names or (hierarchy_name,):
                children = self._collect_dimension_children(dimension_name, current_hierarchy_name)
                if children:
                    hierarchy_children[current_hierarchy_name] = children
            if hierarchy_children:
                dimension_children_by_hierarchy[dimension_name] = hierarchy_children
            if hierarchy_name in hierarchy_children:
                dimension_children[dimension_name] = hierarchy_children[hierarchy_name]
            attribute_values = self._collect_dimension_attribute_values(
                dimension_name=dimension_name,
                hierarchy_name=hierarchy_name,
                attribute_names=dimension_attributes.get(dimension_name, ()),
            )
            if attribute_values:
                dimension_attribute_values[dimension_name] = attribute_values

        basic_logger.info(f"Cube metadata collection for '{cube_name}' complete ({len(dimensions)} dimension(s)).")
        return CubeMetadata(
            cube_name=cube_name,
            dimensions=dimensions,
            measure_dimension_name=measure_dimension_name,
            default_hierarchies=default_hierarchies,
            measure_element_types=measure_element_types,
            dimension_attributes=dimension_attributes,
            dimension_hierarchies=dimension_hierarchies,
            dimension_leaf_elements=dimension_leaf_elements,
            dimension_attribute_values=dimension_attribute_values,
            dimension_element_leaf_expansions={},
            dimension_element_types=dimension_element_types,
            dimension_children=dimension_children,
            dimension_children_by_hierarchy=dimension_children_by_hierarchy,
        )


def build_static_cube_metadata(
    cube_name: str,
    dimensions: Sequence[str],
    *,
    measure_dimension_name: Optional[str] = None,
    default_hierarchies: Optional[Mapping[str, str]] = None,
    measure_element_types: Optional[Mapping[str, int]] = None,
    dimension_attributes: Optional[Mapping[str, Sequence[str]]] = None,
    dimension_hierarchies: Optional[Mapping[str, Sequence[str]]] = None,
    dimension_leaf_elements: Optional[Mapping[str, Sequence[str]]] = None,
    dimension_attribute_values: Optional[Mapping[str, Mapping[str, Mapping[str, Any]]]] = None,
    dimension_element_leaf_expansions: Optional[Mapping[str, Mapping[str, Sequence[str]]]] = None,
    dimension_element_types: Optional[Mapping[str, Mapping[str, str]]] = None,
    dimension_children: Optional[Mapping[str, Mapping[str, Sequence[str]]]] = None,
    dimension_children_by_hierarchy: Optional[
        Mapping[str, Mapping[str, Mapping[str, Sequence[str]]]]
    ] = None,
) -> CubeMetadata:
    return CubeMetadata(
        cube_name=cube_name,
        dimensions=tuple(dimensions),
        measure_dimension_name=measure_dimension_name or (dimensions[-1] if dimensions else None),
        default_hierarchies=dict(default_hierarchies or {}),
        measure_element_types=dict(measure_element_types or {}),
        dimension_attributes={
            dimension_name: tuple(attribute_names)
            for dimension_name, attribute_names in (dimension_attributes or {}).items()
        },
        dimension_hierarchies={
            dimension_name: tuple(hierarchy_names)
            for dimension_name, hierarchy_names in (dimension_hierarchies or {}).items()
        },
        dimension_leaf_elements={
            dimension_name: tuple(element_names)
            for dimension_name, element_names in (dimension_leaf_elements or {}).items()
        },
        dimension_attribute_values={
            dimension_name: {
                element_name: dict(attribute_values)
                for element_name, attribute_values in element_mapping.items()
            }
            for dimension_name, element_mapping in (dimension_attribute_values or {}).items()
        },
        dimension_element_leaf_expansions={
            dimension_name: {
                element_name: tuple(leaf_elements)
                for element_name, leaf_elements in element_mapping.items()
            }
            for dimension_name, element_mapping in (dimension_element_leaf_expansions or {}).items()
        },
        dimension_element_types={
            dimension_name: dict(element_types)
            for dimension_name, element_types in (dimension_element_types or {}).items()
        },
        dimension_children={
            dimension_name: {
                parent_name: tuple(child_names)
                for parent_name, child_names in children_mapping.items()
            }
            for dimension_name, children_mapping in (dimension_children or {}).items()
        },
        dimension_children_by_hierarchy={
            dimension_name: {
                hierarchy_name: {
                    parent_name: tuple(child_names)
                    for parent_name, child_names in children_mapping.items()
                }
                for hierarchy_name, children_mapping in hierarchy_mapping.items()
            }
            for dimension_name, hierarchy_mapping in (dimension_children_by_hierarchy or {}).items()
        },
    )
