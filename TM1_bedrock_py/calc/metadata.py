from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Mapping, Optional, Protocol, Sequence

from TM1_bedrock_py import utility


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


class TM1ServiceMetadataProvider:
    def __init__(self, tm1_service: Any):
        self._tm1_service = tm1_service

    def get_cube_metadata(self, cube_name: str) -> Optional[CubeMetadata]:
        try:
            raw_metadata = utility.TM1CubeObjectMetadata.collect(
                tm1_service=self._tm1_service,
                cube_name=cube_name,
                collect_measure_types=True,
            )
        except Exception:
            return None

        dimensions = tuple(raw_metadata.get_cube_dims() or ())
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
                        for attribute in self._tm1_service.dimensions.attributes.get_all(dimension_name)
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

        return CubeMetadata(
            cube_name=cube_name,
            dimensions=dimensions,
            measure_dimension_name=measure_dimension_name,
            default_hierarchies=default_hierarchies,
            measure_element_types=measure_element_types,
            dimension_attributes=dimension_attributes,
            dimension_hierarchies=dimension_hierarchies,
            dimension_leaf_elements={},
            dimension_attribute_values={},
            dimension_element_leaf_expansions={},
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
    )
