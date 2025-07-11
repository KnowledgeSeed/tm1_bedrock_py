import json
from typing import List, Any, Dict

from model.edge import Edge
from model.element import Element
from model.subset import Subset
from TM1py.Utils import format_url
from TM1py import TM1Service, Hierarchy
from requests import Response

# {
# 	"@type": "Hierarchy",
# 	"Name": "Capex Balance Sheet Assignment Measure",
# 	"Elements": [
# 		{
# 			"Name": "Assignment",
# 			"Type": "Numeric"
# 		},
# 		{
# 			"Name": "Comment",
# 			"Type": "String"
# 		},
# 		{
# 			"Name": "CapexName",
# 			"Type": "String"
# 		},
# 		{
# 			"Name": "BalanceSheetName",
# 			"Type": "String"
# 		},
# 		{
# 			"Name": "Value",
# 			"Type": "Numeric"
# 		}
# 	],
# 	"Subsets@Code.links": []
# }


class Hierarchy:
    def __init__(self, name, elements: List[Element], edges: List[Edge], subsets: List[Subset]):
        self.type = 'Hierarchy'
        self.name = name
        self.elements = elements
        self.edges = edges
        self.subsets = subsets

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "Elements": [obj.__dict__ for obj in self.elements],
            "Edges": [obj.__dict__ for obj in self.edges],
            "Subsets@Code.links": [format_url("{}.subsets/{}.json", self.name, s.name) for s in self.subsets]
        }, indent='\t')

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Hierarchy):
            return NotImplemented
        
        if self.name != other.name:
            return False
        
        if set(self.elements) != set(other.elements):
            return False

        if set(self.edges) != set(other.edges):
            return False
        
        if set(self.subsets) != set(other.subsets):
            return False
            
        return True

    def __hash__(self) -> int:
        return hash((
            self.name,
            frozenset(self.elements),
            frozenset(self.edges),
            frozenset(self.subsets)
        ))

    def to_dict(self):
        return {
            'name': self.name,
            'elements': [e.to_dict() for e in self.elements],
            'edges': [e.to_dict() for e in self.edges],
            'subsets': [s.to_dict() for s in self.subsets]
        }

    def asLink(self, dimension_name):
        # /dimensions/Dimension_A.hierarchies/Dimension_A.json
        return '/dimensions/' + dimension_name + '.hierarchies/' + self.name + '.json'
    
    @staticmethod
    def as_link(dimension_name_base, name):
        # /dimensions/Dimension_A.json
        return '/dimensions/' + dimension_name_base + '.hierarchies/' + name


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_hierarchy(tm1_service: TM1Service, hierarchy: Hierarchy) -> Response:
    hierarchy_object = Hierarchy(
        name=hierarchy.name,
        dimension_name=hierarchy.name,
        elements=hierarchy.elements,
        subsets=hierarchy.subsets
    )

    edges = [{(parent, component), weight} for parent, component, weight in hierarchy.edges]
    hierarchy_object = [hierarchy_object.add_edge(edge[0], edge[1], weight) for edge, weight in edges]
    return tm1_service.hierarchies.create(hierarchy_object)


def update_hierarchy(tm1_service: TM1Service, hierarchy: Dict[str, Any]) -> Response:
    hierarchy_new = hierarchy.get('new')
    hierarchy_object_new = Hierarchy(
        name=hierarchy_new.name,
        dimension_name=hierarchy_new.name,
        elements=hierarchy_new.elements,
        subsets=hierarchy_new.subsets
    )
    edges = [{(parent, component), weight} for parent, component, weight in hierarchy_new.edges]
    hierarchy_object_new = [hierarchy_object_new.add_edge(edge[0], edge[1], weight) for edge, weight in edges]

    if hierarchy.get('new').name == hierarchy.get('old').name:
        return tm1_service.hierarchies.update(hierarchy_object_new)
    else:
        hierarchy_old = hierarchy.get('old')
        hierarchy_object_temp = Hierarchy(
        name=hierarchy_new.name,
        dimension_name=hierarchy_old.name,
        elements=hierarchy_old.elements,
        subsets=hierarchy_old.subsets
    )
        edges = [{(parent, component), weight} for parent, component, weight in hierarchy_old.edges]
        hierarchy_object_temp = [hierarchy_object_temp.add_edge(edge[0], edge[1], weight) for edge, weight in edges]
        response = tm1_service.hierarchies.create(hierarchy_object_temp)

        if response.status_code == 200:
            tm1_service.cubes.delete(hierarchy_old.name)

        return tm1_service.hierarchies.update(hierarchy_object_new)


def delete_hierarchy(tm1_service: TM1Service, dimension: str, hierarchy: Hierarchy) -> Response:
    return tm1_service.hierarchies.delete(dimension, hierarchy.name)
