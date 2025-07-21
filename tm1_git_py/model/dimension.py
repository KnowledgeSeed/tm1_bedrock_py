import json
from typing import List, Any, Dict

import TM1py
from TM1py import TM1Service, Dimension
from requests import Response

from model.element import Element
from model.hierarchy import Hierarchy, create_hierarchy, update_hierarchy
from model.subset import Subset
from TM1py.Utils import format_url
from tm1_bench_py.dimension_builder import hierarchy_to_dataframe


# {
# 	"@type":"Dimension",
# 	"Name":"Taxes Measure",
# 	"Hierarchies@Code.links":
# 	[
# 		"Taxes Measure.hierarchies/Taxes Measure.json"
# 	],
# 	"DefaultHierarchy":
# 	{
# 		"@id":"Dimensions('Taxes Measure')/Hierarchies('Taxes Measure')"
# 	}
# }

class Dimension:
    def __init__(self, name, hierarchies: List[Hierarchy], defaultHierarchy: Hierarchy, source_path: str):
        self.type = 'Dimension'
        self.name = name
        self.hierarchies = hierarchies
        self.defaultHierarchy = defaultHierarchy
        self.source_path = source_path

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "Hierarchies@Code.links": [format_url("{}.hierarchies/{}.json", self.name, h) for h in self.hierarchies],
            "DefaultHierarchy": format_url("Dimensions('{}')/Hierarchies('{}')", self.name, self.defaultHierarchy.name)
        }, indent='\t')
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Dimension):
            return NotImplemented
        
        if self.name != other.name:
            return False
        
        if self.defaultHierarchy.name != other.defaultHierarchy.name:
            return False

        if set(self.hierarchies) != set(other.hierarchies):
            return False
            
        return True

    def __hash__(self) -> int:
        return hash((
            self.name,
            self.defaultHierarchy.name,
            frozenset(self.hierarchies)
        ))

    def to_dict(self):
        return {
            'name': self.name,
            'hierarchies': [h.to_dict() for h in self.hierarchies],
            'defaultHierarchy': self.defaultHierarchy.to_dict()
        }
        

    @staticmethod
    def as_link(name):
        # /dimensions/Dimension_A.json
        return '/dimensions/' + name


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_dimension(tm1_service: TM1Service, dimension: Dimension) -> Response:

    dimension_object = TM1py.Dimension(dimension.name)
    response = tm1_service.dimensions.create(dimension_object)
    """    
    if response.status_code == 201:
        hierarchies = []
        for hierarchy in dimension.hierarchies:
            if not tm1_service.hierarchies.exists(dimension.name, hierarchy.name):
              hierarchies.append(create_hierarchy(tm1_service, hierarchy))
    """
    return response


def update_dimension(tm1_service: TM1Service, dimension: Dict[str, Any]) -> Response:
    dimension_new = dimension.get('new')
    dimension_old = dimension.get('old')

    hierarchies_new = dimension_new.hierarchies
    hierarchies_old = dimension_old.hierarchies

    if tm1_service.dimensions.exists(dimension_name=dimension_new.name):
        dimension_object = tm1_service.dimensions.get(dimension_name=dimension_new.name)

        if hierarchies_new != hierarchies_old:
            hierarchies_to_remove = list(set(hierarchies_old) - set(hierarchies_new))
            hierarchies_to_add = list(set(hierarchies_new) - set(hierarchies_old))

            for hierarchy in hierarchies_to_remove:
                dimension_object.remove_hierarchy(hierarchy_name=hierarchy.name)
            for hierarchy in hierarchies_to_add:
                hierarchy_object = tm1_service.hierarchies.get(dimension_name=dimension_new.name, hierarchy_name=hierarchy.name)
                dimension_object.add_hierarchy(hierarchy_object)

        return tm1_service.dimensions.update(dimension_object)
    else:
        return create_dimension(tm1_service=tm1_service, dimension=dimension_new)



def delete_dimension(tm1_service: TM1Service, dimension_name: str) -> Response:
    return tm1_service.dimensions.delete(dimension_name)
