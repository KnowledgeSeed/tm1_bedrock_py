import json
from typing import List, Any, Dict
from TM1py import TM1Service, Dimension
from requests import Response

from model.element import Element
from model.hierarchy import Hierarchy
from model.subset import Subset
from TM1py.Utils import format_url

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
    def __init__(self, name, hierarchies: List[Hierarchy], defaultHierarchy: Hierarchy):
        self.type = 'Dimension'
        self.name = name
        self.hierarchies = hierarchies
        self.defaultHierarchy = defaultHierarchy

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
    dimension_object = Dimension(dimension.name, dimension.hierarchies)
    return tm1_service.dimensions.create(dimension_object)


def update_dimension(tm1_service: TM1Service, dimension: Dict[str, Any]) -> Response:
    dimension_new = dimension.get('new')
    dimension_object_new = Dimension(name=dimension_new.name, hierarchies=dimension_new.hierarchies)

    if dimension.get('new').name == dimension.get('old').name:
        return tm1_service.dimensions.update(dimension_object_new)
    else:
        dimension_old = dimension.get('old')
        dimension_object_temp = Dimension(name=dimension_new.name, hierarchies=dimension_old.hierarchies)
        response = tm1_service.dimensions.create(dimension_object_temp)
        if response.status_code == 200:
            tm1_service.dimensions.delete(dimension_old.name)

        return tm1_service.dimensions.update(dimension_object_new)


def delete_dimension(tm1_service: TM1Service, dimension: Dimension) -> Response:
    return tm1_service.dimensions.delete(dimension.name)
