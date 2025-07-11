import json
from typing import List, Any, Dict
from TM1py import TM1Service, Cube
from requests import Response

from model.dimension import Dimension
from model.element import Element
from model.hierarchy import Hierarchy
from model.mdxview import MDXView
from model.subset import Subset
from TM1py.Utils import format_url


# {
# 	"@type":"Cube",
# 	"Name":"Channel Csoportos Flat Assignment",
# 	"Dimensions":
# 	[
# 		{
# 			"@id":"Dimensions('Version')"
# 		},
# 		{
# 			"@id":"Dimensions('Period')"
# 		},
# 		{
# 			"@id":"Dimensions('Channel')"
# 		},
# 		{
# 			"@id":"Dimensions('Csoportos Flat')"
# 		},
# 		{
# 			"@id":"Dimensions('Channel Csoportos Flat Assignment Measure')"
# 		}
# 	],
# 	"Views@Code.links":
# 	[
# 		"Channel Csoportos Flat Assignment.views/CsoportosFlatSubsetTechnical.json"
# 	]
# }
class Cube:
    def __init__(self, name, dimensions : List[Dimension], rule, views : List[MDXView], source_path: str):
        self.type = 'Cube'
        self.name = name
        self.dimensions = dimensions
        self.rule = rule
        self.views = views
        self.source_path = source_path

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "Dimensions": [{"@id" : format_url("Dimensions('{}')", d.name)} for d in self.dimensions],
            "Rules@Code.link": format_url("{}.rules", self.name),
            "Views@Code.links" : [format_url("{}.views/{}.json", self.name, v.name) for v in self.views],
        }, indent='\t')
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Cube):
            return NotImplemented
        
        if self.name != other.name:
            return False
        
        self_dim_names = sorted([d.name for d in self.dimensions])
        other_dim_names = sorted([d.name for d in other.dimensions])
        if self_dim_names != other_dim_names:
            return False

        if self.rule != other.rule:
            return False

        if set(self.views) != set(other.views):
            return False
            
        return True

    def __hash__(self) -> int:
        return hash((
            self.name,
            tuple(sorted([d.name for d in self.dimensions])),
            self.rule,
            frozenset(self.views)
        ))
    
    def to_dict(self):
        return {
            'name': self.name,
            'dimensions': [d.to_dict() for d in self.dimensions],
            'rule': self.rule,
            'views': [v.to_dict() for v in self.views]
        }

    @staticmethod
    def as_link(name):
        # /cubes/Cube_A.json
        # /cubes/Cube_A.rules
        return '/cubes/' + name


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_cube(tm1_service: TM1Service, cube: Cube) -> Response:
    cube_object = Cube(cube.name, cube.dimensions, cube.rule)
    return tm1_service.cubes.create(cube_object)


def update_cube(tm1_service: TM1Service, cube: Dict[str, Any]) -> Response:
    cube_new = cube.get('new')
    cube_object_new = Cube(name=cube_new.name, dimensions=cube_new.dimensions, rules=cube_new.rule)

    if cube.get('new').name == cube.get('old').name:
        return tm1_service.cubes.update(cube_object_new)
    else:
        cube_old = cube.get('old')
        cube_object_temp = Cube(name=cube_new.name, dimensions=cube_old.dimensions, rules=cube_old.rule)
        response = tm1_service.cubes.create(cube_object_temp)
        if response.status_code == 200:
            tm1_service.cubes.delete(cube_old.name)

        return tm1_service.cubes.update(cube_object_new)


def delete_cube(tm1_service: TM1Service, cube: str) -> Response:
    return tm1_service.cubes.delete(cube)
