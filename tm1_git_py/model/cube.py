import json
from collections import Counter
from typing import List, Any, Dict

import TM1py
from TM1py import TM1Service, Cube
from requests import Response
from TM1_bedrock_py.bedrock import data_copy_intercube

from model.dimension import Dimension
from model.element import Element
from model.hierarchy import Hierarchy
from model.mdxview import MDXView
from model.subset import Subset
from TM1py.Utils import format_url
from model.rule import Rule


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
    def __init__(self, name, dimensions : List[Dimension], rules : list[Rule], views : List[MDXView], source_path: str):
        self.type = 'Cube'
        self.name = name
        self.dimensions = dimensions
        self.rules = rules
        self.views = views
        self.source_path = source_path

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "Dimensions": [{"@id": format_url("Dimensions('{}')", d.name)} for d in self.dimensions],
            "Rules@Code.link": format_url("{}.rules", self.name),
            "Views@Code.links": [format_url("{}.views/{}.json", self.name, v.name) for v in self.views],
        }, indent='\t')
    
    def get_rule_text(self) -> str:
        if not self.rules: return ""
        content_parts = []
        for rule in self.rules:
            if rule.comment:
                content_parts.append(rule.comment)
            content_parts.append(rule.full_statement)
        return "\n\n".join(content_parts)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Cube):
            return NotImplemented
        
        if self.name != other.name or \
           sorted([d.name for d in self.dimensions]) != sorted([d.name for d in other.dimensions]) or \
           set(self.views) != set(other.views):
            return False
        if set(self.rules) != set(other.rules):
            return False
        return True

    def __hash__(self) -> int:
        return hash((
            self.name,
            tuple(sorted([d.name for d in self.dimensions])),
            frozenset(self.rules),
            frozenset(self.views)
        ))
    
    def to_dict(self):
        return {
            'name': self.name,
            'dimensions': [d.to_dict() for d in self.dimensions],
            'rules': [r.__dict__ for r in self.rules],
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
    dimensions = [dim.name for dim in cube.dimensions]
    rule_text = cube.get_rule_text()
    cube_object = TM1py.Cube(cube.name, dimensions, cube.rule)
    return tm1_service.cubes.create(cube_object)


def update_cube(tm1_service: TM1Service, cube: Dict[str, Any]) -> Response:
    cube_new = cube.get('new')
    cube_old = cube.get('old')
    dimensions_new = [d.name for d in cube_new.dimensions]
    dimensions_old = [d.name for d in cube_old.dimensions]
    cube_object = tm1_service.cubes.get(cube_new.name)

    if dimensions_new != dimensions_old:
        if Counter(dimensions_new) == Counter(dimensions_old):
            cube_object.update_storage_dimension_order(cube_names=cube_new.name, dimension_names=dimensions_new)
        else:
            # TODO: data_copy_intercube to temp
            delete_cube(tm1_service=tm1_service, cube_name=cube_new.name)
            return create_cube(tm1_service=tm1_service, cube=cube_new)
    new_rule_text = cube_new.get_rule_text()
    if cube_object.rules.body != new_rule_text:
        cube_object.rules = Rule(new_rule_text)


def delete_cube(tm1_service: TM1Service, cube_name: str) -> Response:
    return tm1_service.cubes.delete(cube_name)
