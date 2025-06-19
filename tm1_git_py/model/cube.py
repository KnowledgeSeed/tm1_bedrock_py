import json
from typing import List

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
    def __init__(self, name, dimensions : List[Dimension], rule, views : List[MDXView]):
        self.type = 'Cube'
        self.name = name
        self.dimensions = dimensions
        self.rule = rule
        self.views = views

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "Dimensions": [{"@id" : format_url("Dimensions('{}')", d.name)} for d in self.dimensions],
            "Rules@Code.link": format_url("{}.rules", self.name),
            "Views@Code.links" : [format_url("{}.views/{}.json", self.name, v.name) for v in self.views],
        }, indent='\t')
    
    def asCubeLink(self):
        # /cubes/Cube_A.json
        return '/cubes/' + self.name + '.json'
    
    def asRuleLink(self):
        # /cubes/Cube_A.rules
        return '/cubes/' + self.name + '.rules'