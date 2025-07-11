import json
from typing import Any, Dict
from TM1py import TM1Service, Subset
from requests import Response
from TM1_bedrock_py.utility import __get_dimensions_from_set_mdx_list


# {
# 	"@type":"Subset",
# 	"Name":"jhj",
# 	"Expression":"{[Balance Sheet Planning Ledger].[Balance Sheet Planning Ledger].Members}"
# }


class Subset:
    def __init__(self, name, expression):
        self.type = 'Subset'
        self.name = name
        self.expression = expression

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "Expression": self.expression
        }, indent='\t')
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Subset):
            return NotImplemented
        return self.name == other.name and \
               self.expression == other.expression

    def __hash__(self) -> int:
        return hash((self.name, self.expression))
    
    def to_dict(self):
        return {
            'name': self.name,
            'expression': self.expression
        }

    @staticmethod
    def as_link(dimension_name_base, hierarchy_name_base, name):
        # /dimensions/Dimension_A.hierarchies/Dimension_A.subsets/Subset_A.json
        return '/dimensions/' + dimension_name_base + '.hierarchies/' + hierarchy_name_base + '.subsets/' + name


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_subset(tm1_service: TM1Service, subset: Subset) -> Response:
    dimension = __get_dimensions_from_set_mdx_list(subset.expression)[0]
    subset_object = Subset(subset_name=subset.name, dimension_name=dimension)
    return tm1_service.subsets.create(subset_object)


def update_subset(tm1_service: TM1Service, subset: Dict[str, Any]) -> Response:
    subset_new = subset.get('new')
    subset_object_new = Subset(subset_name=subset_new.name, dimension_name=subset_new.dimensions)

    if subset.get('new').name == subset.get('old').name:
        return tm1_service.subsets.update(subset_object_new)
    else:
        subset_old = subset.get('old')
        subset_object_temp = Subset(subset_name=subset_new.name, dimension_name=subset_old.dimensions)
        response = tm1_service.subsets.create(subset_object_temp)
        if response.status_code == 200:
            tm1_service.subsets.delete(subset_old.name)

        return tm1_service.subsets.update(subset_object_new)


def delete_subset(tm1_service: TM1Service, subset: Subset) -> Response:
    dimension = __get_dimensions_from_set_mdx_list(subset.expression)[0]
    return tm1_service.subsets.delete(subset_name=subset.name, dimension_name=dimension)
