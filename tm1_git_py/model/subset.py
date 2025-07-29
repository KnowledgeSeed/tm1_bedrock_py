import json
import re
from typing import Any, Dict

import TM1py
from TM1py import TM1Service, Subset
from requests import Response


# {
# 	"@type":"Subset",
# 	"Name":"jhj",
# 	"Expression":"{[Balance Sheet Planning Ledger].[Balance Sheet Planning Ledger].Members}"
# }


class Subset:
    def __init__(self, name, expression, source_path: str):
        self.type = 'Subset'
        self.name = name
        self.expression = expression
        self.source_path = source_path

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
    dimension_name = re.search(r'/(\w*)(.hierarchies)', subset.source_path).group(1)
    hierarchy_name = re.search(r'/(\w*)(.subsets)', subset.source_path).group(1)

    subset_object = TM1py.Subset(
        subset_name=subset.name, dimension_name=dimension_name,
        hierarchy_name=hierarchy_name, expression=subset.expression
    )
    return tm1_service.subsets.create(subset_object)


def update_subset(tm1_service: TM1Service, subset: Dict[str, Any]) -> Response:
    subset_new = subset.get('new')
    dimension_name = re.search(r'/(\w*)(.hierarchies)', subset_new.source_path).group(1)
    hierarchy_name = re.search(r'/(\w*)(.subsets)', subset_new.source_path).group(1)

    if tm1_service.subsets.exists(subset_name=subset_new.name, dimension_name=dimension_name, hierarchy_name=hierarchy_name):
        subset_object = tm1_service.subsets.get(subset_name=subset_new.name, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
        subset_object.expression = subset_new.expression
        return tm1_service.subsets.update(subset_object)
    else:
        return create_subset(tm1_service=tm1_service, subset=subset_new)


def delete_subset(tm1_service: TM1Service, subset: Subset) -> Response:
    dimension_name = re.search(r'/(\w*)(.hierarchies)', subset.source_path).group(1)
    hierarchy_name = re.search(r'/(\w*)(.subsets)', subset.source_path).group(1)
    return tm1_service.subsets.delete(subset_name=subset.name, dimension_name=dimension_name, hierarchy_name=hierarchy_name)
