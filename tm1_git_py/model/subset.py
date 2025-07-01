import json
from typing import Any

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