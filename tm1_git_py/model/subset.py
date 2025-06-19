import json

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

    @staticmethod
    def as_link(dimension_name_base, hierarchy_name_base, name):
        # /dimensions/Dimension_A.hierarchies/Dimension_A.subsets/Subset_A.json
        return '/dimensions/' + dimension_name_base + '.hierarchies/' + hierarchy_name_base + '.subsets/' + name