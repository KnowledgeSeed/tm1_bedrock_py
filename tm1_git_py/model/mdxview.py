import json
from typing import Any

# {
# 	"@type":"MDXView",
# 	"Name":"CsoportosFlatSubsetTechnical",
# 	"MDX@Code.link":"CsoportosFlatSubsetTechnical.mdx"
# }


class MDXView:
    def __init__(self, name, mdx):
        self.type = 'MDXView'
        self.name = name
        self.mdx = mdx

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "MDX@Code.link": self.name + '.mdx'
        }, indent='\t')

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, MDXView):
            return NotImplemented
        return self.name == other.name and \
               self.mdx == other.mdx

    def __hash__(self) -> int:
        return hash((self.name, self.mdx))
    
    def to_dict(self):
        return {
            'name': self.name,
            'mdx': self.mdx
        }