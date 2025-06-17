import json

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
