import json

# {
#     "ParentName":"Provider Total",
#     "ComponentName":"ProviderTest",
#     "Weight":1
# }

class Edge:
    def __init__(self, parentName, componentName, weight):
        self.parentName = parentName
        self.componentName = componentName
        self.weight = weight

    def as_json(self):
        return json.dumps({
            "ParentName": self.parentName,
            "ComponentName": self.componentName,
            "Weight": self.weight
        }, indent=4)
