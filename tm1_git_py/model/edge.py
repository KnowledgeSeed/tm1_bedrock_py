import json
from typing import Any

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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Edge):
            return NotImplemented
        return self.parentName == other.parentName and \
               self.componentName == other.componentName and \
               self.weight == other.weight

    def __hash__(self) -> int:
        return hash((self.parentName, self.componentName, self.weight))
    
    def to_dict(self):
        return {
            'parentName': self.parentName,
            'componentName': self.componentName,
            'weight': self.weight
        }
    
    def as_json(self):
        return json.dumps({
            "ParentName": self.parentName,
            "ComponentName": self.componentName,
            "Weight": self.weight
        }, indent=4)
