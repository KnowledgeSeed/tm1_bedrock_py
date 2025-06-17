import json

# {
#   "Name": "CapexName",
#   "Type": "String"
# }


class Element:
    def __init__(self, name, type):
        self.name = name
        self.type = type

    def __init__(self, data: dict):
        for key, value in data.items():
            setattr(self, key.lower(), value)

    def as_json(self):
        return json.dumps({
            "Name": self.name,
            "Type": self.type
        }, indent=4)
