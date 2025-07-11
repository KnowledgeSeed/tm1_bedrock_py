import json
from typing import Any, Dict
from TM1py import TM1Service, Element
from requests import Response

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
            #for test
            if not hasattr(self, 'name'):
                self.name = None
            if not hasattr(self, 'type'):
               self.type = None

    def as_json(self):
        return json.dumps({
            "Name": self.name,
            "Type": self.type
        }, indent=4)
    
    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Element):
            return NotImplemented
        return self.name == other.name and \
               self.type == other.type

    def __hash__(self) -> int:
        return hash((self.name, self.type))
    
    def to_dict(self):
        return {
            'name': self.name,
            'type': self.type
        }


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_element(tm1_service: TM1Service, hierarchy: str, dimension: str, element: Element) -> Response:
    element_object = Element(name=element.name, element_type=element.type)
    return tm1_service.elements.create(hierarchy, dimension, element_object)


def update_element(tm1_service: TM1Service, element: Dict[str, Any]) -> Response:
    element_new = element.get('new')
    element_object_new = Element(name=element_new.name, element_type=element_new.type)

    if element.get('new').name == element.get('old').name:
        return tm1_service.elements.update(element_object_new)
    else:
        element_old = element.get('old')
        element_object_temp = Element(name=element_new.name, element_type=element_old.type)
        response = tm1_service.elements.create(element_object_temp)
        if response.status_code == 200:
            tm1_service.elements.delete(element_old.name)

        return tm1_service.elements.update(element_object_new)


def delete_element(tm1_service: TM1Service, hierarchy: str, dimension: str, element: str) -> Response:
    return tm1_service.elements.delete(hierarchy, dimension, element)
