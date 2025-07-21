import json
from typing import Any, Dict

import TM1py
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

def create_element(tm1_service: TM1Service, hierarchy_name: str, dimension_name: str, element: Element) -> Response:
    element_object = TM1py.Element(name=element.name, element_type=element.type)
    return tm1_service.elements.create(hierarchy_name, dimension_name, element_object)


def update_element(tm1_service: TM1Service, hierarchy_name: str, dimension_name: str, element: Element) -> Response:
    if tm1_service.elements.exists(dimension_name=dimension_name, hierarchy_name=hierarchy_name, element_name=element.name):
        element_object = tm1_service.elements.get(dimension_name=dimension_name, hierarchy_name=hierarchy_name, element_name=element.name)
        element_object.element_type = element.type
        return tm1_service.elements.update(element_object)
    else:
        return create_element(tm1_service=tm1_service, hierarchy_name=hierarchy_name, dimension_name=dimension_name, element=element)


def delete_element(tm1_service: TM1Service, hierarchy_name: str, dimension_name: str, element_name: str) -> Response:
    return tm1_service.elements.delete(hierarchy_name, dimension_name, element_name)
