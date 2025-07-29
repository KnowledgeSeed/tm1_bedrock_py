import json

import TM1py
from TM1py import TM1Service, Chore
from requests import Response
from typing import Any, Dict, List
from model.task import Task

# {
# 	"@type": "Chore",
# 	"Name": "ffff",
# 	"StartTime": "2025-04-22T10:07:00+01:00",
# 	"DSTSensitive": true,
# 	"Active": false,
# 	"ExecutionMode": "SingleCommit",
# 	"Frequency": "P01DT00H00M00S",
# 	"Tasks": [
# 		{
# 			"Process@odata.bind": "Processes('zSYS Analogic Load')",
# 			"Parameters": [
# 				{
# 					"Name": "pDimensionFileName",
# 					"Value": "Product.csv"
# 				},
# 				{
# 					"Name": "pElementsFileName",
# 					"Value": "termek.csv"
# 				},
# 				{
# 					"Name": "pEnableDeleteAll",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateBase",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateMovements",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOpening",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOther",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateSubsets",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTDAttributes",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTransactions",
# 					"Value": 0
# 				}
# 			]
# 		},
# 		{
# 			"Process@odata.bind": "Processes('zSYS Analogic Load Channel Dimension Update')",
# 			"Parameters": [
# 				{
# 					"Name": "pVersion",
# 					"Value": "Base"
# 				}
# 			]
# 		},
# 		{
# 			"Process@odata.bind": "Processes('zSYS Analogic Load')",
# 			"Parameters": [
# 				{
# 					"Name": "pDimensionFileName",
# 					"Value": "Product.csv"
# 				},
# 				{
# 					"Name": "pElementsFileName",
# 					"Value": "termek.csv"
# 				},
# 				{
# 					"Name": "pEnableDeleteAll",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateBase",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateMovements",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOpening",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOther",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateSubsets",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTDAttributes",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTransactions",
# 					"Value": 0
# 				}
# 			]
# 		},
# 		{
# 			"Process@odata.bind": "Processes('zSYS Analogic Load')",
# 			"Parameters": [
# 				{
# 					"Name": "pDimensionFileName",
# 					"Value": "Product.csv"
# 				},
# 				{
# 					"Name": "pElementsFileName",
# 					"Value": "termek.csv"
# 				},
# 				{
# 					"Name": "pEnableDeleteAll",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateBase",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateMovements",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOpening",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOther",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateSubsets",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTDAttributes",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTransactions",
# 					"Value": 0
# 				}
# 			]
# 		},
# 		{
# 			"Process@odata.bind": "Processes('zSYS Analogic Load')",
# 			"Parameters": [
# 				{
# 					"Name": "pDimensionFileName",
# 					"Value": "Product.csv"
# 				},
# 				{
# 					"Name": "pElementsFileName",
# 					"Value": "termek.csv"
# 				},
# 				{
# 					"Name": "pEnableDeleteAll",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateBase",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateMovements",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOpening",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateOther",
# 					"Value": 0
# 				},
# 				{
# 					"Name": "pEnableUpdateSubsets",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTDAttributes",
# 					"Value": 1
# 				},
# 				{
# 					"Name": "pEnableUpdateTransactions",
# 					"Value": 0
# 				}
# 			]
# 		}
# 	]
# }

class Chore:
    def __init__(self, name: str, start_time: str, dst_sensitive: bool, active: bool,
                 execution_mode: str, frequency: str, tasks: List[Task], source_path: str):
        self.type = 'Chore'
        self.name = name
        self.start_time = start_time
        self.dst_sensitive = dst_sensitive
        self.active = active
        self.execution_mode = execution_mode
        self.frequency = frequency
        self.tasks = tasks
        self.source_path = source_path

    def as_json(self) -> str:
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "StartTime": self.start_time,
            "DSTSensitive": self.dst_sensitive,
            "Active": self.active,
            "ExecutionMode": self.execution_mode,
            "Frequency": self.frequency,
            "Tasks": [task.as_json_dict() for task in self.tasks],
        }, indent='\t')

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Chore):
            return NotImplemented
        return self.name == other.name and \
               self.start_time == other.start_time and \
               self.dst_sensitive == other.dst_sensitive and \
               self.active == other.active and \
               self.execution_mode == other.execution_mode and \
               self.frequency == other.frequency and \
               set(self.tasks) == set(other.tasks)

    def __hash__(self) -> int:
        return hash((self.name, self.start_time, self.dst_sensitive, self.active,
                     self.execution_mode, self.frequency, frozenset(self.tasks)))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'start_time': self.start_time,
            'dst_sensitive': self.dst_sensitive,
            'active': self.active,
            'execution_mode': self.execution_mode,
            'frequency': self.frequency,
            'tasks': [task.as_json_dict() for task in self.tasks]
        }

    @staticmethod
    def as_link(name :str):
        # /chores/chore.json
        return '/chore/' + name


# ------------------------------------------------------------------------------------------------------------
# Utility: interface between TM1py and tm1_git_py for CRUD operations
# ------------------------------------------------------------------------------------------------------------

def create_chore(tm1_service: TM1Service, chore: Chore) -> Response:
    chore_object = TM1py.Chore(
        name=chore.name,
        start_time=chore.start_time,
        dst_sensitivity=chore.dst_sensitive,
        active=chore.active,
        execution_mode=chore.execution_mode,
        frequency=chore.frequency,
        tasks=chore.tasks
    )
    return tm1_service.chores.create(chore_object)


def update_chore(tm1_service: TM1Service, chore: Dict[str, Any]) -> Response:
    chore_new = chore.get('new')
    chore_object = tm1_service.chores.get(chore_name=chore_new.name)
    chore_object.start_time = chore_new.start_time
    chore_object.dst_sensitivity = chore_new.dst_sensitive
    chore_object.active = chore_new.active
    chore_object.execution_mode = chore_new.execution_mode
    chore_object.frequency = chore_new.frequency
    chore_object.tasks = chore_new.tasks

    return tm1_service.chores.update(chore_object)


def delete_chore(tm1_service: TM1Service, chore: str) -> Response:
    return tm1_service.chores.delete(chore)
