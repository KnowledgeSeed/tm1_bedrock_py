import json
from typing import Any

# {
# 	"@type":"Chore",
# 	"Name":"asas",
# 	"StartTime":"2025-04-22T09:42Z",
# 	"DSTSensitive":true,
# 	"Active":false,
# 	"ExecutionMode":"SingleCommit",
# 	"Frequency":"P0DT01H01M00S",
# 	"Tasks":[]
# }


class Chore:
    def __init__(self, name, start_time, dst_sensitive, active, execution_mode, frequency, tasks, source_path: str):
        self.type = 'Chore'
        self.name = name
        self.start_time = start_time
        #self.dst_sensitivity = dst_sensitive
        self.dst_sensitive = dst_sensitive
        self.active = active
        self.execution_mode = execution_mode
        self.frequency = frequency
        self.tasks = tasks
        self.source_path = source_path

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "StartTime": self.start_time,
            "DSTSensitive": self.dst_sensitive,
            "Active": self.active,
            "ExecutionMode": self.execution_mode,
            "Frequency": self.frequency,
            "Tasks": self.tasks,
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
               self.tasks == other.tasks

    def __hash__(self) -> int:
        return hash((self.name, self.start_time, self.dst_sensitive, self.active,
                     self.execution_mode, self.frequency, json.dumps(self.tasks, sort_keys=True)))
    
    def to_dict(self):
        return {
            'name': self.name,
            'start_time': self.start_time,
            'dst_sensitive': self.dst_sensitive,
            'active': self.active,
            'execution_mode': self.execution_mode,
            'frequency': self.frequency,
            'tasks': self.tasks
        }
    
    @staticmethod
    def as_link(name :str):
        # /chores/chore.json
        return '/chore/' + name