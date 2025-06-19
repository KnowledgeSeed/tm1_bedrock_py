import json

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
    def __init__(self, name, start_time, dst_sensitive, active, execution_mode, frequency, tasks):
        self.type = 'Chore'
        self.name = name
        self.start_time = start_time
        self.dst_sensitivity = dst_sensitive
        self.active = active
        self.execution_mode = execution_mode
        self.frequency = frequency
        self.tasks = tasks

    def as_json(self):
        return json.dumps({
            "@type": self.type,
            "Name": self.name,
            "StartTime": self.start_time,
            "DSTSensitive": self.dst_sensitivity,
            "Active": self.active,
            "ExecutionMode": self.execution_mode,
            "Frequency": self.frequency,
            "Tasks": self.tasks,
        }, indent='\t')
    
    @staticmethod
    def as_link(name :str):
        # /chores/chore.json
        return '/chore/' + name