import json
from typing import List, Dict, Any, Optional
from model.process import Process

class Task:
    def __init__(self, process_name: str, parameters: List[Dict[str, Any]]):
        self.process_name = process_name
        self.parameters = parameters
        self.process: Optional[Process] = None

    def link_process(self, processes: List[Process]):
        for p in processes:
            if p.name == self.process_name:
                self.process = p
                break

    def as_json_dict(self) -> Dict[str, Any]:
        return {
            "Process@odata.bind": f"Processes('{self.process_name}')",
            "Parameters": self.parameters
        }

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, Task):
            return NotImplemented
        return self.process_name == other.process_name and self.parameters == other.parameters

    def __hash__(self) -> int:
        return hash((self.process_name, json.dumps(self.parameters, sort_keys=True)))

    def to_dict(self) -> Dict[str, Any]:
        return {
            'process_name': self.process_name,
            'parameters': self.parameters,
            'process': self.process.to_dict() if self.process else None
        }