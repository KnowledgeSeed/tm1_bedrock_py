import json
from typing import List

from .chore import Chore
from .cube import Cube
from .dimension import Dimension
from .process import Process
from itertools import chain


class Model:
    def __init__(self, cubes: List[Cube], dimensions: List[Dimension], processes: List[Process], chores: List[Chore]):
        self.type = 'Subset'
        self.cubes = cubes
        self.dimensions = dimensions
        self.processes = processes
        self.chores = chores

    def to_dict(self):
        return {
            'cubes': [c.to_dict() for c in self.cubes],
            'dimensions': [d.to_dict() for d in self.dimensions],
            'processes': [p.to_dict() for p in self.processes],
            'chores': [c.to_dict() for c in self.chores]
        }

    def get_all_objects_with_paths(self) -> dict:
        all_objects = {}
        for item in chain(self.cubes, self.dimensions, self.processes, self.chores):
            if hasattr(item, 'source_path'):
                normalized_path = item.source_path.replace('\\', '/')
                all_objects[normalized_path] = item
        return all_objects