import json
from typing import List, Any, Dict

from model.chore import Chore
from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from itertools import chain


class Model:
    def __init__(self, cubes: List[Cube], dimensions: List[Dimension], processes: List[Process], chores: List[Chore], server_configs: List[Any] = None):
        self.type = 'Subset'
        self.cubes = cubes
        self.dimensions = dimensions
        self.processes = processes
        self.chores = chores
        self.server_configs = server_configs if server_configs is not None else []

    def to_dict(self):
        return {
            'cubes': [c.to_dict() for c in self.cubes],
            'dimensions': [d.to_dict() for d in self.dimensions],
            'processes': [p.to_dict() for p in self.processes],
            'chores': [c.to_dict() for c in self.chores]
        }

    def get_all_objects_with_paths(self) -> Dict[str, Any]:
        all_objects = {}
        normalize = lambda path: path.replace('\\', '/')

        for item in chain(self.processes, self.dimensions, self.cubes):
            if hasattr(item, 'source_path'):
                all_objects[normalize(item.source_path)] = item

        for cube in self.cubes:
             if hasattr(cube, 'source_path'):
                if cube.rule:
                    rule_path = f'cubes/{cube.name}.rules'
                    all_objects[rule_path] = cube.rule
                for view in cube.views:
                    view_path = f'cubes/{cube.name}.views/{view.name}.json'
                    all_objects[view_path] = view
        
        for chore in self.chores:
            if hasattr(chore, 'source_path'):
                chore_path = normalize(chore.source_path)
                all_objects[chore_path] = chore
                for i, task in enumerate(chore.tasks):
                    task_path = f"{chore_path}|{task.process_name}|{i}"
                    all_objects[task_path] = task
                    
        return all_objects