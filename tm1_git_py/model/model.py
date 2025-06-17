import json
from typing import List

from model.chore import Chore
from model.cube import Cube
from model.dimension import Dimension
from model.process import Process


class Model:
    def __init__(self, cubes: List[Cube], dimensions: List[Dimension], processes: List[Process], chores: List[Chore]):
        self.type = 'Subset'
        self.cubes = cubes
        self.dimensions = dimensions
        self.processes = processes
        self.chores = chores
