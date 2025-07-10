from typing import List, Dict, Any, TypeVar

from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from model.chore import Chore

T = TypeVar('T', Cube, Dimension, Process, Chore)

class Changeset:
    def __init__(self):
        self.added_cubes: List[Cube] = []
        self.removed_cubes: List[str] = [] #List[Cube] = []
        self.modified_cubes: List[Dict[str, Any]] = [] #{ 'old': Cube, 'new': Cube, 'changes': Cube}

        self.added_dimensions: List[Dimension] = []
        self.removed_dimensions: List[str] = [] #List[Dimension] = []
        self.modified_dimensions: List[Dict[str, Any]] = []

        self.added_processes: List[Process] = []
        self.removed_processes: List[str] = [] #List[Process] = []
        self.modified_processes: List[Dict[str, Any]] = []

        self.added_chores: List[Chore] = []
        self.removed_chores: List[str] = [] #List[Chore] = []
        self.modified_chores: List[Dict[str, Any]] = []

    @property
    def all_removed(self) -> List[str]:
        return self.removed_cubes + self.removed_dimensions + self.removed_processes + self.removed_chores

    def has_changes(self) -> bool:
        return any([
            self.added_cubes, self.removed_cubes, self.modified_cubes,
            self.added_dimensions, self.removed_dimensions, self.modified_dimensions,
            self.added_processes, self.removed_processes, self.modified_processes,
            self.added_chores, self.removed_chores, self.modified_chores
        ])

    def __repr__(self):
        changes = []

        # if self.added_cubes: changes.append(f"Added Cubes: {[c.as_link for c in self.added_cubes]}")
        if self.added_cubes: changes.extend([f"C " + c.as_link for c in self.added_cubes])
        if self.removed_cubes: changes.extend([f"D " + Cube.as_link(c) for c in self.removed_cubes])
        if self.modified_cubes: changes.extend([f"M " + c.as_link for c in self.modified_cube])

        if self.added_dimensions: changes.extend([f"C " + c.as_link for c in self.added_dimensions])
        if self.removed_dimensions: changes.extend([f"D " + Dimension.as_link(c) for c in self.removed_dimensions])
        if self.modified_dimensions: changes.extend([f"M " + c.as_link for c in self.modified_dimensions])

        if self.added_processes: changes.extend([f"C " + c.as_link for c in self.added_processes])
        if self.removed_processes: changes.extend([f"D " + Process.as_link(c) for c in self.removed_processes])
        if self.modified_processes: changes.extend([f"M " + c.as_link for c in self.modified_processes])

        if self.added_chores: changes.extend([f"C " + c.as_link for c in self.added_chores])
        if self.removed_chores: changes.extend([f"D " + Chore.as_link(c) for c in self.removed_chores])
        if self.modified_chores: changes.extend([f"M " + c.as_link for c in self.modified_chores])

        if not changes:
            return "No changes"
        return "Changeset:\n" + "\n".join(changes)
    
def export_changeset(path:str, changeset: Changeset ):
    with open(path, "w") as out:  
            out.writelines(changeset.__repr__()) 
