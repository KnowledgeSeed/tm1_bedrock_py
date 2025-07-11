from typing import List, Dict, Any, TypeVar

from model.cube import Cube, create_cube, update_cube, delete_cube
from model.dimension import Dimension, create_dimension, update_dimension, delete_dimension
from model.process import Process, create_process, update_process, delete_process
from model.chore import Chore, create_chore, update_chore, delete_chore
from TM1py import TM1Service

T = TypeVar('T', Cube, Dimension, Process, Chore)

class Changeset:
    def __init__(self):
        self.added_cubes: List[Cube] = []
        self.removed_cubes: List[Cube] = []
        self.modified_cubes: List[Dict[str, Any]] = [] #{ 'old': Cube, 'new': Cube, 'changes': Cube}

        self.added_dimensions: List[Dimension] = []
        self.removed_dimensions: List[Dimension] = []
        self.modified_dimensions: List[Dict[str, Any]] = []

        self.added_processes: List[Process] = []
        self.removed_processes: List[Process] = []
        self.modified_processes: List[Dict[str, Any]] = []

        self.added_chores: List[Chore] = []
        self.removed_chores: List[Chore] = []
        self.modified_chores: List[Dict[str, Any]] = []

    def has_changes(self) -> bool:
        return any([
            self.added_cubes, self.removed_cubes, self.modified_cubes,
            self.added_dimensions, self.removed_dimensions, self.modified_dimensions,
            self.added_processes, self.removed_processes, self.modified_processes,
            self.added_chores, self.removed_chores, self.modified_chores
        ])

    def __repr__(self):
        changes = []
        if self.added_cubes: changes.append(f"Added Cubes: {[c.name for c in self.added_cubes]}")
        if self.removed_cubes: changes.append(f"Removed Cubes: {[c.name for c in self.removed_cubes]}")
        if self.modified_cubes: changes.append(f"Modified Cubes: {[c['new'].name for c in self.modified_cubes]}")

        if self.added_dimensions: changes.append(f"Added Dimensions: {[d.name for d in self.added_dimensions]}")
        if self.removed_dimensions: changes.append(f"Removed Dimensions: {[d.name for d in self.removed_dimensions]}")
        if self.modified_dimensions: changes.append(f"Modified Dimensions: {[d['new'].name for d in self.modified_dimensions]}")

        if self.added_processes: changes.append(f"Added Processes: {[p.name for p in self.added_processes]}")
        if self.removed_processes: changes.append(f"Removed Processes: {[p.name for p in self.removed_processes]}")
        if self.modified_processes: changes.append(f"Modified Processes: {[p['new'].name for p in self.modified_processes]}")

        if self.added_chores: changes.append(f"Added Chores: {[c.name for c in self.added_chores]}")
        if self.removed_chores: changes.append(f"Removed Chores: {[c.name for c in self.removed_chores]}")
        if self.modified_chores: changes.append(f"Modified Chores: {[c['new'].name for c in self.modified_chores]}")

        if not changes:
            return "No changes"
        return "Changeset:\n" + "\n".join(changes)

    def apply(self, tm1_service: TM1Service) -> List[Any]:
        changes = []

        if self.has_changes():
            if self.added_dimensions: changes.append(
                create_dimension(tm1_service=tm1_service, dimension=d).url for d in self.added_dimensions)
            if self.removed_dimensions: changes.append(
                delete_dimension(tm1_service=tm1_service, dimension=d).url for d in self.removed_dimensions)
            if self.modified_dimensions: changes.append(
                update_dimension(tm1_service=tm1_service, dimension=d).url for d in self.modified_dimensions)

            if self.added_cubes: changes.append(
                create_cube(tm1_service=tm1_service, cube=c).url for c in self.added_cubes)
            if self.removed_cubes: changes.append(
                delete_cube(tm1_service=tm1_service, cube=c).url for c in self.removed_cubes)
            if self.modified_cubes: changes.append(
                update_cube(tm1_service=tm1_service, cube=c).url for c in self.modified_cubes)

            if self.added_processes: changes.append(
                create_process(tm1_service=tm1_service, process=p).url for p in self.added_processes)
            if self.removed_processes: changes.append(
                delete_process(tm1_service=tm1_service, process=p).url for p in self.removed_processes)
            if self.modified_processes: changes.append(
                update_process(tm1_service=tm1_service, process=p).url for p in self.modified_processes)

            if self.added_chores: changes.append(
                create_chore(tm1_service=tm1_service, chore=c).url for c in self.added_chores)
            if self.removed_chores: changes.append(
                delete_chore(tm1_service=tm1_service, chore=c).url for c in self.removed_chores)
            if self.modified_chores: changes.append(
                update_chore(tm1_service=tm1_service, chore=c).url for c in self.modified_chores)

        return changes

    def sort(self):
        """sort the dependency graph to apply changes in correct order"""