import re
from typing import List, Dict, Any, TypeVar, Optional

from requests import Response

from model.cube import Cube, create_cube, update_cube, delete_cube
from model.dimension import Dimension, create_dimension, update_dimension, delete_dimension
from model.hierarchy import Hierarchy, create_hierarchy, update_hierarchy, delete_hierarchy
from model.subset import Subset, create_subset, update_subset, delete_subset
from model.process import Process, create_process, update_process, delete_process
from model.chore import Chore, create_chore, update_chore, delete_chore
from TM1py import TM1Service

from tm1_git_py.model.model import Model
from model.task import Task

T = TypeVar('T', Cube, Dimension, Process, Chore)

class Changeset:

    model: Model

    def __init__(self):

        self.added: List[T] = []
        self.modified: List[Dict[T, Any]] = []
        self.removed: List[T] = []

        self.changes: List[str] = []

    @property
    def all_removed(self) -> List[str]:
        return self.removed

    def has_changes(self) -> bool:
        return any([self.added, self.modified, self.removed])


    def __repr__(self):
        self.changes = []

        if self.added:
            self.changes += [f"C  /{c.source_path}" for c in self.added]
        if self.removed:
            self.changes += [f"D  /{c.source_path}" for c in self.removed]

        if self.modified:
            for m in self.modified:
                new_obj, old_obj = m['new'], m['old']

                if isinstance(new_obj, Chore):
                    old_tasks_set = set(old_obj.tasks)
                    new_tasks_set = set(new_obj.tasks)
                    
                    if old_tasks_set != new_tasks_set:
                        added_tasks = new_tasks_set - old_tasks_set
                        removed_tasks = old_tasks_set - new_tasks_set

                        for task in added_tasks:
                            idx = new_obj.tasks.index(task)
                            self.changes.append(f"C  /{new_obj.source_path}|{task.process_name}|{idx}")
                        for task in removed_tasks:
                            idx = old_obj.tasks.index(task)
                            self.changes.append(f"D  /{old_obj.source_path}|{task.process_name}|{idx}")

                    if new_obj.name != old_obj.name or new_obj.start_time != old_obj.start_time or \
                       new_obj.active != old_obj.active or new_obj.execution_mode != old_obj.execution_mode:
                        self.changes.append(f"U  /{new_obj.source_path}")
                else:
                    self.changes.append(f"U  /{new_obj.source_path}")

        if not self.changes:
            return "No changes"
        else:
            self.sort()
        return "Changeset:\n" + "\n".join(self.changes)

    def apply(self, tm1_service: TM1Service) -> List[Any]:
        changes = []
        self.validate()

        if self.has_changes():
            if self.added:
                changes += [create_object(tm1_service=tm1_service, object_instance=a).url for a in self.added]

            if self.modified:
                changes += [update_object(tm1_service=tm1_service, object_instance=m) for m in self.modified]

            if self.removed:
                changes += [delete_object(tm1_service=tm1_service, object_instance=d).url for d in self.removed]

        return changes


    """
    def validate(self):
        # TODO: execute on has_changes()
        if self.has_changes():
            pass
        else:
            for changes in self.changes:
                flag = re.search(r'\A([UDC]\s{2}/)', changes).group(1)
                source_path = changes.split(flag)[1]
                print(source_path)
    """


    def sort(self):
        flag_precedence = {'C': 0, 'U': 1, 'D': 2}
        object_precedence = {'dimensions': 0, 'hierarchies': 1, 'subsets': 2, 'cubes': 3, 'process': 4, 'chore': 5}

        def __sort_changes(s: str):
            changes_precedence = {'dimensions': 0, 'hierarchies': 1, 'subsets': 2, 'cubes': 3, 'process': 4, 'chore': 5}

            flag = re.search(r'\A([UDC])', s).group(1)
            obj_name = re.search(r'/\b(\w*)/', s).group(1)

            if 'subsets' in s:
                obj_name = 'subsets'
            elif 'hierarchies' in s:
                obj_name = 'hierarchies'

            source_path = s.split(obj_name)[1]

            if flag == 'D':
                changes_precedence = {'cubes': 0, 'subsets': 1, 'hierarchies': 2, 'dimensions': 3, 'chore': 4, 'process': 5}

            key = (
                flag_precedence.get(flag, 99),
                changes_precedence.get(obj_name, 99),
                source_path
            )

            return key

        def __sort_on_source_path(s: T | Dict[T, Any]):

            if isinstance(s, (Cube, Dimension, Hierarchy, Subset, Chore, Process)):
                s = s.source_path
            else:
                s = s["new"].source_path

            obj_name = re.search(r'\A(\w*)/', s).group(1)

            if 'subsets' in s:
                obj_name = 'subsets'
            elif 'hierarchies' in s:
                obj_name = 'hierarchies'

            source_path = s.split(obj_name)[1]

            key = (
                object_precedence.get(obj_name, 99),
                source_path
            )

            return key

        if self.has_changes():
            self.changes.sort(key=__sort_changes)
            self.added.sort(key=__sort_on_source_path)
            self.modified.sort(key=__sort_on_source_path)
            if self.removed:
                object_precedence = {'cubes': 0, 'subsets': 1, 'hierarchies': 2, 'dimensions': 3, 'chore': 4, 'process': 5}
                self.removed.sort(key=__sort_on_source_path)


def create_object(tm1_service: TM1Service, object_instance: T) -> Response:
    if type(object_instance) is Dimension:
        return create_dimension(tm1_service=tm1_service, dimension=object_instance)

    elif type(object_instance) is Hierarchy:
        return create_hierarchy(tm1_service=tm1_service, hierarchy=object_instance)

    elif type(object_instance) is Subset:
        return create_subset(tm1_service=tm1_service, subset=object_instance)

    elif type(object_instance) is Cube:
        return create_cube(tm1_service=tm1_service, cube=object_instance)

    elif type(object_instance) is Process:
        return create_process(tm1_service=tm1_service, process=object_instance)

    elif type(object_instance) is Chore:
        return create_chore(tm1_service=tm1_service, chore=object_instance)

    else: raise ValueError


def delete_object(tm1_service: TM1Service, object_instance: T) -> Response:
    if type(object_instance) is Cube:
        return delete_cube(tm1_service=tm1_service, cube_name=object_instance.name)

    elif type(object_instance) is Subset:
        return delete_subset(tm1_service=tm1_service, subset=object_instance)

    elif type(object_instance) is Hierarchy:
        return delete_hierarchy(tm1_service=tm1_service, hierarchy=object_instance)

    elif type(object_instance) is Dimension:
        return delete_dimension(tm1_service=tm1_service, dimension_name=object_instance.name)

    elif type(object_instance) is Chore:
        return delete_chore(tm1_service=tm1_service, chore=object_instance.name)

    elif type(object_instance) is Process:
        return delete_process(tm1_service=tm1_service, process=object_instance.name)

    else:
        raise ValueError


def update_object(tm1_service: TM1Service, object_instance: Dict[T, Any]) -> Response:
    if type(object_instance['new']) is Dimension:
        return update_dimension(tm1_service=tm1_service, dimension=object_instance)

    elif type(object_instance['new']) is Hierarchy:
        return update_hierarchy(tm1_service=tm1_service, hierarchy=object_instance)

    elif type(object_instance['new']) is Subset:
        return update_subset(tm1_service=tm1_service, subset=object_instance)

    elif type(object_instance['new']) is Cube:
        return update_cube(tm1_service=tm1_service, cube=object_instance)

    elif type(object_instance['new']) is Process:
        return update_process(tm1_service=tm1_service, process=object_instance)

    elif type(object_instance['new']) is Chore:
        return update_chore(tm1_service=tm1_service, chore=object_instance)

    else:
        raise ValueError

def export_changeset(path:str, changeset: Changeset ):
    with open(path, "w") as out:  
            out.writelines(changeset.__repr__()) 