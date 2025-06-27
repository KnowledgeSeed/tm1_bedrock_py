from typing import List, Dict, Any, Callable

from model.model import Model
from changeset import Changeset

from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from model.chore import Chore

class Comparator:
    def compare(self, model1: Model, model2: Model) -> Changeset:
        changeset = Changeset()

        self._compare_object_lists(model1.cubes, model2.cubes,
                                   changeset.added_cubes, changeset.removed_cubes,
                                   changeset.modified_cubes,
                                   object_type_name="Cube")

        self._compare_object_lists(model1.dimensions, model2.dimensions,
                                   changeset.added_dimensions, changeset.removed_dimensions,
                                   changeset.modified_dimensions,
                                   object_type_name="Dimension")

        self._compare_object_lists(model1.processes, model2.processes,
                                   changeset.added_processes, changeset.removed_processes,
                                   changeset.modified_processes,
                                   object_type_name="Process")

        self._compare_object_lists(model1.chores, model2.chores,
                                   changeset.added_chores, changeset.removed_chores,
                                   changeset.modified_chores,
                                   object_type_name="Chore")
        return changeset

    def _compare_object_lists(self,
                               old_list: List[Any],
                               new_list: List[Any],
                               added_list: List[Any],
                               removed_list: List[Any],
                               modified_list: List[Dict[str, Any]],
                               object_type_name: str):
        old_map = {obj.name: obj for obj in old_list}
        new_map = {obj.name: obj for obj in new_list}

        for name, obj in new_map.items():
            if name not in old_map:
                added_list.append(obj)

        for name, obj in old_map.items():
            if name not in new_map:
                removed_list.append(obj)

        for name, new_obj in new_map.items():
            if name in old_map:
                old_obj = old_map[name]
                if old_obj != new_obj:
                    modified_list.append({'old': old_obj, 'new': new_obj, 'changes': f"Content of {object_type_name} '{name}' changed."})