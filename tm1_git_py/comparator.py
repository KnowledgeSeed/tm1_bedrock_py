from typing import List, Dict, Any

from model.model import Model
from changeset import Changeset

from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from model.chore import Chore
from model.task import Task

class Comparator:
    def compare(self, model1: Model, model2: Model, mode: str = 'full') -> Changeset:
        """
        Összehasonlítás:
            model1: A régi modell.
            model2: Az új modell.
            mode: Az összehasonlítás módja 'full' (mindent tárol)
                  vagy 'add_only' (csak a hozzáadott és módosított elemeket tárolja)
        """
        changeset = Changeset()
        self._compare_object_lists(model1.cubes, model2.cubes,
                                   changeset.added, changeset.removed,
                                   changeset.modified,
                                   object_type_name="Cube",
                                   mode=mode)

        self._compare_object_lists(model1.dimensions, model2.dimensions,
                                   changeset.added, changeset.removed,
                                   changeset.modified,
                                   object_type_name="Dimension",
                                   mode=mode)

        for dimension1, dimension2 in zip(model1.dimensions, model2.dimensions):
            self._compare_object_lists(dimension1.hierarchies, dimension2.hierarchies,
                                       changeset.added, changeset.removed,
                                       changeset.modified,
                                       object_type_name="Hierarchy",
                                       mode=mode)

            for hierarchy1, hierarchy2 in zip(dimension1.hierarchies, dimension2.hierarchies):
                self._compare_object_lists(hierarchy1.subsets, hierarchy2.subsets,
                                           changeset.added, changeset.removed,
                                           changeset.modified,
                                           object_type_name="Subset",
                                           mode=mode)

        self._compare_object_lists(model1.processes, model2.processes,
                                   changeset.added, changeset.removed,
                                   changeset.modified,
                                   object_type_name="Process",
                                   mode=mode)

        self._compare_object_lists(model1.processes, model2.processes,
                                   changeset.added, changeset.removed,
                                   changeset.modified,
                                   object_type_name="Chore",
                                   mode=mode)

        return changeset

    def _compare_object_lists(self,
                               old_list: List[Any],
                               new_list: List[Any],
                               added_list: List[Any],
                               removed_list: List[Any],
                               modified_list: List[Dict[str, Any]],
                               object_type_name: str,
                               mode: str):
        
        old_map = {obj.name: obj for obj in old_list}
        new_map = {obj.name: obj for obj in new_list}

        for name, obj in new_map.items():
            if name not in old_map:
                added_list.append(obj)

        if mode == 'full':
            for name, obj in old_map.items():
                if name not in new_map:
                    removed_list.append(obj)

        for name, new_obj in new_map.items():
            if name in old_map:
                old_obj = old_map[name]
                if old_obj != new_obj:
                    modified_list.append({'old': old_obj, 'new': new_obj, 'changes': f"Content of {object_type_name} '{name}' changed."})

    def _compare_chores(self, old_chores: List[Chore], new_chores: List[Chore], changeset: Changeset, mode: str):
        old_map = {chore.name: chore for chore in old_chores}
        new_map = {chore.name: chore for chore in new_chores}

        for name, new_chore in new_map.items():
            if name not in old_map:
                changeset.added.append(new_chore)
            else:
                old_chore = old_map[name]
            
                old_tasks = set(old_chore.tasks)
                new_tasks = set(new_chore.tasks)
                
                if old_tasks != new_tasks or new_chore.start_time != old_chore.start_time or new_chore.active != old_chore.active:
                    for task in (new_tasks - old_tasks):
                        idx = new_chore.tasks.index(task)
                        changeset.changes.append(f"C  /{new_chore.source_path}|{task.process_name}|{idx}")
                    
                    for task in (old_tasks - new_tasks):
                        try:
                            idx = old_chore.tasks.index(task)
                            changeset.changes.append(f"D  /{old_chore.source_path}|{task.process_name}|{idx}")
                        except ValueError:
                            changeset.changes.append(f"D  /{old_chore.source_path}|{task.process_name}")

                    if new_chore.start_time != old_chore.start_time or new_chore.active != old_chore.active:
                         changeset.changes.append(f"U  /{new_chore.source_path}")

        if mode == 'full':
            for name, chore in old_map.items():
                if name not in new_map:
                    changeset.removed.append(chore)