import fnmatch
from typing import List
from model.model import Model
from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from model.chore import Chore

def _expand_task_rules(model: Model, rules: List[str]) -> List[str]:
    final_rules = []
    for rule in rules:
        is_name_based = "/tasks/" in rule and not rule.split('/tasks/')[-1].isdigit()
        if not is_name_based:
            final_rules.append(rule)
            continue
        op, pattern = rule[0], rule[1:].lstrip('/')
        chore_pattern, task_name_pattern = pattern.split('/tasks/')
        for chore in model.chores:
            chore_path = chore.source_path.replace('\\', '/')
            if fnmatch.fnmatch(chore_path, chore_pattern):
                for i, task in enumerate(chore.tasks):
                    if fnmatch.fnmatch(task.process_name, task_name_pattern):
                        final_rules.append(f"{op}{chore_path}/tasks/{i}")
    return final_rules

def _perform_dependency_check(model: Model):
    kept_dim_names = {d.name for d in model.dimensions}
    model.cubes = [c for c in model.cubes if {d.name for d in c.dimensions}.issubset(kept_dim_names)]
    kept_process_names = {p.name for p in model.processes}
    model.chores = [ch for ch in model.chores if all(t.process_name in kept_process_names for t in ch.tasks)]

def filter(model: Model, filter_rules: List[str]) -> Model:
    """
    Szűri a modellt a megadott szabályok alapján egy tiszta, újraírt, eltávolítás-alapú logikával.
    """
    if not filter_rules:
        return model

    # 1. Szabályok előkészítése (task-nevek kibontása)
    final_rules = _expand_task_rules(model, filter_rules)
    
    # 2. "Eltávolítandó" útvonalak halmazának létrehozása
    all_objects = model.get_all_objects_with_paths()
    paths_to_remove = set()
    for path in all_objects.keys():
        matching_rules = []
        for rule in final_rules:
            if len(rule) < 2 or rule[0] not in ['+', '-']: continue
            op, pattern = rule[0], rule[1:].lstrip('/')
            
            is_match = False
            if pattern.endswith('*') and not pattern.endswith('\\*'):
                if path.startswith(pattern[:-1]): is_match = True
            elif fnmatch.fnmatch(path, pattern): is_match = True
            
            if is_match: matching_rules.append({'op': op, 'pattern': pattern})

        if not matching_rules: continue
        
        # Ha van illeszkedő szabály, a legspecifikusabb nyer
        winning_rule = max(
            matching_rules, 
            key=lambda r: (r['pattern'].count('/'), 0 if '*' in r['pattern'] else 1, len(r['pattern']))
        )
        # Ha a nyertes szabály egy kivétel (-), hozzáadjuk a tiltólistához
        if winning_rule['op'] == '-':
            paths_to_remove.add(path)

    # 3. Új, tiszta listák létrehozása a megtartandó objektumokból
    # Azok az objektumok maradnak, amik nincsenek a tiltólistán
    
    final_dims = [dim for dim in model.dimensions if dim.source_path.replace('\\', '/') not in paths_to_remove]
    final_procs = [proc for proc in model.processes if proc.source_path.replace('\\', '/') not in paths_to_remove]
    
    final_cubes = []
    for cube in model.cubes:
        if cube.source_path.replace('\\', '/') not in paths_to_remove:
            cube.views = [v for v in cube.views if f'cubes/{cube.name}/views/{v.name}.json' not in paths_to_remove]
            if f'cubes/{cube.name}.rules' in paths_to_remove: cube.rule = None
            final_cubes.append(cube)

    final_chores = []
    for chore in model.chores:
        if chore.source_path.replace('\\', '/') not in paths_to_remove:
            kept_tasks = []
            chore_path = chore.source_path.replace('\\', '/')
            for i, task in enumerate(chore.tasks):
                if f"{chore_path}/tasks/{i}" not in paths_to_remove:
                    kept_tasks.append(task)
            chore.tasks = kept_tasks
            final_chores.append(chore)
            
    # 4. Új modell létrehozása a végleges listákból
    filtered_model = Model(
        cubes=final_cubes,
        dimensions=final_dims,
        processes=final_procs,
        chores=final_chores
    )

    # 5. Legvégül a függőség-ellenőrzés
    _perform_dependency_check(filtered_model)
    
    return filtered_model