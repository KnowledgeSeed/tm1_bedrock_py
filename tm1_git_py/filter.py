import fnmatch
from typing import List
from model.model import Model
from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from model.chore import Chore
from model.task import Task

def _perform_dependency_check(model: Model):
    kept_dim_names = {d.name for d in model.dimensions}
    model.cubes = [c for c in model.cubes if {d.name for d in c.dimensions}.issubset(kept_dim_names)]
    
    kept_process_names = {p.name for p in model.processes}
    model.chores = [ch for ch in model.chores if all(t.process_name in kept_process_names for t in ch.tasks)]

def import_filter(path: str) -> List[str]:
    rules_path = path
    filter_rules = []
    with open(rules_path, 'r', encoding='utf-8') as f:
        filter_rules = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
    return filter_rules

def filter(model: Model, filter_rules: List[str]) -> Model:
    if not filter_rules:
        return model

    all_objects = model.get_all_objects_with_paths()
    paths_to_remove = set()
    
    for path in all_objects.keys():
        matching_rules = []
        for rule in filter_rules:
            if len(rule) < 2 or rule[0] not in ['+', '-']: continue
            op, pattern = rule[0], rule[1:].lstrip('/')
            
            is_match = False
            is_indexless_task_rule = '|' in pattern and not pattern.endswith('*') and not pattern.split('|')[-1].isdigit()

            if is_indexless_task_rule:
                if path.startswith(pattern + '|'):
                    is_match = True
            else:
                if fnmatch.fnmatch(path, pattern):
                    is_match = True

            if is_match:
                matching_rules.append({'op': op, 'pattern': pattern})

        if not matching_rules: 
            continue
        
        winning_rule = max(
            matching_rules, 
            key=lambda r: (r['pattern'].count('|'), r['pattern'].count('/'), len(r['pattern']), -r['pattern'].count('*'))
        )
        
        if winning_rule['op'] == '-':
            paths_to_remove.add(path)

    expanded_paths_to_remove = set(paths_to_remove)
    for path_to_remove in paths_to_remove:
        for path in all_objects.keys():
            if path.startswith(path_to_remove + '|') or path.startswith(path_to_remove + '/'):
                expanded_paths_to_remove.add(path)
    
    final_dims = [dim for dim in model.dimensions if dim.source_path.replace('\\', '/') not in expanded_paths_to_remove]
    final_procs = [proc for proc in model.processes if proc.source_path.replace('\\', '/') not in expanded_paths_to_remove]
    
    final_cubes = []
    for cube in model.cubes:
        if cube.source_path.replace('\\', '/') not in expanded_paths_to_remove:
            cube.views = [v for v in cube.views if f'cubes/{cube.name}/views/{v.name}.json' not in expanded_paths_to_remove]
            if f'cubes/{cube.name}.rules' in expanded_paths_to_remove: 
                cube.rule = None
            final_cubes.append(cube)

    final_chores = []
    for chore in model.chores:
        chore_path = chore.source_path.replace('\\', '/')
        if chore_path not in expanded_paths_to_remove:
            kept_tasks = []
            for i, task in enumerate(chore.tasks):
                task_path = f"{chore_path}|{task.process_name}|{i}"
                if task_path not in expanded_paths_to_remove:
                    kept_tasks.append(task)
            chore.tasks = kept_tasks
            if chore.tasks or not model.get_all_objects_with_paths().get(chore_path, chore).tasks:
                 final_chores.append(chore)

    filtered_model = Model(
        cubes=final_cubes,
        dimensions=final_dims,
        processes=final_procs,
        chores=final_chores
    )

    #_perform_dependency_check(filtered_model)
    
    return filtered_model