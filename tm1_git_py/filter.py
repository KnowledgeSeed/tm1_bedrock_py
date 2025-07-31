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

def normalize_for_path(text: str) -> str:
    chars_to_remove = "[]'\""
    for char in chars_to_remove:
        text = text.replace(char, '')

    text = text.replace(',', '_').replace(':', '_').replace(' ', '')
    
    return text.lower()

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
            
            if '|[' in pattern:
                cube_part, area_part = pattern.rsplit('|', 1)
                normalized_pattern = f"{cube_part}|{normalize_for_path(area_part)}"
                if path == normalized_pattern:
                    is_match = True
            
            else:
                last_part = pattern.split('|')[-1] if '|' in pattern else ""
                is_indexless_task = '|' in pattern and not last_part.isdigit()
                
                if is_indexless_task:
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
            if path != path_to_remove and (path.startswith(path_to_remove + '|') or path.startswith(path_to_remove + '/')):
                expanded_paths_to_remove.add(path)
    
    final_dims = [dim for dim in model .dimensions if dim.source_path.replace('\\', '/') not in expanded_paths_to_remove]
    final_procs = [proc for proc in model .processes if proc.source_path.replace('\\', '/') not in expanded_paths_to_remove]
    
    final_cubes = []
    for cube in model.cubes:
        cube_path = cube.source_path.replace('\\', '/')
        if cube_path not in expanded_paths_to_remove:
            
            kept_rules = []
            for rule in cube.rules:
                normalized_area = normalize_for_path(rule.area)
                rule_path = f"{cube_path}|{normalized_area}"
                if rule_path not in expanded_paths_to_remove:
                    kept_rules.append(rule)
            cube.rules = kept_rules
            
            final_cubes.append(cube)

    final_chores = []
    for chore in model .chores:
        chore_path = chore.source_path.replace('\\', '/')
        if chore_path not in expanded_paths_to_remove:
            kept_tasks = []
            for i, task in enumerate(chore.tasks):
                task_path = f"{chore_path}|{task.process_name}|{i}"
                if task_path not in expanded_paths_to_remove:
                    kept_tasks.append(task)
            chore.tasks = kept_tasks
            final_chores.append( chore)
            
    filtered_model = Model(
        cubes=final_cubes,
        dimensions=final_dims,
        processes=final_procs,
        chores=final_chores
    )

    #_perform_dependency_check(filtered_model)
    
    return filtered_model