#!/usr/bin/env python3
import fnmatch
import re
from typing import List, Any

from .model.model import Model


def _load_pickle(path: str) -> Model:
    import pickle
    with open(path, "rb") as fh:
        data = pickle.load(fh)
    return data.get("model") if isinstance(data, dict) else data


def _save_pickle(model: Model, output: str) -> None:
    import pickle
    with open(output, "wb") as fh:
        pickle.dump(model, fh)

# def _perform_dependency_check(model: Model):
#     kept_dim_names = {d.name for d in model.dimensions}
    
#     final_kept_cubes = []
#     for cube in model.cubes:
#         cube_dim_names = {d.name for d in cube.dimensions}
#         if cube_dim_names.issubset(kept_dim_names):
#             final_kept_cubes.append(cube)
#         else:
#             broken_links = cube_dim_names - kept_dim_names
#             print(f"'{cube.name}' removed, filtered dimenziokra hivatkozik: {list(broken_links)}")
    
#     model.cubes = final_kept_cubes


def filter(model: Model, filter_rules: List[str]) -> Model:
    if not filter_rules:
        return model

    all_objects = model.get_all_objects_with_paths()
    all_paths = set(all_objects.keys())
    kept_paths = set()

    for rule in filter_rules:
        if len(rule) < 2 or rule[0] not in ['+', '-']:
            continue

        op, pattern = rule[0], rule[1:]
        
        if pattern.startswith('/'):
            pattern = pattern[1:]

        matching_paths = {path for path in all_paths if fnmatch.fnmatch(path, pattern)}
        
        if op == '+':
            kept_paths.update(matching_paths)
        elif op == '-':
            kept_paths.difference_update(matching_paths)

    filtered_model = Model(cubes=[], dimensions=[], processes=[], chores=[])
    
    kept_objects = [obj for path, obj in all_objects.items() if path in kept_paths]
    
    for obj in kept_objects:
        obj_type_name = type(obj).__name__.lower()

        if obj_type_name == 'process':
            obj_type_list_name = 'processes'
        else:
            obj_type_list_name = obj_type_name + 's'

        if hasattr(filtered_model, obj_type_list_name):
            getattr(filtered_model, obj_type_list_name).append(obj)


    # _perform_dependency_check(filtered_model)

    return filtered_model


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description="Filter a model using rules and output a new model pickle"
    )
    parser.add_argument("model", help="Path to input model pickle")
    parser.add_argument("rules", help="Path to filter rules text file")
    parser.add_argument("output", help="Path to output model pickle")
    args = parser.parse_args()

    model = _load_pickle(args.model)
    try:
        with open(args.rules, "r", encoding="utf-8") as fh:
            rules = [line.strip() for line in fh if line.strip() and not line.strip().startswith('#')]
    except FileNotFoundError:
        print(f"Missing rules file: {args.rules}")
        raise SystemExit(1)

    filtered_model = filter(model, rules)
    _save_pickle(filtered_model, args.output)
    print("Filtering completed successfully.")


if __name__ == "__main__":
    main()
