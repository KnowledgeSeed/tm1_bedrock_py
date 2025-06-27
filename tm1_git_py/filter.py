import json
import fnmatch
import re
from typing import List, Any, Dict, Tuple

from model.model import Model
from model.dimension import Dimension
from changeset import Changeset

class ModelFilter:
    def __init__(self, rules_path: str):
        self.rules = {}
        self.patterns = {}
        try:
            with open(rules_path, 'r', encoding='utf-8') as f:
                loaded_rules = json.load(f)
            
            definitions = loaded_rules.get('definitions', {})
            for name, pattern in definitions.items():
                self.patterns[name] = re.compile(pattern)
            
            self.rules = loaded_rules
            print(f"Szűrés innen: '{rules_path}'")
        except FileNotFoundError:
            print(f"nincs ilyen fájl '{rules_path}'")
        except (json.JSONDecodeError, re.error) as e:
            print(f"'{rules_path}' hibás regex/formátum {e}")

    def apply(self, model: Model) -> Changeset:
        removal_changeset = Changeset()
        if not self.rules:
            return removal_changeset

        removal_changeset.removed_cubes = self._filter_object_list(model.cubes, self.rules.get('cubes', {}).get('rules', []))
        removal_changeset.removed_processes = self._filter_object_list(model.processes, self.rules.get('processes', {}).get('rules', []))
        removal_changeset.removed_chores = self._filter_object_list(model.chores, self.rules.get('chores', {}).get('rules', []))
        
        self._filter_dimensions_and_hierarchies(model, removal_changeset)

        self._perform_dependency_check(model, removal_changeset)

        return removal_changeset

    def _resolve_and_match(self, name: str, pattern: str) -> bool:
        if pattern.startswith('regex:'):
            pattern_name = pattern.split(':', 1)[1]
            if pattern_name in self.patterns:
                return bool(self.patterns[pattern_name].match(name))
            return False
        return fnmatch.fnmatch(name, pattern)

    def _filter_object_list(self, objects: List[Any], rules: List[str]) -> List[Any]:
        if not rules:
            return []

        all_object_names = {obj.name for obj in objects}
        kept_names = set()

        include_rules = [r[1:] for r in rules if r.startswith('+')]
        if not include_rules:
            kept_names = all_object_names.copy()
        else:
            for rule in include_rules:
                for name in all_object_names:
                    if self._resolve_and_match(name, rule):
                        kept_names.add(name)

        exclude_rules = [r[1:] for r in rules if r.startswith('-')]
        names_to_exclude = set()
        for rule in exclude_rules:
            for name in kept_names:
                if self._resolve_and_match(name, rule):
                    names_to_exclude.add(name)
        
        kept_names.difference_update(names_to_exclude)

        removed_names = all_object_names - kept_names
        return [obj for obj in objects if obj.name in removed_names]

    def _filter_dimensions_and_hierarchies(self, model: Model, changeset: Changeset):
        dim_config = self.rules.get('dimensions', {})

        removed_dims = self._filter_object_list(model.dimensions, dim_config.get('rules', []))
        changeset.removed_dimensions.extend(removed_dims)
        
        kept_dims = {dim.name: dim for dim in model.dimensions if dim not in removed_dims}
        for rule in dim_config.get('hierarchy_rules', []):
            dim_name_pattern = rule.get('dimensionName')
            if not dim_name_pattern: continue
            
            matching_dim_names = fnmatch.filter(kept_dims.keys(), dim_name_pattern)
            for dim_name in matching_dim_names:
                dim = kept_dims[dim_name]
                hierarchies_to_remove = self._filter_object_list(dim.hierarchies, rule.get('rules', []))
                if hierarchies_to_remove:
                    print(f"Hierarchies '{dim.name}' removed {[h.name for h in hierarchies_to_remove]}")

    def _perform_dependency_check(self, model: Model, changeset: Changeset):
        removed_dim_names = {d.name for d in changeset.removed_dimensions}
        
        kept_cubes = [c for c in model.cubes if c not in changeset.removed_cubes]
        
        newly_removed_cubes = []
        for cube in kept_cubes:
            cube_dim_names = {d.name for d in cube.dimensions}
            
            broken_links = cube_dim_names.intersection(removed_dim_names)
            
            if broken_links:
                print(f"Broken object '{cube.name}', link {list(broken_links)}")
                newly_removed_cubes.append(cube)
        
        if newly_removed_cubes:
            changeset.removed_cubes.extend(newly_removed_cubes)
            print(f"összes remove {len(newly_removed_cubes)}")