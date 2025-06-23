import json
import os
from typing import Dict, List
from TM1py import TM1Service
from TM1py.Utils import format_url

from model.chore import Chore
from model.cube import Cube
from model.dimension import Dimension
from model.element import Element
from model.hierarchy import Hierarchy
from model.mdxview import MDXView
from model.model import Model
from model.subset import Subset
from model.process import Process
import TM1py
import re

from model.ti import TI


def deserialize_model(dir) -> Model:

    dimensions_dir = dir + '/dimensions'
    cubes_dir = dir + '/cubes'
    processes_dir = dir + '/processes'
    chores_dir = dir + '/chores'

    _processes, _process_errors = deserialize_processes(processes_dir)

    _chores, _chore_errors = deserialize_chores(chores_dir)
    
    _dimensions, _dim_errors = deserialize_dimensions(dimensions_dir)

    _cubes, _cube_errors = deserialize_cubes(cubes_dir, _dimensions)

    _model = Model(cubes=_cubes.values(), dimensions=_dimensions.values(), processes=_processes.values(), chores=_chores.values())
    _errors = _dim_errors | _cube_errors | _process_errors | _chore_errors
    return _model, _errors

def deserialize_chores(chore_dir) -> tuple[Dict[str, Chore], Dict[str, str]]:

    chores: Dict[str, Process] = {}
    chores_errors: Dict[str, str] = {}

    files = directory_to_dict(chore_dir)
    for file_name in list(files.keys()):
        files.pop(file_name, None)
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        chore_link = Chore.as_link(file_name)

        if file_name_ext != 'json':
            chores_errors[chore_link] = 'not a chore json'
            continue

        chore_json = None        
        with open(os.path.join(chore_dir, file_name), 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                chore_json = json.loads(data)
            except Exception as e:
                chores_errors[chore_link] = e.__repr__()
                continue
        
        try:
            _chore = Chore(
                name=chore_json['Name'],
                start_time=chore_json['StartTime'],
                dst_sensitive=chore_json['DSTSensitive'],
                active=chore_json['Active'],
                execution_mode=chore_json['ExecutionMode'],
                frequency=chore_json['Frequency'],
                tasks=[task for task in chore_json['Tasks']])
            chores[chore_json['Name']] = _chore
        except Exception as e:
            chores_errors[chore_link] = e.__repr__()

    return chores, chores_errors


def deserialize_processes(process_dir) -> tuple[Dict[str, Process], Dict[str, str]]:

    processes: Dict[str, Process] = {}
    process_errors: Dict[str, str] = {}

    files = directory_to_dict(process_dir)
    for file_name in list(files.keys()):
        
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        process_link = Process.as_link(file_name)
        
        if file_name_ext != 'json' and file_name_ext != 'ti':
            process_errors[process_link] = 'not a process json or ti file'
            continue
        if file_name_ext != 'json':
            continue

        files.pop(file_name, None)
        process_json = None
        process_ti = None
        
        with open(os.path.join(process_dir, file_name), 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                process_json = json.loads(data)
            except Exception as e:
                process_errors[process_link] = e.__repr__()
                continue
           
        ti_file_name = file_name_base + '.ti'
        if ti_file_name not in files:
            process_errors[process_link] = 'related ti not found at ' + Process.as_link(ti_file_name)
            continue

        with open(os.path.join(process_dir, ti_file_name), 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                process_ti = TI.from_string(data)
            except Exception as e:
                process_errors[process_link] = e.__repr__()
            finally:
                files.pop(ti_file_name, None)
            
        try:
            _process = Process(
                name=process_json['Name'], hasSecurityAccess=process_json['HasSecurityAccess'], code_link=process_json['Code@Code.link'], datasource=None,
                parameters=process_json['Parameters'], variables=process_json['Variables'], ti=process_ti)
            processes[process_json['Name']] = _process
        except Exception as e:
            process_errors[process_link] = e.__repr__()

    return processes, process_errors


def deserialize_dimensions(dimension_dir) -> tuple[Dict[str, Dimension], Dict[str, str]]:

    dimensions: Dict[str, Dimension] = {}
    dimension_errors: Dict[str, str] = {}

    files = directory_to_dict(dimension_dir)
    for file_name in list(files.keys()):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        dim_link = Dimension.as_link(file_name)
        
        if file_name_ext not in ['json', 'hierarchies']:
            dimension_errors[dim_link] = 'not a dimension json or .hierarchies folder'
            continue
        if file_name_ext != 'json':
            continue

        files.pop(file_name, None)
        dim_json = None

        with open(os.path.join(dimension_dir, file_name), 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                dim_json = json.loads(data)
            except Exception as e:
                dimension_errors[dim_link] = e.__repr__()
                continue

        try:
            _dimension = Dimension(name=dim_json['Name'], hierarchies=[], defaultHierarchy=None)
        except Exception as e:
            dimension_errors[dim_link] = e.__repr__()
            continue

        hier_dir_name = file_name_base + '.hierarchies'
        hier_dir_path = file_path = os.path.join(dimension_dir, hier_dir_name)

        if hier_dir_name not in files and not os.path.isdir(hier_dir_path):
            dimension_errors[dim_link] = 'no hierarchies found'
            continue

        hiers = files.get(hier_dir_name)
        for hier_file_name in list(hiers.keys()):
            hier_file_name_base, dot, file_name_ext = hier_file_name.rpartition('.')
            hier_link = Hierarchy.as_link(file_name_base, hier_file_name)

            if file_name_ext not in ['json', 'subsets']:
                dimension_errors[hier_link] = 'not a hierarchy json or .subset folder'
                continue
            if file_name_ext != 'json':
                continue

            hiers.pop(hier_file_name, None)

            hier_json = None
            with open(os.path.join(hier_dir_path, hier_file_name), 'r', encoding='utf-8') as file:
                try:
                    data = file.read()
                    hier_json = json.loads(data)
                    _hierarchy = Hierarchy(name=hier_json['Name'], elements=[Element(v) for v in hier_json['Elements']],
                            edges=[Element(v) for v in hier_json['Edges']],
                            subsets=[]) 
                    _dimension.hierarchies.append(_hierarchy)
                    pattern = r"Dimensions\('([^']*)'\)/Hierarchies\('([^']*)'\)"
                    match = re.search(pattern, dim_json['DefaultHierarchy'])
                    if match:
                        dimension, hierarchy = match.groups()
                        if hierarchy == hier_file_name_base:
                            _dimension.defaultHierarchy = _hierarchy
                except Exception as e:
                    dimension_errors[hier_link] = e.__repr__()

                subset_dir_name = hier_file_name_base + '.subsets'
                subset_dir_path = os.path.join(hier_dir_path, subset_dir_name)
                if subset_dir_name in hiers and os.path.isdir(subset_dir_path):
                    subsets = hiers.get(subset_dir_name)
                    for subset_file_name in list(subsets.keys()):
                        subset_link = Subset.as_link(file_name_base, hier_file_name_base, subset_file_name)
                        with open(os.path.join(subset_dir_path, subset_file_name), 'r', encoding='utf-8') as file:
                            try:
                                data = file.read()
                                subset_json = json.loads(data)
                                _subset = Subset(name=subset_json['Name'],
                                                    expression=subset_json['Expression'])
                                _hierarchy.subsets.append(_subset)
                            except Exception as e:
                                dimension_errors[subset_link] = e.__repr__()
        
        if not _dimension.defaultHierarchy:
            dimension_errors[dim_link] = 'no default hierarchy'
            continue
        dimensions[_dimension.name] = _dimension
    return dimensions, dimension_errors       
                                        
def deserialize_cubes(cubes_dir, _dimensions: Dict[str, Dimension]) -> tuple[Dict[str, Cube], Dict[str, str]]:

    cubes: Dict[str, Cube] = {}
    cube_errors: Dict[str, str] = {}

    files = directory_to_dict(cubes_dir)
    for file_name in list(files.keys()):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        cube_link = Cube.as_link(file_name)

        if file_name_ext not in ['json', 'rules']:
            cube_errors[cube_link] = 'not a dimension json or .hierarchies folder'
            continue
        if file_name_ext != 'json':
            continue

        files.pop(file_name, None)
        
        cube_json = None
        rule = None
        
        with open(os.path.join(cubes_dir, file_name), 'r', encoding='utf-8') as file:
            try:
                data = file.read()
                cube_json = json.loads(data)
            except Exception as e:
                cube_errors[cube_link] = e.__repr__()
                continue

        rule_file_name = file_name_base + '.rules'
        if rule_file_name not in files:
            cube_errors[cube_link] = 'rule not found'
            continue
            
        with open(os.path.join(cubes_dir, rule_file_name), 'r', encoding='utf-8') as file:
                try:
                    rule = file.read()
                    files.pop(rule_file_name, None)
                except Exception as e:
                    cube_errors[Cube.as_link(file_name_base + '.rules')] = e.__repr__()
                    continue
                
        if not rule:
            cube_errors[Cube.as_link(file_name_base + '.rules')] = 'rule cannot be parsed'
            continue

        _cube = Cube(name=cube_json['Name'], dimensions=[], rule=rule, views=[])

        for dim in cube_json['Dimensions']:
            pattern = r"Dimensions\('([^']*)'\)"
            match = re.search(pattern, dim['@id'])
            if match:
                dimension = match.groups()
                _dimension = _dimensions.get(dimension[0])
                if _dimension:
                    _cube.dimensions.append(_dimension)
        

        view_dir_name = file_name_base + '.views'
        view_dir_path = file_path = os.path.join(cubes_dir, view_dir_name)
        if view_dir_name in files and os.path.isdir(view_dir_path):
            views = files.get(view_dir_name)
            for view_file_name in list(views.keys()):
                view_file_name_base, dot, file_name_ext = view_file_name.rpartition('.')

                view = None
                mdx = None
                if file_name_ext == 'json':
                    with open(os.path.join(view_dir_path, view_file_name), 'r', encoding='utf-8') as file:
                        try:
                            data = file.read()
                            view = json.loads(data)
                        except Exception as e:
                            cube_errors[file_name_base + '.views/' + view_file_name] = e.__repr__()
                else:
                     continue
                
                mdx_file_name = view_file_name_base + '.mdx'
                if mdx_file_name in views:
                    with open(os.path.join(view_dir_path, mdx_file_name), 'r', encoding='utf-8') as file:
                        try:
                            mdx = file.read()
                        except Exception as e:
                            cube_errors[file_name_base + '.mdx'] = e.__repr__()
                    files.pop(mdx_file_name, None)
                else:
                    cube_errors[mdx_file_name] = 'rule not found'
                    continue
                
                if not mdx:
                    cube_errors[mdx_file_name] = 'mdx cannot be parsed'

                _mdxview = MDXView(name=view['Name'], mdx=mdx)
                _cube.views.append(_mdxview)
        cubes[_cube.name] = _cube
    return cubes, cube_errors


def directory_to_dict(path):
    """Converts a directory structure to a nested dictionary."""
    directory_dict = {}
    for item in os.listdir(path):
        item_path = os.path.join(path, item)
        if os.path.isdir(item_path):
            # If the item is a directory, recursively populate its contents
            directory_dict[item] = directory_to_dict(item_path)
        else:
            # If the item is a file, set it to None or any specific value if needed
            directory_dict[item] = None
    return directory_dict