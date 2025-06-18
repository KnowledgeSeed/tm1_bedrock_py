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

    _dimensions, _process_errors = deserialize_dimensions(dimensions_dir)

    _processes, _process_errors = deserialize_processes(processes_dir)

    # _processes, _process_errors = deserialize_cubes(cubes_dir)

    return Model(cubes=None, dimensions=_dimensions.values(), processes=_processes.values(), chores=None)


def deserialize_processes(process_dir) -> tuple[Dict[str, Process], Dict[str, str]]:

    processes: Dict[str, Process] = {}
    process_errors: Dict[str, str] = {}

    files = directory_to_dict(process_dir)
    for file_name in list(files.keys()):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        
        process_json = None
        process_ti = None
        
        if file_name_ext == 'json':
            with open(os.path.join(process_dir, file_name), 'r') as file:
                try:
                    data = file.read()
                    process_json = json.loads(data)
                except Exception as e:
                    process_errors[file_name] = e.__repr__
                finally:
                    files.pop(file_name, None)
        else:
            continue
            
        ti_file_name = file_name_base + '.ti'
        if ti_file_name in files:
            with open(os.path.join(process_dir, ti_file_name), 'r') as file:
                try:
                    data = file.read()
                    process_ti = TI.from_string(data)
                except Exception as e:
                    process_errors[file_name_base + '.ti'] = e.__repr__
            files.pop(ti_file_name, None)
        else:
            process_errors[ti_file_name] = 'ti not found'
        
        _process = Process(
            name=process_json['Name'], hasSecurityAccess=process_json['HasSecurityAccess'], code_link=process_json['Code@Code.link'], datasource=None,
            parameters=process_json['Parameters'], variables=process_json['Variables'], ti=process_ti)
        processes[process_json['Name']] = _process

    return processes, process_errors


def deserialize_dimensions(dimension_dir) -> tuple[Dict[str, Dimension], Dict[str, str]]:

    dimensions: Dict[str, Dimension] = {}
    dimension_errors: Dict[str, str] = {}

    files = directory_to_dict(dimension_dir)
    for file_name in list(files.keys()):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        
        dim_json = None
        process_ti = None
        
        if file_name_ext == 'json':
            with open(os.path.join(dimension_dir, file_name), 'r') as file:
                try:
                    data = file.read()
                    dim_json = json.loads(data)
                except Exception as e:
                    dimension_errors[file_name] = e.__repr__
                finally:
                    files.pop(file_name, None)
        else:
            continue

        _dimension = Dimension(name=dim_json['Name'], hierarchies=[], defaultHierarchy=None)

        hier_dir_name = file_name_base + '.hierarchies'
        hier_dir_path = file_path = os.path.join(dimension_dir, hier_dir_name)
        if hier_dir_name in files and os.path.isdir(hier_dir_path):
            hiers = files.get(hier_dir_name)
            for hier_file_name in list(hiers.keys()):
                hier_file_name_base, dot, file_name_ext = hier_file_name.rpartition('.')
                if file_name_ext == 'json':
                    with open(os.path.join(hier_dir_path, hier_file_name), 'r') as file:
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
                            dimension_errors[file_name+ '/' + hier_file_name] = e.__repr__
                        finally:
                            files.pop(file_name, None)

                        subset_dir_name = hier_file_name_base + '.subsets'
                        subset_dir_path = os.path.join(hier_dir_path, subset_dir_name)
                        if subset_dir_name in hiers and os.path.isdir(subset_dir_path):
                            subsets = hiers.get(subset_dir_name)
                            for subset_file_name in list(subsets.keys()):
                                with open(os.path.join(subset_dir_path, subset_file_name), 'r') as file:
                                    try:
                                        data = file.read()
                                        subset_json = json.loads(data)
                                        _subset = Subset(name=subset_json['Name'],
                                                            expression=subset_json['Expression'])
                                        _hierarchy.subsets.append(_subset)
                                    except Exception as e:
                                        dimension_errors[file_name+ '/' + hier_file_name + '/' + subset_file_name] = e.__repr__
                                    #  finally:
                                    #     files.pop(file_name, None)
        dimensions[_dimension.name] = _dimension
    return dimensions, dimension_errors       
                                        




def deserialize_cubes(cubes_dir) -> tuple[Dict[str, Cube], Dict[str, str]]:

    cube_errors: Dict[str, str] = {}
    cubes: Dict[str, Cube] = {}

    cubes_json = {}
    for file_name in os.listdir(cubes_dir):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        if not cubes_json.get(file_name_base):
            cubes_json[file_name_base] = {}
            cubes_json[file_name_base]['views'] = {}
        file_path = os.path.join(cubes_dir, file_name)
        if os.path.isfile(file_path):
            if file_name.endswith('.json'):
                with open(file_path, 'r') as file:
                    try:
                        data = file.read()
                        cubes_json[file_name_base]['cube'] = json.loads(data)
                    except Exception as e:
                        cube_errors[file_name] = e.__repr__
            elif file_name.endswith('.rules'):
                with open(file_path, 'r') as file:
                    try:
                        data = file.read()
                        cubes_json[file_name_base]['rule'] = data
                    except Exception as e:
                        cube_errors[file_name] = e.__repr__
            else:
                cube_errors[file_name] = 'Invalid cube or rule file'
        elif os.path.isdir(file_path):
            for view_file_name in os.listdir(file_path):
                view_file_name_base, dot, file_name_ext = view_file_name.rpartition(
                    '.')
                if not cubes_json[file_name_base]['views'].get(view_file_name_base):
                    cubes_json[file_name_base]['views'][view_file_name_base] = {}
                view_file_path = os.path.join(file_path, view_file_name)
                if view_file_name.endswith('.json'):
                    with open(view_file_path, 'r') as file:
                        try:
                            data = file.read()
                            cubes_json[file_name_base]['views'][view_file_name_base]['view'] = json.loads(
                                data)
                        except Exception as e:
                            cube_errors[file_name] = e.__repr__

                elif view_file_name.endswith('.mdx'):
                    with open(view_file_path, 'r') as file:
                        data = file.read()
                        cubes_json[file_name_base]['views'][view_file_name_base]['mdx'] = data
                else:
                    cube_errors[file_name] = 'Invalid view or mdx file'

    for file_name, cube_wrapper in cubes_json.items():

        cube = cube_wrapper.get('cube')
        rule = cube_wrapper.get('rule')
        views = cube_wrapper.get('views')
        if not cube:
            cube_errors[file_name] = 'cube file missing'
        elif not rule:
            cube_errors[file_name] = 'rule file missing'
        else:
            _cube = Cube(name=cube.name, dimensions=[],
                         rule=cube.rules.body_as_dict['Rules'] if cube.has_rules else None, views=[])
            # cubes[cube_name] = _cube
            # for view in views:
            #     view = view.get('view')
            #     mdx = view.get('mdx')
            #     _mdxview = MDXView(name=view.name, mdx=mdx)
            #     _cube.views.append(_mdxview)

    return rules, process_errors


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