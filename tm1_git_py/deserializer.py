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

    return Model(cubes=None, dimensions=_dimensions.values(), processes=None, chores=None)


def deserialize_processes(process_dir) -> tuple[Dict[str, Process], Dict[str, str]]:

    processes: Dict[str, Process] = {}
    process_errors: Dict[str, str] = {}

    process_jsons: Dict[str, object] = {}
    for file_name in os.listdir(process_dir):
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        if not process_jsons.get(file_name_base):
            process_jsons[file_name_base] = {}
        file_path = os.path.join(process_dir, file_name)
        if file_name.endswith('.json'):
            with open(file_path, 'r') as file:
                try:
                    data = file.read()
                    process_jsons[file_name_base]['process'] = json.loads(data)
                except Exception as e:
                    process_errors[file_name] = e.__repr__
        elif file_name.endswith('.ti'):
            with open(file_path, 'r') as file:
                data = file.read()
                process_jsons[file_name_base]['ti'] = TI.from_string(data)
        else:
            process_errors[file_name] = 'Invalid process or ti file'

    for file_name, process_wrapper in process_jsons.items():
        process = process_wrapper.get('process')
        ti = process_wrapper.get('ti')
        if not process:
            process_errors[file_name] = 'ti file missing'
        elif not ti:
            process_errors[file_name] = 'process file missing'
        else:
            _process = Process(
                name=process['Name'], hasSecurityAccess=process['HasSecurityAccess'], code_link=process['Code@Code.link'], datasource=None,
                parameters=process['Parameters'], variables=process['Variables'], ti=ti)
            processes[process['Name']] = _process

    return processes, process_errors


def deserialize_dimensions(dimension_dir) -> tuple[Dict[str, Dimension], Dict[str, str]]:

    dimensions: Dict[str, Dimension] = {}
    dimension_errors: Dict[str, str] = {}

    dimension_jsons: Dict[str, object] = {}
    for file_name in os.listdir(dimension_dir):
        file_path = os.path.join(dimension_dir, file_name)
        file_name_base, dot, file_name_ext = file_name.rpartition('.')
        if not dimension_jsons.get(file_name_base):
            dimension_jsons[file_name_base] = {}
            dimension_jsons[file_name_base]['hiers'] = {}

        if os.path.isfile(file_path):
            if file_name.endswith('.json'):
                with open(file_path, 'r') as file:
                    try:
                        data = file.read()
                        dimension_jsons[file_name_base]['dim'] = json.loads(data)
                    except Exception as e:
                        dimension_errors[file_name] = e.__repr__
            else:
                dimension_errors[file_name] = 'Invalid dimension file'
        else:
            for hier_file_name in os.listdir(file_path):
                hier_file_name_base, dot, file_name_ext = hier_file_name.rpartition('.')
                if not dimension_jsons[file_name_base]['hiers'].get(hier_file_name_base):
                    dimension_jsons[file_name_base]['hiers'][hier_file_name_base] = {}
                    dimension_jsons[file_name_base]['hiers'][hier_file_name_base]['subsets'] = {}
                hier_file_path = os.path.join(file_path, hier_file_name)
                if os.path.isfile(hier_file_path):
                    if hier_file_name.endswith('.json'):
                        with open(hier_file_path, 'r') as file:
                            try:
                                data = file.read()
                                dimension_jsons[file_name_base]['hiers'][hier_file_name_base]['json'] = json.loads(data)
                            except Exception as e:
                                dimension_jsons[file_name] = e.__repr__
                    else:
                        dimension_errors[file_name] = 'Invalid hier file'
                else:
                    for subset_file_name in os.listdir(hier_file_path):
                        subset_file_name_base, dot, file_name_ext = subset_file_name.rpartition('.')
                        subset_file_path = os.path.join(hier_file_path, subset_file_name)
                        if subset_file_name.endswith('.json'):
                            with open(subset_file_path, 'r') as file:
                                try:
                                    data = file.read()
                                    dimension_jsons[file_name_base]['hiers'][hier_file_name_base]['subsets'][subset_file_name_base] = json.loads(data)
                                except Exception as e:
                                    dimension_jsons[file_name] = e.__repr__
                        else:
                            dimension_errors[file_name] = 'Invalid subset file'

    for file_name, dim_wrapper in dimension_jsons.items():
        dim = dim_wrapper.get('dim')
        hiers = dim_wrapper.get('hiers')
        if not dim:
            dimension_errors[file_name] = 'dim json file missing'
        else:
            _dimension = Dimension(name=dim['Name'], hierarchies=[], defaultHierarchy=None)
            dimensions[_dimension.name] = _dimension
            for hier_name, hier_wrapper in hiers.items():
                hier = hier_wrapper['json']
                _hierarchy = Hierarchy(name=hier['Name'], elements=[Element(v) for v in hier['Elements']],
                                   edges=[Element(v) for v in hier['Edges']],
                                   subsets=[])                         
                _dimension.hierarchies.append(_hierarchy)
                pattern = r"Dimensions\('([^']*)'\)/Hierarchies\('([^']*)'\)"
                match = re.search(pattern, dim['DefaultHierarchy'])
                if match:
                    dimension, hierarchy = match.groups()
                    if hierarchy == hier_name:
                        _dimension.defaultHierarchy = _hierarchy
                subsets = hier_wrapper['subsets']
                for subset_name, subset_wrapper in subsets.items():
                    _subset = Subset(name=subset_wrapper['Name'],
                                         expression=subset_wrapper['Expression'])
                    _hierarchy.subsets.append(_subset)
                
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
