import json
import os
from typing import Dict, List
from TM1py import TM1Service
from TM1py.Utils import format_url

from .model.chore import Chore
from .model.cube import Cube
from .model.dimension import Dimension
from .model.element import Element
from .model.hierarchy import Hierarchy
from .model.mdxview import MDXView
from .model.model import Model
from .model.subset import Subset
from .model.process import Process
import TM1py

from .model.ti import TI


def serialize_model(model: Model, dir):

    os.makedirs(dir, exist_ok=True)

    dim_dir = dir + '/dimensions'
    os.makedirs(dim_dir, exist_ok=True)
    serialize_dimensions(model.dimensions, dim_dir)

    cubes_dir = dir + '/cubes'
    os.makedirs(cubes_dir, exist_ok=True)
    serialize_cubes(model.cubes, cubes_dir)

    processes_dir = dir + '/processes'
    os.makedirs(processes_dir, exist_ok=True)
    serialize_processes(model.processes, processes_dir)

    chores_dir = dir + '/chores'
    os.makedirs(chores_dir, exist_ok=True)
    serialize_chores(model.chores, chores_dir)


def serialize_dimensions(dimensions: List[Dimension], dim_dir):
    for dim in dimensions:
        for _hierarchy in dim.hierarchies:
            hierarchy_dir = dim_dir + '/' + dim.name + '.hierarchies'
            os.makedirs(hierarchy_dir, exist_ok=True)
            for _subset in _hierarchy.subsets:
                subsets_dir = hierarchy_dir + '/' + _hierarchy.name + '.subsets'
                os.makedirs(subsets_dir, exist_ok=True)
                with open(subsets_dir + '/' + _subset.name+'.json', 'w', encoding='utf-8') as subset_file:
                    subset_file.write(_subset.as_json())
            with open(hierarchy_dir + '/' + _hierarchy.name+'.json', 'w', encoding='utf-8') as hierarchy_file:
                hierarchy_file.write(_hierarchy.as_json())
        with open(dim_dir + '/' + dim.name+'.json', 'w', encoding='utf-8') as dim_file:
            dim_file.write(dim.as_json())


def serialize_cubes(cubes: List[Cube], cubes_dir):
    for cube in cubes:
        if cube.rule:
            with open(cubes_dir + '/' + cube.name+'.rules', 'w', encoding='utf-8') as rule_file:
                rule_file.write(cube.rule)
        if cube.views:
            views_dir = cubes_dir + '/' + cube.name + '.views'
            os.makedirs(views_dir, exist_ok=True)

            for _mdxview in cube.views:
                with open(views_dir + '/' + _mdxview.name+'.json', 'w', encoding='utf-8') as mdxjson_file:
                    mdxjson_file.write(_mdxview.as_json())

                with open(views_dir + '/' + _mdxview.name+'.mdx', 'w', encoding='utf-8') as mdx_file:
                    mdx_file.write(_mdxview.mdx)
            if cube.rule:
                with open(cubes_dir + '/' + cube.name+'.rules', 'w', encoding='utf-8') as rule_file:
                    rule_file.write(cube.rule)

        with open(cubes_dir + '/' + cube.name+'.json', 'w', encoding='utf-8') as cube_file:
            cube_file.write(cube.as_json())


def serialize_processes(processes: List[Process], process_dir):
    for process in processes:
        with open(process_dir + '/' + process.name + '.ti', 'w', encoding='utf-8') as processti_file:
            processti_file.write(process.ti.ti_as_string())

        with open(process_dir + '/' + process.name + '.json', 'w', encoding='utf-8') as processjson_file:
            processjson_file.write(process.as_json())


def serialize_chores(chores: List[Chore], chores_dir):
    for chore in chores:
        with open(chores_dir + '/' + chore.name + '.json', 'w', encoding='utf-8') as chore_file:
            chore_file.write(chore.as_json())
