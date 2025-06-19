import json
import os
from typing import Dict, List
from TM1py import TM1Service
from TM1py.Utils import format_url

from model.chore import Chore
from model.cube import Cube
from model.dimension import Dimension
from model.edge import Edge
from model.element import Element
from model.hierarchy import Hierarchy
from model.mdxview import MDXView
from model.model import Model
from model.subset import Subset
from model.process import Process
import TM1py

from model.ti import TI


def tm1_connection() -> TM1Service:
    """Creates a TM1 connection before tests and closes it after all tests."""
    # load_dotenv()
    tm1 = TM1Service(
        address=os.environ.get("TM1_ADDRESS"),
        port=os.environ.get("TM1_PORT"),
        user=os.environ.get("TM1_USER"),
        password="",
        ssl=os.environ.get("TM1_SSL")
    )
    # basic_logger.debug("Successfully connected to TM1.")
    return tm1


def tm1_to_model(tm1_conn: TM1Service) -> tuple[Model, Dict[str, str]]:

    _dimensions, _dim_errors = dimensions_to_model(tm1_conn)

    _cubes, _cube_errors = cubes_to_model(tm1_conn, _dimensions)

    _processes, _process_errors = procs_to_model(tm1_conn)

    _chores, _chore_errors = chores_to_model(tm1_conn)

    _model = Model(cubes=_cubes.values(),
                   dimensions=_dimensions.values(),
                   processes=_processes.values(),
                   chores=_chores.values())

    _errors = {}
    _errors['dim'] = _dim_errors
    _errors['cube'] = _cube_errors
    _errors['process'] = _process_errors
    _errors['chore'] = _chore_errors

    return _model, _errors


def chores_to_model(tm1_conn) -> tuple[Dict[str, Chore], Dict[str, str]]:
    all_chores = tm1_conn.chores.get_all_names()

    _chores: Dict[str, Chore] = {}
    _errors: Dict[str, str] = {}

    for chore_name in all_chores:
        chore = tm1_conn.chores.get(chore_name=chore_name)

        _chore = Chore(
            name=chore.name,
            start_time=chore.start_time.start_time_string,
            dst_sensitive=chore.dst_sensitivity,
            active=chore.active,
            execution_mode=chore.execution_mode,
            frequency=chore.frequency.frequency_string,
            tasks=[task.body_as_dict for task in chore.tasks])
        _chores[chore.name] = _chore
    return _chores, _errors


def procs_to_model(tm1_conn) -> tuple[Dict[str, Process], Dict[str, str]]:
    all_procs = tm1_conn.processes.get_all_names(skip_control_processes=False)
    regular_procs = tm1_conn.processes.get_all_names(
        skip_control_processes=True)
    control_procs = list(set(all_procs) - set(regular_procs))

    _processes: Dict[str, Process] = {}
    _errors: Dict[str, str] = {}

    for process_name in regular_procs:
        process = tm1_conn.processes.get(name_process=process_name)

        _ti = TI(prolog_procedure=process.prolog_procedure,
                 metadata_procedure=process.metadata_procedure,
                 data_procedure=process.data_procedure,
                 epilog_procedure=process.epilog_procedure)
        _process = Process(name=process.name, hasSecurityAccess=process.has_security_access,
                           code_link=process_name + '.ti',
                           datasource='',
                           parameters=process.parameters, variables=process.variables, ti=_ti)
        _processes[process.name] = _process
    return _processes, _errors


def cubes_to_model(tm1_conn, _dimensions: Dict[str, Dimension]) -> tuple[Dict[str, Cube], Dict[str, str]]:
    all_cubes = tm1_conn.cubes.get_all_names(skip_control_cubes=False)
    regular_cubes = tm1_conn.cubes.get_all_names(skip_control_cubes=True)
    control_cubes = list(set(all_cubes) - set(regular_cubes))

    _cubes: Dict[str, Cube] = {}
    _errors: Dict[str, str] = {}

    for cube_name in regular_cubes:
        cube = tm1_conn.cubes.get(cube_name=cube_name)

        _cube = Cube(name=cube_name, dimensions=[],
                     rule=cube.rules.body_as_dict['Rules'] if cube.has_rules else None, views=[])
        _cubes[cube_name] = _cube
        if cube.dimensions:
            for dimension in cube.dimensions:
                _dimension = _dimensions.get(dimension)
                if not _dimension:
                    _errors[cube_name] = 'Dimension not found ' + dimension
                else:
                    _cube.dimensions.append(_dimension)

        mdxviews = tm1_conn.views.get_all(cube_name=cube_name)[1]
        if mdxviews:
            for view in mdxviews:
                _mdxview = MDXView(name=view.name, mdx=view.mdx)
                _cube.views.append(_mdxview)
    return _cubes, _errors


def dimensions_to_model(tm1_conn) -> tuple[Dict[str, Dimension], Dict[str, str]]:
    all_dims = tm1_conn.dimensions.get_all_names(skip_control_dims=False)
    regular_dims = tm1_conn.dimensions.get_all_names(skip_control_dims=True)
    control_dims = list(set(all_dims) - set(regular_dims))

    _errors: Dict[str, str] = {}
    _dimensions: Dict[str, Dimension] = {}
    for dim_name in regular_dims:
        dim = tm1_conn.dimensions.get(dimension_name=dim_name)

        _dimension = Dimension(name=dim.name, hierarchies=[],
                               defaultHierarchy=dim.default_hierarchy)
        _dimensions[dim.name] = _dimension

        for hierarchy in dim.hierarchies:
            _hierarchy = Hierarchy(name=hierarchy.name,
                                   elements=[Element(json.loads(v.body))
                                             for k, v in hierarchy.elements.items()],
                                   edges=[Edge(k[0], k[1], v)
                                          for k, v in hierarchy.edges.items()],
                                   subsets=[])

            _dimension.hierarchies.append(_hierarchy)

            if hierarchy.subsets:
                for subset_name in hierarchy.subsets:
                    try:
                        subset = tm1_conn.subsets.get(
                            dimension_name=dim_name, subset_name=subset_name)
                        _subset = Subset(name=subset_name,
                                         expression=subset.expression)
                        _hierarchy.subsets.append(_subset)
                    except Exception as e:
                        _errors[dim_name] = str(e)
    return _dimensions, _errors
