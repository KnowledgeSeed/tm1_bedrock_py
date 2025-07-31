import json
import os
import re
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
from model.task import Task
from model.rule import Rule


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


def export(tm1_conn: TM1Service) -> tuple[Model, Dict[str, str]]:

    _dimensions, _dim_errors = dimensions_to_model(tm1_conn)

    _cubes, _cube_errors = cubes_to_model(tm1_conn, _dimensions)

    _processes, _process_errors = procs_to_model(tm1_conn)

    _chores, _chore_errors = chores_to_model(tm1_conn)

    _model = Model(cubes=_cubes.values(),
                   dimensions=_dimensions.values(),
                   processes=_processes.values(),
                   chores=_chores.values(),
                   server_configs=server_configs_to_model(tm1_conn))

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

        tasks_for_model = []
        for tm1py_task in chore.tasks:
            task_dict = tm1py_task.body_as_dict
            process_name = ""
            process_bind = task_dict.get("Process@odata.bind", "")
            match = re.search(r"Processes\('([^']*)'\)", process_bind)
            if match:
                process_name = match.group(1)
            
            task_obj = Task(
                process_name=process_name,
                parameters=task_dict.get('Parameters', [])
            )
            tasks_for_model.append(task_obj)

        _chore = Chore(
            name=chore.name,
            start_time=chore.start_time.start_time_string,
            dst_sensitive=chore.dst_sensitivity,
            active=chore.active,
            execution_mode=chore.execution_mode,
            frequency=chore.frequency.frequency_string,
            tasks=tasks_for_model,
            source_path=os.path.join('chores', f"{chore_name}.json").replace('\\', '/')
        )
        _chores[chore.name] = _chore
        
    return _chores, _errors


def procs_to_model(tm1_conn) -> tuple[Dict[str, Process], Dict[str, str]]:
    all_procs = tm1_conn.processes.get_all_names(skip_control_processes=False)
    regular_procs = tm1_conn.processes.get_all_names(
        skip_control_processes=True)
    control_procs = list(set(all_procs) - set(regular_procs))

    _processes: Dict[str, Process] = {}
    _errors: Dict[str, str] = {}

    for process_name in all_procs:
        process = tm1_conn.processes.get(name_process=process_name)

        _ti = TI(prolog_procedure=process.prolog_procedure,
                 metadata_procedure=process.metadata_procedure,
                 data_procedure=process.data_procedure,
                 epilog_procedure=process.epilog_procedure)
        _process = Process(name=process.name, hasSecurityAccess=process.has_security_access,
                           code_link=process_name + '.ti',
                           datasource='',
                           parameters=process.parameters, variables=process.variables, ti=_ti,
                           source_path=os.path.join('processes', f"{process_name}.json").replace('\\', '/'))
        _processes[process.name] = _process
    return _processes, _errors


def cubes_to_model(tm1_conn, _dimensions: Dict[str, Dimension]) -> tuple[Dict[str, Cube], Dict[str, str]]:
    all_cubes = tm1_conn.cubes.get_all_names(skip_control_cubes=False)

    _cubes: Dict[str, Cube] = {}
    _errors: Dict[str, str] = {}

    for cube_name in all_cubes:
        try:
            cube = tm1_conn.cubes.get(cube_name=cube_name)

            rule_source_object = cube.rules if cube.has_rules else None

            rule_text = ""
            if cube.has_rules:
                raw_body = cube.rules.body
                try:
                    rule_data = json.loads(raw_body)
                    rule_text = rule_data.get("Rules", "")
                except (json.JSONDecodeError, AttributeError):
                    rule_text = raw_body if isinstance(raw_body, str) else ""
            
            rules_list = _parse_rules(rule_text)
            _cube = Cube(
                name=cube_name,
                dimensions=[],
                rules=rules_list,
                views=[],
                source_path=os.path.join('cubes', cube_name).replace('\\', '/')
            )
            _cubes[cube_name] = _cube
            
            if cube.dimensions:
                for dimension in cube.dimensions:
                    _dimension = _dimensions.get(dimension)
                    if not _dimension:
                        _errors[cube_name] = f"Dimension '{dimension}' not found"
                    else:
                        _cube.dimensions.append(_dimension)

            mdxviews_tuple = tm1_conn.views.get_all(cube_name=cube_name)
            if mdxviews_tuple:
                mdxviews = mdxviews_tuple[1]
                for view in mdxviews:
                    _mdxview = MDXView(
                        name=view.name, 
                        mdx=view.mdx,
                        source_path=os.path.join('cubes', f"{cube_name}.views", f"{view.name}.json").replace('\\', '/')
                    )
                    _cube.views.append(_mdxview)
        

        except Exception as e:
            _errors[cube_name] = str(e)
            
    return _cubes, _errors


def dimensions_to_model(tm1_conn) -> tuple[Dict[str, Dimension], Dict[str, str]]:
    all_dims = tm1_conn.dimensions.get_all_names(skip_control_dims=False)
    regular_dims = tm1_conn.dimensions.get_all_names(skip_control_dims=True)
    control_dims = list(set(all_dims) - set(regular_dims))

    _errors: Dict[str, str] = {}
    _dimensions: Dict[str, Dimension] = {}
    for dim_name in all_dims:
        dim = tm1_conn.dimensions.get(dimension_name=dim_name)

        _dimension = Dimension(name=dim.name, hierarchies=[],
                               defaultHierarchy=dim.default_hierarchy,
                               source_path=os.path.join('dimensions', f"{dim_name}.json").replace('\\', '/'))
        _dimensions[dim.name] = _dimension

        for hierarchy in dim.hierarchies:
            _hierarchy = Hierarchy(name=hierarchy.name,
                                   elements=[Element(json.loads(v.body))
                                             for k, v in hierarchy.elements.items()],
                                   edges=[Edge(k[0], k[1], v)
                                          for k, v in hierarchy.edges.items()],
                                   subsets=[],
                                   source_path=os.path.join('dimensions', f"{dim_name}.hierarchies", f"{hierarchy.name}.json").replace('\\', '/'))

            _dimension.hierarchies.append(_hierarchy)

            if hierarchy.subsets:
                for subset_name in hierarchy.subsets:
                    try:
                        subset = tm1_conn.subsets.get(
                            dimension_name=dim_name, subset_name=subset_name)
                        _subset = Subset(name=subset_name,
                                         expression=subset.expression,
                                         source_path=os.path.join('dimensions', f"{dim_name}.hierarchies", f"{hierarchy.name}.subsets", f"{subset.name}.json").replace('\\', '/'))
                        _hierarchy.subsets.append(_subset)
                    except Exception as e:
                        _errors[dim_name] = str(e)
    return _dimensions, _errors

def _parse_rules(rule_text: str) -> List[Rule]:
    if not rule_text: return []
    rules = []
    pattern = re.compile(r"(?P<comment>(?:#.*(?:\r\n|\n|$)\s*)*)?(?P<statement>\[.*?\][^;]*;)", re.DOTALL)
    header_match = re.match(r'^(.*?)(?=\[|#|$)', rule_text, re.DOTALL)
    last_pos = 0
    if header_match:
        header_text = header_match.group(1).strip()
        if header_text:
            rules.append(Rule(area="[HEADER]", full_statement=header_text, comment=""))
        last_pos = header_match.end()
    for match in pattern.finditer(rule_text, last_pos):
        comment = (match.group('comment') or "").strip()
        statement_text = match.group('statement').strip()
        area_match = re.search(r'(\[.*?\])', statement_text)
        area = area_match.group(1) if area_match else "[UNKNOWN]"
        rules.append(Rule(area=area, full_statement=statement_text, comment=comment))
    return rules

def server_configs_to_model(tm1_conn: TM1Service) -> Dict:
    configs = tm1_conn.configuration.get_active()
    return configs
