from typing import List, Any, Literal, Dict
from requests import Response
from TM1py.Utils import format_url, dimension_hierarchy_element_tuple_from_unique_name, add_url_parameters
import json


# ------------------------------------------------------------------------------------------------------------
# tm1py implementation based RestAPI input call functions
# ------------------------------------------------------------------------------------------------------------


DIR_HANDLER = {
    "right": ">",
    "down": "|",
    "right and down": "|>",
    "": ""
}

MODE_HANDLER = {
    "add": "+",
    "subtract": "~",
    "replace": ""
}


def _post_against_cellset(
        tm1_service: Any, cellset_id: str, payload: Dict, sandbox_name: str = None, **kwargs
) -> Response:
    """ Execute a post request against a cellset

    :param tm1_service:
    :param cellset_id:
    :param payload:
    :param sandbox_name: str
    :param kwargs:
    :return:
    """
    url = format_url("/Cellsets('{}')/tm1.Update", cellset_id)
    url = add_url_parameters(url, **{"!sandbox": sandbox_name})
    return tm1_service._tm1_rest.POST(url=url, data=json.dumps(payload), **kwargs)


def proportional_spread(
        tm1_service: Any,
        mdx: [str],
        value: float,
        cube: str,
        direction: Literal["right", "down", "right and down", ""] = "right and down",
        mode: Literal["add", "subtract", "replace"] = "replace",
        sandbox_name: str = None,
        ** kwargs
) -> Response:
    cellset_id = tm1_service.cells.create_cellset(mdx=mdx, sandbox_name=sandbox_name, **kwargs)
    payload = {
        "BeginOrdinal": 0,
        "Value": "P" + DIR_HANDLER[direction] + MODE_HANDLER[mode] + str(value).replace(',', '.'),
        "ReferenceCell@odata.bind": list(),
        "ReferenceCube@odata.bind":
            format_url("Cubes('{}')", cube)}

    return _post_against_cellset(tm1_service=tm1_service, cellset_id=cellset_id, payload=payload, delete_cellset=True,
                                 sandbox_name=sandbox_name, **kwargs)


def equal_spread(
        tm1_service: Any,
        mdx: [str],
        value: float,
        cube: str,
        direction: Literal["right", "down", "right and down", ""] = "right and down",
        mode: Literal["add", "subtract", "replace"] = "replace",
        sandbox_name: str = None,
        ** kwargs
) -> Response:
    cellset_id = tm1_service.cells.create_cellset(mdx=mdx, sandbox_name=sandbox_name, **kwargs)
    payload = {
        "BeginOrdinal": 0,
        "Value": "S" + DIR_HANDLER[direction] + MODE_HANDLER[mode] + str(value).replace(',', '.'),
        "ReferenceCell@odata.bind": list(),
        "ReferenceCube@odata.bind":
            format_url("Cubes('{}')", cube)}

    return _post_against_cellset(tm1_service=tm1_service, cellset_id=cellset_id, payload=payload, delete_cellset=True,
                                 sandbox_name=sandbox_name, **kwargs)


def repeat_value(
        tm1_service: Any,
        mdx: [str],
        value: float,
        cube: str,
        direction: Literal["right", "down", "right and down", ""] = "right and down",
        mode: Literal["add", "subtract", "replace"] = "replace",
        sandbox_name: str = None,
        ** kwargs
) -> Response:
    cellset_id = tm1_service.cells.create_cellset(mdx=mdx, sandbox_name=sandbox_name, **kwargs)
    payload = {
        "BeginOrdinal": 0,
        "Value": "R" + DIR_HANDLER[direction] + MODE_HANDLER[mode] + str(value).replace(',', '.'),
        "ReferenceCell@odata.bind": list(),
        "ReferenceCube@odata.bind":
            format_url("Cubes('{}')", cube)}

    return _post_against_cellset(tm1_service=tm1_service, cellset_id=cellset_id, payload=payload, delete_cellset=True,
                                 sandbox_name=sandbox_name, **kwargs)


def percentage_change(
        tm1_service: Any,
        mdx: [str],
        value: float,
        cube: str,
        direction: Literal["right", "down", "right and down", ""] = "right and down",
        mode: Literal["add", "subtract", "replace"] = "replace",
        sandbox_name: str = None,
        ** kwargs
) -> Response:
    cellset_id = tm1_service.cells.create_cellset(mdx=mdx, sandbox_name=sandbox_name, **kwargs)
    payload = {
        "BeginOrdinal": 0,
        "Value": "P%" + DIR_HANDLER[direction] + MODE_HANDLER[mode] + str(value).replace(',', '.'),
        "ReferenceCell@odata.bind": list(),
        "ReferenceCube@odata.bind":
            format_url("Cubes('{}')", cube)}

    return _post_against_cellset(tm1_service=tm1_service, cellset_id=cellset_id, payload=payload, delete_cellset=True,
                                 sandbox_name=sandbox_name, **kwargs)


def relative_proportional_spread(
        tm1_service: Any,
        mdx: [str],
        value: float,
        cube: str,
        reference_unique_element_names: List[str],
        # reference_unique_element_mdx: [str],
        reference_cube: str = None,
        mode: Literal["add", "subtract", "replace"] = "replace",
        sandbox_name: str = None,
        ** kwargs
) -> Response:
    cellset_id = tm1_service.cells.create_cellset(mdx=mdx, sandbox_name=sandbox_name, **kwargs)

    payload = {
        "BeginOrdinal": 0,
        "Value": "RP" + MODE_HANDLER[mode] + str(value).replace(',', '.'),
        "ReferenceCell@odata.bind": list(),
        "ReferenceCube@odata.bind":
            format_url("Cubes('{}')", reference_cube if reference_cube else cube)}

    # reference_unique_element_names = utility.__parse_unique_element_names_from_mdx(reference_unique_element_mdx)
    for unique_element_name in reference_unique_element_names:
        payload["ReferenceCell@odata.bind"].append(
            format_url(
                "Dimensions('{}')/Hierarchies('{}')/Elements('{}')",
                *dimension_hierarchy_element_tuple_from_unique_name(unique_element_name)))

    return _post_against_cellset(tm1_service=tm1_service, cellset_id=cellset_id, payload=payload, delete_cellset=True,
                                 sandbox_name=sandbox_name, **kwargs)
