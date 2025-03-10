from typing import Callable, List, Optional, Any

from TM1py import TM1Service
from pandas import DataFrame

from TM1_bedrock_py import utility


# ------------------------------------------------------------------------------------------------------------
# Main: normalized pandas dataframe to cube functions
# ------------------------------------------------------------------------------------------------------------

# todo: deprecate mdx to cube with clear
# todo: refactor datacopy
    # call clear for target
    # call write for target
    # add source zero as end procedure


# rename: clear_cube_with_set_mdx_list
# loader -> utils
def clear_cube(
        clear_function: Optional[Callable[..., Any]] = None,
        **kwargs: Any
) -> None:
    """
    Clears a cube with filters. If no custom function is provided, the default function is used.

    Args:
        clear_function (Optional[Callable]): A function to clear the cube using set MDXs.
                                             Defaults to the built-in TM1 service function.
        **kwargs (Any): Additional keyword arguments for the clear function, which may include:
                        - tm1_service (TM1Service): An active TM1Service object for the server connection.
                        - cube_name (str): The name of the cube to clear.
                        - clear_set_mdx_list (List[str]): A list of valid MDX set expressions defining the clear space.
    """
    if clear_function is None:
        clear_function = __clear_cube_default
    return clear_function(**kwargs)


# rename: clear_cube_with_set_mdx_list
# loader, internal -> utils, internal
def __clear_cube_default(
        tm1_service: TM1Service,
        cube_name: str,
        clear_set_mdx_list: List[str]
) -> None:
    """
    Clears a cube with filters by generating clear parameters from a list of set MDXs.

    Args:
        tm1_service (TM1Service): An active TM1Service object for the TM1 server connection.
        cube_name (str): The name of the cube to clear.
        clear_set_mdx_list (List[str]): A list of valid MDX set expressions defining the clear space.
    """
    clearing_kwargs = utility.__transform_set_mdx_list_to_tm1py_clear_kwargs(clear_set_mdx_list)
    tm1_service.cells.clear(cube_name, **clearing_kwargs)


def dataframe_to_cube(
        write_function: Optional[Callable[..., Any]] = None,
        **kwargs: Any
) -> None:
    """
    Writes a DataFrame to a cube. If no custom function is provided, the default function is used.

    Args:
        write_function (Optional[Callable]): A function to write the DataFrame to the cube.
                                             Defaults to the built-in TM1 service function.
        **kwargs (Any): Additional keyword arguments for the write function.
    """
    if write_function is None:
        write_function = __dataframe_to_cube_default
    return write_function(**kwargs)


def __dataframe_to_cube_default(
        tm1_service: TM1Service,
        dataframe: DataFrame,
        cube_name: str,
        cube_dims: List[str],
        async_write: bool = False,
        use_ti: bool = False,
        use_blob: bool = False,
        increment: bool = False,
        sum_numeric_duplicates: bool = True,
        **kwargs
) -> None:
    """
    Writes a DataFrame to a cube using the TM1 service.

    Args:
        tm1_service (TM1Service): An active TM1Service object for the TM1 server connection.
        dataframe (DataFrame): The DataFrame to write to the cube.
        cube_name (str): The name of the target cube.
        cube_dims (List[str]): A list of dimensions for the target cube.
        async_write (bool, optional): Whether to write data asynchronously. Defaults to False.
        use_ti (bool, optional): Whether to use TurboIntegrator. Defaults to False.
        use_blob (bool, optional): Whether to use the 'blob' method. Defaults to False.
        increment (bool, optional): Increments the values in the cube instead of replacing them. Defaults to False.
        sum_numeric_duplicates (bool, optional): Aggregate numerical values for duplicated intersections.
            Defaults to True.

    Returns:
        None
    """
    function_name = "write_dataframe_async" if async_write else "write_dataframe"

    getattr(tm1_service.cells, function_name)(
        cube_name=cube_name,
        data=dataframe,
        dimensions=cube_dims,
        deactivate_transaction_log=True,
        reactivate_transaction_log=True,
        skip_non_updateable=True,
        use_ti=use_ti,
        use_blob=use_blob,
        increment=increment,
        sum_numeric_duplicates=sum_numeric_duplicates
    )
