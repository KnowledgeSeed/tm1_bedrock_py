import json
import os
from typing import Dict, List
from TM1py import TM1Service
from TM1py.Utils import format_url

from deserializer import deserialize_model
from model.chore import Chore
from model.cube import Cube
from model.dimension import Dimension
from model.element import Element
from serializer import serialize_model
from model.hierarchy import Hierarchy
from model.mdxview import MDXView
from model.model import Model
from model.subset import Subset
from model.process import Process
import TM1py

from model.ti import TI
from tm1_to_model import tm1_to_model


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


# _model, _errors = tm1_to_model(tm1_conn=tm1_connection())

# export_dir(_model=_model, export_dir=os.environ.get("EXPORT_DIR"))

_model = deserialize_model(dir=os.environ.get("EXPORT_DIR"))
serialize_model(_model, dir='export2')
