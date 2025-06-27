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
from comparator import Comparator
from changeset import Changeset

from model.ti import TI
from tm1_to_model import tm1_to_model
from filter import ModelFilter

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
    #basic_logger.debug("Successfully connected to TM1.")
    return tm1


#_model, _errors = tm1_to_model(tm1_conn=tm1_connection())
#serialize_model(_model, dir='export')

# export_dir(_model=_model, export_dir=os.environ.get("EXPORT_DIR"))

#_model, _errors = deserialize_model(dir='export')
#serialize_model(_model, dir='export2')

# def export_filtered_model():
#     model, errors = deserialize_model(dir='export')
#     if any(errors.values()):
#         print("Hibák az export betöltésénél:", errors)

#     model_filter = ModelFilter("tm1project.json")
#     removal = model_filter.apply(model)

#     def remove_filtered(model: Model, changeset: Changeset) -> Model:
#         model.cubes = [c for c in model.cubes if c not in changeset.removed_cubes]
#         model.processes = [p for p in model.processes if p not in changeset.removed_processes]
#         model.chores = [ch for ch in model.chores if ch not in changeset.removed_chores]
#         model.dimensions = [d for d in model.dimensions if d not in changeset.removed_dimensions]
#         return model

#     filtered_model = remove_filtered(model, removal)

#     serialize_model(filtered_model, dir='export3')
#     print("export3")

def compare_tm1():
    model_from_export, export_errors = deserialize_model(dir='export')
    if any(export_errors.values()):
        print(export_errors)

    model_from_export2, export_errors = deserialize_model(dir='export2')
    if any(export_errors.values()):
        print(export_errors)
    print("-- comparator --")
    comparator = Comparator()


    changeset = comparator.compare(model_from_export, model_from_export2)
    
    print(changeset)

compare_tm1()
#export_filtered_model()
print("")

