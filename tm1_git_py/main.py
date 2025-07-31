import json
import os
import sys
from typing import Dict, List
from TM1py import TM1Service
from TM1py.Utils import format_url
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

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
from changeset import Changeset, export_changeset

from model.ti import TI
from exporter import export
from filter import filter, import_filter

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


#filter_rules: List[str] = import_filter('tm1_git_py/tests/filter.txt')
#_model, _errors = export(tm1_conn=tm1_connection())

#_model_filtered = filter(_model, filter_rules=filter_rules)

#serialize_model(_model, dir='export')
#serialize_model(_model_filtered, dir='export_filtered')
#changeset = compare(_model, _model_filtered)

#export_changeset('changeset.txt', changeset)

# def run_test_workflow():
#     ORIGINAL_MODEL_DIR = 'tm1_git_py/tests/export'
#     FILTERED_MODEL_DIR = 'tm1_git_py/tests/export_filtered'
#     FILTER_RULES_PATH = 'tm1_git_py/tests/filter.txt'

#     print("export\n")
#     tm1_conn = tm1_connection()
#     if not tm1_conn:
#         return
    
#     original_model, export_errors = export(tm1_conn=tm1_conn)
#     tm1_conn.logout()
#     if export_errors:
#         print(f"hiba: {export_errors}")

#     serialize_model(original_model, dir=ORIGINAL_MODEL_DIR)

#     filter_rules = import_filter(FILTER_RULES_PATH)
#     if not filter_rules:
#         print("nincs filter fájl")
#         return

#     filtered_model = filter(original_model, filter_rules=filter_rules)

#     print("filtered export \n")
#     serialize_model(filtered_model, dir=FILTERED_MODEL_DIR)

#     print(f"Eredeti modell beolvasása: '{ORIGINAL_MODEL_DIR}'")
#     original_model, errors1 = deserialize_model(dir=ORIGINAL_MODEL_DIR)
#     if errors1:
#         print(f"!! Hibák az eredeti modell beolvasása során: {errors1}")

#     print(f"Módosított modell beolvasása: '{FILTERED_MODEL_DIR}'")
#     filtered_model, errors2 = deserialize_model(dir=FILTERED_MODEL_DIR)
#     if errors2:
#         print(f"Hiba beolvasáskor: {errors2}")

#     print("összehasonlítás ... \n")
#     comparator = Comparator()
#     changeset = comparator.compare(original_model, filtered_model)
#     export_changeset('changeset.txt', changeset)
#     print(f" eredmény: changeset.txt")


# run_test_workflow()

# print("")

