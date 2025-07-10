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
from comparator import Comparator, compare
from changeset import Changeset, export_changeset

from model.ti import TI
from tm1_to_model import tm1_to_model
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


filter_rules: List[str] = import_filter('tm1_git_py/Test/filter.txt')
_model, _errors = tm1_to_model(tm1_conn=tm1_connection())

_model_filtered = filter(_model, filter_rules=filter_rules)

serialize_model(_model, dir='export')
serialize_model(_model_filtered, dir='export_filtered')
changeset = compare(_model, _model_filtered)

export_changeset('changeset.txt', changeset)


#     rules_path = 'filter.txt'
#     filter_rules = []
#     try:
#         with open(rules_path, 'r', encoding='utf-8') as f:
#             filter_rules = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
#         print(f"\n2. '{rules_path}' létezik:")
#     except FileNotFoundError:
#         print(f"\n2. nincs: '{rules_path}'")


# export_dir(_model=_model, export_dir=os.environ.get("EXPORT_DIR"))

#_model, _errors = deserialize_model(dir='export')
#serialize_model(_model, dir='export2')

# def compare_tm1():
#     model_from_export, export_errors = deserialize_model(dir='export')
#     if any(export_errors.values()):
#         print(export_errors)

#     model_from_export2, export_errors = deserialize_model(dir='export2')
#     if any(export_errors.values()):
#         print(export_errors)
#     comparator = Comparator()

#     print("\n--- full ---")
#     changeset_full = comparator.compare(model_from_export, model_from_export2, mode='full')
#     print(changeset_full)

#     print("\n--- add_only ---")
#     changeset_add_only = comparator.compare(model_from_export, model_from_export2, mode='add_only')
#     print(changeset_add_only)
# compare_tm1()

# def run_filter_and_export():
#     source_directory = 'export'
#     print(f"1. Modell betöltése innen'{source_directory}'")
#     original_model, errors = deserialize_model(dir=source_directory)
#     if errors:
#         print("modell betöltés hiba", errors)

#     rules_path = 'filter.txt'
#     filter_rules = []
#     try:
#         with open(rules_path, 'r', encoding='utf-8') as f:
#             filter_rules = [line.strip() for line in f if line.strip() and not line.strip().startswith('#')]
#         print(f"\n2. '{rules_path}' létezik:")
#     except FileNotFoundError:
#         print(f"\n2. nincs: '{rules_path}'")

#     print("\n3. Filtering")
#     filtered_model = filter(original_model, filter_rules)

#     export_directory = 'export3'
#     print(f"\n4. A filtered modell mentése '{export_directory}'")
#     serialize_model(filtered_model, dir=export_directory)
# run_filter_and_export()

print("")

