import sys
import os
import unittest

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from comparator import Comparator, compare
from changeset import Changeset
from deserializer import deserialize_model

EXPORT_DIR = os.path.join(current_dir, 'export')
EXPORT2_DIR = os.path.join(current_dir, 'export2')

class BaseMockObject:
    def __init__(self, name, content="default"): self.name = name; self.content = content
    def __repr__(self): return f"{self.__class__.__name__}(name='{self.name}')"
    def __eq__(self, other): return isinstance(other, self.__class__) and self.name == other.name and self.content == other.content
    def __hash__(self): return hash((self.name, self.content))

class Cube(BaseMockObject): pass
class Dimension(BaseMockObject): pass
class Process(BaseMockObject): pass
class Chore(BaseMockObject): pass

class Model:
    def __init__(self, cubes=[], dimensions=[], processes=[], chores=[]):
        self.cubes = list(cubes); self.dimensions = list(dimensions); self.processes = list(processes); self.chores = list(chores)

class TestComparator(unittest.TestCase):

    def setUp(self):
        self.comparator = Comparator()
        self.model1 = Model(cubes=[Cube("Sales", content="v1")], dimensions=[Dimension("Region")], processes=[Process("daily_load")])
        self.model2 = Model(cubes=[Cube("Sales", content="v2"), Cube("Inventory")], dimensions=[Dimension("Region")], chores=[Chore("nightly_run")])

    def test_comparison_modes_with_mock_data(self):
        changeset_full = compare(self.model1, self.model2, mode='full')

        self.assertEqual(len(changeset_full.added_cubes), 1)
        self.assertEqual(changeset_full.added_cubes[0].name, 'Inventory')

        self.assertEqual(len(changeset_full.modified_cubes), 1)
        self.assertEqual(changeset_full.modified_cubes[0]['new'].name, 'Sales')

        self.assertEqual(len(changeset_full.removed_processes), 1)
        self.assertEqual(changeset_full.removed_processes[0], 'daily_load')

        self.assertEqual(len(changeset_full.added_chores), 1)
        self.assertEqual(changeset_full.added_chores[0].name, 'nightly_run')
        
        self.assertFalse(changeset_full.added_dimensions)
        self.assertFalse(changeset_full.removed_dimensions)
        self.assertFalse(changeset_full.modified_dimensions)

        changeset_add_only = compare(self.model1, self.model2, mode='add_only')

        self.assertFalse(changeset_add_only.removed_processes, "Az add_only módnak nem szabadna Removed elemet tartalmaznia.")
        
        self.assertEqual(changeset_full.added_cubes, changeset_add_only.added_cubes)
        self.assertEqual(changeset_full.modified_cubes, changeset_add_only.modified_cubes)
        self.assertEqual(changeset_full.added_chores, changeset_add_only.added_chores)

    def test_no_changes_with_mock_data(self):
        identical_model = Model(
            cubes=[Cube("Sales", content="v1")], 
            dimensions=[Dimension("Region")], 
            processes=[Process("daily_load")]
        )
        changeset = compare(self.model1, identical_model)
        self.assertFalse(changeset.has_changes())

    @unittest.skipIf(
        not os.path.isdir(EXPORT_DIR) or not os.path.isdir(EXPORT2_DIR),
        f"integrációs test skip: A teszthez szükséges 'export' vagy 'export2' mappa nem található."
    )
    def test_compare_full_vs_add_only_modes_with_real_data(self):
        model1, _ = deserialize_model(dir=EXPORT_DIR)
        model2, _ = deserialize_model(dir=EXPORT2_DIR)
        self.assertIsNotNone(model1, "Az 'export' mappából nem sikerült a modellt betölteni.")
        self.assertIsNotNone(model2, "Az 'export2' mappából nem sikerült a modellt betölteni.")

        #print("\n--- Running comparison with full flag ---")
        changeset_full = compare(model1, model2, mode='full')
        #print(changeset_full)
        
        full_added_items = changeset_full.added_cubes + changeset_full.added_dimensions + changeset_full.added_processes + changeset_full.added_chores
        full_removed_items = changeset_full.removed_cubes + changeset_full.removed_dimensions + changeset_full.removed_processes + changeset_full.removed_chores
        
        self.assertTrue(full_added_items, "A full mód nem talált Added elemeket. (Ellenőrizd, hogy van-e egyedi fájl az export2-ben)")
        self.assertTrue(full_removed_items, "A full mód nem talált Removed elemeket. (Ellenőrizd, hogy van-e egyedi fájl az export-ban)")

        #print("\n--- Running comparison with add_only flag ---")
        changeset_add_only = compare(model1, model2, mode='add_only')
        #print(changeset_add_only)

        add_only_removed_items = changeset_add_only.removed_cubes + changeset_add_only.removed_dimensions + changeset_add_only.removed_processes + changeset_add_only.removed_chores
        self.assertFalse(add_only_removed_items, "Az add_only módnak nem szabadna Removed elemeket tartalmaznia.")

        self.assertEqual(changeset_full.added_cubes, changeset_add_only.added_cubes)
        self.assertEqual(changeset_full.modified_cubes, changeset_add_only.modified_cubes)


if __name__ == '__main__':
    unittest.main()