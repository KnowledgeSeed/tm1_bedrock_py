import sys
import os
import unittest
from unittest.mock import patch, mock_open

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from filter import filter as model_filter
from deserializer import deserialize_model

EXPORT_DIR = os.path.join(current_dir, 'export')

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
    def get_all_objects_with_paths(self):
        all_objects = {}
        for obj in self.cubes: all_objects[f"cubes/{obj.name}"] = obj
        for obj in self.dimensions: all_objects[f"dimensions/{obj.name}"] = obj
        for obj in self.processes: all_objects[f"processes/{obj.name}"] = obj
        for obj in self.chores: all_objects[f"chores/{obj.name}"] = obj
        return all_objects

class TestModelFilter(unittest.TestCase):
    def setUp(self):
        self.mock_model = Model(
            cubes=[Cube("Sales"), Cube("Inventory")],
            dimensions=[Dimension("Region"), Dimension("Product")],
            processes=[Process("daily_load"), Process("cleanup")],
            chores=[Chore("nightly_run")]
        )

    def test_filter_with_mock_data_keep_type(self):
        rules = ["+/cubes/*"]
        filtered_model = model_filter(self.mock_model, rules)
        self.assertEqual(len(filtered_model.cubes), 2)
        self.assertEqual(len(filtered_model.dimensions), 0)

    def test_filter_with_mock_data_remove_specific(self):
        rules = ["+/*", "-/dimensions/Product"]
        filtered_model = model_filter(self.mock_model, rules)
        self.assertEqual(len(filtered_model.cubes), 2)
        self.assertEqual(len(filtered_model.dimensions), 1)
        self.assertEqual(filtered_model.dimensions[0].name, "Region")
    
    @unittest.skipIf(not os.path.isdir(EXPORT_DIR), f"integrációs test skip: Az export mappa nem található a '{current_dir}' könyvtárban, a teszt kihagyva.")
    def test_filter_with_deserialized_model_from_export(self):
        full_model, errors = deserialize_model(dir=EXPORT_DIR)
        self.assertTrue(any([full_model.cubes, full_model.dimensions]), "A modell betöltése az export mappából sikertelen volt, vagy a mappa üres.")
        
        rules = ["+/dimensions/*", "+/cubes/Channel*"]
        expected_channel_cube_count = sum(1 for cube in full_model.cubes if cube.name.startswith("Channel"))
        self.assertGreater(expected_channel_cube_count, 0, "A tesztmodell nem tartalmaz Channel nevű kockákat az export mappában a teszt futtatásához.")

        filtered_model = model_filter(full_model, rules)
        
        self.assertIsNotNone(filtered_model, "A szűrés nem tért vissza modellel.")
        self.assertEqual(len(filtered_model.dimensions), len(full_model.dimensions), "Nem az összes dimenzió maradt meg.")
        self.assertEqual(len(filtered_model.cubes), expected_channel_cube_count, "Nem a várt számú Channel kocka maradt meg a szűrés után.")
        all_cubes_are_channel = all(cube.name.startswith("Channel") for cube in filtered_model.cubes)
        self.assertTrue(all_cubes_are_channel, "A szűrés után maradt olyan kocka is, aminek a neve nem Channel-el kezdődik.")
        self.assertEqual(len(filtered_model.processes), 0, "Maradtak processzek a szűrés után.")
        self.assertEqual(len(filtered_model.chores), 0, "Maradtak chore-ok a szűrés után.")

if __name__ == '__main__':
    unittest.main()