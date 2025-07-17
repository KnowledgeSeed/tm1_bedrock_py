import sys
import os
import unittest
from unittest.mock import patch, mock_open

# Szükséges, hogy a tesztfájl megtalálja a projekt többi részét
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.insert(0, parent_dir)

from filter import filter as model_filter
from deserializer import deserialize_model
# A modell osztályok importálása a teszteléshez
from model.model import Model
from model.cube import Cube
from model.dimension import Dimension
from model.process import Process
from model.chore import Chore
from model.mdxview import MDXView
from model.task import Task
from model.hierarchy import Hierarchy


EXPORT_DIR = os.path.join(current_dir, 'export')

class TestAdvancedFilter(unittest.TestCase):
    def setUp(self):
        self.hier_region = Hierarchy(name="Region", elements=[], edges=[], subsets=[], source_path="dimensions/Region/hierarchies/Region.json")
        self.hier_product = Hierarchy(name="Product", elements=[], edges=[], subsets=[], source_path="dimensions/Product/hierarchies/Product.json")

        self.dim_region = Dimension(name="Region", hierarchies=[self.hier_region], defaultHierarchy=self.hier_region, source_path="dimensions/Region.json")
        self.dim_product = Dimension(name="Product", hierarchies=[self.hier_product], defaultHierarchy=self.hier_product, source_path="dimensions/Product.json")
        
        self.view_default = MDXView(name="default", mdx="", source_path="cubes/Sales/views/default.json")
        
        self.proc_load = Process(
            name="daily_load", source_path="processes/daily_load.pro",
            hasSecurityAccess=False, code_link="", datasource=None, parameters=[], variables=[], ti=""
        )
        self.proc_report = Process(
            name="generate_report", source_path="processes/generate_report.pro",
            hasSecurityAccess=False, code_link="", datasource=None, parameters=[], variables=[], ti=""
        )
        
        self.task_load = Task(process_name="daily_load", parameters=[])
        self.task_report = Task(process_name="generate_report", parameters=[])

        self.cube_sales = Cube(
            name="Sales", dimensions=[self.dim_region, self.dim_product], rule="['Sales'] = N: 100;",
            views=[self.view_default], source_path="cubes/Sales.json"
        )
        self.cube_inventory = Cube(
            name="Inventory", dimensions=[self.dim_product], rule=None,
            views=[], source_path="cubes/Inventory.json"
        )
        
        self.chore_nightly = Chore(
            name="nightly_run", tasks=[self.task_load, self.task_report],
            start_time="", dst_sensitive=False, active=True, execution_mode="", frequency="",
            source_path="chores/nightly_run.json"
        )
        
        self.model = Model(
            cubes=[self.cube_sales, self.cube_inventory],
            dimensions=[self.dim_region, self.dim_product],
            processes=[self.proc_load, self.proc_report],
            chores=[self.chore_nightly]
        )

    def test_specificity_by_depth(self):
        rules = ["+*", "-cubes/Sales/views/default.json"]
        filtered_model = model_filter(self.model, rules)
        
        self.assertEqual(len(filtered_model.cubes), 2)
        sales_cube_after_filter = next(c for c in filtered_model.cubes if c.name == "Sales")
        self.assertEqual(len(sales_cube_after_filter.views), 0)

    def test_specificity_by_wildcard_and_length(self):
        rules = ["+*", "-cubes/Inventory.json"]
        filtered_model = model_filter(self.model, rules)
        
        self.assertEqual(len(filtered_model.cubes), 1)
        self.assertEqual(filtered_model.cubes[0].name, "Sales")

    def test_sub_object_filtering_rule(self):
        rules = ["+*", "-cubes/Sales.rules"]
        filtered_model = model_filter(self.model, rules)

        self.assertEqual(len(filtered_model.cubes), 2)
        sales_cube_after_filter = next(c for c in filtered_model.cubes if c.name == "Sales")
        self.assertIsNone(sales_cube_after_filter.rule)

    def test_dependency_check_cube_dimension(self):
        rules = ["+*", "-dimensions/Region.json"] 
        filtered_model = model_filter(self.model, rules)

        self.assertEqual(len(filtered_model.dimensions), 1)
        self.assertEqual(filtered_model.dimensions[0].name, "Product")
        self.assertEqual(len(filtered_model.cubes), 1)
        self.assertEqual(filtered_model.cubes[0].name, "Inventory")

    def test_dependency_check_chore_process(self):
        rules = ["+*", "-processes/daily_load.pro"]
        filtered_model = model_filter(self.model, rules)

        self.assertEqual(len(filtered_model.processes), 1)
        self.assertEqual(len(filtered_model.chores), 0)

    def test_name_based_task_filtering(self):
        rules = ["+*", "-chores/nightly_run.json/tasks/daily_load"]
        filtered_model = model_filter(self.model, rules)
        
        self.assertEqual(len(filtered_model.chores), 1)
        filtered_chore = filtered_model.chores[0]
        self.assertEqual(len(filtered_chore.tasks), 1)
        self.assertEqual(filtered_chore.tasks[0].process_name, "generate_report")

    @unittest.skipIf(not os.path.isdir(EXPORT_DIR), f"Integrációs teszt kihagyva: Az '{EXPORT_DIR}' export mappa nem található.")
    def test_filter_with_deserialized_model_from_export(self):
        full_model, errors = deserialize_model(dir=EXPORT_DIR)
        self.assertTrue(any([full_model.cubes, full_model.dimensions]), "A modell betöltése sikertelen volt.")
        
        rules = ["+/dimensions/*", "+/cubes/Channel*"]
        expected_channel_cube_count = sum(1 for cube in full_model.cubes if cube.name.startswith("Channel"))
        self.assertGreater(expected_channel_cube_count, 0, "Nincsenek 'Channel' kockák a teszt futtatásához.")

        filtered_model = model_filter(full_model, rules)
        
        self.assertIsNotNone(filtered_model)
        self.assertEqual(len(filtered_model.dimensions), len(full_model.dimensions))
        self.assertEqual(len(filtered_model.cubes), expected_channel_cube_count)
        self.assertTrue(all(c.name.startswith("Channel") for c in filtered_model.cubes))
        self.assertEqual(len(filtered_model.processes), 0)
        self.assertEqual(len(filtered_model.chores), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)