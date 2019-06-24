import unittest
import sys

sys.path.append('../lib')
from graph_manager_advanced import GraphManagerAdvanced


class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.g_manager = GraphManagerAdvanced('localhost', 'root', 'servantes')

    # def test_upsert(self):
    #     self.g_manager.upsert_entity({'_key': "0", 'name': 'parent 0', 'external_id': 23, "reference": {"id": 4}})
    #     self.g_manager.upsert_entity({'_key': "4", 'name': 'child 0_1', 'external_id': 24, "reference": {"id": 8}})
    #     self.g_manager.upsert_entity({'_key': "8", 'name': 'child 1_2 ', 'external_id': 25})

    def test_upsert_batch(self):
        batch = [{'_key': "0", 'name': 'parent 0', 'external_id': 23, 'entity_type_id': 4,
                  "reference": {"25": 4, "27": 123}},
                 {'_key': "4", 'name': 'child 0_1', 'external_id': 24, 'entity_type_id': 4,
                  "reference": {"25": 8, "27": 12}},
                 {'_key': "8", 'name': 'child 1_2 ', 'entity_type_id': 4, 'external_id': 25}]
        self.g_manager.upsert_batch(batch)

    # def test_parents_id(self):
    #     name = self.g_manager.get_all_parents_with_id(1)
    #     print(name.join(","))

    # def test_edge(self):
    #     batch = [{'_key': "0", 'name': 'parent 0', 'external_id': 23, "reference": {"id": 4}},
    #                            {'_key': "4", 'name': 'child 0_1', 'external_id': 24, "reference": {"id": 8}},
    #                            {'_key': "8", 'name': 'child 1_2 ', 'external_id': 25}]
    #     self.g_manager.upsert_arango_import(batch)


if __name__ == '__main__':
    unittest.main()
