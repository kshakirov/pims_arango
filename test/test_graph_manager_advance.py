import unittest
import sys

sys.path.append('../lib')
from graph_manager_advanced import GraphManagerAdvanced


class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.g_manager = GraphManagerAdvanced('localhost', 'root', 'servantes')

    def test_add(self):
        self.g_manager.add_pair({'_key': "0", 'name': 'root1', 'external_id': 23, "reference": {"id": 4}})
        # self.g_manager.add_pair({'id': 4, 'name': 'child 3', 'reference_id': 0})
        # self.g_manager.add_pair({'id': 2, 'name': 'child 2', 'reference_id': 0})

    # def test_parents_id(self):
    #     name = self.g_manager.get_all_parents_with_id(1)
    #     print(name.join(","))


if __name__ == '__main__':
    unittest.main()
