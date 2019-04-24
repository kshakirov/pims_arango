import unittest
import sys
sys.path.append('../lib')
from graph_manager import GraphManager


class TestStringMethods(unittest.TestCase):
    def setUp(self):
        self.g_manager = GraphManager('localhost','root','servantes')
    # def test_add(self):
    #      self.g_manager.add_entity({'id': 0, 'name': 'root'})
    #     # self.g_manager.add_pair({'id': 4, 'name': 'child 3', 'reference_id': 0})
    #     # self.g_manager.add_pair({'id': 2, 'name': 'child 3', 'reference_id': 0})
    def test_parents(self):
        name = self.g_manager.get_all_parents(1)
        print(name.join(","))

if __name__ == '__main__':
    unittest.main()