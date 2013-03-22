from chapman import model as M
from chapman.context import g

from .test_base import TaskTest

class TestMisc(TaskTest):

    def test_context(self):
        self.assertIsNone(g.task)
        self.assertIsNone(g.message)
        with g.set_context(1,2):
            self.assertEqual(g.task, 1)
            self.assertEqual(g.message, 2)
        self.assertIsNone(g.task)
        self.assertIsNone(g.message)
