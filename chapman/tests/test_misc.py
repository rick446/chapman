from chapman import model as M
from chapman.context import g

from .test_base import TaskTest

class TestMisc(TaskTest):

    def test_context(self):
        assert not hasattr(g, 'task')
        assert not hasattr(g, 'message')
        with g.set_context(task=1, message=2):
            self.assertEqual(g.task, 1)
            self.assertEqual(g.message, 2)
        assert not hasattr(g, 'task')
        assert not hasattr(g, 'message')
