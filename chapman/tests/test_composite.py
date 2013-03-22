from chapman.task import Composite

from .test_base import TaskTest

class TestComposite(TaskTest):

    def test_composite_is_abstract(self):
        t = Composite.n()
        self.assertRaises(NotImplementedError, t.run, None)
        self.assertRaises(NotImplementedError, t.retire_subtask, None)
