from chapman.task import Barrier
from chapman import model as M

from .test_base import TaskTest


class TestBarrier(TaskTest):

    def test_ignore_sub_results(self):
        t = Barrier.n(
            self.doubler.n(),
            self.doubler.n())
        t.start(2)
        self._handle_messages()
        t.refresh()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)
        self.assertEqual(t.result.get(), None)
