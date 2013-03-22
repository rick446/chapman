from chapman.task import Pipeline
from chapman import model as M
from chapman import exc

from .test_base import TaskTest

class TestPipeline(TaskTest):

    def test_2stage(self):
        t = Pipeline.s([
            self.doubler.s(),
            self.doubler.s()])
        t.start(2)
        self._handle_messages()
        t.refresh()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)
        self.assertEqual(t.result.get(), 8)

    def test_2stage_err(self):
        t = Pipeline.s([
            self.doubler.s(),
            self.doubler.s()])
        t.start(None)
        self._handle_messages()
        t.refresh()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)
        with self.assertRaises(exc.TaskError) as err:
            t.result.get()
        self.assertEqual(err.exception.args[0], TypeError)
        
