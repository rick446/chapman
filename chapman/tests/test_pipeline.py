import unittest

import ming
from mongotools import mim

from chapman.task import Task, Function, Pipeline
from chapman import model as M

class TestPipeline(unittest.TestCase):

    def setUp(self):
        M.doc_session.bind = ming.create_datastore(
            'test', bind=ming.create_engine(
                use_class=lambda *a,**kw: mim.Connection.get()))
        mim.Connection.get().clear_all()
        self.doubler = Function.decorate('double')(self._double)

    def _double(self, x):
        return x * 2

    def test_twostage(self):
        t = Pipeline.s([
            self.doubler.s(),
            self.doubler.s()])
        t.start(2)
        while True:
            m,s = M.Message.reserve('foo', ['chapman'])
            if s is None: break
            print 'Handling %s' % m
            task = Task.from_state(s)
            task.handle(m)
        t.refresh()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)
        self.assertEqual(t.get(), 8)

