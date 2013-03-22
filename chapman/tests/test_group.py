import unittest

import ming
from mongotools import mim

from chapman.task import Task, Function, Group
from chapman import model as M
from chapman import exc

class TestGroup(unittest.TestCase):

    def setUp(self):
        M.doc_session.bind = ming.create_datastore(
            'test', bind=ming.create_engine(
                use_class=lambda *a,**kw: mim.Connection.get()))
        mim.Connection.get().clear_all()
        self.doubler = Function.decorate('double')(self._double)

    def _double(self, x):
        return x * 2

    def test_two(self):
        t = Group.s([
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
        self.assertEqual(t.result.get(), [4,4])

    def test_two_err(self):
        @Function.decorate('doubler_result')
        def err(x):
            raise TypeError, 'Always raises error'
        t = Group.s([
            self.doubler.s(),
            self.doubler.s(),
            err.s()])
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
        self.assertEqual('failure', t.result.status)
        self.assertEqual('success', t.result[0].status)
        self.assertEqual('success', t.result[1].status)
        self.assertEqual('failure', t.result[2].status)
        with self.assertRaises(exc.TaskError) as err:
            t.result.get()
        self.assertEqual(err.exception.args[0], TypeError)
