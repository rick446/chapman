import unittest

import ming
from mongotools import mim

from chapman.task import Task, Function
from chapman import model as M
from chapman import exc

class TestBasic(unittest.TestCase):

    def setUp(self):
        M.doc_session.bind = ming.create_datastore(
            'test', bind=ming.create_engine(
                use_class=lambda *a,**kw: mim.Connection.get()))
        mim.Connection.get().clear_all()
        self.doubler = Function.decorate('double')(self._double)

    def _double(self, x):
        return x * 2

    def test_abstract_task(self):
        t = Task.s()
        with self.assertRaises(NotImplementedError):
            t.run(None)

    def test_call_direct(self):
        self.assertEqual(4, self.doubler(2))

    def test_create_task(self):
        self.doubler.s()
        self.assertEqual(1, M.TaskState.m.find().count())

    def test_create_message(self):
        t = self.doubler.s()
        msg = M.Message.s(t, 'run', 1, 2, a=3)
        self.assertEqual(msg.task_id, t.id)
        self.assertEqual(msg.task_repr, repr(t))
        self.assertEqual(msg.schedule.status, 'pending')
        self.assertEqual(msg.slot, 'run')
        self.assertEqual(msg.args, (1,2))
        self.assertEqual(msg.kwargs, {'a': 3})
        self.assertEqual(1, M.Message.m.find().count())
        
    def test_start(self):
        t = self.doubler.s()
        msg = t.start(2)
        self.assertEqual(msg.task_id, t.id)
        self.assertEqual(msg.schedule.status, 'ready')
        self.assertEqual(msg.slot, 'run')
        self.assertEqual(msg.args, (2,))
        self.assertEqual(msg.kwargs, {})
        self.assertEqual(1, M.Message.m.find().count())

    def test_run_message(self):
        t = self.doubler.s()
        msg = t.start(2)
        t.handle(msg)
        self.assertEqual(t.result.get(), 4)
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)

    def test_ignore_result(self):
        t = self.doubler.s(ignore_result=True)
        msg = t.start(2)
        self.assertEqual(1, M.Message.m.find().count())
        self.assertEqual(1, M.TaskState.m.find().count())
        t.handle(msg)
        self.assertEqual(0, M.Message.m.find().count())
        self.assertEqual(0, M.TaskState.m.find().count())
        
    def test_run_message_callback(self):
        @Function.decorate('doubler_result')
        def doubler_result(result):
            return result.get() * 2
        t0 = self.doubler.s()
        t1 = doubler_result.s()
        t0.link(t1, 'run')
        t0.start(2)
        self.assertEqual(2, M.Message.m.find().count())

        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)
        self.assertEqual(t.result.get(), 8)
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)

    def test_run_message_callback_error(self):
        t0 = self.doubler.s()
        t1 = self.doubler.s()
        t0.link(t1, 'run')
        t0.start(None)
        self.assertEqual(2, M.Message.m.find().count())

        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        with self.assertRaises(exc.TaskError):
            t.result.get()
        
    def test_run_message_callback_failed_not_immutable(self):
        @Function.decorate()
        def nothing():
            pass
        t0 = self.doubler.s()
        t1 = nothing.s()
        t0.link(t1, 'run')
        t0.start(1)
        
        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        with self.assertRaises(exc.TaskError) as te:
            t.result.get()
        self.assertEqual(te.exception.args[0], TypeError)
        
    def test_run_message_callback_immutable(self):
        @Function.decorate(immutable=True)
        def nothing():
            return 42
        t0 = self.doubler.s()
        t1 = nothing.s()
        t0.link(t1, 'run')
        t0.start(1)
        
        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        m,s = M.Message.reserve('foo', ['chapman'])
        t = Task.from_state(s)
        t.handle(m)

        self.assertEqual(t.result.get(), 42)
        
            
        
        
