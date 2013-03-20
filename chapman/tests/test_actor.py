import bson
import unittest

import ming

from chapman import Actor, FunctionActor, Worker
from chapman import Group, Pipeline
from chapman import exc, g, actor
from chapman import model as M


class TestBasic(unittest.TestCase):

    def setUp(self):
        ming.config.configure_from_nested_dict(dict(
                chapman=dict(uri='mim://')))
        ming.mim.Connection.get().clear_all()
        self.worker = Worker('test')
        self.doubler = FunctionActor.decorate('double')(self._double)

    def _double(self, x):
        return x * 2

    def test_create_actor(self):
        a = self.doubler.create()
        self.assertIsInstance(a, Actor)
        self.assertIsInstance(a.id, bson.ObjectId)

    def test_run_actor(self):
        a = self.doubler.create()
        M.Message.post(a.id, args=(4,))
        msg, a = self.worker.actor_iterator().next()
        result = a.handle(msg)
        self.assertEqual(result.get(), 8)

    def test_run_actor_error(self):
        a = self.doubler.create()
        M.Message.post(a.id)
        msg, a = self.worker.actor_iterator().next()
        result = a.handle(msg)
        with self.assertRaises(exc.ActorError) as err:
            result.get()
        print '\n'.join(err.exception.format())

    def test_run_actor_error_debug(self):
        a = self.doubler.create()
        M.Message.post(a.id)
        msg, a = self.worker.actor_iterator().next()
        with self.assertRaises(TypeError):
            a.handle(msg, raise_errors=True)

    def test_spawn(self):
        a = self.doubler.spawn(4)
        msg, a = self.worker.actor_iterator().next()
        print a.handle(msg).get()

    def test_create_args(self):
        a = self.doubler.create((4,))
        msg = M.Message.post(a.id)
        self.assertEqual(8, a.handle(msg, raise_errors=True).get())

    def test_curry(self):
        a = self.doubler.create()
        a.curry(4)
        msg = M.Message.post(a.id)
        self.assertEqual(8, a.handle(msg, raise_errors=True).get())

    def test_ignore(self):
        l = []
        @actor(ignore_result=True)
        def appender(x):
            l.append(x)
        a = appender.create()
        self.assertEqual(1, M.ActorState.m.find().count())
        a.start((5,))
        self.worker.run_all()
        self.assertEqual([5], l)
        self.assertEqual(0, M.ActorState.m.find().count())
        
    def test_ignore_later(self):
        l = []
        @actor()
        def appender(x):
            l.append(x)
        a = appender.s(4)
        a.set_options(ignore_result=True)
        self.assertEqual(1, M.ActorState.m.find().count())
        a.start()
        self.worker.run_all()
        self.assertEqual([4], l)
        self.assertEqual(0, M.ActorState.m.find().count())
        
        

class TestCanvas(unittest.TestCase):
    
    def setUp(self):
        ming.config.configure_from_nested_dict(dict(
                chapman=dict(uri='mim://')))
        ming.mim.Connection.get().clear_all()
        self.worker = Worker('test')
        self.doubler = FunctionActor.decorate('double')(self._double)
        self.fact = FunctionActor.decorate('fact')(self._fact)
        self.fact2 = FunctionActor.decorate('fact2')(self._fact2)
        self.sum = FunctionActor.decorate('sum')(sum)

    def _double(self, x):
        return x * 2

    def _fact(self, x, acc=1):
        print '_fact(%s, %s)' % (x, acc)
        if x == 0:
            return acc
        raise self.fact.s().chain('run', x-1, acc*x)

    def _fact2(self, x, acc=1):
        print '_fact2(%s, %s)' % (x, acc)
        if x == 0:
            return acc
        raise self.fact2.s().chain('run', x-1, acc*x)

    def test_group(self):
        subtasks = [ self.doubler.create((x,)) for x in range(5) ]
        a = Group.spawn(subtasks)
        self.worker.run_all()
        a.refresh()
        self.assertEqual(a.result.get(), [0, 2, 4, 6, 8 ])

    def test_barrier(self):
        events = []
        @actor()
        def subtask(x):
            events.append(x)
        @actor(immutable=True)
        def tail(*args):
            events.append(None)
        t = tail.create()
        a = Group.create(
            [], cb_id=M.Message.post(t.id, stat='pending')._id)
        for x in range(5):
            st = subtask.s(x)
            a.append(st)
        self.worker.run_all(raise_errors=True)
        for event in events:
            print event
        self.assertEqual(6, len(events))
        self.assertIsNone(events[-1])
        self.assertEqual(set(events), set([0,1,2,3,4,None]))

    def test_pipeline(self):
        st0 = self.doubler.create((2,))
        st1 = self.doubler.create()
        a = Pipeline.spawn([st0, st1])
        self.worker.run_all(raise_errors=True)
        a.refresh()
        self.assertEqual(a.result.get(), 8)

    def test_pipeline_immutable(self):
        st0 = self.doubler.create((2,))
        st1 = self.doubler.create((2,), immutable=True)
        a = Pipeline.spawn([st0, st1])
        self.worker.run_all(raise_errors=True)
        a.refresh()
        self.assertEqual(a.result.get(), 4)

    def test_chord(self):
        subs = [self.doubler.s(x) for x in range(5) ]
        a0 = Group.create(subs)
        a1 = self.sum.create()
        a = Pipeline.spawn([a0, a1])
        self.assertEqual(15, M.ActorState.m.find().count())
        self.worker.run_all(raise_errors=True)
        a.refresh()
        self.assertEqual(20, a.result.get())
        self.assertEqual(1, M.ActorState.m.find().count())
        a.forget()
        self.assertEqual(0, M.ActorState.m.find().count())
        self.assertEqual(0, M.Message.m.find().count())
        
    def test_chain(self):
        a = self.fact.create(immutable=True)
        a.start((4,))
        self.worker.run_all(raise_errors=True)
        a.refresh()
        self.assertEqual(24, a.result.get())

    def test_chain2(self):
        a = self.fact2.si()
        a.start((4,))
        self.worker.run_all(raise_errors=True)
        a.refresh()
        self.assertEqual(24, a.result.get())
        
