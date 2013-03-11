import bson
import unittest

import ming

from cleese import Actor, FunctionActor, Worker
from cleese import Group, Pipeline
from cleese import exc, g
from cleese import model as M


class TestBasic(unittest.TestCase):

    def setUp(self):
        ming.config.configure_from_nested_dict(dict(
                cleese=dict(uri='mim://')))
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
        Actor.send(a.id, 'run', (4,))
        a = self.worker.actor_iterator().next()
        result = a.handle()
        self.assertEqual(result.get(), 8)

    def test_run_actor_error(self):
        a = self.doubler.create()
        Actor.send(a.id, 'run')
        a = self.worker.actor_iterator().next()
        result = a.handle()
        with self.assertRaises(exc.ActorError) as err:
            result.get()
        print '\n'.join(err.exception.format())

    def test_run_actor_error_debug(self):
        a = self.doubler.create()
        Actor.send(a.id, 'run')
        a = self.worker.actor_iterator().next()
        with self.assertRaises(TypeError):
            a.handle(raise_errors=True)

    def test_spawn(self):
        a = self.doubler.spawn(4)
        a.reserve('foo')
        print a.handle().get()

    def test_create_args(self):
        a = self.doubler.create((4,))
        Actor.send(a.id, 'run')
        a.reserve('foo')
        self.assertEqual(8, a.handle().get())

    def test_curry(self):
        a = self.doubler.create()
        a.curry(4)
        Actor.send(a.id, 'run')
        a.reserve('foo')
        self.assertEqual(8, a.handle(raise_errors=True).get())

class TestCanvas(unittest.TestCase):
    
    def setUp(self):
        ming.config.configure_from_nested_dict(dict(
                cleese=dict(uri='mim://')))
        ming.mim.Connection.get().clear_all()
        self.worker = Worker('test')
        self.doubler = FunctionActor.decorate('double')(self._double)
        self.fact = FunctionActor.decorate('fact')(self._fact)
        self.sum = FunctionActor.decorate('sum')(sum)

    def _double(self, x):
        return x * 2

    def _fact(self, x, acc=1):
        print '_fact(%s, %s)' % (x, acc)
        if x == 0: return acc
        raise exc.Chain(g.actor.id, 'run', x-1, acc*x)

    def test_group(self):
        subtasks = [ self.doubler.create((x,)) for x in range(5) ]
        a = Group.spawn(subtasks)
        self.worker.run_all()
        a.refresh()
        self.assertEqual(a.result.get(), [0, 2, 4, 6, 8 ])

    def test_pipeline(self):
        st0 = self.doubler.create((2,))
        st1 = self.doubler.create()
        a = Pipeline.spawn([st0, st1])
        self.worker.run_all()
        a.refresh()
        self.assertEqual(a.result.get(), 8)

    def test_pipeline_immutable(self):
        st0 = self.doubler.create((2,))
        st1 = self.doubler.create((2,), immutable=True)
        a = Pipeline.spawn([st0, st1])
        self.worker.run_all()
        a.refresh()
        self.assertEqual(a.result.get(), 4)

    def test_chord(self):
        subids = [self.doubler.create((x,)).id for x in range(5) ]
        a0 = Group.create(args=(subids,))
        a1 = self.sum.create()
        a = Pipeline.spawn([a0, a1])
        self.assertEqual(8, M.ActorState.m.find().count())
        self.worker.run_all()
        a.refresh()
        self.assertEqual(20, a.result.get())
        self.assertEqual(1, M.ActorState.m.find().count())
        a.forget()
        self.assertEqual(0, M.ActorState.m.find().count())
        
    def test_chain(self):
        a = self.fact.create(immutable=True)
        a.start((4,))
        self.worker.run_all()
        a.refresh()
        self.assertEqual(24, a.result.get())
