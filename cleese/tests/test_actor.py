import bson
import unittest

import ming

from cleese import Actor, FunctionActor, Worker
from cleese import exc


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

    
