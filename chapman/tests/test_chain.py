from chapman.task import Function, Chain
from chapman import model as M
from chapman import exc

from .test_base import TaskTest


class TestChain(TaskTest):

    def test_chain_ok(self):
        @Function.decorate('fact')
        def fact(n, acc=1):
            print 'fact(%s, %s)' % (n, acc)
            if n > 1:
                raise Chain.call(fact.n(), n - 1, n * acc)
            return acc
        t = fact.n()
        t.start(5)
        self._handle_messages()
        t.refresh()
        self.assertEqual(t.result.get(), 120)
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)

    def test_chain_error(self):
        @Function.decorate('fact_err')
        def fact_err(n, acc=1):
            print 'fact(%s, %s)' % (n, acc)
            if n == 2:
                raise TypeError('fact')
            elif n > 1:
                raise Chain.call(fact_err.n(), n - 1, n * acc)
            else:
                return acc
        t = fact_err.n()
        t.start(5)
        self._handle_messages()
        t.refresh()
        with self.assertRaises(exc.TaskError) as err:
            t.result.get()
        self.assertEqual(err.exception.args[0], TypeError)
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)
