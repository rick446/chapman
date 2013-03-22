import unittest

import ming
from mongotools import mim

from chapman.task import Task, Function
from chapman import model as M

class TaskTest(unittest.TestCase):

    def setUp(self):
        M.doc_session.bind = ming.create_datastore(
            'test', bind=ming.create_engine(
                use_class=lambda *a,**kw: mim.Connection.get()))
        mim.Connection.get().clear_all()
        self.doubler = Function.decorate('double')(self._double)

    def _double(self, x):
        return x * 2

    def _handle_messages(self, worker='foo', queues=None):
        while True:
            m,s = self._reserve_message(worker, queues)
            if s is None: break
            self._handle_message(m, s)

    def _reserve_message(self, worker='foo', queues=None):
        if queues is None: queues = ['chapman']
        return M.Message.reserve('foo', ['chapman'])

    def _handle_message(self, msg, state):
        print 'Handling %s' % msg
        print '... task is %s' % state.status
        task = Task.from_state(state)
        task.handle(msg)
        

