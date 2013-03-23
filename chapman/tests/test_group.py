from chapman.task import Function, Group
from chapman import model as M
from chapman import exc
from chapman.decorators import task

from .test_base import TaskTest

class TestGroup(TaskTest):

    def test_two(self):
        t = Group.n(
            self.doubler.n(),
            self.doubler.n())
        t.start(2)
        self._handle_messages()
        t.refresh()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)
        self.assertEqual(t.result.get(), [4,4])

    def test_two_ignore(self):
        runs = []
        @task(raise_errors=True)
        def _save_arg_in_runs(arg):
            runs.append(arg)
        t = Group.new(
            [ _save_arg_in_runs.n(),
              _save_arg_in_runs.n() ],
            ignore_result=True)
        t.start(10)
        # Start the group and the subtasks
        self._handle_messages(limit=3)
        self.assertEqual(M.Message.m.find().count(), 2)
        self.assertEqual(M.TaskState.m.find().count(), 3)
        self.assertEqual(runs, [ 10, 10 ])
        self._handle_messages(limit=1)
        self.assertEqual(M.Message.m.find().count(), 1)
        self.assertEqual(M.TaskState.m.find().count(), 2)
        self._handle_messages(limit=1)
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 0)

    def test_group_order_invert(self):
        t = Group.n()
        st0 = self.doubler.n()
        st1 = self.doubler.n()
        self.assertEqual(M.TaskState.m.find().count(), 3)
        self.assertEqual(M.Message.m.find().count(), 0)
        t.append(st0); 
        t.append(st1); 
        self.assertEqual(M.Message.m.find().count(), 2)
        self.assertEqual(M.TaskState.m.find().count(), 3)
        st0.start(2)
        st1.start(3)        
        self.assertEqual(M.Message.m.find().count(), 4)
        self._handle_messages()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 3)
        t.start()
        self._handle_messages()
        t.refresh()
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(t.result.get(), [4,6])

    def test_two_err(self):
        @Function.decorate('doubler_result')
        def err(x):
            raise TypeError, 'Always raises error'
        t = Group.n(
            self.doubler.n(),
            self.doubler.n(),
            err.n())
        t.start(2)
        self._handle_messages()
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
        self.assertEqual(M.Message.m.find().count(), 0)
        self.assertEqual(M.TaskState.m.find().count(), 1)

    def test_enqueue_while_busy(self):
        t = Group.n(
            self.doubler.n(),
            self.doubler.n())
        t.start(2)
        m_start_group, s = self._reserve_message()
        self._handle_message(m_start_group, s)
        m0, s0 = self._reserve_message()
        m1, s1 = self._reserve_message()
        self.assertEqual(m0.slot, 'run')
        self.assertEqual(m1.slot, 'run')
        self._handle_message(m0, s0)
        self._handle_message(m1, s1)
        m0, s0 = self._reserve_message()
        m1, s1 = self._reserve_message()
        self.assertEqual(m0.slot, 'retire_subtask')
        self.assertEqual(m1.slot, 'retire_subtask')
        self.assertIsNotNone(s0)
        self.assertIsNone(s1)
        self.assertEqual([m0._id, m1._id], M.TaskState.m.get(_id=s0._id).mq)
        self._handle_message(m0, s0)
        self.assertEqual([m1._id], M.TaskState.m.get(_id=s0._id).mq)
        m, s = self._reserve_message()
        self.assertEqual(m.slot, 'retire_subtask')
        self.assertIsNotNone(s)
        self._handle_message(m, s)
        t.refresh()
        self.assertEqual([4,4], t.result.get())

