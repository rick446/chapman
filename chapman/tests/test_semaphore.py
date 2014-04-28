from chapman.task import Function
from chapman import model as M
from chapman.decorators import task

from .test_base import TaskTest

class TestSemaphoreTask(TaskTest):

    def setUp(self):
        super(TestSemaphoreTask, self).setUp()
        self.echo1 = Function.decorate(
            'echo1', semaphores=['foo'])(self._echo)
        self.echo2 = Function.decorate(
            'echo2', semaphores=['foo', 'bar'])(self._echo)
        self.sem0 = M.Semaphore.make(dict(_id='foo', value=2))
        self.sem0.m.save()
        self.sem1 = M.Semaphore.make(dict(_id='bar', value=2))
        self.sem1.m.save()

    def test_cant_acquire_too_many(self):
        msgs = [M.Message.n(self.echo1.n('hi'), 'run') for i in range(5)]
        for msg in msgs:
            msg.send()
        # Should acquire first 2 and queue final 2
        for x in range(4):
            msg, ts = M.Message.reserve('foo', ['chapman'])
            if x < 2:
                self.assertEqual(msg.s.status, 'busy')
            else:
                self.assertEqual(msg.s.status, 'queued')
                assert ts is None
        msgs[0].retire()
        msg, ts = M.Message.reserve('foo', ['chapman'])
        self.assertEqual(msg.s.status, 'busy')
        msg, ts = M.Message.reserve('foo', ['chapman'])
        self.assertEqual(msg.s.status, 'queued')
        print M.Semaphore.m.get()

    def test_multi_sem(self):
        msgs = [
            M.Message.n(self.echo1.n('hi'), 'run'),
            M.Message.n(self.echo1.n('hi'), 'run'),
            M.Message.n(self.echo2.n('hi'), 'run'),
            M.Message.n(self.echo2.n('hi'), 'run')]
        for m in msgs:
            m.send()
        for x in range(4):
            msg, ts = M.Message.reserve('foo', ['chapman'])
            if x < 2:
                self.assertEqual(msg.s.status, 'busy')
            else:
                self.assertEqual(msg.s.status, 'queued')
                assert ts is None
        print M.Semaphore.m.get(_id='foo')
        print M.Semaphore.m.get(_id='bar')
        msgs[0].retire()
        msg, ts = M.Message.reserve('foo', ['chapman'])
        print M.Semaphore.m.get(_id='foo')
        print M.Semaphore.m.get(_id='bar')


    def test_sem_doesnt_muck_things_up(self):
        runs = []
        @task(raise_errors=True, semaphores=['foo'])
        def _save_arg_in_runs(arg):
            runs.append(arg)
        _save_arg_in_runs.spawn(10)
        self._handle_messages()
        self.assertEqual(runs, [10])
        print self.sem0

    def _echo(self, x):
        return x

