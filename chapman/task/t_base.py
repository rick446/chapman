import time
import logging

from chapman.context import g
from chapman.meta import RegistryMetaclass
from chapman.model import TaskState, Message
from chapman import exc

log = logging.getLogger(__name__)


class Task(object):
    __metaclass__ = RegistryMetaclass
    _registry = {}

    def __init__(self, state):
        self._state = state

    def __repr__(self):
        return '<%s %s %s>' % (
            self.__class__.__name__,
            self._state._id,
            self._state.options.path)

    def run(self, msg, raise_errors=False):
        '''Do the work of the task'''
        raise NotImplementedError('run')

    @property
    def id(self):
        return self._state._id

    @property
    def status(self):
        return self._state.status

    @property
    def path(self):
        return self._state.options.path

    @property
    def result(self):
        return self._state.result

    @classmethod
    def new(cls, data, status='pending', **options):
        options.setdefault('path', getattr(g, 'path', None))
        state = TaskState.make(dict(
            type=cls.name,
            status=status,
            options=options,
            data=data))
        state.m.insert()
        return cls(state)

    @classmethod
    def from_state(cls, state):
        Class = cls.by_name(state.type)
        return Class(state)

    @classmethod
    def by_id(cls, id):
        state = TaskState.m.get(_id=id)
        Class = cls.by_name(state.type)
        return Class(state)

    def set_options(self, **kwargs):
        updates = dict(
            ('options.' + k, v)
            for k, v in kwargs.items())
        self._state.m.set(updates)

    def schedule_options(self):
        return dict(
            q=self._state.options.queue,
            pri=self._state.options.priority,
            semaphores=self._state.options.semaphores)

    def refresh(self):
        self._state = TaskState.m.get(_id=self.id)

    def wait(self, timeout=-1):
        start = time.time()
        self.refresh()
        while self._state.status in ('pending', 'active'):
            if timeout >= 0 and time.time() - start > timeout:
                break
            chan = Message.channel.new_channel()
            for ev in chan.cursor(await=True):
                break
            self.refresh()

    def start(self, *args, **kwargs):
        '''Send a 'run' message & update state'''
        self._state.m.set(dict(status='active'))
        msg = Message.new(self, 'run', args, kwargs, send=True)
        return msg

    def schedule(self, after, *args, **kwargs):
        '''Send a 'run' message & update state'''
        self._state.m.set(dict(status='pending'))
        msg = Message.new(self, 'run', args, kwargs, after=after)
        msg.send()
        return msg

    def link(self, task, slot, *args, **kwargs):
        '''Add an on_complete message with the given args'''
        msg = Message.n(task, slot, *args, **kwargs)
        self._state.m.set(dict(on_complete=msg._id))
        return msg

    def complete(self, result):
        state = self._state
        TaskState.set_result(state._id, result)
        if state.on_complete:
            msg = Message.m.get(_id=state.on_complete)
            if msg is not None:
                msg.send(result)
        if state.options.ignore_result and result.status == 'success':
            self.forget()
        else:
            self.refresh()
            chan = Message.channel.new_channel()
            chan.pub('complete', state._id)

    def forget(self):
        self._state.m.delete()

    def handle(self, msg):
        while msg:
            with g.set_context(task=self, message=msg):
                if self._state.status in ('success', 'failure'):
                    log.warning(
                        'Ignoring message to %s task: %r',
                        self._state.status, msg)
                else:
                    method = getattr(self, msg.slot)
                    method(msg)
                msg.retire()
                msg = None


class Result(object):

    def __init__(self, task_id, status, data):
        self.task_id = task_id
        self.status = status
        self.data = data

    def __repr__(self):  # pragma no cover
        return '<Result %s for %s>' % (
            self.status, self.task_id)

    @classmethod
    def success(cls, task_id, value):
        return cls(task_id, 'success', value)

    @classmethod
    def failure(cls, task_id, message, ex_type, ex_value, tb):
        return cls(
            task_id, 'failure',
            exc.TaskError.from_exc_info(
                message, ex_type, ex_value, tb))

    def forget(self):
        TaskState.m.remove({'_id': self.task_id})

    def get(self):
        if self.status == 'success':
            return self.data
        elif self.status == 'failure':
            raise self.data
        else:  # pragma no cover
            assert False
