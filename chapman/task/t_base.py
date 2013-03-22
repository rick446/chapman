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

    @property
    def id(self):
        return self._state._id

    @property
    def result(self):
        return self._state.result

    @classmethod
    def new(cls, data, status='active', **options):
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
            for k,v in kwargs.items())
        self._state.m.set(updates)

    def schedule_options(self):
        return dict(
            q=self._state.options.queue,
            pri=self._state.options.priority)

    def refresh(self):
        self._state = TaskState.m.get(_id=self.id)

    def run(self, msg, raise_errors=False):
        '''Do the work of the task'''
        raise NotImplementedError, 'run'

    def error(self, msg):
        self.complete(msg.args[0])

    def start(self, *args, **kwargs):
        '''Send a 'run' message & update state'''
        msg = Message.new(self, 'run', args, kwargs)
        msg.send()
        return msg

    def link(self, task, slot, *args, **kwargs):
        '''Add an on_complete message with the given args'''
        msg = Message.n(task, slot, *args, **kwargs)
        self._state.m.set(dict(on_complete=msg._id))
        return msg

    def complete(self, result):
        TaskState.set_result(self.id, result)
        if self._state.on_complete:
            msg = Message.m.get(_id=self._state.on_complete)
            if result.status == 'success':
                msg.send(result)
            else:
                msg.m.set(dict(slot='error'))
                msg.send(result)
            if not self._state.options.preserve_result:
                self.forget()
        elif ( self._state.options.ignore_result
               and not self._state.options.preserve_result
               and result.status == 'success'):
            self.forget()
        else:
            self.refresh()

    def forget(self):
        self._state.m.delete()

    def handle(self, msg):
        with g.set_context(self, msg):
            if self._state.status == 'complete': # pragma no cover
                log.warning('Ignoring message to complete task: %r', msg)
            else:
                method = getattr(self, msg.slot)
                method(msg)
            msg.retire()

class Result(object):

    def __init__(self, task_id, status, data):
        self.task_id = task_id
        self.status = status
        self.data = data

    def __repr__(self): # pragma no cover
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

    def get(self):
        if self.status == 'success':
            return self.data
        elif self.status == 'failure':
            raise self.data
        else: # pragma no cover
            assert False

