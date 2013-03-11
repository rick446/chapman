__all__ = ( 'Actor', 'Result' )
import sys

from . import exc
from . import meta
from . import model as M

class Actor(object):
    __metaclass__ = meta.SlotsMetaclass
    _registry = {}

    def __init__(self, state):
        self._state = state

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._state._id)

    @classmethod
    def create(cls):
        '''Create an actor instance'''
        state = M.ActorState.make(dict(type=cls.name))
        state.m.insert()
        return cls(state)

    @classmethod
    def send(cls, id, slot, args=None, kwargs=None, cb_id=None, cb_slot=None):
        return M.ActorState.send(id, slot, args, kwargs, cb_id, cb_slot)

    @property
    def id(self):
        return self._state._id

    def reserve(self, worker):
        '''Reserve a single message for the actor

        This is mainly used for testing. In normal cases, you'd use
        ActorState.reserve to get the next available actor/message to handle.
        '''
        self._state = M.ActorState.reserve(worker=worker, actor_id=self.id)

    def handle(self, raise_errors=False):
        msg = self._state.active_message()
        method = getattr(self, msg['slot'])
        try:
            result = Result.success(self.id, method(*msg['args'], **msg['kwargs']))
        except:
            if raise_errors:
                raise
            result = Result.failure(self.id, 'Error in %r' % self, *sys.exc_info())
        if msg['cb_id']:
            M.ActorState.send(msg['cb_id'], msg['cb_slot'], (result,))
        return result

    def refresh(self):
        self._state = M.ActorState.m.get(_id=self.id)

class Result(object):

    def __init__(self, actor_id, status, data):
        self.actor_id = actor_id
        self.status = status
        self.data = data

    @classmethod
    def success(cls, actor_id, value):
        return cls(actor_id, 'success', value)

    @classmethod
    def failure(cls, actor_id, message, ex_type, ex_value, tb):
        return cls(
            actor_id, 'failure',
            exc.ActorError.from_exc_info(
                message, ex_type, ex_value, tb))

    def get(self):
        if self.status == 'success':
            return self.data
        else:
            raise self.data
