P__all__ = ( 'Actor', 'Result' )
import sys

from . import exc
from . import meta
from . import model as M
from .context import g

class Actor(object):
    __metaclass__ = meta.SlotsMetaclass
    _registry = {}

    def __init__(self, state):
        self._state = state

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._state._id)

    @classmethod
    def create(cls, args=None, kwargs=None, immutable=False):
        '''Create an actor instance'''
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        state = M.ActorState.make(dict(type=cls.name,
                                       immutable=immutable))
        state.data = dict(cargs=list(args), ckwargs=kwargs)
        state.m.insert()
        return cls(state)

    @classmethod
    def send(cls, id, slot, args=None, kwargs=None, cb_id=None, cb_slot=None):
        return M.ActorState.send(id, slot, args, kwargs, cb_id, cb_slot)

    @classmethod
    def spawn(cls, *args, **kwargs):
        obj = cls.create(args=args, kwargs=kwargs)
        obj.start()
        obj.refresh()
        return obj

    @classmethod
    def by_id(cls, id):
        state = M.ActorState.m.get(_id=id)
        ActorClass = cls.by_name(state.type)
        return ActorClass(state)

    @property
    def id(self):
        return self._state._id

    @property
    def result(self):
        return self._state.data['result']

    def start(self, args=None, kwargs=None, cb_id=None, cb_slot=None):
        self.send(self.id, 'run', args, kwargs, cb_id, cb_slot)

    def reserve(self, worker):
        '''Reserve a single message for the actor

        This is mainly used for testing. In normal cases, you'd use
        ActorState.reserve to get the next available actor/message to handle.
        '''
        self._state = M.ActorState.reserve(worker=worker, actor_id=self.id)

    def handle(self, raise_errors=False):
        msg = self._state.active_message()
        with g.set_context(self, msg):
            method = getattr(self, msg['slot'])
            try:
                result = method(*msg['args'], **msg['kwargs'])
                if not isinstance(result, Result):
                    result = Result.success(self.id, result)
            except exc.Chain, c:
                M.ActorState.send(c.actor_id, c.slot, c.args, c.kwargs,
                                  msg['cb_id'], msg['cb_slot'])
                self._state.unlock()
                return
            except:
                if raise_errors:
                    raise
                result = Result.failure(self.id,
                                        'Error in %r' % self, *sys.exc_info())
            self.update_data(result=result)
            if msg['cb_id']:
                M.ActorState.send(msg['cb_id'], msg['cb_slot'], (result,))
            self._state.unlock()
            return result

    def refresh(self):
        self._state = M.ActorState.m.get(_id=self.id)

    def update_data(self, **kwargs):
        data = self._state.data
        data.update(kwargs)
        self._state.data = data
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$set': { 'data': self._state._data } })

    def forget(self):
        M.ActorState.m.remove({'_id': self.id})

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
