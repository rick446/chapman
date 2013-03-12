__all__ = ( 'Actor', 'Result' )
import sys
import time

from . import exc
from . import meta
from . import model as M
from .context import g

class Actor(object):
    __metaclass__ = meta.SlotsMetaclass
    _registry = {}
    _options = {}

    def __init__(self, state):
        self._state = state

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._state._id)

    @classmethod
    def create(cls, args=None, kwargs=None, **options):
        '''Create an actor instance'''
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        all_options = dict(cls._options)
        all_options.update(options)
        state = M.ActorState.make(dict(type=cls.name, options=all_options))
        state.data = dict(cargs=list(args), ckwargs=kwargs)
        state.m.insert()
        return cls(state)

    @classmethod
    def s(cls, *args, **kwargs):
        return cls.create(args=args, kwargs=kwargs)


    @classmethod
    def si(cls, *args, **kwargs):
        return cls.create(args=args, kwargs=kwargs, immutable=True)

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

    def wait(self, timeout=None):
        start = time.time()
        while True:
            self.refresh()
            if self._state.status == 'complete':
                return self
            now = time.time()
            if timeout is not None:
                if now-start > timeout:
                    raise exc.Timeout()

    def get(self, timeout=None, forget=True):
        self.wait(timeout)
        result = self.result
        if forget:
            self.forget()
        return result.get()

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
                self.update_data(result=result)
                if msg['cb_id']:
                    M.ActorState.send(
                        msg['cb_id'], msg['cb_slot'], (result,))
                self._state.unlock('complete')
                if self._state.options.ignore_result:
                    self.forget()
                else:
                    self._state.unlock('complete')
                return result
            except exc.Suspend:
                self._state.unlock('ready')
            except exc.Chain, c:
                M.ActorState.send(
                    c.actor_id, c.slot, c.args, c.kwargs,
                    msg['cb_id'], msg['cb_slot'])
                self._state.unlock('ready')
            except:
                if raise_errors:
                    raise
                result = Result.failure(
                    self.id, 'Error in %r' % self, *sys.exc_info())
                self.update_data(result=result)
                if msg['cb_id']:
                    M.ActorState.send(msg['cb_id'], msg['cb_slot'], (result,))
                self._state.unlock('error')
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

    def set_options(self, **kwargs):
        updates = dict(
            ('options.%s' % k, v)
            for k,v in kwargs.items())
        self._state.options.update(kwargs)
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$set': updates })

    def forget(self):
        self._state.m.delete()

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
