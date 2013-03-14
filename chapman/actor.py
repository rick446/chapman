__all__ = ( 'Actor', 'Result' )
import sys
import time
import logging

from . import exc
from . import meta
from . import model as M
from .context import g
from .decorators import slot

log = logging.getLogger(__name__)

class Actor(object):
    __metaclass__ = meta.SlotsMetaclass
    _registry = {}
    _options = {}

    def __init__(self, state):
        self._state = state

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._state._id)

    @slot()
    def trampoline_chain_result(self, result):
        'trampoline the result up the callback chain'
        if str(result.actor_id) not in self._state.data['chain']:
            print self._state.data['chain']
            import ipdb; ipdb.set_trace()
        chain_data = self._state.data['chain'][str(result.actor_id)][0]
        M.ActorState.m.update_partial(
            { '_id': self.id },
            { '$pop': { 'data.chain.%s' % result.actor_id: -1 } } )
        log.info('Trampoline %r to %r', result, chain_data)
        g.message.update(**chain_data)
        result.actor_id = self.id
        return result

    @classmethod
    def create(cls, args=None, kwargs=None, **options):
        '''Create an actor instance'''
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        all_options = dict(cls._options)
        all_options.update(options)
        state = M.ActorState.make(dict(type=cls.name, options=all_options))
        state.update_data(cargs=list(args), ckwargs=kwargs)
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
        if state is None: return None
        ActorClass = cls.by_name(state.type)
        return ActorClass(state)

    @property
    def id(self):
        return self._state._id

    @property
    def result(self):
        return self._state.get_data('result')

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

    def start(self, args=None, kwargs=None, cb_id=None, cb_slot='run'):
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
                if self._state.options.ignore_result:
                    log.info('Forget all about %r', self)
                    self.forget()
                else:
                    self._state.unlock('complete')
                return result
            except exc.Suspend, s:
                self._state.unlock(s.status)
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
        '''Updates by setting pickled values'''
        return self._state.update_data(**kwargs)

    def update_data_raw(self, **kwargs):
        '''Updates by setting unpickled values'''
        return self._state.update_data_raw(**kwargs)

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

    def chain(self, slot, *args, **kwargs):
        # Update the current actor's 'data.chain.OBJECTID' field
        M.ActorState.m.update_partial(
            { '_id': g.actor.id },
            { '$push': {
                    'data.chain.%s' % self.id: dict(
                        cb_id=g.message['cb_id'],
                        cb_slot=g.message['cb_slot']) } } )
        # Send a message to the next actor in the chain, telling *it* to call
        # back to the current trampoline_chain_result slot
        M.ActorState.send(
            self.id, slot, args, kwargs,
            g.actor.id, 'trampoline_chain_result')
        # Disable callback on this message (it will be handled in trampoline_chain_result
        g.message['cb_id'] = g.message['cb_slot'] = None
        raise exc.Suspend('ready')

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
