__all__ = ( 'Actor', 'Result' )
import sys
import time
import logging

from . import exc
from . import meta
from . import model as M
from .context import g

log = logging.getLogger(__name__)

class Actor(object):
    __metaclass__ = meta.SlotsMetaclass
    _registry = {}
    _options = {}

    def __init__(self, state):
        self._state = state

    def __repr__(self):
        return '<%s %s>' % (self.__class__.__name__, self._state._id)

    @classmethod
    def create(cls, args=None, kwargs=None, cb_id=None, **options):
        '''Create an actor instance'''
        if args is None: args = ()
        if kwargs is None: kwargs = {}
        all_options = dict(cls._options)
        all_options.update(options)
        state = M.ActorState.make(dict(
                type=cls.name, options=all_options,
                cb_id=cb_id))
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
        return self._state.result

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

    def start(self, args=None, kwargs=None):
        M.Message.post(self.id, args=args, kwargs=kwargs)

    def handle(self, msg, raise_errors=False):
        with g.set_context(self, msg):
            method = getattr(self, msg.slot)
            try:
                result = method(*msg.args, **msg.kwargs)
                result = self._retire(msg, result)
                return result
            except exc.Suspend, s:
                self._state.unlock(msg, s.status)
            except:
                if raise_errors:
                    raise
                result = Result.failure(
                    self.id, 'Error in %r' % self, *sys.exc_info())
                result = self._retire(msg, result)
                return result

    def _retire(self, message, result):
        if not isinstance(result, Result):
            result = Result.success(self.id, result)
        result = self._state.retire(message, result)
        # M.Message.post_callback(self._state.cb_id, result)
        if self._state.options.ignore_result and not self._state.parent_id:
            log.info('Forget all about %r', self)
            self.forget()
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
        '''child.chain('run', ...) => continue execution in the child actor'''
        M.ActorState.chain(
            parent_id=g.actor.id,
            message=g.message,
            child_id=self.id,
            slot=slot,
            args=args,
            kwargs=kwargs)
        raise exc.Suspend('chained')

class Result(object):

    def __init__(self, actor_id, status, data):
        self.actor_id = actor_id
        self.status = status
        self.data = data

    def __repr__(self):
        return '<Result %s for %s>' % (
            self.status, self.actor_id)

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
        elif self.status == 'failure':
            raise self.data
        else:
            assert False

