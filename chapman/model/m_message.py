import logging
from datetime import datetime
from cPickle import loads
from random import getrandbits

from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_base import doc_session, dumps, ChannelProxy
from .m_task import TaskState, TaskStateResource
from .m_semaphore import SemaphoreResource

log = logging.getLogger(__name__)


class Message(Document):
    missing_worker = '-' * 10
    channel = ChannelProxy('chapman.event')

    class __mongometa__:
        name = 'chapman.message'
        session = doc_session
        indexes = [
            [('s.status', 1), ('s.pri', -1), ('s.ts', 1), ('s.q', 1)],
            [('s.q', 1), ('s.status', 1), ('s.sub_status', -1), ('s.pri', -1), ('s.ts', 1)],
            [('task_id', 1)],
        ]
    _id = Field(int, if_missing=lambda: getrandbits(63))
    task_id = Field(int, if_missing=None)
    task_repr = Field(str, if_missing=None)
    slot = Field(str)
    _args = Field('args', S.Binary)
    _kwargs = Field('kwargs', S.Binary)
    _send_args = Field('send_args', S.Binary)
    _send_kwargs = Field('send_kwargs', S.Binary)
    schedule = Field('s', dict(
        status=S.String(if_missing='pending'),
        sub_status=S.Int(if_missing=0),
        ts=S.DateTime(if_missing=datetime.utcnow),
        after=S.DateTime(if_missing=datetime.fromtimestamp(0)),
        q=S.String(if_missing='chapman'),
        pri=S.Int(if_missing=10),
        w=S.String(if_missing=missing_worker),
        semaphores=[str]))

    def __repr__(self):
        return '<msg (%s:%s) %s to %s %s on %s>' % (
            self.schedule.status, self.schedule.sub_status,
            self._id, self.slot, self.task_repr,
            self.schedule.w)

    @classmethod
    def n(cls, task, slot, *args, **kwargs):
        '''Convenience method for Message.new'''
        return cls.new(task, slot, args, kwargs)

    @classmethod
    def new(cls, task, slot, args, kwargs, after=None):
        if args is None:
            args = ()
        if kwargs is None:
            kwargs = {}
        self = cls.make(dict(
            task_id=task.id,
            task_repr=repr(task),
            slot=slot,
            s=task.schedule_options()))
        if after is not None:
            self.s.after = after
        self.args = args
        self.kwargs = kwargs
        self.m.insert()
        return self

    @classmethod
    def reserve(cls, worker, queues):
        '''Reserve a message & try to lock the task state.

        - If no message could be reserved, return (None, None)
        - If a message was reserved, but the resources could not be acquired,
          return (msg, None)
        - If a message was reserved, and the resources were acquired, return
          (msg, task)
        '''
        qspec = {'$in': queues}
        return cls._reserve(worker, qspec)

    @classmethod
    def reserve_qspec(cls, worker, qspec):
        '''Reserve according to a queue specification to allow for
        more interesting queue topologies
        '''
        return cls._reserve(worker, qspec)

    def retire(self):
        '''Retire the message.'''
        self._release_resources()
        self.m.delete()

    def unlock(self):
        '''Make a message ready for processing'''
        self._release_resources()
        # Re-dispatch this message
        self.__class__.m.update_partial(
            {'_id': self._id},
            {'$set': {
                's.status': 'ready',
                's.sub_status': 0,
                's.w': self.missing_worker}})
        self.channel.pub('send', self._id)

    def send(self, *args, **kwargs):
        self.m.set(
            {'s.status': 'ready',
             's.ts': datetime.utcnow(),
             'send_args': dumps(args),
             'send_kwargs': dumps(kwargs)})
        self.channel.pub('send', self._id)

    @property
    def args(self):
        result = []
        if self._send_args is not None:
            result += loads(self._send_args)
        if self._args is not None:
            result += loads(self._args)
        return tuple(result)

    @args.setter
    def args(self, value):
        self._args = dumps(value)

    @property
    def kwargs(self):
        result = {}
        if self._kwargs is not None:
            result.update(loads(self._kwargs))
        if self._send_kwargs is not None:
            result.update(loads(self._send_kwargs))
        return result

    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = dumps(value)

    @property
    def resources(self):
        for sem in self.s.semaphores:
            yield SemaphoreResource(sem)
        yield TaskStateResource(self.task_id)

    @classmethod
    def _reserve(cls, worker, qspec):
        '''Reserves a message.'''
        now = datetime.utcnow()
        # Begin acquisition of resources
        self = cls.m.find_and_modify(
            {'s.q': qspec, 's.status': 'ready', 's.after': {'$lte': now}},
            sort=[('s.sub_status', -1), ('s.pri', -1), ('s.ts', 1)],
            update={'$set': {'s.w': worker, 's.status': 'acquire'}},
            new=True)
        if self is None:
            return None, None

        # Acquire any resources necessary
        for i, res in enumerate(self.resources):
            if i < self.s.sub_status:  # already acquired
                continue
            if not res.acquire(self._id):
                self.m.set({'s.status': 'queued'})
                return self, None
            else:
                res = cls.m.update_partial(
                    {'_id': self._id},
                    {'$set': {'s.sub_status': i + 1}})
        self.m.set({'s.status': 'busy'})
        return self, TaskState.m.get(_id=self.task_id)

    def _release_resources(self):
        msg_id = None
        for res in reversed(list(self.resources)):
            to_release = res.release(self._id)
            res = Message.m.update_partial(
                {'_id': {'$in': to_release}, 's.status': 'queued'},
                {'$set': {'s.status': 'ready'}},
                multi=True)
            if to_release and res['updatedExisting']:
                msg_id = to_release[0]
        if msg_id is not None:
            self.channel.pub('send', msg_id)


