from datetime import datetime

from mongotools.util import LazyProperty
from mongotools.pubsub import Channel
from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_base import doc_session, pickle_property, dumps
from .m_task import TaskState

class ChannelProxy(object):

    def __init__(self, name):
        self._name = name

    @LazyProperty
    def _channel(self):
        return self.new_channel()

    def __getattr__(self, name):
        return getattr(self._channel, name)

    def __get__(self, obj, cls=None):
        if obj is None:
            return self
        return self._channel

    def new_channel(self):
        return Channel(doc_session.db, self._name)

class Message(Document):
    missing_worker = '-' * 10
    channel = ChannelProxy('chapman.event')
    class __mongometa__:
        name = 'chapman.message'
        session = doc_session
    _id=Field(S.ObjectId)
    task_id=Field(S.ObjectId, if_missing=None)
    task_repr=Field(str, if_missing=None)
    slot=Field(str)
    _args=Field('args', S.Binary)
    _kwargs=Field('kwargs', S.Binary)
    schedule = Field('s', dict(
            status=S.String(if_missing='pending'),
            ts=S.DateTime(if_missing=datetime.utcnow),
            q=S.String(if_missing='chapman'),
            pri=S.Int(if_missing=10),
            w=S.String(if_missing=missing_worker)))

    def __repr__(self):
        return '<msg (%s) %s to %s %s on %s>' % (
            self.schedule.status, self._id, self.slot, self.task_repr, 
            self.schedule.w)

    @classmethod
    def s(cls, task, slot, *args, **kwargs):
        self = cls.make(dict(
                task_id=task.id,
                task_repr=repr(task),
                slot=slot))
        self.args = args
        self.kwargs = kwargs
        self.m.insert()
        return self

    @classmethod
    def _reserve_next(cls, worker, queues):
        '''Reserves a message in 'next' status.

        'next' messages can be immediately worked on, since they are guaranteed
        to be the next message to obtain the task lock.
        '''
        self = cls.m.find_and_modify(
            { 's.status': 'next',
              's.q': { '$in': queues } },
            sort=[('s.pri', -1), ('s.ts', 1) ],
            update={'$set': { 's.w': worker, 's.status': 'busy' } },
            new=True)
        if self is None: return None, None
        state = TaskState.m.get(_id=self.task_id)
        return self, state
        
    @classmethod
    def _reserve_ready(cls, worker, queues):
        '''Reserves a message in 'ready' status.

        Ready messages must move through 'queued' status before they become
        'busy', since there may already be a message locking the task.
        '''
        # Reserve message
        self = cls.m.find_and_modify(
            { 's.status': 'ready',
              's.q': { '$in': queues } },
            sort=[('s.pri', -1), ('s.ts', 1) ],
            update={'$set': { 's.w': worker, 's.status': 'queued' } },
            new=True)
        if self is None: return None, None
        # Enqueue on TaskState
        state = TaskState.m.find_and_modify(
            { '_id': self.task_id },
            update={'$push': { 'mq': self._id } },
            new=True)
        if state.mq[0] == self._id:
            # We are the first in the queue, so we get to go
            self.m.set({'s.status': 'busy'})
            return self, state
        else:
            return self, None
                    
    @classmethod
    def reserve(cls, worker, queues):
        '''Reserve a message & try to lock the task state.

        - If no message could be reserved, return (None, None)
        - If a message was reserved, but the task could not be locked, return
          (msg, None)
        - If a message was reserved, and the task was locked, return
          (msg, task)
        '''
        msg, state = cls._reserve_next(worker, queues)
        if state is not None: return msg, state
        return cls._reserve_ready(worker, queues)

    def retire(self):
        '''Retire the message.'''
        state = TaskState.m.find_and_modify(
            { '_id': self.task_id },
            update={ '$pull': { 'mq': self._id } },
            new=True)
        if state is not None and state.mq:
            next_msg = Message.m.find_and_modify(
                { '_id': state.mq[0], 's.status': 'queued' },
                update={ '$set': { 's.status': 'next' } },
                new=True)
            if next_msg:
                self.channel.pub('send', next_msg._id)
        self.m.delete()

    def send(self, *args, **kwargs):
        new_args = args + self.args
        new_kwargs = self.kwargs
        new_kwargs.update(kwargs)
        self.m.set(
            { 's.status': 'ready',
              'args': dumps(new_args),
              'kwargs': dumps(new_kwargs) })
        self.channel.pub('send', self._id)

    args = pickle_property('_args')
    kwargs = pickle_property('_kwargs')


