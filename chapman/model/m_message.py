from cPickle import loads, dumps

from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_session import doc_session
from .m_event import notify

class Message(Document):
    class __mongometa__:
        name = 'chapman.message'
        session = doc_session
    _id=Field(S.ObjectId)
    task_id=Field(S.ObjectId, if_missing=None)
    slot=Field(str)
    _status=Field('status', str, if_missing='pending')
    _args=Field('args', S.Binary)
    _kwargs=Field('kwargs', S.Binary)

    @classmethod
    def s(cls, task_id, slot, *args, **kwargs):
        self = cls.make(
            task_id=task_id,
            slot=slot)
        self.args = args
        self.kwargs = kwargs
        self.m.insert()
        return self

    def send(self):
        Message.m.update_partial(
            {'_id' : self._id},
            {'$set': { 'status': 'ready' } } )
        notify('send', self)

    @property
    def args(self):
        if self._args is None: return None
        return loads(self._args)
    @args.setter
    def args(self, value):
        self._args = dumps(value)
    @property
    def kwargs(self):
        if self._kwargs is None: return None
        return loads(self._kwargs)
    @kwargs.setter
    def kwargs(self, value):
        self._kwargs = dumps(value)

