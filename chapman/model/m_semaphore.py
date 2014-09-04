import logging

from ming import Field
from ming.declarative import Document

from .m_base import doc_session, Resource

log = logging.getLogger(__name__)


class Semaphore(Document):

    class __mongometa__:
        name = 'chapman.semaphore'
        session = doc_session

    _id = Field(str)
    value = Field(int)
    active = Field([int])
    queued = Field([int])

    @classmethod
    def ensure(cls, sem_id, value):
        return cls.m.find_and_modify(
            {'_id': sem_id},
            update={'$setOnInsert': {
                'value': 5,
                'active': [],
                'queued': []}},
            upsert=True,
            new=1)


class SemaphoreResource(Resource):
    cls=Semaphore

    def __init__(self, id, value=None):
        self.id = id
        if value is not None:
            self.cls.m.update_partial(
                {'_id': id},
                {'$setOnInsert': {'value': value, 'active': [], 'queued': []}},
                upsert=True)

    def __repr__(self):
        obj = self.cls.m.get(_id=self.id)
        return '<SemaphoreResource({}:{}): {}>'.format(
            self.id, obj.value, obj.mq)

    def acquire(self, msg_id):
        value = Semaphore.m.get(_id=self.id).value
        return super(SemaphoreResource, self).acquire(msg_id, value)

    def release(self, msg_id):
        value = Semaphore.m.get(_id=self.id).value
        return super(SemaphoreResource, self).release(msg_id, value)
