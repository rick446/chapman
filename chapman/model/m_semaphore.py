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
    mq = Field([int])


class SemaphoreResource(Resource):

    def __init__(self, id, value=None):
        self.id = id
        if value is not None:
            Semaphore.m.update_partial(
                {'_id': id},
                {'$setOnInsert': {'value': value, 'mq': []}},
                upsert=True)

    def __repr__(self):
        obj = Semaphore.m.get(_id=self.id)
        return '<SemaphoreResource({}:{}): {}>'.format(
            self.id, obj.value, obj.mq)

    def is_acquired(self, msg_id):
        sem = Semaphore.m.get(_id=msg_id, mq=msg_id)
        if sem and msg_id in sem.mq[:sem.value]:
            return True
        else:
            return False

    def acquire(self, msg_id):
        sem = Semaphore.m.find_and_modify(
            {'_id': self.id, 'mq': {'$ne': msg_id}},
            update={'$push': {'mq': msg_id}},
            new=True)
        if not sem:
            log.error('Trying to acquire sem %s that is already acquired',
                self.id)
            sem = Semaphore.m.get(_id=self.id)
        if msg_id in sem.mq[:sem.value]:
            log.debug('{} has successfully acquired {}'.format(msg_id, self.id))
            return True
        else:
            log.debug('{} has enqueued on {}'.format(msg_id, self.id))
            return False

    def release(self, msg_id):
        obj = Semaphore.m.find_and_modify(
            {'_id': self.id, 'mq': msg_id},
            update={'$pull': {'mq': msg_id}},
            new=True)
        if obj is None:
            raise ValueError(
                '{} is not holding the semaphore {}'.format(msg_id, self.id))
        log.debug('{} has released on {} and released {}'.format(
            msg_id, self.id, obj.mq[:obj.value]))
        return obj.mq[:obj.value]
