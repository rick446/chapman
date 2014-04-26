from ming import Field
from ming.declarative import Document

from .m_base import doc_session, Resource


class Semaphore(Document):

    class __mongometa__:
        name = 'chapman.semaphore'
        session = doc_session

    _id = Field(str)
    value = Field(int)
    mq = Field([id])

class SemaphoreResource(Resource):

    def __init__(self, id, value=None):
        self.id = id
        if value is not None:
            Semaphore.m.update_partial(
                {'_id': id},
                {'$setOnInsert': {'value': value, 'mq': []}},
                upsert=True)

    def acquire(self, msg_id):
        sem = Semaphore.m.find_and_modify(
            {'_id': self.id},
            {'$push': {'mq': msg_id}},
            new=True)
        if msg_id in sem.mq[:sem.value]:
            return True
        else:
            return False

    def release(self, msg_id):
        obj = Semaphore.m.find_and_modify(
            {'_id': self.id, 'mq': msg_id},
            {'$pull': {'mq': msg_id}},
            new=True)
        if obj is None:
            raise ValueError(
                '{} is not holding the semaphore {}'.format(msg_id, self.id))
        Message.m.update_partial(
            {'_id': obj.mq[:obj.value], 's.status': 'acquire'},
            {'$set': {'s.status': 'ready'}},
            multi=True)
        return obj.mq[:obj.value]
