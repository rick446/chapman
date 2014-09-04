from random import getrandbits
from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_base import doc_session, dumps, pickle_property, Resource
from .m_semaphore import SemaphoreResource

class TaskState(Document):

    class __mongometa__:
        name = 'chapman.task'
        session = doc_session
        indexes = [
            [('parent_id', 1), ('data.composite_position', 1)],
        ]

    _id = Field(int, if_missing=lambda: getrandbits(63))
    type = Field(str)
    parent_id = Field(int, if_missing=None)
    status = Field(str, if_missing='pending')
    _result = Field('result', S.Binary)
    data = Field({str: None})
    options = Field(dict(
        queue=S.String(if_missing='chapman'),
        priority=S.Int(if_missing=10),
        immutable=S.Bool(if_missing=False),
        ignore_result=S.Bool(if_missing=False),
        semaphores = [str],
    ))
    on_complete = Field(int, if_missing=None)
    mq = Field([int])

    result = pickle_property('_result')

    @classmethod
    def set_result(cls, id, result):
        cls.m.update_partial(
            {'_id': id},
            {'$set': {
                'result': dumps(result),
                'status': result.status}})


class TaskStateResource(Resource):

    def __init__(self, id):
        self.id = id

    def __repr__(self):
        obj = TaskState.m.get(_id=self.id)
        return '<TaskStateResource({}:{}): {}>'.format(
            obj.type, obj._id, obj.mq)

    def is_acquired(self, msg_id):
        ts = TaskState.m.find({'_id': self.id, 'mq.0': msg_id}).limit(1).first()
        return ts is not None

    def acquire(self, msg_id):
        ts = TaskState.m.find_and_modify(
            {'_id': self.id, 'mq': {'$ne': msg_id}},
            update={'$push': {'mq': msg_id}},
            new=True)
        if not ts:
            log.error('Trying to acquire tsr %s that is already acquired',
                self.id)
        if msg_id == ts.mq[0]:
            return True
        return False

    def release(self, msg_id):
        ts = TaskState.m.find_and_modify(
            {'_id': self.id, 'mq': msg_id},
            update={'$pull': {'mq': msg_id}},
            new=True)
        if ts is None:
            return []
        return ts.mq[:1]
