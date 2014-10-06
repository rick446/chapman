import logging
from random import getrandbits

from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_base import doc_session, dumps, pickle_property, Resource

log = logging.getLogger(__name__)

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
        path=S.String(if_missing=None),
        semaphores = [str],
    ))
    on_complete = Field(int, if_missing=None)
    active = Field([int])     # just one message active
    queued = Field([int])   # any number queued

    result = pickle_property('_result')

    @classmethod
    def set_result(cls, id, result):
        cls.m.update_partial(
            {'_id': id},
            {'$set': {
                'result': dumps(result),
                'status': result.status}})

    def __repr__(self):
        parts = [self.type, self._id]
        if self.options['path']:
            parts.append(self.options['path'])
        return '<{}>'.format(
            ' '.join(map(str, parts)))


class TaskStateResource(Resource):
    cls=TaskState

    def __init__(self, id):
        self.id = id

    def __repr__(self):
        obj = TaskState.m.get(_id=self.id)
        return '<TaskStateResource({}:{}): {} / {}>'.format(
            obj.type, obj._id, obj.active, obj.queued)

    def acquire(self, msg_id):
        return super(TaskStateResource, self).acquire(msg_id, 1)

    def release(self, msg_id):
        return super(TaskStateResource, self).release(msg_id, 1)
