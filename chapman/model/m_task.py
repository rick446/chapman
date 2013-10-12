from random import random
from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_base import doc_session, dumps, pickle_property


class TaskState(Document):

    class __mongometa__:
        name = 'chapman.task'
        session = doc_session
        indexes = [
            [('parent_id', 1), ('data.composite_position', 1)],
        ]

    _id = Field(int, if_missing=lambda: hash(random()))
    type = Field(str)
    parent_id = Field(S.ObjectId, if_missing=None)
    status = Field(str, if_missing='pending')
    _result = Field('result', S.Binary)
    data = Field({str: None})
    options = Field(dict(
        queue=S.String(if_missing='chapman'),
        priority=S.Int(if_missing=10),
        immutable=S.Bool(if_missing=False),
        ignore_result=S.Bool(if_missing=False)
    ))
    on_complete = Field(int, if_missing=None)
    mq = Field([S.ObjectId()])

    result = pickle_property('_result')

    @classmethod
    def set_result(cls, id, result):
        cls.m.update_partial(
            {'_id': id},
            {'$set': {
                'result': dumps(result),
                'status': result.status}})
