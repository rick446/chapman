from ming import Field
from ming.declarative import Document
from ming import schema as S

from .m_session import doc_session

class TaskState(Document):
    class __mongometa__:
        name='chapman.task'
        session = doc_session

    _id=Field(S.ObjectId)
    parent_id=Field(S.ObjectId, if_missing=None)
    status=Field(str)
    result=Field(None)
    data=Field(None)
    on_complete=Field(S.ObjectId, if_missing=None)
