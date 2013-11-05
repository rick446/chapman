from ming import Field, Session, create_datastore
from ming.declarative import Document
from ming import schema as S

from .m_base import doc_session


class Shard(Document):

    class __mongometa__:
        name = 'chapman.shard'
        session = doc_session

    _id = Field(S.ObjectId)
    uri = Field(str)
    kwargs = Field({str: None})

    def session(self):
        return Session(
            create_datastore(
                self.uri, **self.kwargs))
