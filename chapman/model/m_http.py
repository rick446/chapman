import json
import logging
from datetime import datetime, timedelta
from random import getrandbits

import bson

from ming import Field
from ming.declarative import Document
from ming import schema as S

from chapman import util

from .m_base import doc_session, ChannelProxy

log = logging.getLogger(__name__)


class HTTPMessage(Document):
    missing_client = '-' * 10
    channel = ChannelProxy('chapman.event')

    class __mongometa__:
        name = 'chapman.http_message'
        session = doc_session
        indexes = [
            [('s.status', 1), ('s.pri', -1), ('s.ts_enqueue', 1), ('s.q', 1)],
            [('s.q', 1), ('s.status', 1), ('s.pri', -1), ('s.ts_enqueue', 1)],
        ]

    _id = Field(int, if_missing=lambda: getrandbits(63))
    _data = Field('data', S.Binary(if_missing=bson.Binary('{}')))
    schedule = Field('s', dict(
        status=S.String(if_missing='ready'),
        ts_enqueue=S.DateTime(if_missing=datetime.utcnow),
        ts_reserve=S.DateTime(if_missing=None),
        ts_timeout=S.DateTime(if_missing=None),
        timeout=S.Int(if_missing=300),
        after=S.DateTime(if_missing=datetime.utcnow),
        q=S.String(if_missing='chapman.http'),
        pri=S.Int(if_missing=10),
        cli=S.String(if_missing=missing_client)))

    def __json__(self, request):
        k = request.route_url('chapman.1_0.message', message_id=self._id)
        return {k: self.data}

    @property
    def data(self):
        return json.loads(self._data)
    @data.setter
    def data(self, value):
        self._data = bson.Binary(json.dumps(value, default=util.default_json))

    @classmethod
    def new(cls, data, timeout=300, after=None, q='chapman.http', pri=10):
        _data = bson.Binary(json.dumps(data, default=util.default_json))
        if after is None:
            after = datetime.utcnow()
        self = cls.make(dict(
            data=_data,
            s=dict(timeout=timeout, after=after, q=q, pri=pri)))
        self.m.insert()
        self.channel.pub('enqueue', self._id)
        return self

    @classmethod
    def reserve(cls, cli, queues):
        return cls._reserve(cli, {'$in': queues})

    @classmethod
    def _reserve(cls, cli, qspec):
        now = datetime.utcnow()
        self = cls.m.find_and_modify(
            {'s.status': 'ready', 's.q': qspec, 's.after': {'$lte': now}},
            sort=[('s.pri', -1), ('s.ts_queue', 1)],
            update={'$set': {
                's.cli': cli,
                's.status': 'reserved',
                's.ts_reserve': now}},
            new=True)
        if self is not None and self.s.timeout:
            self.m.set({'s.ts_timeout': now + timedelta(seconds=self.s.timeout)})
        return self

