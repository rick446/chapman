import re
import sys
import time
import random
import logging
import threading

from paste.deploy.converters import asint

import model as M
from .context import g

log = logging.getLogger(__name__)


class ShardWorker(object):
    '''Sharded worker'''

    def __init__(self, name, app_context):
        '''Sharded Worker - this worker reserves messages from a
        parent instance and then redispatches it locally. It also
        reserves messages locally and redispatches them to the
        parent instance.

        In the parent, any queue starting with 'shard:' will have the
        prefix stripped and be redispatched to the local instance.

        In the local instance, any queue starting with 'unshard:' will
        have the prefix stripped and be redispatched to the parent
        instance.
        '''
        self.name = name
        g.app_context = app_context
        settings = app_context['registry'].settings

        self.sleep = asint(settings.get(
            'chapman.sleep', '200')) / 1000.0

        self._shutdown = False  # flag to indicate worker is shutting down
        self._model = dict(
            parent=dict(
                Message=M.ParentMessage,
                TaskState=M.ParentTaskState),
            local=dict(
                Message=M.Message,
                TaskState=M.TaskState))

    def start(self):
        M.doc_session.db.collection_names()  # force connection & auth
        parent = self._model['parent']
        local = self._model['local']
        d_shard = DispatchThread(self, 'shard:', parent, local, True)
        d_unshard = DispatchThread(self, 'unshard:', local, parent)
        e_shard = EventThread(self, d_shard, parent['Message'])
        e_unshard = EventThread(self, d_unshard, local['Message'])
        self._dispatch_threads = [d_shard, d_unshard]
        self._event_threads = [e_shard, e_unshard]
        self._threads = self._dispatch_threads + self._event_threads
        for t in self._threads:
            t.setDaemon(True)
            t.start()

    def shutdown(self):
        for t in self._dispatch_threads:
            t.shutdown()

    def run(self):
        while True:
            for t in self._threads:
                if t.is_alive():
                    break
            else:
                break
            time.sleep(10)


class EventThread(threading.Thread):

    def __init__(self, worker, dispatcher, Message):
        self._worker = worker
        self._dispatcher = dispatcher
        self._Message = Message
        super(EventThread, self).__init__()

    def run(self):
        db = self._Message.m.session.db
        conn = db.connection
        log.info('Entering event thread on %s', db)
        conn.start_request()
        chan = self._Message.channel.new_channel()
        chan.pub('start', self._worker.name)
        chan.sub('ping', self._handle_ping)
        chan.sub('kill', self._handle_kill)
        chan.sub('shutdown', self._handle_shutdown)
        chan.sub('send', self._handle_send)

        while self._dispatcher.is_alive():
            chan.handle_ready(await=True, raise_errors=True)
            time.sleep(self._worker.sleep)

    def _handle_ping(self, chan, msg):
        data = msg['data']
        if data['worker'] in (self._worker.name, '*'):
            data['worker'] = self._worker.name
            chan.pub('pong', data)

    def _handle_kill(self, chan, msg):
        if msg['data'] in (self._worker.name, '*'):
            log.error('Received %r, exiting', msg)
            sys.exit(0)

    def _handle_shutdown(self, chan, msg):
        if msg['data'] in (self._worker.name, '*'):
            log.error('Received %r, shutting down gracefully', msg)
            self.worker.shutdown()

    def _handle_send(self, chan, msg):
        self._dispatcher.notify()


class DispatchThread(threading.Thread):

    def __init__(self, worker, prefix, source, sink, prob=False):
        self._worker = worker
        self._prefix = prefix
        self._source = source
        self._sink = sink
        self._prob = prob
        self._event = threading.Event()
        self._shutdown = False
        self._re_qname = re.compile(r'^%s' % prefix)
        super(DispatchThread, self).__init__()

    def run(self):
        SourceMessage = self._source['Message']
        while not self._shutdown:
            msg, state = SourceMessage.reserve_qspec(
                self._worker.name, self._re_qname)
            if msg is None:
                self._event.clear()
                self._event.wait(self._worker.sleep)
            if state is None:
                continue
            self._handle(msg, state)

    def notify(self):
        self._event.set()

    def shutdown(self):
        self._shutdown = True

    def _handle(self, msg, state):
        log.info('Dispatch %s', msg)
        assert state.on_complete is None, "Can't handle sharded on_complete"
        assert state.options.ignore_result, "Can't handle sharded results"

        # Strip the prefix
        qname = state.options.queue[len(self._prefix):]
        state.options.queue = msg.s.q = qname
        msg.s.status = 'ready'
        msg.s.w = M.Message.missing_worker

        # Create the taskstate and msg in the subsession
        sink_msg = self._sink['Message'].make(msg)
        sink_state = self._sink['TaskState'].make(state)
        sink_state.m.insert()
        sink_msg.m.insert()
        sink_msg.channel.pub('send', sink_msg._id)

        # Delete the original message/state
        msg.m.delete()
        state.m.delete()

        if self._prob:
            num_tasks = M.TaskState.m.find().count()
            sleep_time = random.random() * num_tasks
            log.info("Backoff for %.0f seconds", sleep_time)
            time.sleep(sleep_time)
