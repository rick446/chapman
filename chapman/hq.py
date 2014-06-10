import sys
import json
import time
import logging
import threading
from Queue import Queue

import requests

from . import util

log = logging.getLogger(__name__)


class Retry(Exception): pass


class HQueue(object):

    def __init__(self, queue_url, secret):
        self.queue_url = queue_url
        self.session = requests.Session()
        self.session.headers.update(
            {'Authorization': 'chapman ' + secret})

    def put(self, msg, timeout=300, delay=0, priority=10, tags=None):
        if tags is None:
            tags = []
        params = dict(
            timeout=timeout, delay=delay, priority=priority)
        for i, tag in enumerate(tags):
            params['tags-%d' % i] = tag
        resp = self.session.post(
            self.queue_url,
            params=params,
            data=json.dumps(msg, default=util.default_json))
        return resp

    def get(self, client_name, count=1, timeout=30):
        resp = self.session.get(
            self.queue_url,
            params=dict(client=client_name, count=count, timeout=timeout))
        return resp

    def retire(self, message_id):
        return self.session.delete(message_id)

    def retry(self, message_id):
        return self.session.post(message_id)


class Listener(object):

    def __init__(self, hqueue, client_name, n_thread=1, sleep_ms=30000):
        self.hqueue = hqueue
        self.client_name = client_name
        self.n_thread = n_thread
        self.sleep_ms = sleep_ms

    def start(self):
        sem = threading.Semaphore(self.n_thread)
        q = Queue()
        self._threads = [
            threading.Thread(
                name='dispatch',
                target=self.dispatcher,
                args=(sem, q))]
        self._threads += [
            threading.Thread(
                name='w-%d' % x,
                target=self.worker,
                args=(sem, q))
            for x in range(self.n_thread)]
        for t in self._threads:
            t.setDaemon(True)
            t.start()

    def run(self):
        while True:
            time.sleep(30)

    def dispatcher(self, sem, q):
        log.info('Entering dispatcher')
        while True:
            count = _sem_multi_acquire(sem, self.n_thread)
            try:
                try:
                    resp = self.hqueue.get(self.client_name, count=count)
                    log.debug('Got from queue %s', resp)
                except Exception:
                    log.exception('Could not GET from queue, wait 5s')
                    _sem_multi_release(sem, count)
                    time.sleep(5)
                    continue
                if not resp.ok:
                    _sem_multi_release(sem, count)
                    log.error(
                        'Error reserving message: %r\n%s',
                        resp, resp.content)
                    continue
                if resp.status_code == 204:  # no content
                    _sem_multi_release(sem, count)
                    time.sleep(self.sleep_ms / 1000.0)
                    continue
                try:
                    resp_json = resp.json()
                except Exception:
                    _sem_multi_release(sem, count)
                    raise
                # sem.release() for each message requested but NOT received
                _sem_multi_release(sem, count - len(resp_json))
                for k, v in resp_json.items():
                    log.info('Handle %s', k)
                    try:
                        q.put((k, v))
                    except Exception:
                        sem.release()  # since it wasn't enqueued
            except Exception:
                log.exception('Unknown exception in dispatcher, wait 5s')
                time.sleep(5)

    def worker(self, sem, q):
        while True:
            id, msg = q.get()
            try:
                self.handle(id, msg)
            except Retry:
                try:
                    self.hqueue.retry(id)
                except Exception:
                    log.exception('Failed to retry %s', id)
            except Exception:
                log.exception('Error handling request, waiting 5s')
            else:
                try:
                    self.hqueue.retire(id)
                except Exception:
                    log.exception('Failed to retire %s', id)
            finally:
                sem.release()

    def handle(self, id, msg):
        raise NotImplementedError('handle')


def _sem_multi_acquire(sem, max_acquire=sys.maxint):
    '''sem.acquire() up to max_acquire times, returning the number of
    times the semaphore was acquired
    '''
    count = 0
    while count < max_acquire and sem.acquire(blocking=0):
        count += 1
    if not count:
        sem.acquire(blocking=1)
        count = 1
    return count


def _sem_multi_release(sem, count):
    '''sem.release() the given number of times'''
    for x in range(count):
        sem.release()



