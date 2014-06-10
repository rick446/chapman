import time
import logging

from docopt import docopt

from chapman import hq

logging.basicConfig(level=logging.WARN)


CHUNKSIZE = 4096

log = logging.getLogger(__name__)


def hq_ping():
    args = docopt("""Usage:
        chapman-hq-ping <secret> <qname>

    Options:
        -h --help                 show this help message and exit
    """)

    listener = PingListener(args['<qname>'], args['<secret>'])
    listener.start()

    while True:
        listener.ping()
        time.sleep(1)


class PingListener(hq.Listener):

    def __init__(self, qname, secret):
        self.q0 = hq.HQueue(qname, secret)
        self.q1 = hq.HQueue(qname, secret)
        self.msgs = {}
        super(PingListener, self).__init__(self.q1, 'listener')

    def ping(self):
        now = time.time()
        result = self.q0.put({}).content
        self.msgs[result] = now

    def handle(self, id, msg):
        now = time.time()
        elapsed = now - self.msgs.pop(id, 0)
        print 'Latency %s: %dms' % (
            id, (elapsed * 1000))
