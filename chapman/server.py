import re
import gzip
from cStringIO import StringIO

from gevent import reinit
from gevent.pywsgi import WSGIServer
from gevent.monkey import patch_all

__all__ = [
    "pywsgi_server_factory",
    "pywsgi_server_factory_patched",
]


def pywsgi_server_factory(global_conf, host, port):
    port = int(port)
    reinit()

    def serve(app):
        WSGIServer((host, port), app).serve_forever()
    return serve


def pywsgi_server_factory_patched(global_conf, host, port):
    port = int(port)
    reinit()
    patch_all(dns=False)

    def serve(app):
        WSGIServer((host, port), app).serve_forever()
    return serve
