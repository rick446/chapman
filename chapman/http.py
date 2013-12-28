from pyramid.config import Configurator
from pyramid.events import NewRequest
import pyramid.httpexceptions as exc

import ming


def http_main(global_config, **local_settings):
    """ This function returns a (minimal) Pyramid WSGI application.
    """
    settings = dict(global_config)
    settings.update(local_settings)
    config = Configurator(settings=settings)
    config.add_renderer(None, 'pyramid.renderers.json_renderer_factory')
    config.add_route('chapman.1_0.queue', '/1.0/q/{qname}/')
    config.add_route('chapman.1_0.message', '/1.0/m/{message_id}/')
    config.add_subscriber(
        RequireHeader('Authorization', 'chapman %s' % settings['chapman.secret']),
        NewRequest)
    config.scan('chapman.views')
    if not ming.Session._datastores:
        ming.configure(**settings)
    app = config.make_wsgi_app()
    db = ming.Session.by_name('chapman').db
    app = MongoMiddleware(app, db.connection)
    return app


class MongoMiddleware(object):

    def __init__(self, app, conn):
        self.app = app
        self.conn = conn

    def __call__(self, environ, start_response):
        self.conn.start_request()
        try:
            result = self.app(environ, start_response)
            if isinstance(result, list):
                self.conn.end_request()
                return result
            else:
                return self._cleanup_iterator(result)
        except:
            self.conn.end_request()
            raise

    def _cleanup_iterator(self, result):
        for x in result:
            yield x
        self.conn.end_request()


class RequireHeader(object):

    def __init__(self, header, value):
        self.header = header
        self.value = value

    def __call__(self, event):
        request = event.request
        if self.header not in request.headers:
            raise exc.HTTPForbidden()
        elif request.headers[self.header] != self.value:
            raise exc.HTTPForbidden()
