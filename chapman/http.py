from pyramid.config import Configurator
from pyramid.events import NewRequest
import pyramid.httpexceptions as exc

import ming

from sutil import util


def http_main(global_config, **local_settings):
    """ This function returns a (minimal) Pyramid WSGI application.
    """
    settings = dict(global_config)
    settings.update(local_settings)
    util.update_settings_from_environ(settings)
    config = Configurator(settings=settings)
    config.add_renderer(None, 'pyramid.renderers.json_renderer_factory')
    config.add_route('chapman.1_0.queue', '/1.0/q/{qname}/')
    config.add_route('chapman.1_0.message', '/1.0/m/{message_id}/')
    config.add_subscriber(
        RequireHeader('Authorization', 'chapman %s' % settings['chapman.secret']),
        NewRequest)
    config.scan('chapman.views')
    config.scan('sutil.error_views')
    ming.configure(**settings)
    app = config.make_wsgi_app()
    return app


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
