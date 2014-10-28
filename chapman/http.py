from pyramid.config import Configurator
from pyramid.events import NewRequest
import pyramid.httpexceptions as exc


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
        RequireHeader('Authorization', settings['authorization']),
        NewRequest)
    config.scan('chapman.views')
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
