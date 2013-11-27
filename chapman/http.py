from pyramid.config import Configurator

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
    config.scan('chapman.views')
    config.scan('sutil.error_views')
    ming.configure(**settings)
    app = config.make_wsgi_app()
    return app


