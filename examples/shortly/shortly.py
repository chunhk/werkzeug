# -*- coding: utf-8 -*-
"""
    shortly
    ~~~~~~~

    A simple URL shortener using Werkzeug and redis.

    :copyright: (c) 2011 by the Werkzeug Team, see AUTHORS for more details.
    :license: BSD, see LICENSE for more details.
"""
import os
import ConfigParser
import logging
import random
import redis
import string
import urlparse
from werkzeug.wrappers import Request, Response
from werkzeug.routing import Map, Rule
from werkzeug.exceptions import HTTPException, NotFound
from werkzeug.wsgi import SharedDataMiddleware
from werkzeug.utils import redirect

from jinja2 import Environment, FileSystemLoader

log = logging.getLogger("shortly")
log.setLevel(logging.INFO)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

fh = logging.FileHandler('shortly.log')
fh.setLevel(logging.INFO)
fh.setFormatter(formatter)

ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
ch.setFormatter(formatter)

log.addHandler(fh)
log.addHandler(ch)

def base36_encode(number):
    assert number >= 0, 'positive integer required'
    if number == 0:
        return '0'
    base36 = []
    while number != 0:
        number, i = divmod(number, 36)
        base36.append('0123456789abcdefghijklmnopqrstuvwxyz'[i])
    return ''.join(reversed(base36))


def is_valid_url(url):
    parts = urlparse.urlparse(url)
    return parts.scheme in ('http', 'https')


def get_hostname(url):
    return urlparse.urlparse(url).netloc


class Shortly(object):

    def __init__(self, config):
        log.info("redis master: " + config['master'])
        self.redis_master = self._init_redis(config['master'])
        log.info("redis slaves: " + string.join(config['slaves'], ","))
        self.redis_slave_ids = config['slaves']
        self.redis_slaves = [self._init_redis(x) for x in config['slaves']]
        self.redis_slave_num = len(self.redis_slaves)
        template_path = os.path.join(os.path.dirname(__file__), 'templates')
        self.jinja_env = Environment(loader=FileSystemLoader(template_path),
                                     autoescape=True)
        self.jinja_env.filters['hostname'] = get_hostname

        self.url_map = Map([
            Rule('/', endpoint='new_url'),
            Rule('/<short_id>', endpoint='follow_short_link'),
            Rule('/<short_id>+', endpoint='short_link_details')
        ])

    def _init_redis(self, host):
        host_port = host.rsplit(":")
        if len(host_port) == 1:
          host_port.append(6379)

        return redis.Redis(host_port[0], host_port[1])

    def _get_redis_slave(self):
        idx = random.randint(0,self.redis_slave_num-1)
        log.info("using redis slave " + self.redis_slave_ids[idx])
        return self.redis_slaves[idx]
        
    def on_new_url(self, request):
        error = None
        url = ''
        if request.method == 'POST':
            url = request.form['url']
            if not is_valid_url(url):
                error = 'Please enter a valid URL'
            else:
                short_id = self.insert_url(url)
                return redirect('/%s+' % short_id)
        return self.render_template('new_url.html', error=error, url=url)

    def on_follow_short_link(self, request, short_id):
        link_target = self._get_redis_slave().get('url-target:' + short_id)
        if link_target is None:
            raise NotFound()
        self.redis_master.incr('click-count:' + short_id)
        return redirect(link_target)

    def on_short_link_details(self, request, short_id):
        link_target = self._get_redis_slave().get('url-target:' + short_id)
        if link_target is None:
            raise NotFound()
        click_count = int(self._get_redis_slave().get('click-count:' + short_id) or 0)
        return self.render_template('short_link_details.html',
            link_target=link_target,
            short_id=short_id,
            click_count=click_count
        )

    def error_404(self):
        response = self.render_template('404.html')
        response.status_code = 404
        return response

    def insert_url(self, url):
        short_id = self._get_redis_slave().get('reverse-url:' + url)
        if short_id is not None:
            return short_id
        url_num = self.redis_master.incr('last-url-id')
        short_id = base36_encode(url_num)
        self.redis_master.set('url-target:' + short_id, url)
        self.redis_master.set('reverse-url:' + url, short_id)
        return short_id

    def render_template(self, template_name, **context):
        t = self.jinja_env.get_template(template_name)
        return Response(t.render(context), mimetype='text/html')

    def dispatch_request(self, request):
        adapter = self.url_map.bind_to_environ(request.environ)
        try:
            endpoint, values = adapter.match()
            return getattr(self, 'on_' + endpoint)(request, **values)
        except NotFound, e:
            return self.error_404()
        except HTTPException, e:
            return e

    def wsgi_app(self, environ, start_response):
        request = Request(environ)
        response = self.dispatch_request(request)
        return response(environ, start_response)

    def __call__(self, environ, start_response):
        return self.wsgi_app(environ, start_response)


def create_app(redis_config={'master':'localhost:6379',
        'slaves':['localhost:6379']}, with_static=True):
    app = Shortly(redis_config)
    if with_static:
        app.wsgi_app = SharedDataMiddleware(app.wsgi_app, {
            '/static':  os.path.join(os.path.dirname(__file__), 'static')
        })
    return app

def parse_config(path='shortly.cfg'):
    config = ConfigParser.ConfigParser()
    config.read(path)
    master = config.get('redis', 'master')
    slaves = config.get('redis', 'slaves').rsplit(",")
    return {'master': master, 'slaves': slaves}

if __name__ == '__main__':
    from werkzeug.serving import run_simple
    app = create_app(parse_config())
    run_simple('0.0.0.0', 5000, app, use_debugger=True, use_reloader=True)
