import asyncio
import logging
import logging.config

from aiohttp import web
import aiohttp
import aiohttp_jinja2
import jinja2

import requests

import pandas as pd
import numpy as np
import json,os
import datetime as dt

from web.router import configure_handlers, routes
from web.utils.assets import AssetManager
from web.utils.settings import get_config

import sqlalchemy

here_folder = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger(__name__)


with open('config.json') as json_data:
    config = json.load(json_data)

def connect_db(url):
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    meta = sqlalchemy.MetaData(bind=con, reflect=True)
    return con, meta

# obtain token, subscribe to topics
r = requests.get('https://api.vk.com/method/streaming.getServerUrl',
                 params={'access_token': '9222b7889222b7889222b788fa927fa290992229222b788cb78779aea9215c6c94abac3'})
[api_host, access_key] = r.json()['response']['endpoint'], r.json()['response']['key']

r = requests.get('https://{}/rules?key={}'.format(api_host, access_key))
if r.json()['rules'] is not None:
    for x in r.json()['rules']:
        r = requests.delete('https://{}/rules?key={}'.format(api_host, access_key), json={'tag': x['tag']})

rule_names = []
for i in range(len(config)):
    payload = {
        'rule': {'value': config[i]['keywords'], 'tag': config[i]['topic']}
    }
    rule_names.append(config[i]['topic'])
    r = requests.post('https://{}/rules?key={}'.format(api_host, access_key), json=payload)


wss_url = 'wss://{}/stream?key={}'.format(api_host, access_key)

# prepare emoji sentiment source data + formula from the paper

emoji = pd.read_csv('emoji.csv')
emoji['sentiment_score'] = (emoji['Positive'] + 1) / \
                           (emoji['Occurrences'] + 3) - (emoji['Negative'] + 1) / (emoji['Occurrences'] + 3)

emoji_dict = set(list(emoji['Emoji']))

# init stats data structure

con, meta = connect_db(os.environ["DATABASE_URL"])

def round_time(t):
    # t - dt.timedelta(minutes=t.minute,seconds=t.second,microseconds=t.microsecond)
    return t - dt.timedelta(seconds=t.second, microseconds=t.microsecond)


async def main_page_handler(request):
    overall_sent, top_emoji = pd.read_sql('overall_sent',con), pd.read_sql('top_emoji',con)

    overall_sent.set_index('tag', inplace=True)
    top_emoji.drop('index', 1, inplace=True)

    #print(overall_sent)
    #print(top_emoji)

    chart_data = pd.read_sql_table('chart_data',con)
    chart_data.set_index(['tag', 'time-slot'], inplace=True)

    tag = request.query['tag'] if 'tag' in request.query else rule_names[0]


    #print(tag)
    #print(rule_names)

    def get_top_emoji_by_tag(tag):
        if len(top_emoji) == 0:
            return []
        return list(top_emoji[top_emoji['tag'] == tag].drop('tag', 1).to_records(index=False))

    def get_top_emoji_str(tag):
        if len(top_emoji) == 0:
            return ''
        return ''.join(list(top_emoji[top_emoji['tag'] == tag]['emoji']))

    def get_chart(tag):
        if len(chart_data)==0:
            return {'x':[],'y':[]}
        chart = chart_data.reset_index()[chart_data.reset_index()['tag'] == tag].drop('tag', 1)
        return {'x': list(chart['time-slot'].astype(str)), 'y': list(chart['sent'])}

    def get_sent(tag):
        if len(top_emoji)==0:
            return 0
        return overall_sent.ix[tag,0] if tag in overall_sent.index else 0

    emoji_data = {'top': get_top_emoji_by_tag(tag),
                  'emoji_str': get_top_emoji_str(tag),
                  'chart_data': get_chart(tag),
                  'overall_sent': get_sent(tag),
                  'tags': rule_names,
                  'query_tag': tag}

    log.debug(emoji_data)

    response = aiohttp_jinja2.render_template('index.jinja2',
                                              request,
                                              emoji_data)

    response.headers['Content-Type'] = 'text/html;charset=utf-8'

    return response


def build_app(settings_path, loop=None):
    settings = get_config(settings_path)

    logging.config.dictConfig(settings['logging'])

    loop = loop or asyncio.get_event_loop()

    aio_debug = settings.get('asyncio_debug_enabled', False)
    if aio_debug is True:
        loop.set_debug(True)

    middlewares = [

    ]

    application = web.Application(
        loop=loop,
        middlewares=middlewares
    )

    application.settings = settings

    # templates

    template_dir = os.path.join(os.path.dirname(__file__), 'templates')
    env = aiohttp_jinja2.setup(
        application, loader=jinja2.FileSystemLoader(template_dir)
    )

    # shutdown connection clean-up
    async def on_shutdown_close_conns(app):
        if app.connections:
            log.info('Force closing %s open stream connections.', len(app.connections))
            for resp in app.connections:
                resp.should_stop = True

    application.connections = set()
    application.on_shutdown.append(on_shutdown_close_conns)

    return application


if os.environ.get('ENV') == 'DEVELOPMENT':
    conf_file = os.path.join(here_folder, '../config/dev.conf')
else:
    conf_file = os.path.join(here_folder, '../config/web.conf')
main = build_app(settings_path=conf_file)

main.router.add_get('/', main_page_handler)

