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


here_folder = os.path.dirname(os.path.abspath(__file__))
log = logging.getLogger(__name__)


with open('config.json') as json_data:
    config = json.load(json_data)


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

if os.path.exists('stats-state.csv'):
    stats = pd.read_csv('stats-state.csv', parse_dates=['time-slot'])
else:
    stats = pd.DataFrame(columns=['emoji', 'time-slot', 'count', 'tag'])
    stats['time-slot'] = pd.to_datetime(stats['time-slot'])

stats['count'] = stats['count'].astype(int)
stats.set_index(['emoji', 'time-slot', 'tag'], inplace=True)

if os.path.exists('chart_data.csv'):
    chart_data = pd.read_csv('chart_data.csv', parse_dates=['time-slot'])
else:
    chart_data = pd.DataFrame(columns=['time-slot', 'sent', 'tag'])
    chart_data['sent'] = chart_data['sent'].astype(float)
chart_data.set_index(['tag', 'time-slot'], inplace=True)

def round_time(t):
    # t - dt.timedelta(minutes=t.minute,seconds=t.second,microseconds=t.microsecond)
    return t - dt.timedelta(seconds=t.second, microseconds=t.microsecond)


def process_message(msg):
    tags = json.loads(msg)['event']['tags']

    msg = np.unique([m for m in msg if m in emoji_dict])
    curr_timeslot = round_time(dt.datetime.now())

    # drop older
    stats.drop(stats.index[stats.reset_index()['time-slot'] < (curr_timeslot - dt.timedelta(hours=24))], inplace=True)

    for m in msg:
        for tag in tags:
            if (m, curr_timeslot, tag) not in stats.index:
                stats.ix[(m, curr_timeslot, tag), 'count'] = 0
            stats.ix[(m, curr_timeslot, tag), 'count'] = stats.ix[(m, curr_timeslot, tag), 'count'] + 1

    stats.to_csv('stats-state.csv')

    recalc_stats()

    print("< {}".format(msg))


def recalc_stats():
    curr_timeslot = round_time(dt.datetime.now())

    df = stats.copy().reset_index()
    if len(df) == 0:
        return 0, pd.DataFrame(columns=['tag', 'emoji', 'count', 'last_hour_count'])

    df['hour_weight'] = (curr_timeslot - df['time-slot']).dt.components.hours + \
                        (curr_timeslot - df['time-slot']).dt.components.minutes / 60 + 1

    emoji_sent_map = emoji.rename(columns={'Emoji': 'emoji'})[['emoji', 'sentiment_score']]
    sent_map = df.groupby(['tag', 'emoji', 'hour_weight']).agg({'count': sum}).reset_index().merge(emoji_sent_map)

    sent_map['sent'] = sent_map['count'] * sent_map['sentiment_score'] / sent_map['hour_weight']
    overall_sent = sent_map.groupby('tag').apply(
        lambda x: x['sent'].sum() / ((x['count'] / x['hour_weight']).sum() + 0.1))

    last_hour_count = \
    df[(dt.datetime.now() - df['time-slot']) < dt.timedelta(hours=1)].groupby(['tag', 'emoji']).agg({'count': sum})[
        'count']

    top_emoji = df.groupby(['tag', 'emoji']).agg({'count': sum})
    last_hour_count = last_hour_count.reindex(top_emoji.index, fill_value=0)
    top_emoji['last_hour_count'] = np.array(last_hour_count)
    top_emoji = top_emoji.astype(int)
    top_emoji = top_emoji.reset_index().sort_values('count', ascending=False).groupby('tag').head(5).reset_index()[
        ['tag', 'emoji', 'count', 'last_hour_count']]

    for tag in sent_map['tag'].unique():
        chart_data.ix[(tag, curr_timeslot), 'sent'] = overall_sent.ix[tag]
    chart_data.to_csv('chart_data.csv')

    return overall_sent, top_emoji


async def main_page_handler(request):
    overall_sent, top_emoji = recalc_stats()

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
        return overall_sent.ix[tag] if tag in overall_sent.index else 0

    emoji_data = {'top': get_top_emoji_by_tag(tag),
                  'emoji_str': get_top_emoji_str(tag),
                  'chart_data': get_chart(tag),
                  'overall_sent': get_sent(tag),
                  'tags': rule_names,
                  'query_tag': tag}

    response = aiohttp_jinja2.render_template('index.jinja2',
                                              request,
                                              emoji_data)

    response.headers['Content-Type'] = 'text/html;charset=utf-8'

    return response


async def listen_feed():
    while True:
        print('connecting to {}'.format(wss_url))
        session = aiohttp.ClientSession()
        async with session.ws_connect(wss_url) as ws:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    process_message(msg.data)
                elif msg.type == aiohttp.WSMsgType.CLOSED:
                    break
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break


async def start_background_tasks(app):
    app['listen_feed'] = app.loop.create_task(listen_feed())


async def cleanup_background_tasks(app):
    app['listen_feed'].cancel()
    await app['listen_feed']


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

main.on_startup.append(start_background_tasks)
main.on_cleanup.append(cleanup_background_tasks)
main.router.add_get('/', main_page_handler)

