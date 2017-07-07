import aiohttp
import asyncio

import requests

import json
import pandas as pd

import urllib.parse as urlparse

import os

import datetime as dt

import sqlalchemy
from sqlalchemy import TIMESTAMP


def connect_db(url):
    con = sqlalchemy.create_engine(url, client_encoding='utf8')
    meta = sqlalchemy.MetaData(bind=con, reflect=True)
    return con, meta

with open('config.json') as json_data:
    config = json.load(json_data)

emoji = pd.read_csv('emoji.csv')
emoji['sentiment_score'] = (emoji['Positive'] + 1) / \
                           (emoji['Occurrences'] + 3) - (emoji['Negative'] + 1) / (emoji['Occurrences'] + 3)

emoji_dict = set(list(emoji['Emoji']))

con, meta = connect_db(os.environ["DATABASE_URL"])

def create_empty_df():
    stats = pd.DataFrame(columns=['emoji', 'time-slot', 'count', 'tag'])
    stats['time-slot'] = pd.to_datetime(stats['time-slot'])
    stats['count'] = stats['count'].astype(int)
    stats.set_index(['emoji', 'time-slot', 'tag'], inplace=True)

    chart_data = pd.DataFrame(columns=['time-slot', 'sent', 'tag'])
    chart_data['sent'] = chart_data['sent'].astype(float)
    chart_data.set_index(['tag', 'time-slot'], inplace=True)

    return stats, chart_data

if (con.dialect.has_table(con, 'stats')) & (con.dialect.has_table(con, 'chart_data')):
    stats = pd.read_sql_table('stats',con)
    chart_data = pd.read_sql_table('chart_data',con)
    stats.set_index(['emoji', 'time-slot', 'tag'], inplace=True)
    chart_data.set_index(['tag', 'time-slot'], inplace=True)
else:
    stats, chart_data = create_empty_df()


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

def round_time(t):
    return t - dt.timedelta(seconds=t.second, microseconds=t.microsecond)

def process_message(msg):
    tags = json.loads(msg)['event']['tags']

    msg = list(set([m for m in msg if m in emoji_dict]))
    curr_timeslot = round_time(dt.datetime.now())

    # drop older
    stats.drop(stats.index[stats.reset_index()['time-slot'] < (curr_timeslot - dt.timedelta(hours=24))], inplace=True)

    for m in msg:
        for tag in tags:
            if (m, curr_timeslot, tag) not in stats.index:
                stats.ix[(m, curr_timeslot, tag), 'count'] = 0
            stats.ix[(m, curr_timeslot, tag), 'count'] = stats.ix[(m, curr_timeslot, tag), 'count'] + 1

    stats.to_sql('stats',con,if_exists='replace',dtype={'time-slot': TIMESTAMP(timezone=False)})

    overall_sent, top_emoji = recalc_stats()

    overall_sent.to_sql('overall_sent',con,if_exists='replace')
    top_emoji.to_sql('top_emoji',con,if_exists='replace')

    print("< {} {}".format(''.join(tags), msg))


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
    top_emoji['last_hour_count'] = list(last_hour_count)
    top_emoji = top_emoji.astype(int)
    top_emoji = top_emoji.reset_index().sort_values('count', ascending=False).groupby('tag').head(5).reset_index()[
        ['tag', 'emoji', 'count', 'last_hour_count']]

    for tag in sent_map['tag'].unique():
        chart_data.ix[(tag, curr_timeslot), 'sent'] = overall_sent.ix[tag]
    chart_data.to_sql('chart_data', con, if_exists='replace',dtype={'time-slot': TIMESTAMP(timezone=False)})

    return overall_sent, top_emoji


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

loop = asyncio.get_event_loop()
#process_message('{"event":{"tags":["Лето"],"text":"⏰😊"}}')

loop.run_until_complete(listen_feed())

