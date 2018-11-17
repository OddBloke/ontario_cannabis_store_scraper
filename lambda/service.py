# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta

import boto3
import requests
import twitter


TWEET_PREFIX = (
    'New availability on Ontario Cannabis Store:\n{name} by {brand_twitter}')
TWEET_SUFFIX = '\n#ocs\n{url}'
BRAND_TWITTERS = {
    '7ACRES': '@7acresMJ',
    # No tweets from the account yet, and not pointed at from their website
    # 'Ace Valley': '@acevalleyco',
    # Alta Vie is a MedReleaf brand, so doesn't have its own account
    # 'Alta Vie': '',
    'Aurora': '@aurora_mmj',
    'Canna Flower': '@CannaFarmsLtd',
    # Possibly at @covecrafted but account is currently protected
    # 'Cove': '',
    'DNA Genetics': '@dnagenetics',
    # No tweets from the account yet, and not pointed at from their website
    # 'Edison': '@edisoncannabis',
    # 'Fireside': '',
    'Flowr': '@flowrcanada',
    'Haven St.': '@ExploreHavenSt',
    'Hexo': '@hexo',
    'High Tide': '@RealHighTideInc',
    # 'Irisa': '',
    # 'LBS': '',
    'Liiv': '@liivcannabis',
    # 'Northern Harvest': '',
    # 'Plain Packaging': '',
    # 'RIFF': '',
    'Redecan': '@redecanca',
    # Recreational brand of United Greeneries, no dedicated account
    # 'Royal High': '',
    'SYNR.G': '@SynrgCannabis',
    # No tweets from the account yet, and not pointed at from their website
    # 'San Rafael': '@SanRafael71',
    'Seven Oaks': '@sevenoakscanada',
    # Recreational brand of Aphria, no dedicated account
    # 'Solei': '',
    'Symbl': '@SymblCannabis',
    'Tweed': '@TweedInc',
    'Up': '@updotca',
    # 'Vertical': '',
    'WeedMD': '@WeedMD',
    # 'Woodstock': '',
}

LOW_STOCK_MSG = ('Ontario Cannabis Store are running low on:\n{name} by'
                 ' {brand_twitter}\nOnly {combined_total} {units} left!'
                 + TWEET_SUFFIX)


def _do_request(query):
    morph_api_url = "https://api.morph.io/OddBloke/ontario_cannabis_store_scraper/data.json"
    morph_api_key = os.environ['MORPH_API_KEY']
    r = requests.get(morph_api_url, params={
        'key': morph_api_key,
        'query': query,
    })
    ret = r.json()
    print(ret)
    return ret


def _fix_image(image):
    if image is not None and not image.startswith('http'):
        return 'https:' + image
    return


def _format_status(entry, content):
    entry['brand_twitter'] = BRAND_TWITTERS.get(entry['brand'], entry['brand'])
    return (TWEET_PREFIX + content + TWEET_SUFFIX).format(**entry)


def _get_standalone_tweet_content(entry):
    entry['price'] = float(entry['price'])
    return _format_status(entry, ' (${price:.2f}, {standalone_availability} left)')


def _get_variant_tweet_content(entry):
    # Determine the relevant variants
    data = _do_request(
        'SELECT * FROM history_availability'
        ' WHERE timestamp={timestamp} AND brand="{brand}"'
        ' AND name="{name}"'.format(**entry))
    variants = {d['size']: d for d in data if d['availability']}
    print(variants)
    content = '\n'
    present_variants = []
    for size in sorted(variants):
        availability = variants[size]['availability']
        if availability is None or availability == 0:
            continue
        content = content + '{:.3g}g (${:.2f}, {} left)\n'.format(
            size, float(variants[size]['price'])/100, availability)
    return _format_status(entry, content)


def low_stock_tweets(current_state):
    statuses = []
    data = _do_request(
        'SELECT brand,sku,image,url,name,standalone_availability,COALESCE(total, 0) + COALESCE(standalone_availability, 0) AS combined_total FROM (SELECT h.brand,h.name,h.sku,image,url,standalone_availability,SUM(size*availability) as total FROM history h LEFT JOIN history_availability ha ON h.timestamp = ha.timestamp AND h.brand = ha.brand AND h.name = ha.name WHERE h.timestamp = (SELECT DISTINCT timestamp FROM history ORDER BY timestamp DESC LIMIT 1) GROUP BY h.sku) WHERE combined_total < 100 ORDER BY combined_total',
    )
    update_cutoff = datetime.now() - timedelta(hours=8)
    print 'Update cutoff:', update_cutoff
    last_updates = current_state.get('low_stock_updates', {})
    for entry in data:
        last_update = datetime.fromtimestamp(last_updates.get(entry['sku'], 0))
        print 'Last update for', entry['sku'], 'at', last_update
        if last_update >= update_cutoff:
            print('Skipping')
            continue
        image = _fix_image(entry.get('image'))
        units = ('units' if entry.get('standalone_availability') is not None
                 else 'grams')
        entry['brand_twitter'] = BRAND_TWITTERS.get(entry['brand'],
                                                    entry['brand'])
        status = LOW_STOCK_MSG.format(units=units, **entry)
        last_updates[entry['sku']] = int(datetime.now().strftime('%s'))
        print(status, len(status))
        statuses.append((status, image))
        break  # We only want to put out one of these updates at a time
    current_state['low_stock_updates'] = last_updates
    return current_state, statuses


def fun_fact_tweets(current_state):
    statuses = []
    update_cutoff = datetime.now() - timedelta(hours=8)
    last_updates = current_state.get('fun_facts', {})
    if datetime.fromtimestamp(last_updates.get('24h_best_sellers', 0)) < update_cutoff:
        data = _do_request(
            "SELECT old.brand,old.name,old.image,old.total AS before,new.total AS after,(COALESCE(old.total, 0)-COALESCE(new.total, 0))/1000 as sold FROM (SELECT history.brand,history.name,history.image,SUM(availability*size) AS total FROM history JOIN history_availability ON history.brand = history_availability.brand AND history.name = history_availability.name AND history.timestamp = history_availability.timestamp AND history.timestamp = (SELECT DISTINCT timestamp FROM history WHERE timestamp < strftime('%s', 'now', '-1 day') ORDER BY timestamp DESC LIMIT 1) GROUP BY history.brand, history.name) old LEFT JOIN (SELECT history.brand,history.name,SUM(availability*size) AS total FROM history JOIN history_availability ON history.brand = history_availability.brand AND history.name = history_availability.name AND history.timestamp = history_availability.timestamp AND history.timestamp = (SELECT DISTINCT timestamp FROM history ORDER BY timestamp DESC LIMIT 1) GROUP BY history.brand, history.name) new ON old.brand = new.brand AND old.name = new.name ORDER BY sold DESC LIMIT 3"
        )
        status = 'Top selling strains on Ontario Cannabis Store (last 24 hours):\n'
        image = None
        for entry in data:
            entry['brand_twitter'] = BRAND_TWITTERS.get(entry['brand'],
                                                        entry['brand'])
            if image is None:
                image = _fix_image(entry.get('image'))
            status += '{name} by {brand_twitter} ({sold:.3f}kg sold)\n'.format(
                **entry)
        status += '\n#ocs'
        print(status, len(status))
        last_updates['24h_best_sellers'] = int(datetime.now().strftime('%s'))
        statuses = [(status, image)]
        print(statuses)
    current_state['fun_facts'] = last_updates
    return current_state, statuses


def handler_for_timestamp(current_state, debug=False):
    timestamp = current_state['last_timestamp']
    data = _do_request(
        'SELECT * FROM history'
        ' WHERE timestamp = (SELECT DISTINCT timestamp FROM history ORDER BY timestamp DESC LIMIT 1)'
        ' AND url NOT IN (SELECT url FROM history WHERE timestamp = {})'.format(timestamp))

    statuses = []
    new_timestamp = None
    for entry in data:
        image = _fix_image(entry.get('image'))
        if entry['standalone_price'] is not None:
            status = _get_standalone_tweet_content(entry)
        else:
            status = _get_variant_tweet_content(entry)
        print(status, len(status))
        statuses.append((status, image))
        new_timestamp = entry['timestamp']

    if new_timestamp is not None:
        current_state['last_timestamp'] = str(new_timestamp)

    if not statuses:
        # No new products, look for low-stock products to notify about
        current_state, statuses = low_stock_tweets(current_state)

    if not statuses:
        # No new product or low-stock updates, fun fact time
        current_state, statuses = fun_fact_tweets(current_state)

    if not debug:
        api = twitter.Api(
            consumer_key=os.environ['TWITTER_CONSUMER_KEY'],
            consumer_secret=os.environ['TWITTER_CONSUMER_SECRET'],
            access_token_key=os.environ['TWITTER_ACCESS_TOKEN_KEY'],
            access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET'])
        for (status, image) in statuses:
            print(api.PostUpdate(status, media=image))
    return str(data), current_state


def handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ocs_tweeter')
    response = table.scan()
    assert len(response['Items']) == 1
    current_state = response['Items'][0]
    last_timestamp = current_state['last_timestamp']

    response_text, new_state = handler_for_timestamp(current_state)

    if new_state is not None:
        table.delete_item(
            Key={'last_timestamp': last_timestamp})
        table.put_item(Item=new_state)

    return {
        'statusCode': 200,
        'body': response_text,
    }


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        low_stock_updates = {sku: int(sys.argv[1]) for sku in sys.argv[2:]}
        print(handler_for_timestamp(
            {
                'last_timestamp': sys.argv[1],
                'low_stock_updates': low_stock_updates
            },
            debug=True,
        ))
