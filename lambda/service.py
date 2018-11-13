# -*- coding: utf-8 -*-
import os
from datetime import datetime, timedelta

import boto3
import requests
import twitter


TWEET_PREFIX = 'Newly listed on Ontario Cannabis Store:\n{name} by {brand}'
TWEET_SUFFIX = '\n#ocs\n{url}'


def _format_status(entry, content):
    return (TWEET_PREFIX + content + TWEET_SUFFIX).format(**entry)


def _get_standalone_tweet_content(entry):
    entry['price'] = float(entry['price'])
    return _format_status(entry, ' (${price:.2f}, {standalone_availability} left)')


def _get_variant_tweet_content(entry):
    # Determine the relevant variants
    present_variants = sorted([
        key.split('_')[0] for key in entry
        if key.endswith('_price') and entry[key] is not None])
    content = '\n'
    for variant in present_variants:
        availability = entry[variant + '_availability']
        if availability is None or availability == 0:
            continue
        content = content + '{} (${:.2f}, {} left)\n'.format(variant, float(entry[variant + '_price'])/100, entry[variant + '_availability'])
    return _format_status(entry, content)


def handler_for_timestamp(current_state, debug=False):
    # We're always asking for json because it's the easiest to deal with
    morph_api_url = "https://api.morph.io/OddBloke/ontario_cannabis_store_scraper/data.json"

    # Keep this key secret!
    morph_api_key = os.environ['MORPH_API_KEY']

    timestamp = current_state['last_timestamp']
    r = requests.get(morph_api_url, params={
        'key': morph_api_key,
        'query': "SELECT * FROM history WHERE timestamp = (SELECT DISTINCT timestamp FROM history ORDER BY timestamp DESC LIMIT 1) AND url NOT IN (SELECT url FROM history WHERE timestamp = {})".format(timestamp),
    })
    print(r.json())

    statuses = []
    new_timestamp = None
    for entry in r.json():
        image = entry.get('image')
        if image is not None and not image.startswith('http'):
            image = 'https:' + image
        if entry['standalone_price'] is not None:
            status = _get_standalone_tweet_content(entry)
        else:
            status = _get_variant_tweet_content(entry)
        print(status, len(status))
        statuses.append((status, image))
        new_timestamp = entry['timestamp']

    item = {
        'last_timestamp': str(new_timestamp) if new_timestamp is not None else timestamp,
    }

    if not statuses:
        # No new products, look for low-stock products to notify about
        r = requests.get(morph_api_url, params={
            'key': morph_api_key,
            'query': "SELECT brand,sku,image,url,name,standalone_availability,COALESCE(total, 0) + COALESCE(standalone_availability, 0) AS combined_total FROM (SELECT brand,h.sku,image,url,name,standalone_availability,SUM(size*availability) as total FROM history h LEFT JOIN history_availability ha ON h.timestamp = ha.timestamp AND h.sku = ha.sku WHERE h.timestamp = (SELECT DISTINCT timestamp FROM history ORDER BY timestamp DESC LIMIT 1) GROUP BY h.sku) WHERE combined_total < 100 ORDER BY combined_total",
        })
        print(r.json())
        update_cutoff = datetime.now() - timedelta(hours=24)
        last_updates = current_state.get('low_stock_updates', {})
        for entry in r.json():
            last_update = last_updates.get(entry['sku'], 0)
            if datetime.fromtimestamp(last_update) >= update_cutoff:
                continue
            image = entry.get('image')
            if image is not None and not image.startswith('http'):
                image = 'https:' + image
            if entry.get('standalone_availability') is not None:
                status = 'Ontario Cannabis Store are running low on:\n{name} by {brand}\nOnly {combined_total} units left!\n#ocs\n{url}'.format(**entry)
            else:
                status = 'Ontario Cannabis Store are running low on:\n{name} by {brand}\nOnly {combined_total} grams left!\n#ocs\n{url}'.format(**entry)
            last_updates[entry['sku']] = int(datetime.now().strftime('%s'))
            print(status, len(status))
            statuses.append((status, image))
            break  # We only want to put out one of these updates at a time
        item['low_stock_updates'] = last_updates

    if not debug:
        api = twitter.Api(
            consumer_key=os.environ['TWITTER_CONSUMER_KEY'],
            consumer_secret=os.environ['TWITTER_CONSUMER_SECRET'],
            access_token_key=os.environ['TWITTER_ACCESS_TOKEN_KEY'],
            access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET'])
        for (status, image) in statuses:
            print(api.PostUpdate(status, media=image))
    return r.text, item


def handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ocs_tweeter')
    response = table.scan()
    assert len(response['Items']) == 1
    current_state = response['Items'][0]

    response_text, new_state = handler_for_timestamp(current_state)

    if new_state is not None:
        table.delete_item(
            Key={'last_timestamp': current_state['last_timestamp']})
        table.put_item(Item=new_state)

    return {
        'statusCode': 200,
        'body': response_text,
    }


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        handler_for_timestamp({'last_timestamp': sys.argv[1]}, debug=True)
