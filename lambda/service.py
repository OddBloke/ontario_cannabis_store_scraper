# -*- coding: utf-8 -*-
import os

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


def handler_for_timestamp(timestamp, debug=False):
    # We're always asking for json because it's the easiest to deal with
    morph_api_url = "https://api.morph.io/OddBloke/ontario_cannabis_store_scraper/data.json"

    # Keep this key secret!
    morph_api_key = os.environ['MORPH_API_KEY']

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
        new_timestamp = entry['timestamp']

    if not debug:
        api = twitter.Api(
            consumer_key=os.environ['TWITTER_CONSUMER_KEY'],
            consumer_secret=os.environ['TWITTER_CONSUMER_SECRET'],
            access_token_key=os.environ['TWITTER_ACCESS_TOKEN_KEY'],
            access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET'])
        for (status, image) in statuses:
            print(api.PostUpdate(status, media=image))
    return new_timestamp


def handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ocs_tweeter')
    response = table.scan()
    assert len(response['Items']) == 1
    timestamp = response['Items'][0]['last_timestamp']

    new_timestamp = handler_for_timestamp(timestamp)

    if new_timestamp is not None:
        table.delete_item(Key={'last_timestamp': timestamp})
        table.put_item(Item={'last_timestamp': str(new_timestamp)})

    return {
        'statusCode': 200,
        'body': r.text,
    }


if __name__ == '__main__':
    import sys
    if len(sys.argv) > 1:
        handler_for_timestamp(sys.argv[1], debug=True)
