# -*- coding: utf-8 -*-
import os

import boto3
import requests
import twitter


def handler(event, context):
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('ocs_tweeter')
    response = table.scan()
    assert len(response['Items']) == 1
    timestamp = response['Items'][0]['last_timestamp']

    # We're always asking for json because it's the easiest to deal with
    morph_api_url = "https://api.morph.io/OddBloke/ontario_cannabis_store_scraper/data.json"

    # Keep this key secret!
    morph_api_key = os.environ['MORPH_API_KEY']

    r = requests.get(morph_api_url, params={
        'key': morph_api_key,
        'query': "SELECT * FROM history WHERE timestamp = (SELECT DISTINCT timestamp FROM history ORDER BY timestamp DESC LIMIT 1) AND url NOT IN (SELECT url FROM history WHERE timestamp = {})".format(timestamp),
    })
    print(r.json())

    api = twitter.Api(consumer_key=os.environ['TWITTER_CONSUMER_KEY'],
                      consumer_secret=os.environ['TWITTER_CONSUMER_SECRET'],
                      access_token_key=os.environ['TWITTER_ACCESS_TOKEN_KEY'],
                      access_token_secret=os.environ['TWITTER_ACCESS_TOKEN_SECRET'])
    new_timestamp = None
    for entry in r.json():
        entry['price'] = float(entry['price'])
        status = 'Newly listed on Ontario Cannabis Store:\n{name} by {brand} (${price:.2f})\n#ocs\n{url}'.format(**entry)
        image = entry.get('image')
        if image is not None and not image.startswith('http'):
            image = 'https:' + image
        print(api.PostUpdate(status, media=image))
        new_timestamp = entry['timestamp']

    if new_timestamp is not None:
        table.delete_item(Key={'last_timestamp': timestamp})
        table.put_item(Item={'last_timestamp': str(new_timestamp)})

    return {
        'statusCode': 200,
        'body': r.text,
    }
