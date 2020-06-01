import boto3
import tweepy
import decimal
import json

class ACNHStreamListener(tweepy.StreamListener):

    def __init__(self, dynamo_table):
        super(ACNHStreamListener, self).__init__()
        self.tweets = []
        self.dynamo = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamo.Table(dynamo_table)

    def convert_float(num):
        return decimal.Decimal(str(round(float(num), 2)))

    def on_status(self, status):
        item = status.__dict__['_json']
        json_dump = json.dumps(item)
        json_item = json.loads(json_dump, parse_float=self.convert_float)
        self.table.put_item(Item=json_item)
