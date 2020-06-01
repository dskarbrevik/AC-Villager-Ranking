import boto3
import tweepy

class ACNHStreamListener(tweepy.StreamListener):

    def __init__(self, dynamo_table):
        super(ACNHStreamListener, self).__init__()
        self.tweets = []
        self.dynamo = boto3.resource('dynamodb')
        self.table = self.dynamo.Table(dynamo_table)
    def on_status(self, status):
        item = status.__dict__['_json']
        self.table.put_item(Item=item)
