import boto3
from boto3.dynamodb.conditions import Attr,Key
from botocore.exceptions import ClientError
import tweepy
import os
import time
from textblob import TextBlob
from decimal import Decimal
from datetime import date
import traceback

class ACNHStreamListener(tweepy.StreamListener):

    def __init__(self, villager_data, dynamo_table_name):
        super(ACNHStreamListener, self).__init__()
        # self.current_file_name, self.current_path = self.new_paths()
        # self.file_size = file_cutoff_size*1000000
        # self.s3 = boto3.client('s3')
        # self.s3_bucket = s3_bucket
        # if not os.path.exists("./tweet_files"):
        #     os.mkdir("./tweet_files")
        self.dynamo = boto3.resource('dynamodb', region_name='us-east-1')
        self.table = self.dynamo.Table(dynamo_table_name)
        print(self.table)
        self.villagers = villager_data

    def get_attrs(self):
        today = date.today()
        pos_attr = f"pos_{today.strftime('%m_%d_%Y')}"
        neg_attr = f"neg_{today.strftime('%m_%d_%Y')}"
        return pos_attr, neg_attr

    def parse_tweet(self, tweet):

        keep = {}
        keep['created_at'] = tweet.get('created_at')
        keep['id_str'] = tweet.get('id_str')
        keep['text'] = tweet.get('text')
        keep['possibly_sensitive'] = tweet.get('possibly_sensitive')
        keep['user_id'] = tweet.get('user', {}).get('id')
        keep['user_location'] =tweet.get('user', {}).get('location')
        keep['hashtags'] = tweet.get('entities',{}).get('hashtags')
        keep['retweeted_long_text'] = tweet.get('retweeted_status',{}).get('extended_tweet',{}).get('full_text')
        keep['retweeted_hashtags'] = tweet.get('retweeted_status',{}).get('entities',{}).get('hashtags')

        if keep['retweeted_long_text']:
            keep['text'] = keep['retweeted_long_text']
            keep.pop('retweeted_long_text', None)
        if keep['retweeted_hashtags']:
            keep['hashtags'] == keep['retweeted_hashtags']
            keep.pop('retweeted_hashtags', None)

        return keep

    # def new_paths(self):
    #     file_name = "{0}.txt".format(str(round(time.time(),2)).replace(".",""))
    #     file_path = "./tweet_files/{0}".format(file_name)
    #     return file_name, file_path
    #
    # def too_big(self):
    #     size = os.path.getsize(self.current_path)
    #     if size >= self.file_size:
    #         return True
    #     else:
    #         return False
    #
    # def save_to_s3(self):
    #     self.s3.upload_file(self.current_path, self.s3_bucket, self.current_file_name)


    def get_villager_data(self, tweet):

        tweet = tweet['text'].lower()
        animals = []
        sentiment = ""
        for villager in self.villagers:
            if villager.lower() in tweet:
                animals.append(villager)
        if animals:
            sentiment_score = TextBlob(tweet).sentiment.polarity
            if sentiment_score >= 0:
                sentiment = "positive"
            elif sentiment_score < 0:
                sentiment = "negative"

        return (animals,sentiment)

    def update_dynamo(self, animal, sentiment):

        if sentiment=="positive":
            pos_counter = 1
            neg_counter = 0
        elif sentiment=="negative":
            pos_counter = 0
            neg_counter = 1
        else:
            raise Exception("sentiment not negative or positive... can't update dynamodb.")

        pos_attr, neg_attr = self.get_attrs()

        try:
            Key={'villager_name':animal}
            UpdateExpression=f"set {pos_attr} = {pos_attr} + :pos_increment, " + \
                            f"{neg_attr} = {neg_attr} + :neg_increment, " + \
                            f"pos_total = pos_total + :pos_increment, " + \
                            f"neg_total = neg_total + :neg_increment"

            ExpressionAttributeValues={
                ':pos_increment': pos_counter,
                ':neg_increment': neg_counter
            }

            table.update_item(Key=Key,
                              UpdateExpression=UpdateExpression,
                              ExpressionAttributeValues=ExpressionAttributeValues,
                              ConditionExpression=Attr(pos_attr).exists() & Attr(neg_attr).exists())

        except ClientError as e:
            # if we don't pass ConditionExpression we need to initialize the attributes
            Key={'villager_name':animal}
            UpdateExpression=f"set {pos_attr} = :pos_increment, " + \
                             f"{neg_attr} = :neg_increment, " + \
                             f"pos_total = pos_total + :pos_increment, " + \
                             f"neg_total = neg_total + :neg_increment"

            ExpressionAttributeValues={
                ':pos_increment': pos_counter,
                ':neg_increment': neg_counter
            }

            table.update_item(Key=Key,
                              UpdateExpression=UpdateExpression,
                              ExpressionAttributeValues=ExpressionAttributeValues)

            if e.response['Error']['Code'] != 'ConditionalCheckFailedException':
                raise

    def on_status(self, status):
        try:
            tweet = self.parse_tweet(status.__dict__['_json'])

            data = self.get_villager_data(tweet)

            if data[0]:
                for animal in data[0]:
                    self.update_dynamo(animal, data[1])

        except Exception as e:
            print(traceback.print_exc())
