from tweepy_listener import ACNHStreamListener
import tweepy
import json
import os
import boto3
from datetime import datetime

if __name__=='__main__':


    # setup credentials and terms to track
    with open('./config.json','rb') as file:
        config = json.load(file)

    with open(config['twitter_terms_path'],'r') as file2:
        lines = file2.readlines()
    terms = []
    for line in lines:
        terms.append(line.strip())

    with open(config['acnh_animals_path'],'r') as file3:
        lines = file3.readlines()
        villager_data = []
        for line in lines:
            villager_data.append(line.strip())


    consumer_key = config['twitter_credentials']['consumer_key']
    consumer_secret = config['twitter_credentials']['consumer_secret']
    access_token = config['twitter_credentials']['access_token']
    access_token_secret = config['twitter_credentials']['access_token_secret']

    sns = boto3.resource(service_name='sns',region_name='us-east-1')
    topic = sns.Topic(config['sns_error_topic'])
    # setup twitter stream
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    print(f"Starting Twitter stream to track the following terms: {terms}")
    try:
        stream_listener = ACNHStreamListener(villager_data=villager_data,
                                             dynamo_villager_table=config['dynamo_villager_table'],
                                             dynamo_sysinfo_table=config['dynamo_sysinfo_table'],
                                             dynamo_tweet_table=config['dynamo_tweet_table'],
                                             sns_error_topic=config['sns_error_topic'])
        stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
        stream.filter(track=terms)
    except Exception as e:
        topic.publish(message=f"Shutting down scraper.\nError = {e}\nShutdown time = {datetime.now().strftime('%m/%d/%Y %H:%M:%S')}")
        sys.exit()
