from tweepy_listener import ACNHStreamListener
import tweepy
import json
import os

if __name__=='__main__':


    # setup credentials and terms to track
    with open('./config.json','rb') as file:
        config = json.load(file)

    with open(config['twitter_terms_path'],'r') as file2:
        lines = file.readlines()
    terms = []
    for line in lines:
        terms.append(line.strip())

    consumer_key = config['twitter_credentials']['consumer_key']
    consumer_secret = config['twitter_credentials']['consumer_secret']
    access_token = config['twitter_credentials']['access_token']
    access_token_secret = config['twitter_credentials']['access_token_secret']

    # setup twitter stream
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    api = tweepy.API(auth)

    print(f"Starting Twitter stream to track the following terms: {terms}")

    stream_listener = ACNHStreamListener(dynamo_table=config['dynamo_table'])
    stream = tweepy.Stream(auth=api.auth, listener=stream_listener)
    stream.filter(track=terms)
