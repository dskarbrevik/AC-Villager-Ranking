import boto3
import tweepy
import os
import time

class ACNHStreamListener(tweepy.StreamListener):

    def __init__(self, s3_bucket, file_cutoff_size=10):
        super(ACNHStreamListener, self).__init__()
        self.current_file_name, self.current_path = self.new_paths()
        self.file_size = file_cutoff_size*1000000
        self.s3 = boto3.client('s3')
        self.s3_bucket = s3_bucket
        if not os.path.exists("./tweet_files"):
            os.mkdir("./tweet_files")

    def new_paths(self):
        file_name = "{0}.txt".format(str(round(time.time(),2)).replace(".",""))
        file_path = "./tweet_files/{0}".format(file_name)
        return file_name, file_path

    def too_big(self):
        size = os.path.getsize(self.current_path)
        if size >= self.file_size:
            return True
        else:
            return False

    def save_to_s3(self):
        self.s3.upload_file(self.current_path, self.s3_bucket, self.current_file_name)

    def on_status(self, status):
        try:
            tweet = str(status.__dict__['_json'])
            with open(self.current_path,'a') as file:
                file.write(tweet)
                file.write("\n<SEP>\n")
            if self.too_big():
                self.save_to_s3()
                os.remove(self.current_path)
                self.current_file_name, self.current_path = self.new_paths()


        except Exception as e:
            print(e)
