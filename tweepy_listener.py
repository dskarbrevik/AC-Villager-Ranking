class ACNHStreamListener(tweepy.StreamListener):

    def __init__(self):
        super(ACNHStreamListener, self).__init__()
        self.tweets = []

    def on_status(self, status):
        self.tweets.append(status)
