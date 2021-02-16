from kafka import KafkaProducer
import kafka
import json
import tweepy
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener

consumer_key = "DvAmlzyyYiJPl62XFk5qmv90R"
consumer_secret = "KGugZn1Q9EB8odKUaaAlvnGXLrf7Z8KOSAdtxEET0qwVpGCjs1"
access_token = "2412563076-gWmFBMCSsoNeXOFEnw8yHNqqe6k96fH4gQqYhgF"
access_secret = "BBS9qpx6y6vopZq7qmdrWC7dYxwv7R1zcLHAa0Qzf2A2C"


hashtag = "corona"
auth = OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_secret)

api = tweepy.API(auth)
class KafkaPushListener(StreamListener):
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    def on_data(self, data):
        self.producer.send("twitter_" + hashtag +"_tweet", data.encode('utf-8'))
        print(data)
        return True

    def on_error(self, status):
        print(status)
        return True


twitter_stream = Stream(auth, KafkaPushListener())

hashStr = "#"+ hashtag

twitter_stream.filter(track=[hashStr])


