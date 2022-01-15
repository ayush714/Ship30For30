import numpy as np
import pandas as pd
import tweepy
from application_logger import CustomApplicationLogger
import json


class DataIngestion:
    def __init__(self) -> None:
        self.logger = CustomApplicationLogger()
        self.file_obj = open(
            r"E:\QnAMedical\Ship30For30\logs\DataIngestionLogs.txt", "a+"
        )
        self.logger.log(self.file_obj, "Data Ingestion Class is created")

    def authenticate_tweepy(self):
        try:
            consumer_key = json.loads(open("keys.json").read())["consumer_key"]
            consumer_secret = json.loads(open("keys.json").read())["consumer_secret"]
            access_token = json.loads(open("keys.json").read())["access_token"]
            access_token_secret = json.loads(open("keys.json").read())[
                "access_token_secret"
            ]

            auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
            auth.set_access_token(access_token, access_token_secret)
            api = tweepy.API(auth)
            self.logger.log(self.file_obj, "Authentication is successful")
            return api
        except Exception as e:
            self.logger.log(self.file_obj, "Authentication is not successful")
            self.logger.log(self.file_obj, str(e))
            raise e

    def get_tweets(self):
        # get the tweets which are of hashtag "Ship30For30"
        try:
            api = self.authenticate_tweepy()
            tweets = tweepy.Cursor(api.search_tweets, q="#Ship30For30").items(100)
            self.logger.log(self.file_obj, "Tweets are collected successfully")
            return tweets
        except Exception as e:
            self.logger.log(self.file_obj, "Tweets are not collected successfully")
            self.logger.log(self.file_obj, str(e))
            raise e

    def search_by_hashtag(self, api, words, number_of_tweets): 
        try:  
            self.logger.log(self.file_obj, "Started Collecting Tweets")
            id = []
            created_at = []
            username = []
            location = []
            retweetcount = []
            text = []
            likes = []
            links = []
            tweets = tweepy.Cursor(api.search_tweets, q=words, tweet_mode = "extended").items(number_of_tweets)
            list_tweets = [tweet for tweet in tweets]

            for tweet in list_tweets:
                id.append(tweet.id)
                created_at.append(tweet.created_at)
                username.append(tweet.user.screen_name)
                location.append(tweet.user.location)
                likes.append(tweet.favorite_count)  
                text.append(tweet.full_text)
                # if 'retweeted_status' in dir(tweet):  
                #     retweetcount.append(tweet.retweeted_status.full_text)   
                # else:  
                #     # retweetcount.append("NO RETWEET")
                links.append("https://twitter.com/twitter/statuses/" + str(tweet.id))

            df = pd.DataFrame()
            df["id"] = id
            df["created_at"] = created_at
            df["username"] = username
            # df["retweetcount"] = retweetcount
            df["text"] = text
            df["likes"] = likes
            df["links"] = links
            return df  
        except Exception as e: 
            self.logger.log(self.file_obj, "Tweets are not collected successfully")
            self.logger.log(self.file_obj, str(e))
            raise e 

        

if __name__ == "__main__":
    # DataIngestions = DataIngestion()
    # words = "#Ship30For30"
    # date_since = "2021-12-01"
    # date_until = "2022-01-05"
    # api = DataIngestions.authenticate_tweepy()
    # df  = DataIngestions.search_by_hashtag(api, words) 
    pass 
# following = tweet.user.friends_count
#             followers = tweet.user.followers_count
#             totaltweets = tweet.user.statuses_count
#             retweetcount = tweet.retweet_count

