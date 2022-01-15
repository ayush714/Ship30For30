import requests
import os
from tqdm import tqdm
import pandas as pd  

from data_ingestion import DataIngestion
DataIngestions = DataIngestion()
api = DataIngestions.authenticate_tweepy()

image_origins = {
    "tweet_url": [],
    "image_id": [],
    "image_url": [],
}

dfs = pd.read_csv(r"E:\QnAMedical\Ship30For30\src\dfs.csv") 
links = dfs["links"]
folder_name = "downloaded_media"
# keep track of IDs of downloaded images to avoid re-downloads
downloaded_img_ids = []

for tweet in all_results:
    
    tweet_url = get_tweet_url(tweet)
    
    if tweet.media:
        
        for media in tweet.media: # a tweet can have multiple images/videos
            
            media_id = str(media.id)
            media_url = media.media_url
            
            if not(media_id in downloaded_img_ids): # don't re-download images
                
                file_name = media_id
                file_type = os.path.splitext(media_url)[1]

                urllib.request.urlretrieve(media_url, os.path.join(folder_name,file_name+file_type))
                
                downloaded_img_ids.append(media_id)
                
                # save image origin info
                image_origins["tweet_url"].append(tweet_url)
                image_origins["image_id"].append(media_id)
                image_origins["image_url"].append(media_url)


if __name__ == "__main__": 
    pass