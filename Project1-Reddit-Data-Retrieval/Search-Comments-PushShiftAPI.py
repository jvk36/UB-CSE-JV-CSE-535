import time

import requests
import json
from datetime import datetime

import pandas as pd
import itertools
import pickle


def convert_date(timestamp):
    # change the created_utc column date format from UTC to YYYY-MM-DDThh:mm:ssZ
    # return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)

# ENDPOINTS:
# - For fetching submissions: GET https://api.pushshift.io/reddit/search/submission
# - For fetching comment_ids: GET
# https://api.pushshift.io/reddit/submission/comment_ids/<submission_id>
# - For fetching comments: GET https://api.pushshift.io/reddit/search/comment
# Follow the documentation for parameters: https://reddit-api.readthedocs.io/en/latest/#

def get_comments(keyword, how_many=500):
    # fields parameter details for comments -
    # *** DIRECT FIELDS: fields - id, subreddit, full_link, body, author, parent_id
    # *** CUSTOM FIELDS - parent_body, is_submission, created_at - first two are TO DO
    # *** converted created_utc to created_at using the format YYYY-MM-DDThh:mm:ssZ
    url = "https://api.pushshift.io/reddit/search/comment?q="
    params = "&before=10d&size=" + str(how_many) + "&fields=id,subreddit,full_link,body,author,parent_id,created_utc"

    while True:
        try:
            res = requests.get(url + keyword + params).json()
            break
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            time.sleep(100)
    return res


def get_comments_df(keywords, how_many=500):
    dframes = []
    for keyword in keywords:
        records = get_comments(keyword, how_many)
        df = pd.DataFrame.from_dict(records["data"])
        dframes.append(df)
    df = pd.concat(dframes)
    created_at = []
    submission_col = []
    body_col = []
    for s in df['created_utc']:
        created_at.append(convert_date(s))
        submission_col.append(True)
        body_col.append("")
    df.pop('created_utc')
    df['created_at'] = created_at
    df['is_submission'] = submission_col
    df['parent_body'] = body_col
    return df


# PART 1: Data Ingestion Requirements
# 4. Get a minimum of 20,000 comments
topics = ["metaverse", "crypto", "nft", "tesla", "bitcoin", "google", "twitter", "instagram", "tiktok", "facebook",
          "amazon", "google", "twitter", "snapchat", "apple", "microsoft", "salesforce", "workday", "twilio", "roblox",
          "football", "soccer", "cricket", "swimming", "hockey", "baseball", "tennis", "badminton", "ping pong",
          "surfing",
          "psychology", "sociology", "science", "physics", "chemistry", "math", "toxicology", "foresnics",
          "pharmacology", "physiology",
          "democracy", "socialism", "dictatorship", "imperialism", "democrat", "republican", "biden", "trump", "obama",
          "despot",
          "esg", "epa", "pollution", "climate change", "global warming", "sustainability", "carbon emission",
          "oil majors", "hydropower", "green initiative",
          "tech", "saas", "paas", "iaas", "social media", "blockchain", "youtube", "netflix", "streaming", "spotify",
          "medicaid", "medicare", "medicare advantage", "medical insurance", "value based care", "affordable care act",
          "pcp", "hmo", "fsa", "hsa",
          "university", "homeschooling", "special education", "k12", "kindergarten", "preschool", "highschool",
          "certification", "dipoloma", "career",
          "usa", "uk", "china", "india", "france", "russia", "ukraine", "germany", "taiwan", "australia"
          ]

df = get_comments_df(topics, how_many=200)
display_df(df)

