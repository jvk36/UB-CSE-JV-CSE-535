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


def get_submissions(keyword, how_many=500, is_search=True):
    search_url = "https://api.pushshift.io/reddit/search/submission/?q="
    subreddit_url = "https://api.pushshift.io/reddit/search/submission/?subreddit="
    url = search_url if is_search else subreddit_url

    # fields parameter details for submission -
    # *** DIRECT FIELDS: fields - id, subreddit, full_link, title, selftext, author
    # *** CUSTOM FIELDS - is_submission, topic, created_at - first two are TO DO
    # *** converted created_utc to created_at using the format YYYY-MM-DDThh:mm:ssZ
    params = "&selftext:not=[removed]&before=365d&size=" + str(
        how_many) + "&num_comments='>10'&fields=id,subreddit,full_link,title,selftext,author,created_utc"

    while True:
        try:
            res = requests.get(url + keyword + params).json()
            break
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            time.sleep(100)
    return res


def get_submissions_df(keywords, how_many=500, is_search=True, t_keywords=None):
    #    print(get_records(keywords[0], 5, is_search, is_submission))
    t_keywords = t_keywords if t_keywords else []
    dframes = []
    topic = []
    for (keyword, t_keyword) in itertools.zip_longest(keywords, t_keywords):
        records = get_submissions(keyword, how_many, is_search)
        df = pd.DataFrame.from_dict(records["data"])
        dframes.append(df)
        topic.extend([t_keyword] * len(df.index))
    df = pd.concat(dframes)
    created_at = []
    submission_col = []
    for s in df['created_utc']:
        created_at.append(convert_date(s))
        submission_col.append(True)
    df.pop('created_utc')
    df['created_at'] = created_at
    df['is_submission'] = submission_col
    df['topic'] = topic
    return df



# ENDPOINTS:
# - For fetching submissions: GET https://api.pushshift.io/reddit/search/submission
# - For fetching comment_ids: GET
# https://api.pushshift.io/reddit/submission/comment_ids/<submission_id>
# - For fetching comments: GET https://api.pushshift.io/reddit/search/comment
# Follow the documentation for parameters: https://reddit-api.readthedocs.io/en/latest/#

# PART 1: Data Ingestion Requirements
# 1. Get a minimum of 100 submissions from each of these subreddits:
#     - ExplainLikeImFive
#     - FoodForThought
#     - ChangeMyView
#     - TodayILearned

subreddits = ["ExplainLikeImFive", "FoodForThought", "ChangeMyView", "TodayILearned"]
df1 = get_submissions_df(subreddits, how_many=120, is_search=False)
# display_df(df1)

# PART 1: Data Ingestion Requirements
# 2. Get a minimum of 200 submissions for each of these topics:
#   - Politics, Environment, Technology, Healthcare, and Education.
#   - You are required to come up with custom keywords to get submissions related to these
#       topics.

topics = [
    # politics
    "democracy", "socialism", "dictatorship", "imperialism",
    # environment
    "esg", "epa", "pollution", "climate change",
    # technology
    "tech", "saas", "social media", "blockchain",
    # healthcare
    "medical insurance", "value based care", "affordable care act", "medicare advantage",
    # education
    "university", "homeschooling", "special education", "k12"
]

topic_keywords = [*["Politics"] * 4, *["Environment"] * 4, *["Technology"] * 4, *["Healthcare"] * 4, *["Education"] * 4]
# print(topic_keywords)
df2 = get_submissions_df(topics, how_many=55, t_keywords = topic_keywords)
# display_df(df2)

# PART 1: Data Ingestion Requirements
# 3. Get a minimum of 2,000 submissions
topics = ["metaverse", "crypto", "nft", "tesla", "bitcoin", "google",
          "twitter", "instagram", "tiktok", "facebook", "amazon", "google"]
df3 = get_submissions_df(topics, how_many=200)
# display_df(df3)

# Submissions Dataframe for Part 1.1 to 1.3
df = pd.concat([df1, df2, df3])
display_df(df)

df.to_pickle("pushshift-submissions.pkl")

# PART 1: Data Ingestion Requirements
# 4. Get a minimum of 20,000 comments


# df.to_pickle("comments.pkl")

# with open('submissions.pkl', 'rb') as f:
#     data = pickle.load(f)
# display_df(data)
# print(len(data.index))

# get it as a list of dictionary for use in apache solr
# collection2 = data.to_dict('records')

