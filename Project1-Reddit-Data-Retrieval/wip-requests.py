import time

import requests
import json
from datetime import datetime

import pandas as pd
import itertools
import pickle


def convert_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%SZ')


def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)


def get_comment_body(id):
    records = get_comments(id)
    if len(records["data"]) == 0:
        return ""
    df = pd.DataFrame.from_dict(records["data"])
    return df['body'].iloc[0]


def get_comments(ids):
    url = "https://api.pushshift.io/reddit/search/comment?ids="
    params = "" # "&fields=id,subreddit,body,author,parent_id,created_utc"

    while True:
        try:
            res = requests.get(url + ids + params).json()
            break
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            time.sleep(100)
    return res


def get_comments_df(ids):
    # ids_csl = ",". join(ids_list)
    records = get_comments(ids)
    if len(records["data"]) == 0:
        return pd.DataFrame()
    return pd.DataFrame.from_dict(records["data"])


def get_submissions(ids):
    url = "https://api.pushshift.io/reddit/search/submission/?ids="
    params = "" # "&fields=id,subreddit,body,author,parent_id,created_utc"

    while True:
        try:
            res = requests.get(url + ids + params).json()
            break
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            time.sleep(100)
    return res


def get_submissions_df(ids):
    # ids_csl = ",". join(ids_list)
    records = get_submissions(ids)
    if len(records["data"]) == 0:
        return pd.DataFrame()
    return pd.DataFrame.from_dict(records["data"])


df = get_comments_df('t1_h8k7xbq,t1_h8jr1q9')
display_df(df)

df = get_submissions_df('p1vex8')
display_df(df)

# print(get_comment_body('t1_h8k7xbq'))


# gen=api.search_comments(
#         ids=['t3_xe6wri', 't3_xe6quj'], subreddit=['explainlikeimfive'],
#     filter=['body'])
# df = pd.DataFrame(gen)
# if len(df.index > 0):
#     print(df['body'].iloc[0])
# else:
#     print("Invalid id")
#
# gen=api.search_submissions(
#     ids=['xe6wri', 'xe6quj'], subreddit=['explainlikeimfive'],
#     filter=['selftext'])
# df = pd.DataFrame(gen)
# if len(df.index > 0):
#     print(df['body'].iloc[0])
# else:
#     print("Invalid id")
#
