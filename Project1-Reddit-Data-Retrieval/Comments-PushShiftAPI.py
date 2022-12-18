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

def get_comment(id):
    # fields parameter details for comments -
    # *** DIRECT FIELDS: fields - id, subreddit, full_link, body, author, parent_id
    # *** CUSTOM FIELDS - parent_body, is_submission, created_at - first two are TO DO
    # *** converted created_utc to created_at using the format YYYY-MM-DDThh:mm:ssZ
    # url = "https://api.pushshift.io/reddit/search/comment?ids="
    url = "https://api.pushshift.io/reddit/search/comment?link_id="
    params = "&fields=id,subreddit,body,author,parent_id,created_utc"

    while True:
        try:
            res = requests.get(url + id + params).json()
            break
        except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
            time.sleep(100)
    return res


def get_comments_df(id):
    # ids_csl = ",". join(ids_list)
    records = get_comment(id)
    if len(records["data"]) == 0:
        return pd.DataFrame()
    df = pd.DataFrame.from_dict(records["data"])
    created_at = []
    submission_col = []
    body_col = []
    for s in df['created_utc']:
        created_at.append(convert_date(s))
        submission_col.append(False)
        body_col.append("")
    df.pop('created_utc')
    df['created_at'] = created_at
    df['is_submission'] = submission_col
    df['parent_body'] = body_col
    return df


# def get_comment_ids(submission_id):
#     url = "https://api.pushshift.io/reddit/submission/comment_ids/"
#     while True:
#         try:
#             res = requests.get(url+submission_id).json()
#             break
#         except (requests.exceptions.ConnectionError, json.decoder.JSONDecodeError):
#             time.sleep(100)
#     return res


# PART 1: Data Ingestion Requirements
# 4. Get a minimum of 20,000 comments

# df.to_pickle("comments.pkl")

with open('submissions.pkl', 'rb') as f:
    submissions_df = pickle.load(f)
# display_df(submissions_df)
# print(len(submissions_df.index))

dframes_list = []
count = 0
df = pd.DataFrame()
for id in submissions_df.id:
    df = get_comments_df(id)
    count = count + len(df.index)
    if len(df.index) > 0:
        dframes_list.append(df)
    if count > 1000:
        break

df_all_comments = pd.concat(dframes_list)
display_df(df_all_comments)

# get it as a list of dictionary for use in apache solr
# collection2 = data.to_dict('records')

