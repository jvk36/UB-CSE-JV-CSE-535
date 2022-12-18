import time
import requests
import json
from datetime import datetime
import pandas as pd
import itertools
from psaw import PushshiftAPI
import pickle


def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)

with open('psaw-comments.pkl', 'rb') as f:
    df = pickle.load(f)

# with open('psaw-submissions.pkl', 'rb') as f:
#     submissions_df = pickle.load(f)
#
# for id, selftext in zip(submissions_df.id, submissions_df.selftext):
#     for comment_id, parent_id in zip(df.id, df.parent_id[3:]):
#         if id == parent_id:
#             df.loc[df['id'] == comment_id, 'parent_body'] = selftext


# print(f"[deleted], [removed], or empty body column count: "
#       f"{ len(df[df['body'] == '[deleted]']) + len(df[df['body'] == '[removed]']) + len(df[df['body'] == ''])}"
#       )
# print(f"[deleted], [removed], or empty parent_body column count: "
#       f"{ len(df[df['parent_body'] == '[deleted]']) + len(df[df['parent_body'] == '[removed]']) + len(df[df['parent_body'] == ''])}"
#       )

# **** code to strip first 3 characters of parent_id column ****
# df['parent_id'] = df['parent_id'].str[3:]

# **** code to replace date with different format
# df['created_at'] = df['created_at'] + 'Z'

# *** code to rename the permalink column as full_link
# df['full_link'] = df['permalink']
# df.pop('permalink')

print(len(df['body']))
display_df(df)

# df.to_pickle("psaw-comments.pkl")


df.to_csv('psaw-comments.csv', index=False)

# df(submissions_df)
