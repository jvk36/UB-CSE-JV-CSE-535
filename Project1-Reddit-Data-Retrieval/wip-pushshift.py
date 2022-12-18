import time
import requests
import json
from datetime import datetime
import pandas as pd
import itertools
from psaw import PushshiftAPI
import pickle

api = PushshiftAPI()

# gen=api.search_comments(
#         link_id='p1vex8', subreddit=['explainlikeimfive'],
#     body="not [removed]" and "not [deleted]" and 'not "" ',
#     filter=['body'])
# df = pd.DataFrame(gen)
# if len(df.index > 0):
#     print(df['body'].iloc[0])
# else:
#     print("Invalid id")

gen=api.search_submissions(
    ids='p1vex8', subreddit=['explainlikeimfive'],
    filter=['selftext'])
df = pd.DataFrame(gen)
if len(df.index > 0):
    print(df['body'].iloc[0])
else:
    print("Invalid id")

