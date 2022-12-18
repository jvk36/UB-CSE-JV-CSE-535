import pandas as pd
from datetime import datetime
from psaw import PushshiftAPI


def convert_date(timestamp):
    return datetime.fromtimestamp(timestamp).strftime('%Y-%m-%dT%H:%M:%S')

def display_df(df):
    pd.options.display.width = None
    pd.options.display.max_columns = None
    pd.set_option('display.max_rows', 3000)
    pd.set_option('display.max_columns', 3000)
    print(df)

api = PushshiftAPI()

# fields parameter details for comments -
# *** DIRECT FIELDS: fields - id, subreddit, full_link, body, author, parent_id
# *** CUSTOM FIELDS - parent_body, is_submission, created_at - first two are TO DO
# *** converted created_utc to created_at using the format YYYY-MM-DDThh:mm:ssZ
gen = api.search_comments(q='science', limit=10,
                         filter=['id', 'subreddit', 'full_link', 'body', 'author', 'parent_id', 'created_utc'])
df = pd.DataFrame(gen)

created_at = []
for s in df['created_utc']:
    created_at.append(convert_date(s))
df.pop('created_utc')
df.pop('d_')
df.pop('created')
df['created_at'] = created_at

print(len(df.index))
display_df(df)
