import os
import pysolr
import requests
import pickle

CORE_NAME = "IRF22P1"
GCP_IP = "localhost"


def delete_core(core=CORE_NAME):
    print(os.system('sudo su - solr -c "/opt/solr/bin/solr delete -c {core}"'.format(core=core)))


def create_core(core=CORE_NAME):
    print(os.system(
        'sudo su - solr -c "/opt/solr/bin/solr create -c {core} -n data_driven_schema_configs"'.format(
            core=core)))


# collection1 - submissions

with open('psaw-submissions.pkl', 'rb') as f:
    data = pickle.load(f)
# display_df(data)
print(len(data.index))

# get it as a list of dictionary for use in apache solr
collection1 = data.to_dict('records')
# print(collection1[0])

# collection2 - comments

with open('psaw-comments.pkl', 'rb') as f:
    data = pickle.load(f)
# display_df(data)
print(len(data.index))

data = data.rename(columns={'permalink': 'full_link'})

# get it as a list of dictionary for use in apache solr
collection2 = data.to_dict('records')
# print(collection2[0])


class Indexer:
    def __init__(self):
        self.solr_url = f'http://{GCP_IP}:8983/solr/'
        self.connection = pysolr.Solr(self.solr_url + CORE_NAME, always_commit=True, timeout=5000000)

    def do_initial_setup(self):
        delete_core()
        create_core()

    def create_documents(self, docs):
        print(self.connection.add(docs))

    def add_fields(self):
        data = {
            "add-field": [
                # fields for submissions doc
                {
                    "name": "subreddit",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "full_link",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "title",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "selftext",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "author",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "is_submission",
                    "type": "boolean",
                    "multiValued": False
                },
                {
                    "name": "topic",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "created_at",
                    "type": "pdate",
                    "multiValued": False
                },
                # additional fields for comments doc
                {
                    "name": "body",
                    "type": "text_en",
                    "multiValued": False
                },
                {
                    "name": "parent_id",
                    "type": "string",
                    "multiValued": False
                },
                {
                    "name": "parent_body",
                    "type": "text_en",
                    "multiValued": False
                },
            ]
        }

        print(requests.post(self.solr_url + CORE_NAME + "/schema", json=data).json())


if __name__ == "__main__":
    i = Indexer()
    i.do_initial_setup()
    i.add_fields()
    i.create_documents(collection1)
    i.create_documents(collection2)
