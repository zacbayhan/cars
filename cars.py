import json
from datetime import datetime
from elasticsearch import Elasticsearch
from pprint import pprint
import requests

esLocal = Elasticsearch()

def query_local():
    res = esLocal.search(
        index='cars',
        doc_type='models',
        body={
            'query': {
                'match_all':{}
            }
        })
    return res['hits']['hits']

def load_file(fid):
    with open(fid) as data_file:
        data = json.load(data_file)

    index_from_file(data)

def index_from_file(data):
    data = data['Results']
    print len(data)
    for i in range(0, len(data)):
        esLocal.index(
                    index='cars',
                    doc_type='makes',
                    id=data[i]['Make_ID'],
                    body=data[i]
        )

def get_count():
    count = esLocal.count(
        index='cars',
        doc_type='makes',
        body={
          "query": {
            "match_all": {}
            }
        }
    )
    return count['count']



def list_makes(count):
    makes = esLocal.search(
            index='cars',
            doc_type='makes',
            body={
              "from": 0,
              "size": count,
              "sort": [
                {
                  "Make_ID": {
                    "order": "asc"
                  }
                }
              ],
              "query": {
                "match_all": {}
              }
            }
    )
    makes = makes['hits']['hits']
    for i in range(0, len(makes)):
        get_models(makes[i])
def get_models(Make_ID):
    pprint(Make_ID)


def main():
    if(get_count() == 0):
        fid = 'manufactors.json'
        data = load_file(fid)

    count = get_count()
    get_models(count)


main()
