#! /usr/bin/python3
# -*- coding: utf-8 -*-
import time
import sys
import json
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
ES = Elasticsearch()


def set_mapping(es_client):
    """
       create index with mapping
    """
    sport_mapping = {
        "mappings": {
            "athlete": {
                "properties": {
                    "birthdate": {
                        "type": "date",
                        "format": "dateOptionalTime"
                    },
                    "location": {
                        "type": "geo_point"
                    },
                    "name": {
                        "type": "string",
                        "fielddata": True
                    },
                    "rating": {
                        "type": "integer"
                    },
                    "sport": {
                        "type": "string",
                        "fielddata": True
                    }
                }
            }
        }
    }
    create_index = es_client.indices.create('sports', body=sport_mapping)
    print(create_index)


def set_sports_data(es, index, type):
    bulk_data = []
    json_data = json.load(open('sports.json'))
    # with open('sports_data.json') as json_data:
    #     d = json.load(json_data)
    for athlete in json_data:
        bulk_data.append({
            '_index': index,
            '_type': type,
            '_source': athlete
        })

    success, _ = bulk(es, bulk_data, index=index, raise_on_error=True)
    if (success == len(json_data)):
        print("success")
    else:
        print("error on ", len(json_data) - success)


if __name__ == '__main__':
    # print("Bonjour", u"à", "tous")
    # print('toto')

    ES.indices.delete(index='sports')
    print("sports index has been deleted")
    set_mapping(ES)
    set_sports_data(ES, 'sports', 'athlete')

    #     print(d)
    # print(json_data)
