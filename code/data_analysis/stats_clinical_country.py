#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description: Creates a csv file which contain list of countries on every study
"""

from pymongo import MongoClient
import pandas as pd
from config import *
import json

try:
    client = MongoClient(mongoinstance)
    trialsDB = client['clinical-trials']
    collections = trialsDB.list_collection_names()
    if "trials" in collections:
        print("Collection 'trials' found in 'clinical-trials' DB")
    trialsCollection = trialsDB['trials']
except Exception as err:
    print("Problems initiating MongoDB - {}".format(err))
    exit(1)

def listingCountry():
    # Storing countries on each study
    result = []

    # Storing number of study on 0, 1, 2... country
    count = {}
    
    all_studies = trialsCollection.find()
    for study in all_studies:
        countries = []
        for country in study['LocationCountry']:
            if country not in countries:
                countries.append(country)

        counter = len(countries)
        if counter in count.keys():
            count[counter] += 1
        else:
            count[counter] = 1

        result.append({
            "_id": str(study['_id']),
            "countries": countries
            })
    with open('./data/countries per study.json', 'w') as outfile:
        json.dump(result, outfile)
    
    with open('./data/counting.json', 'w') as test:
        json.dump(count, test)

listingCountry()