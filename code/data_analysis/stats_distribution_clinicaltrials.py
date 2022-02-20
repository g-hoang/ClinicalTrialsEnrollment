#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description: Creates a report with information such as percentage of missing entries about the clinical trials data
"""

from pymongo import MongoClient
from csv import writer
import json
from config import *

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

of = open("logs/stats_clinicaltrials.txt", 'w', encoding="utf-8")
csv = open("logs/stats_clinicaltrials.csv", "w", encoding="utf-8")

try:
    client = MongoClient(mongoinstance)
    trialsDB = client['clinical-trials']
    clinicaltrials = trialsDB.list_collection_names()
    if "trials" in clinicaltrials:
        print("Collection 'trials' found in 'clinical-trials' DB", file=of)
    trialsCollection = trialsDB['trials']
except Exception as err:
    print("Problems initiating MongoDB - {}".format(err), file=of)
    exit(1)

def verteilung(attr, add_rest=True):
    print(f"\n-- Distribution of {attr} --", file=of)
    values = trialsCollection.distinct(attr)
    criteria = {
        '$and': [
            { attr: { '$ne': []} },
            { attr: { '$ne': ""} },
            { attr: { '$ne': 0} },
            { attr: { '$ne': None} },
            { attr: ''}
        ]
    }

    data = {}
    for i in values:
        criteria['$and'][4][attr] = i
        data[i] = trialsCollection.count_documents(criteria)

    data = {i:data[i] for i in sorted(data, key=data.get, reverse=True)}
    
    if len(data.keys()) > 10:
        rest_sum = 0
        for rest in list(data.keys())[10:]:
            rest_sum += data[rest]
            del data[rest]
        if add_rest: data['Rest'] = rest_sum

    criteria = {
        '$or': [
            { attr: { '$eq': []} },
            { attr: { '$eq': ""} },
            { attr: { '$eq': 0} },
            { attr: { '$eq': None} }
        ]}
    no_value = trialsCollection.count_documents(criteria)
    #print(f"Wihout {attr}: ", no_value, file=of)
    data['No Value'] = no_value

    nr_trials = 0
    for value in data.values():
        nr_trials += value
    
    fig, ax = plt.subplots()
    #fig.tight_layout()
    plt.bar(list(data.keys()), list(data.values()))
    plt.xticks(rotation=45)
    ax.set_title(f"Distribution of {attr}")
    ax.set_xlabel('Distinct Values')
    ax.set_ylabel('Count #')

    rects = ax.patches
    labels = data.values()
    for rect, label in zip(rects, labels):
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2, height + 5, label, ha='center', va='bottom')
    plt.show()

# Anzahl Dokumente
total = trialsCollection.count_documents({})
print(f"Number of documents: {total}", file=of)
csv.write(f"Number of documents: {total}")
print(f"Filters: {','.join(studyFilters)}\n\n", file=of)
csv.write(f"\nFilters: {','.join(studyFilters)}\n\n")
print("{:37} {:<20} {:<20} {:<20} {:<35} {:<20} {:<20}".format("Attribute", "Missing (#)", "Missing (%)", "Data Structure", "Data Type", "Distinct Values", "Examples"), file=of)
csv.write("{};{};{};{};{};{};{}".format("Attribute", "Missing (#)", "Missing (%)", "Data Structure", "Data Type", "Distinct Values", "Examples"))

data = {}
for attr in columns:
    criteria = {
        '$or': [
            { attr: { '$eq': []} },
            { attr: { '$eq': ""} },
            { attr: { '$eq': 0} },
            { attr: { '$eq': None} }
        ]}
    missing = trialsCollection.count_documents(criteria)
    missing_quote = (missing * 100)/total

    # single value or array? check first 100 examples, where attribute has a value, if len(array) always ==1 or not
    check_number = 100
    pipeline = [
        {"$match": {
            '$and': [
            {attr: {'$ne': []} },
            { attr: { '$ne': ""} },
            { attr: { '$ne': 0} },
            { attr: { '$ne': None} }
            ]}},
        {"$project": {'_id': 0, attr: 1}},
        {"$skip": 100},
        {"$limit": check_number}
    ]
    number_values = [len(i[attr]) for i in list(trialsCollection.aggregate(pipeline)) if isinstance(i[attr], list) and len(i[attr]) > 1]
    if number_values == []:
        data_structure = "single value"
    else:
        data_structure = "list"
    
    data_structure = "single value"
    for i in list(trialsCollection.aggregate(pipeline)):
        if isinstance(i[attr], list): data_structure = "list"
    
    distinct_values = set()
    number_to_display = 10
    examples = []
    try:
        distinct_values = set(trialsCollection.distinct(attr))
        if len(distinct_values) == 0:
            data_type = '---'
        else:
            example = distinct_values.pop()
            if isinstance(example, int):
                data_type = 'integer'
            elif isinstance(example, float):
                data_type = 'float'
            elif len(distinct_values) <= 1000:
                data_type = 'categorical' 
            elif len(distinct_values) > 30000:
                data_type = 'free text'
            else:
                data_type = 'categorical (huge diversity)'   
                
            distinct_values.add(example)
            if len(distinct_values) < number_to_display: number_to_display = len(distinct_values) 
            for i in range(number_to_display):
                example = str(distinct_values.pop())
                if len(example)>70: 
                    examples.append("'" + example[0:30].replace("\n","\\") + "...'")
                else:
                    examples.append(example)
                distinct_values.add(example)
    except Exception:
        examples.append("")
    
    if(distinct_values == 0):
        distinct_value_to_display = ""
    else:
        distinct_value_to_display = str(len(distinct_values))
    print(f"{attr:37} {missing:<20} {missing_quote:<20.2f} {data_structure:<20} {data_type:<35} {distinct_value_to_display:<20} {'; '.join(examples):<20}", file=of)
    csv.write(f"\n{attr};{missing};{missing_quote:.2f};{data_structure};{data_type};{len(distinct_values)};{', '.join(examples)}")

print("\n### TRIALS WITH START DATE, END DATE AND ENROLMENT COUNT", file=of)
criteria = {
    '$and': [
        { 'StartDate': { '$ne': ''} },
        { 'CompletionDate': { '$ne': ''} },
        { 'EnrollmentCount': { '$ne': ''} },
        { 'EnrollmentCount': { '$ne': None} }
    ]
}
print(trialsCollection.count_documents(criteria), file=of)

print("\n### TRIALS WITH ENROLMENT COUNT != 0", file=of)
criteria = {
    '$and': [
        { 'StartDate': { '$ne': ''} },
        { 'CompletionDate': { '$ne': ''} },
        { 'EnrollmentCount': { '$ne': ''} },
        { 'EnrollmentCount': { '$ne': None} },
        {'EnrollmentCount': { '$ne': 0} }
    ]
}
print(trialsCollection.count_documents(criteria), file=of)

# print("\n### DISTRIBUTION", file=of)
# verteilung("OrgClass")
# #verteilung("Phase")
# verteilung("LeadSponsorClass")
# verteilung("CollaboratorClass", add_rest=False)
# #verteilung("Gender")
# #verteilung("StdAge")
# verteilung("LocationCountry", add_rest=False)