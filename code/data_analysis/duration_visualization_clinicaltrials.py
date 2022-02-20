#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description: Visualization of trial duration in a certain time range and visualization of the relation between duration and enrolment count
"""

from pymongo import MongoClient
from datetime import datetime
import numpy as np
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import matplotlib.cbook as cbook
from config import *

try:
    client = MongoClient(mongoinstance)
    trialsDB = client['clinical-trials']
    clinicaltrials = trialsDB.list_collection_names()
    if "trials" in clinicaltrials:
        print("Collection 'trials' found in 'clinical-trials' DB")
    trialsCollection = trialsDB['trials']
except Exception as err:
    print("Problems initiating MongoDB - {}".format(err))
    exit(1)

def plotCountry(country="", startYear=1970, endYear=datetime.now().year, duration=True, relDurationCount=True):
    try:
        startYear=datetime(startYear, 1, 1)
        endYear=datetime(endYear, 12, 31)
    except Exception as err:
        print(err)
        exit(1)
    
    criteria = {
        '$and': [
            { 'StartDate': { '$ne': ''} },
            { 'CompletionDate': { '$ne': ''} },
            {'EnrollmentCount': { '$ne': 0} }
        ]
    }
    if country != "": 
        criteria['$and'].append({'LocationCountry': {'$eq': country}})
    else:
        country = "the world"

    results = list(trialsCollection.find(criteria, projection={"_id": 0, 'NCTId':1, 'StartDate': 1, 'CompletionDate': 1, 'EnrollmentCount':1, 'LocationCountry':1}))
    print(len(results))
    durations = []
    durations_days = []
    nctids = []
    nctids_double = []
    times = []
    enrolment_counts = []
    for trial in results:
        try:
            start = datetime.strptime(trial['StartDate'], '%B %Y')
        except ValueError:
            start = datetime.strptime(trial['StartDate'][0], '%B %Y')
        
        try:
            end = datetime.strptime(trial['CompletionDate'], '%B %Y')
        except ValueError:
            end = datetime.strptime(trial['CompletionDate'][0], '%B %Y')

        if start >= startYear and end <= endYear:
            nctid = trial['NCTId'][0]
            nctids += nctid
            nctids_double += [nctid, nctid]
                
            times += [start, end]
            delta = end - start
            durations.append(delta)
            durations_days.append(delta.days)
            count = int(trial['EnrollmentCount'])
            enrolment_counts.append(count)
            countries = len(set(trial['LocationCountry']))
            #print(f"{nctid}: {datetime.strftime(start, '%B %Y')} -> {datetime.strftime(end, '%B %Y')}, {delta.days} days with {count} participants in {countries} different countries")

    print("Number: ", len(durations))
    print("Längste Studie: ", max(durations))
    print("Kürzeste Studie: ", min(durations))

    if duration:
        years = mdates.YearLocator()
        months = mdates.MonthLocator()
        years_fmt = mdates.DateFormatter('%Y')
        #dates = matplotlib.dates.date2num(times)

        fig, ax = plt.subplots()
        fig.tight_layout()
        for i in range(0, len(nctids_double[:2000]), 2):
            #print(i)
            #print(times[i:i+2])
            #print(nctids_double[i:i+2])
            plt.plot(times[i:i+2], nctids_double[i:i+2], 'ro-')

        ax.xaxis.set_major_locator(years)
        ax.xaxis.set_major_formatter(years_fmt)
        ax.xaxis.set_minor_locator(months)
        ax.set_xlabel("Year")
        ax.set_ylabel("NCTID")
        plt.title(f"Clinical Trial Durations in {country} between {startYear.year} and {endYear.year + 1}")
        plt.show()
    
    if relDurationCount:
        fig, ax = plt.subplots()
        plt.plot(durations_days, enrolment_counts, "ob")
        plt.ylim(0, 4000)
        ax.set_xlabel("Duration in Days")
        ax.set_ylabel("Enrolment Count")
        plt.title(f"Relation between Duration and Enrolment Count in clinical trials in {country} between {startYear.year} and {endYear.year + 1}")
        plt.show()

#
#plotCountry(duration=False)
#plotCountry("Germany", startYear=2010, endYear=2011)
plotCountry(startYear=1990, duration=False)
#plotCountry("Portugal", startYear=2000)

