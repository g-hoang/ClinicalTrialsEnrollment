#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description: Retrieves data from clinicaltrials.gov in a two step approach and saves the data into a sqlite db, mongodb and csv file
List of attributes: https://clinicaltrials.gov/api/info/study_fields_list
Debug Mode: requests only a limited number of trials, deletes csv and sqlite files in the directory
"""
import requests
import argparse
import zipfile
import logging
import json
import sys
import os

from os import path
from datetime import datetime

from config import *
from functions import *

deb, stats = False, False
csv, sqlite, mongo = True, True, True
url_prefix = 'https://clinicaltrials.gov/api/query/'

""" Parameter handling """
parser = argparse.ArgumentParser(description='Retrieves data from clinicaltrials.gov in a two step approach and saves the data into a SQLite DB, MongoDB and csv file.')
parser.add_argument('--debug', action="store_true", help='debug mode for test purposes')
parser.add_argument('--stats', action="store_true", help='print stats about th data retrieved by the API')
parser.add_argument('--mongo', action="store_true", help='save data only to MongoDB')
parser.add_argument('--sqlite', action="store_true", help='save data only to SQLite DB')
parser.add_argument('--csv', action="store_true", help='save data only to csv file')
args = parser.parse_args()

if(args.debug): deb = True
if(args.stats): stats = True
if(args.mongo): csv, sqlite, mongo = False, False, True
if(args.sqlite): csv, sqlite, mongo = False, True, False
if(args.csv): csv, sqlite, mongo = True, False, False

""" Logging config """
logging.basicConfig(
    filename=logname,
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

""" Statistics """
if stats:
    url_suffix = '&field=NCTId&fmt=json'

    def getNumberOf(url_nctlist):
        try:
            resp = requests.get(url=url_nctlist)
            data = resp.json()
            if resp.status_code == 200: return len(data['FieldValuesResponse']['FieldValues'])
        except Exception as err:
            return f"problems requesting the data - {err}"

    def checkMissing(attribute):
        url_suffix = '&field=' + attribute + '&fmt=json'
        filterString = createFilterString(["OverallStatus=Completed", "StudyType=Interventional","InterventionType=drug", "Phase=Phase 2,Phase 3,Phase 4"])
        url_nctlist = url_prefix + 'field_values' + filterString + url_suffix

        try:
            resp = requests.get(url=url_nctlist)
            data = resp.json()
            if resp.status_code == 200:
                missing = data['FieldValuesResponse']['MissingValues']['NStudiesMissingValueFound']
                available = 0
                for fieldvalue in data['FieldValuesResponse']['FieldValues']:
                    available += fieldvalue["NStudiesFoundWithValue"]
                logging.info("----- {}:".format(attribute))
                logging.info(f"{'       with value: '.ljust(100)} {available:,}")
                logging.info(f"{'       without value: '.ljust(100)} {missing:,}")
        except Exception as err:
            logging.error("Problems requesting the data - {}".format(err))

    # Number of studies
    url_nctlist = url_prefix + 'field_values' + "?expr=" + url_suffix
    logging.info(f"{'Total number of studies:' } {getNumberOf(url_nctlist):,}")

    # Number of completed studies
    filterString = createFilterString(["OverallStatus=Completed"])
    url_nctlist = url_prefix + 'field_values' + filterString + url_suffix
    logging.info(f"{'- Number of studies completed:'.ljust(100)} {getNumberOf(url_nctlist):,}")

    # Number of completed studies, interventional
    filterString = createFilterString(["OverallStatus=Completed", "StudyType=Interventional"])
    url_nctlist = url_prefix + 'field_values' + filterString + url_suffix
    logging.info(f"{'-- Number of interventional studies completed:'.ljust(100)} {getNumberOf(url_nctlist):,}")

    # Number of completed studies, interventional, study type "drugs"
    filterString = createFilterString(["OverallStatus=Completed", "StudyType=Interventional","InterventionType=drug"])
    url_nctlist = url_prefix + 'field_values' + filterString + url_suffix
    logging.info(f"{'--- Number of interventional studies completed, intervention type = drug:'.ljust(100)} {getNumberOf(url_nctlist):,}")

    # Number of completed studies, interventional, study type "drugs", phase 3 and phase 4
    filterString = createFilterString(["OverallStatus=Completed", "StudyType=Interventional","InterventionType=drug", "Phase=Phase 2,Phase 3,Phase 4"])
    url_nctlist = url_prefix + 'field_values' + filterString + url_suffix
    logging.info(f"{'---- Number of interventional studies completed, intervention type = drug, in phase 2,3 and 4:'.ljust(100)} {getNumberOf(url_nctlist):,}")

    # Number of completed studies, interventional, study type "drugs", phase 3 and phase 4, various attributes
    checkMissing("ResultsFirstPostDate")
    checkMissing("StartDate")
    checkMissing("CompletionDate")
    checkMissing("LocationFacility")
    checkMissing("LocationCity")
    checkMissing("LocationCountry")

""" Prepare columns and filters, depending on query type """
lengthColumns = len(columns)
filterString = createFilterString(studyFilters)
logging.info(f"Number of attributes {lengthColumns}")

""" Define the number of trials to be returned """
minimun_rank = '1'
maximum_rank = str(deblimit)

if deb: url_suffix = '&min_rnk=' + minimun_rank + '&max_rnk=' + maximum_rank + '&field=NCTId&fmt=json'
else: url_suffix = '&field=NCTId&fmt=json'

""" Request the list of trials according to the defined filer """
url_nctlist = url_prefix + 'field_values' + filterString + url_suffix

try:
    resp = requests.get(url=url_nctlist)
    data = resp.json()
    logging.info("Status Code: {}".format(str(resp.status_code)))
    if resp.status_code == 200:
        length = len(data['FieldValuesResponse']['FieldValues'])
        logging.info("Number of records: {}".format(length))
except Exception as err:
    logging.error("Problems requesting the list of trials - {}".format(err))
    exit(1)

""" Check if csv file already exists: if yes, open in append mode; otherwise create a new file """
if csv:
    from csv import writer

    columnNames = ";".join(columns) + '\n'
    try:
        if path.exists(csvname):
            logging.info("csv file exists.")
            if deb:
                csvfile = open(csvname, "w+", encoding="utf-8")
                if os.path.isfile(dbname): os.remove(dbname)
                csvfile.write(columnNames)
            else:
                csvfile = open(csvname, "a+", encoding="utf-8")
        else:
            logging.info("csv file does not exist.")
            csvfile = open(csvname, "w+", encoding="utf-8")
            csvfile.write(columnNames)
    except Exception as err:
        logging.error("Problems preparing csv file - {}".format(err))
        exit(1)

""" Initiate MongoDB """
if mongo:
    from pymongo import MongoClient

    try:
        client = MongoClient(mongoinstance)
        trialsDB = client['clinical-trials'] # create the db, if it does not exist
        clinicaltrials = trialsDB.list_collection_names()
        if "trials" in clinicaltrials:
            logging.info("Collection 'trials' found in 'clinical-trials' DB")
        trialsCollection = trialsDB['trials']
    except Exception as err:
        logging.error("Problems initiating MongoDB - {}".format(err))
        exit(1)

""" Initiate SQLite DB """
if sqlite:
    import sqlite3

    try:
        connection = sqlite3.connect(dbname)
        cursor = connection.cursor()
        createTableStatement = "CREATE TABLE IF NOT EXISTS 'clinicaltrials' ('ID' INTEGER PRIMARY KEY AUTOINCREMENT"
        for attr in columns:
            createTableStatement += ",'" + attr + "' TEXT"
        createTableStatement += ",UNIQUE(NCTId));"
        cursor.execute(createTableStatement)
    except Exception as err:
        logging.error("Problems initiating SQLite - {}".format(err))
        exit(1)

""" Request detailed data per nctid and write in excel file, SQLite DB and MongoDB """
count = 0
for i in data['FieldValuesResponse']['FieldValues']:
    count += 1
    if count % 1000 == 0: logging.info("Meilenstein {}".format(str(count)))

    nctid = str(i['FieldValue'])
    if mongo:
        if trialsCollection.count_documents({'NCTId': nctid}) > 0: continue

    # Request data
    nctidFilter = '?expr=SEARCH[Study](AREA[NCTId] ' + nctid + ')'
    columns_tmp = columns
    study_data = {}
    try:
        while len(columns_tmp) > 0:
            if len(columns_tmp) >= 20: selectString = createSelectString(columns_tmp[:20])
            else: selectString = createSelectString(columns_tmp)
        
            url_singleStudies = url_prefix + 'study_fields' + nctidFilter + selectString + '&min_rnk=' + minimun_rank + '&max_rnk=' + maximum_rank + '&fmt=json'
            resp = requests.get(url=url_singleStudies)
            returned_data = resp.json()['StudyFieldsResponse']['StudyFields']
            if len(returned_data) > 1: logging.info("More than one study returned for {}".format(nctid))
            study_data = {**study_data, **returned_data[0]}

            if len(columns_tmp) >= 20: columns_tmp = columns_tmp[20:]
            else: columns_tmp = []
    except Exception as err:
            logging.error("Request for {} failed - {}".format(nctid,err))
            continue

    # save to MongoDB
    if mongo:
        try:
            #trialsCollection.insert(study_data)
            trialsCollection.replace_one({'NCTId': nctid}, study_data, upsert=True)
        except Exception as err:
            logging.error("Update MongoDB: {} could not be added - {}".format(nctid, err))
    
    # clean up the data
    if csv or sqlite:
        rowData = []
        for i in range(lengthColumns):
            index = columns[i]
            if index in study_data:
                rowData.append(cleanUp(", ".join(set(study_data[index])) if len(study_data[index]) else ""))
            else:
                rowData.append("")

    # save to csv file
    if csv:
        rowDataInput = ";".join(rowData)
        rowDataInput = rowDataInput + '\n'
        csvfile.write(rowDataInput)

    # prepare statement and save to SQLite DB
    if sqlite:
        del rowData[0]
        try:
            updateTableStatement = "INSERT OR IGNORE INTO 'clinicaltrials' ("
            updateTableStatement += ",".join(columns) 
            updateTableStatement += ") VALUES ('" + nctid + "'"
            for value in rowData:
                updateTableStatement += ",'" + value + "'"
            updateTableStatement += ");"

            cursor.execute(updateTableStatement)
            connection.commit()
        except Exception as err:
            logging.error("Update SQLite DB: {} could not be added - {}".format(nctid, err))

# zip and name with current date
cwd = os.getcwd()
date = datetime.strftime(datetime.now(), '%d-%b')
target = cwd + f"/../data/clinicalTrials-{date}.zip"
zipf = zipfile.ZipFile(target, 'w', zipfile.ZIP_DEFLATED)
if sqlite:
    zipf.write(dbname)
if csv:
    zipf.write(csvname)
zipf.close()

logging.info("DONE")