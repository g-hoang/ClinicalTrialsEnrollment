"""
Description: Retrieves data from clinicaltrials.gov in a two step approach
List of attributes: https://clinicaltrials.gov/api/info/study_fields_list
Debug Mode: requests only a limited number of trials, deletes csv and sqlite files in the directory
"""
import requests
import json
import sys
import logging
import os.path
import csv
import pymongo
import config
import sshtunnel

from csv import writer
from os import path
from pymongo import MongoClient
from config import *
from sshtunnel import SSHTunnelForwarder

url_prefix = "https://id.nlm.nih.gov/mesh/lookup/details?descriptor=http%3A%2F%2Fid.nlm.nih.gov%2Fmesh%2F"
csv, sqlite, mongo = True, True, True
csvmeshData = "../data/meshdata.csv"


# """ Initiate Server"""
# MONGO_HOST = "134.155.110.156"
# MONGO_DB = "clinical-trials"
# MONGO_USER = "ubuntu"
# MONGO_PASS = "SovantaTP*()"
#
# server = SSHTunnelForwarder(
#     (MONGO_HOST,22),
#     ssh_username=MONGO_USER,
#     ssh_private_key= "C:/a/SSH_Key_project.pem",
#     ssh_private_key_password=MONGO_PASS,
#     remote_bind_address=('127.0.0.1', 27017)
# )
#
# server.start()
#
# client = pymongo.MongoClient('127.0.0.1',
#                              server.local_bind_port,
#                              username='sovanta',
#                              password='Si8T8TtsViHYenjx',
#                              authSource=MONGO_DB)
#
# db = client[MONGO_DB]
# print(db.trials.count({}))

# """ Initiate MongoDB and retrieve MeshId"""
# if mongo:
#     try:
#         client = MongoClient(mongoinstance)
#         trialsDB = client['clinical-trials'] # create the db, if it does not exist
#         clinicaltrials = trialsDB.list_collection_names()
#         if "trials" in clinicaltrials:
#             logging.info("Collection 'trials' found in 'clinical-trials' DB")
#         trialsCollection = trialsDB['trials']
#     except Exception as err:
#         logging.error("Problems initiating MongoDB - {}".format(err))
#         exit(1)
#
# trialsCollection.find(filter={}, projection= {"ConditionMeshId" : 1, "_id" : 0})


meshid = "D009293"

url = url_prefix + meshid

try:
    resp = requests.get(url=url)
    data = resp.json()
    logging.info("Status Code: {}".format(str(resp.status_code)))
    if resp.status_code == 200:
        data = data['terms']
except Exception as err:
    logging.error("Problems requesting the list of trials - {}".format(err))
    exit(1)

preferredTerms = []
synonymTerms = []
for i in data:
    if i['preferred']:
        preferredTerms.append(i['label'])
    else:
        synonymTerms.append(i['label'])


""" Check if csv file already exists: if yes, open in append mode; otherwise create a new file """
if csv:
    try:
        if path.exists(csvmeshData):
            logging.info("csv file exists.")
        else:
            logging.info("csv file does not exist.")
            csvfile = open(csvmeshData, "w+", encoding="utf-8").close()
    except Exception as err:
        logging.error("Problems preparing csv file - {}".format(err))
        exit(1)
    try:
        if path.exists(csvmeshData):
            logging.info("csv file exists.")
        else:
            logging.info("csv file does not exist.")
            csvfile = open(csvmeshData, "w+", encoding="utf-8").close()
    except Exception as err:
        logging.error("Problems preparing csv file - {}".format(err))
        exit(1)
