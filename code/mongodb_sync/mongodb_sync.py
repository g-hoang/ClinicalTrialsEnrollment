#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
1. Install according to https://docs.mongodb.com/manual/tutorial/install-mongodb-on-windows/
2. cd C:\Program Files\MongoDB\Server\4.4\bin\ 
3. run mongo.exe
4. Connect to localhost and create DB 'hyperopt'
- start fmin and then run in console: hyperopt-mongo-worker --mongo=localhost:27017/hyperopt --poll-interval=0.1
"""
from pymongo import MongoClient
import argparse
import json
import os

parser = argparse.ArgumentParser()
parser.add_argument('--pull', action="store_true", help='saves current state from remote mongodb to local mongodb')
parser.add_argument('--push', action="store_true", help='pushes new documents (delta) from local mongodb to remote mongodb')
parser.add_argument('--test', action="store_true", help='testing')
parser.add_argument('--cleanRemote', action="store_true", help='Cleaning Remote')
parser.add_argument('--cleanLocal', action="store_true", help='Cleaning Local')
parser.add_argument('--initialPush', action="store_true", help='Initial pushing')
args = parser.parse_args()

# if not args.pull or not args.push:
#     print("Choose a method. More with --help.")
#     exit(1)

""" Connect to local mongodb """
try:
    local_client = MongoClient("mongodb://127.0.0.1:27017")
    local_db = local_client['hyperopt']
    print("Successfully connected to local mongodb")
except Exception as err:
    print("Not possible to connect to local mongodb")
    exit(1)

""" Connect to central mongodb """
try:
    remote_client = MongoClient("mongodb+srv://sovanta:14FgeCSTCZF9HNVw@cluster0-3yb0b.mongodb.net/test?retryWrites=true&w=majority")
    remote_db = remote_client['hyperopt']
    print("Successfully connected to remote mongodb")
except Exception as err:
    print("Not possible to connect to remote mongodb")
    exit(1)

collections = ['fs.files', 'job_ids', 'jobs']
JSON_FILE = "mongodb.temp"

def test():
    """ PULL PROCESS """
    # Delete temp json file
    try: os.remove(JSON_FILE)
    except OSError: pass

    # Make a copy of remote into local and create a list of objects ids
    ids_dict = {}
    for collection in collections:
        print(f"Pulling {collection}")
        # delete all documents in collection
        # local_db[collection].remove({})
        # make a copy of remote collection into local
        # for remote_object in list(remote_db[collection].find()):
        #     local_db[collection].insert_one(remote_object)
        # save ids into dict
        objects_ids = [str(obj['_id']) for obj in list(local_db[collection].find())]
        ids_dict[collection] = objects_ids
    
    # Save dict to json file
    print(ids_dict)
    with open(JSON_FILE, 'w') as outfile:
        json.dump(ids_dict, outfile)

def cleanRemoteDB():
    for collection in collections:
        print(f"Deleting {collection}")
        # delete all documents in collection
        remote_db[collection].remove({})

def cleanLocalDB():
    for collection in collections:
        print(f"Deleting {collection}")
        # delete all documents in collection
        local_db[collection].remove({})

def pull():
    """ PULL PROCESS """
    # Delete temp json file
    try: os.remove(JSON_FILE)
    except OSError: pass

    # Make a copy of remote into local and create a list of objects ids
    ids_dict = {}
    for collection in collections:
        print(f"Pulling {collection}")
        # delete all documents in collection
        local_db[collection].remove({})
        # make a copy of remote collection into local
        for remote_object in list(remote_db[collection].find()):
            local_db[collection].insert_one(remote_object)
        # save ids into dict
        objects_ids = [str(obj['_id']) for obj in list(local_db[collection].find())]
        ids_dict[collection] = objects_ids

    # Save dict to json file
    with open(JSON_FILE, 'w') as outfile:
        json.dump(ids_dict, outfile)

def push():
    """ PUSH PROCESS """
    if args.initialPush:
        old_ids = {}
        for collection in collections:
            old_ids[collection] = []
    else:
        # Read the json file (old ids)
        try:
            # print(JSON_FILE)
            with open(JSON_FILE, 'r') as outfile:
                # print(outfile)
                old_ids = json.load(outfile)
        except Exception as err:
            print("Error while reading json")
            exit(1)

    new_ids_dict = {}
    delta_dict = {}
    for collection in collections:
        print(f"Pushing {collection}")
        # Create list of new ids
        objects_ids = [str(obj['_id']) for obj in list(local_db[collection].find())]
        new_ids_dict[collection] = objects_ids

        # new list - old list = delta
        delta_dict[collection] = [item for item in new_ids_dict[collection] if item not in old_ids[collection]]

        # Remove object ids from dict
        # for delta_id in delta_dict[collection]:
        #     obj = local_db[collection].find_one({'_id': delta_id}) => testing it

        for obj in list(local_db[collection].find()):
            if str(obj['_id']) in delta_dict[collection]:

                if collection == 'jobs' and obj['result']['status'] != 'ok': # Only push objects that have result.status == 'ok'
                    continue
                # Push delta and create objects without ids
                temp_dict = obj.copy()
                del temp_dict['_id']
                remote_db[collection].insert_one(temp_dict)

        # Delete documents in local collections
        # local_db[collection].remove({})
            
    
    # print(f"new: {new_ids_dict['jobs']}")
    # print(f"delta: {new_ids_dict['jobs']}")

if args.push or args.initialPush:
    print("Starting copying documents from local to remote")
    push()
if args.pull:
    print("Starting copying documents from remote to local")
    pull()
if args.test:
    print("Start testing")
    test()
if args.cleanRemote:
    print("Start cleaning")
    cleanRemoteDB()
if args.cleanLocal:
    print("Start cleaning")
    cleanLocalDB()

print("Synchronisation done")
exit(0)
