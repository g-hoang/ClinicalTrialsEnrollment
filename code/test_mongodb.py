#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description: Test consistency of data saved in MongoDB
More: https://docs.python.org/3/library/unittest.html#unittest.TestCase.debug
Note: keep tests isolated from each other
"""
import unittest
import requests
import json

from pymongo import MongoClient
from config import *

class TestConsistency(unittest.TestCase):

    def setUp(self):
        """Call before every test case."""
        self.nctid = 'NCT90000000'

        self.data_record1 = {
            'NCTId': ['NCT90000000'], 
            'OrgFullName': 'Organisation1', 
            'OrgClass': ['NIH'], 
            'BriefTitle': ['This is the first title']
        }

        self.data_record2 = {
            'NCTId': ['NCT90000000'], 
            'OrgFullName': 'Organisation2', 
            'OrgClass': ['INDUSTRY'], 
            'BriefTitle': ['This is the second title'],
            'Condition': ['Headache']
        }

        try:
            self.client = MongoClient(mongoinstance)
            self.trialsDB = self.client['clinical-trials']
            self.clinicaltrials = self.trialsDB.list_collection_names()
            if "trials" not in self.clinicaltrials:
                print("Collection not found")
                exit(1)
            self.trialsCollection = self.trialsDB['trials']
        except Exception as err:
            print("Problems initiating MongoDB - {}".format(err))
            exit(1)
    
    def tearDown(self):
        """Call after every test case."""
        self.trialsCollection.delete_many({"NCTId": f'{self.nctid}'})
    
    def test_addNewDocument(self):        
        should = self.trialsCollection.count_documents({"NCTId": f'{self.nctid}'}) + 1
        self.trialsCollection.insert_one(self.data_record1)
        current = self.trialsCollection.count_documents({"NCTId": f'{self.nctid}'})
        self.assertEqual(should, current)
        self.trialsCollection.delete_many({"NCTId": f'{self.nctid}'})

    def test_deleteDocuments(self):
        self.trialsCollection.insert_one(self.data_record1)
        self.trialsCollection.delete_many({"NCTId": f'{self.nctid}'})
        current = self.trialsCollection.count_documents({"NCTId": f'{self.nctid}'})
        self.assertEqual(0, current)
    
    def test_redundancies(self):
        "tests if only one document is in DB, after adding two documents with the same NCTId"
        self.trialsCollection.insert_one(self.data_record1)
        should = self.trialsCollection.count_documents({"NCTId": f'{self.nctid}'})
        self.trialsCollection.replace_one({'NCTId': f'{self.nctid}'}, self.data_record2, upsert=True)
        current = self.trialsCollection.count_documents({"NCTId": f'{self.nctid}'})
        self.assertEqual(should, current)
        self.trialsCollection.delete_many({"NCTId": f'{self.nctid}'})
    
    def test_update(self):
        "tests if one document has the newest values after adding a new document with the same NCTId"
        self.trialsCollection.insert_one(self.data_record1)
        self.trialsCollection.replace_one({'NCTId': f'{self.nctid}'}, self.data_record2, upsert=True)
        cursor = self.trialsCollection.find_one({"NCTId": f'{self.nctid}'})
        self.assertEqual(cursor['NCTId'], self.data_record2['NCTId'])
        self.assertEqual(cursor['OrgFullName'], self.data_record2['OrgFullName'])
        self.assertEqual(cursor['OrgClass'], self.data_record2['OrgClass'])
        self.assertEqual(cursor['BriefTitle'], self.data_record2['BriefTitle'])
        self.assertEqual(cursor['Condition'], self.data_record2['Condition'])
        current = self.trialsCollection.count_documents({"NCTId": f'{self.nctid}'})
        self.assertEqual(1, current)
        self.trialsCollection.delete_many({"NCTId": f'{self.nctid}'})

if __name__ == '__main__':
    unittest.main()



    
    
