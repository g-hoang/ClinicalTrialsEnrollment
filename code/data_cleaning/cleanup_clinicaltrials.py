#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Description: Initially all attributes in clinical trials were arrays (json files from clinicaltrials.org).
This scripts saves the values in the correct format - attributes where we always expect a single value is are extracted from the array and saved directly to the key.
"""
import logging
import json
from pymongo import MongoClient
from csv import writer
from config import *

""" Logging config """
logging.basicConfig(
    filename="logs/clinicaltrials-cleanup.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

try:
    client = MongoClient(mongoinstance)
    trialsDB = client['clinical-trials']
    clinicaltrials = trialsDB.list_collection_names()
    if "trials" in clinicaltrials:
        logging.info("Collection 'trials' found in 'clinical-trials' DB")
    trialsCollection = trialsDB['trials']
except Exception as err:
    logging.error("Problems initiating MongoDB - {}".format(err))
    exit(1)

single_value_attributes = [
    "NCTId",
    "OrgFullName",
    "OrgClass",
    "BriefTitle",
    "OfficialTitle",
    "BriefSummary",
    "StudyType",
    "OverallStatus",
    "EnrollmentType",
    "HealthyVolunteers",
    "Gender",
    "MinimumAge",
    "MaximumAge",
    "LocationStatus",
    "IsFDARegulatedDevice",
    "IsFDARegulatedDrug",
    "EventsTimeFrame",
    "FlowDropWithdrawComment",
    "FlowPreAssignmentDetails",
    "FlowReasonComment",
    "FlowReasonNumUnits",
    "FlowRecruitmentDetails",
    "FlowTypeUnitsAnalyzed",
    #"BaselineDenomUnits",
    #"BaselineMeasureDenomCountGroupId",
    #"BaselineMeasureDenomUnits",
    "BaselinePopulationDescription",
    "BaselineTypeUnitsAnalyzed",
    "BioSpecDescription",
    "DesignAllocation",
    "DesignInterventionModel",
    "DesignInterventionModelDescription",
    "DesignPrimaryPurpose",
    "DesignTimePerspective",
    "ResultsFirstPostDate",
    "ResultsFirstSubmitDate"
]

documents = list(trialsCollection.find({}))
logging.info("All documents retrieved")

""" Checking if single value attributes are really single valued """
nr = 0
for document in documents:
    nr += 1
    for attr in single_value_attributes:
        if isinstance(document["EventsFrequencyThreshold"], list):
            if len(document["EventsFrequencyThreshold"]) > 1:
                logging.error(f"Attribute 'EventsFrequencyThreshold' has more than one value in {document['_id']}")
                exit(1)
        if isinstance(document[attr], list):
             if len(document[attr]) > 1:
                logging.error(f"Attribut {attr} has more than one value in {document['_id']}") 
                exit(1)
logging.info(f"{str(nr)} documents checked")

""" Updating data """
for document in documents:
    data = {}
    # String attributes
    for attr in single_value_attributes:
        if isinstance(document[attr], list):
            if len(document[attr]) == 1:
                data[attr] = document[attr][0]
            elif len(document[attr]) == 0:
                data[attr] = None
            else:
                logging.info(f"Attribute {attr} in {document['_id']} has more than one value!")
    
    # Numerical attributes    
    if isinstance(document["EnrollmentCount"], list):
        if len(document["EnrollmentCount"]) == 1:
            data["EnrollmentCount"] = document["EnrollmentCount"][0]
        elif len(document["EnrollmentCount"]) == 0:
            data["EnrollmentCount"] = None
        else:
            logging.info(f"Attribute 'EnrollmentCount' in {document['_id']} has more than one value!")
    
    if isinstance(document["EventsFrequencyThreshold"], list):
        if len(document["EventsFrequencyThreshold"]) == 1:
            data["EventsFrequencyThreshold"] = document["EventsFrequencyThreshold"][0]
        elif len(document["EventsFrequencyThreshold"]) == 0:
            data["EventsFrequencyThreshold"] = None
        else:
            logging.info(f"Attribute 'EventsFrequencyThreshold' in {document['_id']} has more than one value!")

    if isinstance(document["NCTId"], list):
        logging.info(f"Updating {document['NCTId'][0]}")
    else:
        logging.info(f"Updating {document['NCTId']}")

    if len(data.keys()) > 0:
        trialsCollection.update_one(
            {"_id": document["_id"]},
            {"$set": data}
        )
        logging.info("Updated.")
    #else:
    #    logging.info("Nothing to update.")