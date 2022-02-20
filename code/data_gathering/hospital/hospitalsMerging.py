import logging
import json
from bson.json_util import dumps
from bson.objectid import ObjectId
import requests
import config
import pymongo
import math
import pandas as pd

""" Logging config """
logging.basicConfig(
    filename="./code/logs/hospitalsMultipleCandidates2.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

result = []
emptyObject = {}

# Checking duplication on the new collection, return true if the location (place_id) is not yet in the collection, false otherwise
def duplicationChecking(place_id):
    try:
        result = newHospitals.find({"place_id": place_id}).count()
        if result == 0:
            return True
        else:
            return False
    except Exception as err:
        logging.error('Function duplicationChecking, connection issue: {}'.format(err))
        exit(1)

def hospitalEnrichment():
    if "hospitals" in collectionNames:
        hospitalsCollection = trialsDB['hospitals']
        newHospitals = trialsDB['hospitalsClone']

        hospitals = hospitalsCollection.find()

        for hospital in hospitals:
            record = emptyObject.copy()

            # Copy information from hospitals collection
            record = {
                "name": hospital['Name'],
                "link": hospital['Link'],
                "worldRank": int(hospital['World Rank']) if 'World Rank' in hospital.keys() and hospital['World Rank']!=None else "",
                "coordinate": hospital['coordinate'] if 'coordinate' in hospital.keys() and hospital['coordinate']!=None  and type(hospital['coordinate'])==str else "",
                "hospitalBeds": hospital['hospitalBeds'] if 'hospitalBeds' in hospital.keys() else ""
            }
 
            # Create parameters for Google Place API
            payload = config.GgPlaceParams.copy()
            payload['input'] = hospital['Name']

            # Use locationbias if available
            try:
                if record['coordinate'] not in [None, ""]:
                    a = record['coordinate'].split("(")
                    b = a[1].split(" ")
                    c = b[1].split(")")
                    payload['locationbias'] = 'point:{},{}'.format(c[0],b[0]) #long lat => lat long
                else:
                    payload.pop('locationbias')
            except KeyError as err:
                # handle documents don't have location feature
                payload.pop('locationbias')
            
            try:
                resp = requests.get(url = config.ggPlaceURL, params = payload)
                if resp.status_code == 200:
                    data = resp.json()

                    # The request is in correct format, now take care of the response

                    # GENERAL RULE: Only insert to new collection if in the 'record' contains place_id

                    # The response can be still not ok in case of no candidates, or exceed API limitation. Showing log...
                    if data['status'] == 'OK':
                        
                        candidates = data['candidates']
                        if len(candidates) > 0:
                            if len(candidates) == 1:
                                candidate = candidates[0]
                                # Check if in the result has already had place_id (duplication checking)
                                try:

                                    # This line may be improved, for example, checking data with no place found on GG API. For now, I skipped all hospitals that cannot found
                                    hospitalCounter = newHospitals.find({"place_id": candidate["place_id"]}).count()
                                    if hospitalCounter == 0: # No duplication found
                                        for key in candidate.keys():
                                            record[key] = candidate[key]
                                        try:
                                            # Insert record to the new collection
                                            newHospitals.insert_one(record)
                                        except Exception as err:
                                            logging.info('Cannot insert record - {}\nThe record: {}'.format(err, record))

                                except Exception as err:
                                    # Cannot get information for comparing result
                                    logging.info('Cannot query hospitalsClone - {}'.format(err))
                            else:
                                # Check which candidate has more information
                                logging.info('This document has {} candidate: {}, named: {}'.format(len(candidates), hospital['_id'], hospital['Name']))
                            
                    elif data['status'] == 'ZERO_RESULTS':
                        logging.info('Cannot found the document {} named {} via Google API'.format(hospital['_id'], hospital['Name']))
                        # try:
                        #     newHospitals.insert_one(record)
                        # except Exception as err:
                        #     logging.info('Cannot insert record - {}\nThe record: {}'.format(err, record))
                    elif data['status'] == 'OVER_QUERY_LIMIT':
                        print('API key reached its limitation. Take a break or switch key')
                    else:
                        logging.info('Google API error - status not ok - at document: {}\nThe response: {}'.format(hospital['_id'], resp.json()))
                
                else:
                    logging.info('Google API error at document: {}\nThe response: {}'.format(hospital['_id'], resp))
            
            except Exception as err:
                # If request error because of wrong parameter or wrong URL, it comes here
                print("Problems request to Google API - {}, document: {}".format(err, hospital['_id']))
                exit(1)
        
    else:
        print('No hospitals collection found') 

def handleMultipleCandidates():
    # call list of multiple candidates
    candidatesList = pd.read_csv('./data/Multiple Candidates.csv', encoding="utf-8")

    # Create parameters for Google Place API
    payload = {
        # 'key': 'AIzaSyAS-tPZOE6w-OXEimN-J6eqTbpp_dIKaUU', # Lukas's key
        'key': 'AIzaSyBQo2nUei85kArWNEkF9CVADojvqKnOE6U', # Giang's key
        'query': '',
        'language':'en', # There are some none english data as well
    }
    fields = ["business_status", "formatted_address", "name", "place_id", "rating", "types", "user_ratings_total"]

    for item in candidatesList.iterrows():
        # Step 1: create query to original hospital collection
        hospitals = hospitalsCollection.find({"_id": ObjectId(item[1]["_id"])})
        try:
            for hospital in hospitals:
                query = hospital["Name"]
                # Use more parameters if available
                if 'coordinate' in hospital.keys() and hospital['coordinate'] not in [None, ""] and type(hospital['coordinate'])==str:
                    a = hospital['coordinate'].split("(")
                    b = a[1].split(" ")
                    c = b[1].split(")")
                    location = '{},{}'.format(c[0],b[0]) # long lat => lat long
                    payload['location'] = location
                    payload['radius'] = 50000 # 50k meters

                if 'Country' in hospital.keys() and hospital['Country'] not in [None, ""]:
                    payload['region'] = hospital['Country']
                
                if 'location' in hospital.keys() and hospital['location'] not in [None, ""]:
                    query = "{} {}".format(query, hospital['location'])
                
                payload['query'] = query

                # Copy information from hospitals collection
                record = {
                    "name": hospital['Name'],
                    "link": hospital['Link'],
                    "worldRank": int(hospital['World Rank']) if 'World Rank' in hospital.keys() and hospital['World Rank']!=None else "",
                    "coordinate": hospital['coordinate'] if 'coordinate' in hospital.keys() and hospital['coordinate']!=None  and type(hospital['coordinate'])==str else "",
                    "hospitalBeds": hospital['hospitalBeds'] if 'hospitalBeds' in hospital.keys() else ""
                }

                try:
                    # Use text search api for this case
                    resp = requests.get(url = 'https://maps.googleapis.com/maps/api/place/textsearch/json', params = payload)
                    if resp.status_code == 200:
                        data = resp.json()

                        # GENERAL RULE: Only insert to new collection if in the 'record' contains place_id

                        # The response can be still not ok in case of no candidates, or exceed API limitation. Showing log...
                        if data['status'] == 'OK':
                            # In text search api, data lying inside "results"
                            candidates = data['results']
                            if len(candidates) > 0:
                                if len(candidates) == 1:
                                    candidate = candidates[0]
                                    # Check if in the result has already had place_id (duplication checking)
                                    try:
                                        dupCheck = duplicationChecking(candidate['place_id'])
                                        if dupCheck == True:
                                            for field in fields:
                                                if field in candidate.keys():
                                                    record[field] = candidate[field]
                                            
                                            logging.info("Put {} to db".format(item[1]["_id"]))
                                            try:
                                                # Insert record to the new collection
                                                newHospitals.insert_one(record)
                                            except Exception as err:
                                                logging.info('Cannot insert record - {}\Item: {}'.format(err, item[1]["_id"]))
                                        else:
                                            logging.info("Throw {} away".format(item[1]["_id"]))

                                    except Exception as err:
                                        # Cannot get information for comparing result
                                        logging.info('Cannot query hospitalsClone - {}'.format(err))
                                else:
                                    # removing unmatched candidates
                                    matchedCandidates = [candidate for candidate in candidates if duplicationChecking(candidate['place_id']) == True 
                                    if ('hospital' in candidate['types'] or 'university' in candidate['types'] or 'health' in candidate['types'])]

                                    if len(matchedCandidates) == 0:
                                        logging.info('All candidates are removed: {}'.format(item[1]["_id"]))
                                    elif len(matchedCandidates) == 1:
                                        candidate = matchedCandidates[0]
                                        for field in fields:
                                            if field in candidate.keys():
                                                record[field] = candidate[field]
                                        logging.info("Put {} to db".format(item[1]["_id"]))
                                        try:
                                            # Insert record to the new collection
                                            newHospitals.insert_one(record)
                                        except Exception as err:
                                            logging.info('Cannot insert record - {}\Item: {}'.format(err, item[1]["_id"]))
                                    else:
                                        # If location is used, use the first one (because it's the most nearby the provided location)
                                        if 'location' in payload.keys():
                                            candidate = matchedCandidates[0]
                                            for field in fields:
                                                if field in candidate.keys():
                                                    record[field] = candidate[field]
                                            try:
                                                # Insert record to the new collection
                                                newHospitals.insert_one(record)
                                            except Exception as err:
                                                logging.info('Cannot insert record - {}\nItem: {}'.format(err, item[1]["_id"]))
                                            logging.info("Put {} to db".format(item[1]["_id"]))
                                        else:
                                            # otherwise compare user_ratings_total
                                            chosenOne = 0
                                            highestTotalRating = matchedCandidates[0]['user_ratings_total']
                                            for i in range(1, len(matchedCandidates)):
                                                if matchedCandidates[i]['user_ratings_total'] > highestTotalRating:
                                                    chosenOne = i
                                                    highestTotalRating = matchedCandidates[i]['user_ratings_total']
                                            candidate = matchedCandidates[chosenOne]
                                            for field in fields:
                                                if field in candidate.keys():
                                                    record[field] = candidate[field]
                                            try:
                                                # Insert record to the new collection
                                                newHospitals.insert_one(record)
                                            except Exception as err:
                                                logging.info('Cannot insert record - {}\nItem: {}'.format(err, item[1]["_id"]))
                                            logging.info("Put {} to db".format(item[1]["_id"]))

                        elif data['status'] == 'ZERO_RESULTS':
                            logging.info('{}: zero candidate'.format(hospital['_id']))
                        elif data['status'] == 'OVER_QUERY_LIMIT':
                            print('API key reached its limitation. Take a break or switch key')
                        else:
                            logging.info('Google API error - status not ok - at document: {}\nThe response: {}'.format(hospital['_id'], resp.json()))
                    
                    else:
                        logging.info('Google API error at document: {}\nThe response: {}'.format(hospital['_id'], resp))
                
                except Exception as err:
                    # If request error because of wrong parameter or wrong URL, it comes here
                    print("Problems request to Google API - {}, document: {}".format(err, hospital['_id']))
                    exit(1)
        except Exception as err:
            logging.error("Getting document error - {}".format(err))
            exit(1)
            
try:
    client = pymongo.MongoClient(config.mongoinstance)
    trialsDB = client['clinical-trials'] 
    collectionNames = trialsDB.list_collection_names()
    if 'hospitals' in collectionNames and 'hospitalsClone' in collectionNames:
        hospitalsCollection = trialsDB['hospitals']
        newHospitals = trialsDB['hospitalsClone']
    
        # hospitalEnrichment()
        handleMultipleCandidates()


except Exception as err:
    print("Problems initiating MongoDB - {}".format(err))
    exit(1)