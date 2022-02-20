import logging
from pymongo import MongoClient
import json
from bson.json_util import dumps
import requests
import config
import pymongo

""" Logging config """
logging.basicConfig(
    filename="logs/hospital.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

# Create parameters for Google Place API
payload = config.GgPlaceParams.copy()

#AIzaSyBQo2nUei85kArWNEkF9CVADojvqKnOE6U

try:
    # production connection
    client = pymongo.MongoClient("mongodb+srv://sovanta:Si8T8TtsViHYenjx@clinicaltrials-exomh.mongodb.net/test?retryWrites=true&w=majority")
    trialsDB = client['clinical-trials']   
    print(trialsDB)
    clinicaltrials = trialsDB.list_collection_names()
    
    if "trials" in clinicaltrials:
        logging.info("Collection 'trials' found in 'clinical-trials' DB")
        trialsCollection = trialsDB['trials']
    
    all_docs = trialsCollection.find({'LocationFacility': {'$exists': True}}) #get documents from mongo
    results = [] #result of everything will be put here

    for doc in all_docs:
        locationFacility = doc['LocationFacility']
        LocationZip = doc['LocationZip']
        LocationCountry = doc['LocationCountry']


        #need to see the statistics first
        if len(locationFacility) > 0 :
            # input log
            logging.info(dumps(doc['LocationFacility']))

            for i in range(len(locationFacility)):
                # country should be added before running, bcz it's more accurate
                # possible_location_strings = locationFacility[i].split(",")
                # "this is a location, sunny beach, but there is other text, 76345"
                location = locationFacility[i]
                try:
                    # might have error bcz not all LocationFacilities have country data
                    country = '\'' + doc['LocationCountry'][i] + '\''
                except IndexError:
                    country = ''
                    country = bool(country) # return False
                    print("The location {} has no country data.".format(location))

                payload['input'] = location + ' ' + country if country else location
                resp = requests.get(url = config.ggPlaceURL, params = payload)

                if resp.status_code == 200:
                    data = resp.json()                  
                    if len(data['candidates']) > 0:
                        if len(data['candidates']) > 1:
                            logging.info("The location {} has more than one candidates from Google Maps API".format(location))
                            print(data['candidates'], 'more than one results')
                            # error fixing: if zipcode exists in both dataset, choose the candidate with the same zipcode
                        
                            # if formatted_address exists, then we compare zipcode
                            try:
                                zipcode = '\'' + doc['LocationZip'][i] + '\''
                                found_zip = []
                                for candidate in data['candidates']:
                                    if zipcode in candidate['formatted_address']:
                                        logging.info("Found zipcode in: {}, at location {}".format(candidate,location))
                                        print("Found zipcode in: {}, at location {}".format(candidate,location))
                                        found_zip.append(candidate)
                                if len(found_zip) >= 1:
                                    #if found one or more with the same zipcode, then found the one with same location_name
                                    #otherwise, just simply choose the first one
                                    same_name = []
                                    for j in range(len(found_zip)): 
                                        if locationFacility[i] in found_zip[j]['name']:
                                            same_name.append(found_zip[j])
                                            #r = same_name[0]
                                            #print(r,"same_name")
                                    if len(same_name) > 1:
                                        r = same_name[0]
                                        print(r,"same_name_same_zip")
                                    else:
                                        #simply choose the first candidate bcz no further info
                                        r = found_zip[0]
                                        print(r,"same_zip")
                                else: #no zipcode inside
                                    #r = data['candidates'][0]
                                    same_name = []
                                    for j in range(len(data['candidates'])): 
                                        if locationFacility[i] in data['candidates'][j]['name']:
                                            same_name.append(data['candidates'][j])
                                    if len(same_name) > 1:
                                        r = same_name[0]
                                        print(r,"same_name_no_zip")
                                    else:
                                        ###find most completed data
                                        ##firstly compare user_ratings, then check empty fields
                                        #simply choose the first candidate bcz no further info
                                        user_ratings_compare = -1
                                        store_j = 0
                                        for j in range(len(data['candidates'])): 
                                            if int(data['candidates'][j]['user_ratings_total']) > user_ratings_compare:
                                                user_ratings_compare = int(data['candidates'][j]['user_ratings_total'])
                                                store_j = j
                                            # but there might be this scenerio 0 -> 0 -> 1 
                                            #if int(data['candidates'][j]['user_ratings_total']) == user_ratings_compare:
                                             #   store_j.append(j)
                                            
                                        r = data['candidates'][store_j]
                                        print(r,"no_zip_no_name_compare_ratings")
                                    break
            
                            except IndexError:
                                zipcode = ""
                                zipcode = bool(zipcode) # return False
                                print("The location {} has no zip code data.".format(location))  
                        
                                # find another solution than zipcode
                                # if name are the same
                                same_name = []
                                for k in range(len(data['candidates'])):
                                    if data['candidates'][k]["name"] in locationFacility[i]:
                                        same_name.append(data['candidates'][k])
                                        
                                #if same_name exists, use same_name
                                    
                                if len(same_name) > 0:
                                    r = same_name[0]
                                    print(r,"same_name withoutzip")
                                else:
                                    user_ratings_compare = -1
                                    store_j = 0
                                    for j in range(len(data['candidates'])): 
                                        if int(data['candidates'][j]['user_ratings_total']) > user_ratings_compare:
                                            user_ratings_compare = int(data['candidates'][j]['user_ratings_total'])
                                            store_j = j
                                        r = data['candidates'][store_j]
                        
                                    print(r,"no name no zip in trials compare ratings")
                                    
                                continue
                                
            
                        else:
                            data['candidates'][0]['clinical_trials_name'] = location
                            r = data['candidates'][0] 
                            print(r,'unique result')
                            #log outcome
                            logging.info(r)

                    else:
                        r = {'clinical_trials_name': location, 'note': 'Location not found'}
                        #logging.info(r)
                        print(r,'not found')

                    results.append(r)
                    
    logging.info("Done")
    logging.info(results)

except Exception as err:
    print("Problems initiating MongoDB - {}".format(err))
    exit(1)
