import logging
import json
import requests
from bson.json_util import dumps
from bson.objectid import ObjectId
import pymongo
import pandas as pd
import numpy
from datetime import datetime

""" Logging config """
logging.basicConfig(
    filename="./logs/duplicates1.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

payload = {
    # 'key': 'AIzaSyAS-tPZOE6w-OXEimN-J6eqTbpp_dIKaUU', # Lukas's key
    # 'key': 'AIzaSyBQo2nUei85kArWNEkF9CVADojvqKnOE6U', # Giang's key
    # 'key': 'AIzaSyD708I-aJ4I28GBsEHhb3uzFmwkwCaeNuA', # An consult
    # 'key': 'AIzaSyDt_O1abDXrd2Wb3V1cObWzDvEfxyiMF6w', # 2
    'key': 'AIzaSyA9ShzICAC1f5F6RFTxfGg8skvsUz25_LI', # 3
    # 'key': 'AIzaSyAUU3Cfzjdfwu4aU7xxmhszIKCYbqJVnnI', # Dung
    'input': '',
    'inputtype': 'textquery',
    'fields': 'business_status,formatted_address,geometry,name,place_id,types',
    'language':'en', # There are some non-english data as well,
    'region': '',
    'locationbias': ''
}

url = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

with open('../trialsCollection - 07072020.json', encoding="utf8") as f:
  data = json.load(f)

counter = 32422
stop = False

try:
    # client = pymongo.MongoClient("mongodb+srv://sovanta:14FgeCSTCZF9HNVw@cluster0-3yb0b.mongodb.net/test?retryWrites=true&w=majority")
    # trialsDB = client['trials'] 

    # Localhost connection
    client = pymongo.MongoClient(host='localhost', port=27017)
    trialsDB = client['sovanta']

    # client2 = pymongo.MongoClient("mongodb+srv://sovanta:Si8T8TtsViHYenjx@clinicaltrials-exomh.mongodb.net/test?retryWrites=true&w=majority")
    
    # primaryDB = client2['clinical-trials']
    trialsCollection = trialsDB['trials']
    populationCollection = trialsDB['populationDensity']
    blackListCollection = trialsDB['blacklist']
        
    # hospitalCollection = primaryDB['hospitalClone']
except Exception as err:
    logging.exception("Problems with MongoDB - {}".format(err))
    exit(1)

def checkDuplicationByName(facilityName):
    try:
        result = trialsCollection.find_one({"LocationFacility": {'$eq': facilityName}})
        if result != None:
            # Get only the first result
            return result
        else:
            return False
        
    except Exception as err:
        logging.error('Function duplicationChecking, connection issue: {}'.format(err))
        exit(1)

def checkDuplicationByFormattedName(facilityName):
    try:
        result = trialsCollection.find_one({"LocationFormattedName": {'$eq': facilityName}})
        return result
    except Exception as err:
        logging.error('Function duplicationChecking, connection issue: {}'.format(err))

def addLocationInformation(originalStudy, newFields, candidate):
    study = originalStudy.copy()
    insertingArray = []
    # Check null and missing key
    if 'geometry' in candidate.keys() and 'location' in candidate['geometry'].keys():
        insertingArray.append([candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat']])
    else:
        insertingArray.append([None, None])

    if 'place_id' in candidate.keys() and candidate['place_id'] != None:
        insertingArray.append(candidate['place_id'])
    else:
        insertingArray.append(None)
    
    if 'name' in candidate.keys() and candidate['name'] != None:
        insertingArray.append(candidate['name'])
    else:
        insertingArray.append(None)
    
    for i in range(len(newFields)):
        study[newFields[i]].append(insertingArray[i])
    
    return study

def addEleToFields(originalStudy, editedFields, editedValues):
    result = originalStudy.copy()
    for i in range(len(editedFields)):
        result[editedFields[i]].append(editedValues[i])
    return result

def getStudyYear(startDate):
    # StartDate = string(Month Year)
    # e.g. December 1994
    year = datetime.strptime(startDate, "%B %Y").year

    if year < 2002:
        year = 2000
    elif year < 2007:
        year = 2005
    elif year < 2012:
        year = 2010
    elif year < 2017:
        year = 2015
    else:
        year = 2020
    return year

def readGoogleAPI(locationName):
    payload['input'] = locationName
    resp = requests.get(url = url, params = payload)
    logging.info(f"Searching {locationName}")
    if resp.status_code == 200:
        cr = resp.json()
        if cr['status'] == 'OK':
            logging.info(f"{locationName} found")
        elif cr['status'] == 'ZERO_RESULTS':
            blackListCollection.insert_one({"name": locationName})
            logging.warning(f"Google search not found {locationName}")
        elif cr['status'] == 'OVER_QUERY_LIMIT':
            logging.warning('API key reached its limitation')
        else:
            logging.error(f"Error: {cr}")

        return cr

def readPopulationDensityData(lng, lat, year):
    densityResult = populationCollection.find_one({
        "location": {
            "$near": {
                "$geometry": {
                    "type": "Point",
                    "coordinates": [lng, lat]
                }
            }
        }, "year": year})
    return densityResult['popDensity']

def isStudyExist(NCTId):
    try:
        result = trialsCollection.find_one({"NCTId": NCTId})
        if result != None:
            return True
        else:
            return False
    except Exception as err:
        logging.error('Function isStudyExist, connection issue: {}'.format(err))

def isInBlackList(facilityName):
    result = blackListCollection.find_one({"name": facilityName})
    return result
    # if result != None:
    #     return True
    # else:
    #     return False

def checkDuplicationByPlaceID(placeId):
    try:
        result = trialsCollection.find_one({"LocationPlaceIds": {'$eq': placeId}})
        if result != None:
            targetIndex = result['LocationPlaceIds'].index(placeId)
            collectedData = [result['LocationCoordinates'][targetIndex], placeId, result['LocationFormattedName'][targetIndex], result['LocationPopulationDensity'][targetIndex]]
            return collectedData
        else:
            logging.info("No duplication by place id was found")
            return None
    except Exception as err:
        logging.error('Function duplicationChecking, connection issue: {}'.format(err))

def requestPlaceID(locationName): # Check place id. This request is free of charge
    params = payload.copy()
    params['input'] = locationName
    del params['fields']
    resp = requests.get(url = url, params = params)
    logging.info(f"Requesting place id for {locationName}")
    if resp.status_code == 200:
        data = resp.json()
        if data['status'] == 'OK':
            placeId = data['candidates'][0]['place_id']
            collectedData = checkDuplicationByPlaceID(placeId)
            if collectedData != None:
                logging.info(f"Original name: {locationName} - Formatted Name: {collectedData[2]}")
                r = {
                    "status": "OK",
                    "candidate": collectedData
                }
                return r
            else:
                return {"status": "NO_DUPLICATION"}

        elif data['status'] == 'ZERO_RESULTS':
            blackListCollection.insert_one({"name": locationName})
            logging.warning(f"Google search not found {locationName}")
            return data
        else:
            logging.error(f"Error in requestPlaceID: {data}")
            return data

# for i in range(23497, 23498):
for i in range(counter, len(data)+1):
    e = False
    if e == True:
        break
    else:
        study = data[i].copy()
        # if isStudyExist(data[i]['NCTId']) == True:
        #     logging.info('Found a same NCTId in collection. Abort.')
        #     break
        # else:
        logging.info(f"Processing {study['NCTId']}")
        del study['_id']
        study['LocationCoordinates'] = []
        study['LocationPlaceIds'] = []
        study['LocationFormattedName'] = []
        study['LocationPopulationDensity'] = []
        study['LocationPopulationDensityRatio'] = []
        populationDensitySum = 0
        studyYear = getStudyYear(study['StartDate'])
        for cityIndex in range(len(study['LocationCity'])):
            if e == True:
                break
            else:
                city = study['LocationCity'][cityIndex]
                country = study['LocationCountry'][cityIndex]
                # Do the city search
                # check in black list
                if isInBlackList(f"{city}, {country}") != None: # This location appear in the black list, cannot be found via google search
                    # Switching to search country, no need to check black list
                    # Check duplication
                    duplicationResult = checkDuplicationByFormattedName(country)
                    if duplicationResult != None: # Duplication found
                        facilityIndex = duplicationResult['LocationFormattedName'].index(study['LocationCountry'][cityIndex])
                        logging.info(f"Duplication found in {duplicationResult['NCTId']}")
                        study['LocationCoordinates'].append(duplicationResult['LocationCoordinates'][facilityIndex])
                        study['LocationPlaceIds'].append(duplicationResult['LocationPlaceIds'][facilityIndex])
                        study['LocationFormattedName'].append(duplicationResult['LocationFormattedName'][facilityIndex])
                        study['LocationPopulationDensity'].append(duplicationResult['LocationPopulationDensity'][facilityIndex])
                        populationDensitySum += duplicationResult['LocationPopulationDensity'][facilityIndex]
                        # facilityIsFinished = True
                    else:
                        # new country
                        resp = readGoogleAPI(country)
                        if resp['status'] == "OK": # Status for searching country is likely ok, inserting to db. Other cases will be caught later
                            candidate = resp['candidates'][0]
                            study['LocationCoordinates'].append([candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat']])
                            study['LocationPlaceIds'].append(candidate['place_id'])
                            study['LocationFormattedName'].append(candidate['name'])
                            # Searching for population density by the given location and study year
                            popDensity = readPopulationDensityData(candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat'], studyYear)
                            study['LocationPopulationDensity'].append(popDensity)
                            populationDensitySum += popDensity
                            # facilityIsFinished = True
                        else:
                            logging.warning("The first error case, break")
                            e = True
                            break
                        
                else: # city not in black list

                    # Check duplication
                    checkResult = checkDuplicationByFormattedName(city)
                    if checkResult != None: # Duplication found
                        facilityIndex = checkResult['LocationFormattedName'].index(city)
                        logging.info(f"Duplication found in {checkResult['NCTId']}")
                        study['LocationCoordinates'].append(checkResult['LocationCoordinates'][facilityIndex])
                        study['LocationPlaceIds'].append(checkResult['LocationPlaceIds'][facilityIndex])
                        study['LocationFormattedName'].append(checkResult['LocationFormattedName'][facilityIndex])
                        study['LocationPopulationDensity'].append(checkResult['LocationPopulationDensity'][facilityIndex])
                        populationDensitySum += checkResult['LocationPopulationDensity'][facilityIndex]
                        # facilityIsFinished = True

                    else: # No duplication found. Search this city + country name
                        kw = f"{city}, {country}"
                        # Check duplication by place id
                        checkPlaceId = requestPlaceID(kw)
                        if checkPlaceId['status'] == "OK":
                            study['LocationCoordinates'].append(checkPlaceId['candidate'][0])
                            study['LocationPlaceIds'].append(checkPlaceId['candidate'][1])
                            study['LocationFormattedName'].append(checkPlaceId['candidate'][2])
                            study['LocationPopulationDensity'].append(checkPlaceId['candidate'][3])
                            populationDensitySum += checkPlaceId['candidate'][3]
                        elif checkPlaceId['status'] == "NO_DUPLICATION":
                            response = readGoogleAPI(kw)
                            if response['status'] == 'OK':
                                # Get the first result
                                candidate = response['candidates'][0]
                                study['LocationCoordinates'].append([candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat']])
                                study['LocationPlaceIds'].append(candidate['place_id'])
                                study['LocationFormattedName'].append(candidate['name'])
                                # Searching for population density by the given location and study year
                                popDensity = readPopulationDensityData(candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat'], studyYear)
                                study['LocationPopulationDensity'].append(popDensity)
                                populationDensitySum += popDensity

                            elif response['status'] == 'ZERO_RESULTS': # Not found on google search => search for country

                                # Check duplication
                                duplicationResult = checkDuplicationByFormattedName(country)
                                if duplicationResult != None: # Duplication found
                                    facilityIndex = duplicationResult['LocationFormattedName'].index(study['LocationCountry'][cityIndex])
                                    logging.info(f"Duplication found in {duplicationResult['NCTId']}")
                                    study['LocationCoordinates'].append(duplicationResult['LocationCoordinates'][facilityIndex])
                                    study['LocationPlaceIds'].append(duplicationResult['LocationPlaceIds'][facilityIndex])
                                    study['LocationFormattedName'].append(duplicationResult['LocationFormattedName'][facilityIndex])
                                    study['LocationPopulationDensity'].append(duplicationResult['LocationPopulationDensity'][facilityIndex])
                                    populationDensitySum += duplicationResult['LocationPopulationDensity'][facilityIndex]
                                    # facilityIsFinished = True

                                else:
                                    resp = readGoogleAPI(study['LocationCountry'][cityIndex])
                                    if resp['status'] == "OK": # Status for searching country is likely ok, inserting to db. Other cases will be caught later
                                        candidate = resp['candidates'][0]
                                        study['LocationCoordinates'].append([candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat']])
                                        study['LocationPlaceIds'].append(candidate['place_id'])
                                        study['LocationFormattedName'].append(candidate['name'])
                                        # Searching for population density by the given location and study year
                                        popDensity = readPopulationDensityData(candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat'], studyYear)
                                        study['LocationPopulationDensity'].append(popDensity)
                                        populationDensitySum += popDensity
                                        # facilityIsFinished = True
                                    else:
                                        logging.warning("The second error case, break")
                                        e = True
                                        break

                            elif response['status'] == 'OVER_QUERY_LIMIT':
                                e = True
                                break
                            else:
                                logging.warning("The third error case, break")
                                e = True
                                break
                        elif checkPlaceId['status'] == "ZERO_RESULTS":
                                # Check duplication
                            duplicationResult = checkDuplicationByFormattedName(country)
                            if duplicationResult != None: # Duplication found
                                facilityIndex = duplicationResult['LocationFormattedName'].index(study['LocationCountry'][cityIndex])
                                logging.info(f"Duplication found in {duplicationResult['NCTId']}")
                                study['LocationCoordinates'].append(duplicationResult['LocationCoordinates'][facilityIndex])
                                study['LocationPlaceIds'].append(duplicationResult['LocationPlaceIds'][facilityIndex])
                                study['LocationFormattedName'].append(duplicationResult['LocationFormattedName'][facilityIndex])
                                study['LocationPopulationDensity'].append(duplicationResult['LocationPopulationDensity'][facilityIndex])
                                populationDensitySum += duplicationResult['LocationPopulationDensity'][facilityIndex]
                                # facilityIsFinished = True

                            else:
                                resp = readGoogleAPI(study['LocationCountry'][cityIndex])
                                if resp['status'] == "OK": # Status for searching country is likely ok, inserting to db. Other cases will be caught later
                                    candidate = resp['candidates'][0]
                                    study['LocationCoordinates'].append([candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat']])
                                    study['LocationPlaceIds'].append(candidate['place_id'])
                                    study['LocationFormattedName'].append(candidate['name'])
                                    # Searching for population density by the given location and study year
                                    popDensity = readPopulationDensityData(candidate['geometry']['location']['lng'], candidate['geometry']['location']['lat'], studyYear)
                                    study['LocationPopulationDensity'].append(popDensity)
                                    populationDensitySum += popDensity
                                    # facilityIsFinished = True
                                else:
                                    logging.warning("The second error case, break")
                                    e = True
                                    break

                        else:
                            logging.warning("The fourth error case, break")
                            e = True
                            break
            
        if len(study['LocationPopulationDensity']) != len(study['LocationCity']):
            logging.error('Length of density != length of city. Something wrong.')
            logging.info(f"formatted name: {study['LocationFormattedName']}  - list city: {study['LocationCity']}")
            e = True
            break
        else:
            for density in study['LocationPopulationDensity']:
                study['LocationPopulationDensityRatio'].append(density/populationDensitySum)
            trialsCollection.insert_one(study)
            logging.info(f"{study['NCTId']} done")