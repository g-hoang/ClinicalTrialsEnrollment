import requests
import logging
from pymongo import MongoClient
import config
import datetime
import sys
# import localConfig

# To-do; find a way to apply schema for validating data
# from schema import Schema, And, Use, Optional

""" Logging config """
logging.basicConfig(
    filename="./code/logs/countryEnrichment.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

client = MongoClient(config.mongoinstance)
trials = client['clinical-trials']
collections = trials.list_collection_names()
country = trials['country']

indicatorId = sys.argv[1]
featureName = sys.argv[2]
# schema = Schema([{
#     'country': And(object),
#     'countryiso3code':  And(str),
#     'date': And(int),
#     'value': And(int),
#     }])

# To do

# Get list of all country name, coutry code, CapitalCity

def getCountryList():

    countryList = []
    # api-endpoint 
    URL = "https://api.worldbank.org/v2/country"
    payload = {
        'format': 'json',
        'page': 1
        }

    try:
        resp = requests.get(url=URL, params=payload)
        if resp.status_code == 200:
            data = resp.json()
            for i in range(len(data[1])):

                # Remove continental and region, getting country only
                if data[1][i]['capitalCity'] not in (None, ''):
                    schema = {
                        'countryCode': data[1][i]['id'],
                        'countryName': data[1][i]['name'],
                        'capitalCity': data[1][i]['capitalCity']
                    }
                    countryList.append(schema)
                
            while data[0]['page'] < data[0]['pages']:
                payload = {'format': 'json', 'page': data[0]['page'] + 1}
                logging.info("request page {}".format(data[0]['page'] + 1))
                resp = requests.get(url=URL, params=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    for i in range(len(data[1])):
                        # Remove continental and region, getting country only
                        if data[1][i]['capitalCity'] not in (None, ''):
                            schema = {
                                'countryCode': data[1][i]['id'],
                                'countryName': data[1][i]['name'],
                                'capitalCity': data[1][i]['capitalCity']
                            }
                            countryList.append(schema)   
                else:
                    logging.info(resp.status_code)
            logging.info('Done')
            try:
                country.insert_many(countryList)
                
            except Exception as err:
                logging.error("Problems initiating MongoDB - {}".format(err))
                exit(1)
        else:
            logging.info(resp.status_code)
    except Exception as err:
        logging.error("Problems requesting the data - {}".format(err))
        logging.error("Status code - {}".format(resp))

def migrating(populationData, data):
    for i in range(len(data[1])):
        # Get the lastest value only
        if data[1][i]['value'] != None:

            try:
                logging.info('Searching {}'.format(data[1][i]['countryiso3code']))

                # Check with country list
                searchResultFromNewVer = country.find({'countryCode': data[1][i]['countryiso3code']})

                # If this country exists in country database
                if searchResultFromNewVer.count() > 0:

                    # Check with current list
                    for item in populationData:
                        if data[1][i]['countryiso3code'] == item['countryCode']:
                            if int(data[1][i]['date']) >= item['populationYear']:
                                logging.info('Updating a country')
                                item['populationYear'] = int(data[1][i]['date'])
                                item['population'] = int(data[1][i]['value'])
                            break
                    else:
                        logging.info('Inserting new country: {}'.format(data[1][i]['countryiso3code']))
                        # Check with wikidata
                        searchResultFromOldVer = country.find({'Code': data[1][i]['countryiso3code']})
                        if searchResultFromOldVer.count() > 0:
                            # In case there's similar data from wikidata
                            for r in searchResultFromOldVer:

                                # Reformat year
                                populationYear = datetime.datetime.strptime(r['PopulationDate'], '%Y-%m-%dT%H:%M:%SZ').year

                                # If the year from WB API larger or equal the year from wikidata, take the WB one
                                # (because WB estimates in mid-year, while wikidata estimates in early-year)
                                if int(data[1][i]['date']) >= populationYear:
                                    schema = {
                                        'countryCode': data[1][i]['countryiso3code'],
                                        'populationYear': int(data[1][i]['date']),
                                        'population': int(data[1][i]['value'])
                                    }
                                else:
                                    schema = {
                                        'countryCode': r['Code'],
                                        'populationYear': populationYear,
                                        'population': int(r['Population'])
                                    }
                                populationData.append(schema)
                        else:
                            # In case there's a new country
                            schema = {
                                'countryCode': data[1][i]['countryiso3code'],
                                'populationYear': int(data[1][i]['date']),
                                'population': int(data[1][i]['value'])
                            }
                            populationData.append(schema)


            except Exception as err:
                logging.error(err)

# Fill latest population and population year; reformat PopulationDate => year
def getWikiData():
    populationData = []
    # api-endpoint 
    URL = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
    payload = {
        'format': 'json',
        'page': 57
        }
        
    try:
        # Request population data
        resp = requests.get(url=URL, params=payload)
        if resp.status_code == 200:
            data = resp.json()
            migrating(populationData, data)

            while data[0]['page'] < data[0]['pages']:
                payload = {'format': 'json', 'page': data[0]['page'] + 1}
                logging.info("request page {}".format(data[0]['page'] + 1))
                resp = requests.get(url=URL, params=payload)
                if resp.status_code == 200:
                    data = resp.json()
                    migrating(populationData, data)
            
            logging.info(populationData)
            for population in populationData:
                try:
                    country.update({'countryCode': population['countryCode']}, { '$set': { "population" : population['population'], 'populationYear': population['populationYear'] }})
                except Exception as err:
                    logging.error(err)
        
    except Exception as err:
        logging.error("Problems requesting the data - {}".format(err))

# Insert country feature
def insertCountryFeature(indicatorId='', featureName=''):
    yearVariable = '{}Year'.format(featureName)
    result = []

    if indicatorId != '' and featureName != '': # two require parameters
        # api-endpoint 
        URL = "https://api.worldbank.org/v2/country/all/indicator/{}".format(indicatorId)
        payload = {
            'format': 'json',
            'page': 1
            }

        try:
            # Request population data
            resp = requests.get(url=URL, params=payload)
            if resp.status_code == 200:
                data = resp.json()
                for i in range(len(data[1])):

                    if data[1][i]['value'] not in (None, ''):
                        # Check with country list
                        checkCountry = country.find({'countryCode': data[1][i]['countryiso3code']})
                        # If this country exists in country database
                        if checkCountry.count() > 0:

                            # Check with current list
                            for item in result:
                                if data[1][i]['countryiso3code'] == item['countryCode']:
                                    if int(data[1][i]['date']) > item['year']:
                                        item['year'] = int(data[1][i]['date'])
                                        item['value'] = int(data[1][i]['value'])
                                    break
                            else:
                                print('Inserting new country: {}'.format(data[1][i]['countryiso3code']))
                                schema = {
                                    'countryCode': data[1][i]['countryiso3code'],
                                    'year': int(data[1][i]['date']),
                                    'value': int(data[1][i]['value'])
                                }
                                result.append(schema)
                
                while data[0]['page'] < data[0]['pages']:
                    payload = {'format': 'json', 'page': data[0]['page'] + 1}
                    print('Requesting page {}'.format(data[0]['page'] + 1))
                    resp = requests.get(url=URL, params=payload)
                    if resp.status_code == 200:
                        data = resp.json()
                        for i in range(len(data[1])):
                            if data[1][i]['value'] not in (None, ''):
                                # Check with country list
                                searchResult = country.find({'countryCode': data[1][i]['countryiso3code']})

                                # If this country exists in country database
                                if searchResult.count() > 0:

                                    # Check with current list
                                    for item in result:
                                        if data[1][i]['countryiso3code'] == item['countryCode']:
                                            if int(data[1][i]['date']) > item['year']:
                                                item['year'] = int(data[1][i]['date'])
                                                item['value'] = int(data[1][i]['value'])
                                            break
                                    else:
                                        print('Inserting new country: {}'.format(data[1][i]['countryiso3code']))
                                        schema = {
                                            'countryCode': data[1][i]['countryiso3code'],
                                            'year': int(data[1][i]['date']),
                                            'value': int(data[1][i]['value'])
                                        }
                                        result.append(schema)
                for item in result:
                    try:
                        country.update({'countryCode': item['countryCode']}, { '$set': { featureName : item['value'], yearVariable: item['year'] }})
                    except Exception as err:
                        logging.error(err)
        except Exception as err:
            logging.error("Problems requesting the data - {}".format(err))
    else:
        print('Key of API and feature name must be both fullfilled')

insertCountryFeature(indicatorId, featureName)