import requests
import logging
from pymongo import MongoClient
import config

# To-do; find a way to apply schema for validating data
# from schema import Schema, And, Use, Optional

""" Logging config """
logging.basicConfig(
    filename="./code/logs/country.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

# api-endpoint 
URL = "https://api.worldbank.org/v2/country/all/indicator/SP.POP.TOTL"
payload = {'format': 'json'}
# schema = Schema([{
#     'country': And(object),
#     'countryiso3code':  And(str),
#     'date': And(int),
#     'value': And(int),
#     }])
resultDocuments = []

try:
    resp = requests.get(url=URL, params=payload)
    if resp.status_code == 200:
        data = resp.json()
        for i in range(len(data[1])):
            if data[1][i]['value'] != None:
                schema = {
                    'countryId': data[1][i]['country']['id'],
                    'countryName': data[1][i]['country']['value'],
                    'countryiso3code': data[1][i]['countryiso3code'],
                    'year': data[1][i]['date'],
                    'population': data[1][i]['value']
                }
                resultDocuments.append(schema)

        while data[0]['page'] < data[0]['pages']:
            payload = {'format': 'json', 'page': data[0]['page'] + 1}
            logging.info("request page {}".format(data[0]['page'] + 1))
            resp = requests.get(url=URL, params=payload)
            if resp.status_code == 200:
                data = resp.json()
                for i in range(len(data[1])):
                    if data[1][i]['value'] != None:
                        schema = {
                            'countryId': data[1][i]['country']['id'],
                            'countryName': data[1][i]['country']['value'],
                            'countryiso3code': data[1][i]['countryiso3code'],
                            'year': data[1][i]['date'],
                            'population': data[1][i]['value']
                        }
                        resultDocuments.append(schema)   
            else:
                logging.info(resp.status_code)
        logging.info('Done')
        try:
                                        
            # production connection
            client = MongoClient(config.mongoinstance)

            # create the db, if it does not exist
            trials = client['clinical-trials'] 
            collections = trials.list_collection_names()
            if "country" in collections:
                logging.info("Collection 'country' found in 'clinical-trials' DB")

            country = trials['country']
            country.insert(resultDocuments)
            
        except Exception as err:
            logging.error("Problems initiating MongoDB - {}".format(err))
            exit(1)
    else:
        logging.info(resp.status_code)
except Exception as err:
    logging.error("Problems requesting the data - {}".format(err))
    logging.error("Status code - {}".format(resp))