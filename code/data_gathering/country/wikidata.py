# pip install sparqlwrapper
# https://rdflib.github.io/sparqlwrapper/

import sys
import pandas as pd
import requests
import logging
import csv
import numpy as np
import pymongo
import os

from os import path
from pymongo import MongoClient
from SPARQLWrapper import SPARQLWrapper, JSON
from collections import OrderedDict
from config import *
from csv import writer

endpoint_url = "https://query.wikidata.org/sparql"

countryName = '"Germany"'

csv, sqlite, mongo = True, True, True
csvwikidatacountries = "../data/wikiCountries.csv"
csvwikidatahospitals = "../data/wikiHospitals.csv"

country_query = """
SELECT ?country ?countryLabel ?popValue ?popDate ?UnempValue ?UnempDate ?lifeExpectancy ?ageOfMajority ?medianIncome ?continentLabel
?capitalLabel ?headOfStateLabel ( GROUP_CONCAT ( DISTINCT ?executiveBodyLabel; separator="; " ) AS ?executiveBodys ) 
( GROUP_CONCAT ( DISTINCT ?legislativeBodyLabel; separator="; " ) AS ?legislativeBodys ) ?GDP ?GDPperCapita ?HumanDevelopmentIndex 
?InflationRate ( GROUP_CONCAT ( DISTINCT ?MemberOrgLabel; separator="; " ) AS ?MemberOrgs )
WHERE {
  ?country wdt:P31 wd:Q6256;
    p:P1082 ?population.
  ?country ?label """+countryName+"""@en.
  OPTIONAL{
    ?country p:P1198 ?UnemploymentLabel.
    ?UnemploymentLabel ps:P1198 ?UnempValue.
    ?UnemploymentLabel pq:P585 ?UnempDate.
    }
  OPTIONAL{
    ?population ps:P1082 ?popValue.
    ?population pq:P585 ?popDate.             
  }
  OPTIONAL { ?country wdt:P2250 ?lifeExpectancy;
                      wdt:P2997 ?ageOfMajority;
                      wdt:P3529 ?medianIncome;
                      wdt:P30 ?continent;
                      wdt:P36 ?capital;
                      wdt:P1906 ?headOfState;
                      wdt:P2131 ?GDP;
                      wdt:P2132 ?GDPperCapita;
                      wdt:P1081 ?HumanDevelopmentIndex;
                      wdt:P1279 ?InflationRate.}
  OPTIONAL { ?country wdt:P463 ?MemberOrg. 
             ?MemberOrg rdfs:label ?MemberOrgLabel
  FILTER ( LANGMATCHES ( LANG ( ?MemberOrgLabel ), "en" ))}
  OPTIONAL { ?country wdt:P208 ?executiveBody. 
             ?executiveBody rdfs:label ?executiveBodyLabel
  FILTER ( LANGMATCHES ( LANG ( ?executiveBodyLabel ), "en" ))}
  OPTIONAL { ?country wdt:P194 ?legislativeBody. 
             ?legislativeBody rdfs:label ?legislativeBodyLabel
  FILTER ( LANGMATCHES ( LANG ( ?legislativeBodyLabel ), "en" ))}
  FILTER NOT EXISTS {
    ?country p:P1198/pq:P585 ?unemp_date_ .
    FILTER (?unemp_date_ > ?UnempDate)
  }
   FILTER NOT EXISTS {
    ?country p:P1082/pq:P585 ?pop_date_ .
    FILTER (?pop_date_ > ?popDate)
  }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "en".}
} 
GROUP BY ?country ?countryLabel ?popValue ?popDate ?lifeExpectancy ?capitalLabel ?continentLabel ?headOfStateLabel ?formOfGovernmentLabel 
?GDP ?GDPperCapita ?HumanDevelopmentIndex ?UnempValue ?UnempDate ?InflationRate ?medianIncome ?ageOfMajority
"""


def get_results(endpoint_url, query):
    user_agent = "WDQS-example Python/%s.%s" % (sys.version_info[0], sys.version_info[1])
    # TODO adjust user agent; see https://w.wiki/CX6
    sparql = SPARQLWrapper(endpoint_url, agent=user_agent)
    sparql.setQuery(query)
    sparql.setReturnFormat(JSON)
    return sparql.query().convert()


results_country = get_results(endpoint_url, country_query)

countries = []
for item in results_country['results']['bindings']:
    countries.append(OrderedDict({
        'country': item['country']['value'],
        'countryName': item['countryLabel']['value'],
        'populationValue': item['popValue']['value'],
        'populationDate': item['popDate']['value']
        if 'popDate' in item else None,
        'lifeExpectancy': item['lifeExpectancy']['value']
        if 'lifeExpectancy' in item else None,
        'capital': item['capitalLabel']['value']
        if 'capitalLabel' in item else None,
        'continent': item['continentLabel']['value']
        if 'continentLabel' in item else None,
        'headOfState': item['headOfStateLabel']['value']
        if 'headOfStateLabel' in item else None,
        'GDP': item['GDP']['value']
        if 'GDP' in item else None,
        'GDPperCapita': item['GDPperCapita']['value']
        if 'GDPperCapita' in item else None,
        'HumanDevelopmentIndex': item['HumanDevelopmentIndex']['value']
        if 'HumanDevelopmentIndex' in item else None,
        'UnemploymentRateValue': item['UnempValue']['value']
        if 'UnempValue' in item else None,
        'UnemploymentRateDate': item['UnempDate']['value']
        if 'UnempDate' in item else None,
        'InflationRate': item['InflationRate']['value']
        if 'InflationRate' in item else None,
        'medianIncome': item['medianIncome']['value']
        if 'medianIncome' in item else None,
        'ageOfMajority': item['ageOfMajority']['value']
        if 'ageOfMajority' in item else None,
        'ageOfMajority': item['ageOfMajority']['value']
        if 'executiveBodys' in item else None,
        'executiveBodys': item['executiveBodys']['value']
        if 'legislativeBodys' in item else None,
        'legislativeBodys': item['legislativeBodys']['value']
        if 'MemberOrgs' in item else None}))
df_countries = pd.DataFrame(countries)
df_countries.set_index('country', inplace=True)
df_countries.head()

hospital_query = """
SELECT DISTINCT ?hospital ?hospitalLabel ?countryLabel ?locationLabel ?coordinate ?hospitalBeds ?website WHERE {
  ?hospital wdt:P31 wd:Q16917.
  ?hospital wdt:P17 ?country.
  OPTIONAL { ?hospital wdt:P131 ?location.}
  OPTIONAL { ?hospital wdt:P625 ?coordinate.}
  OPTIONAL { ?hospital wdt:P6801 ?hospitalBeds.}
  OPTIONAL { ?hospital wdt:P856 ?website.}
  FILTER NOT EXISTS {?hospital wdt:P31 wd:Q64578911.}
  SERVICE wikibase:label { bd:serviceParam wikibase:language "de, en, fr". }}
"""

results_hospital = get_results(endpoint_url, hospital_query)

hospitals = []
for item in results_hospital['results']['bindings']:
    hospitals.append(OrderedDict({
        'hospital': item['hospitalLabel']['value']
        if 'hospitalLabel' in item else None,
        'country': item['countryLabel']['value'],
        'location': item['locationLabel']['value']
        if 'locationLabel' in item else None,
        'coordinate': item['coordinate']['value']
        if 'coordinate' in item else None,
        'website': item['website']['value']
        if 'website' in item else None,
        'hospitalBeds': item['hospitalBeds']['value']
        if 'hospitalBeds' in item else None}))
df_hospitals = pd.DataFrame(hospitals)
df_hospitals.set_index('hospital', inplace=True)
df_hospitals.head()

# Get WHO Country Codes
who_country_url = "https://ghoapi.azureedge.net/api/DIMENSION/COUNTRY/DimensionValues"

try:
    resp = requests.get(url=who_country_url)
    data = resp.json()
    if resp.status_code == 200:
        data = data['value']
except Exception as err:
    exit(1)

"""Enhance with CountryCodes from WHO"""
df_countries['CountryCode'] = ""
for country in data:
    for index, row in df_countries.iterrows():
        print(country['Title'])
        print(row['countryName'])
        if country['Title'] == row['countryName']:
            df_countries.at[index, 'CountryCode'] = country['Code']


""" Check if csv file already exists: if yes, open in append mode; otherwise create a new file """
if csv:
    try:
        if path.exists(csvwikidatacountries):
            logging.info("csv file exists.")
        else:
            logging.info("csv file does not exist.")
            csvfile = open(csvwikidatacountries, "w+", encoding="utf-8").close()
    except Exception as err:
        logging.error("Problems preparing csv file - {}".format(err))
        exit(1)
    try:
        if path.exists(csvwikidatahospitals):
            logging.info("csv file exists.")
        else:
            logging.info("csv file does not exist.")
            csvfile = open(csvwikidatahospitals, "w+", encoding="utf-8").close()
    except Exception as err:
        logging.error("Problems preparing csv file - {}".format(err))
        exit(1)

df_countries.to_csv(csvwikidatacountries, index=False, header=True)
df_hospitals.to_csv(csvwikidatahospitals, index=False, header=True)


""" Initiate MongoDB and retrieve MeshIDs"""

try:
    client = MongoClient(mongoinstance)
    trialsDB = client['clinical-trials']  # create the db, if it does not exist
    clinicaltrials = trialsDB.list_collection_names()
    if "wikidata" in clinicaltrials:
        logging.info("Collection 'wikidata' found in 'clinical-trials' DB")
    trialsCollection = trialsDB['wikidata']
except Exception as err:
    logging.error("Problems initiating MongoDB - {}".format(err))
    exit(1)

# if mongo:
#     from pymongo import MongoClient
#
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
