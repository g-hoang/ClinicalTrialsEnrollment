#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Description: starts web service and provides API to access data and prediction results
"""
from flask import Flask, json, request, jsonify, send_file
from flask_cors import CORS
import logging
import pandas as pd
from pymongo import MongoClient
import collections
import os.path
import joblib

import math
import json
from math import sqrt
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import cross_val_score
from sklearn.pipeline import Pipeline
from sklearn.model_selection import train_test_split

import matplotlib.pyplot as plt 
import calendar
from datetime import datetime
from dateutil.relativedelta import *
from sklearn.base import BaseEstimator, TransformerMixin

from functools import lru_cache

# Custom
from mandatory_transformers import *
from optional_transformers import *

""" Constants """
TARGET = ['EnrollmentDuration']
UNIQUE_FEATS = ['ConditionMeshId']
CAT_SINGLE_FEATS = ['HealthyVolunteers', 'Gender', 'IsFDARegulatedDrug', 'IsFDARegulatedDevice', 'DesignPrimaryPurpose', 'EnrollmentType', 'OrgClass','DesignAllocation','DesignInterventionModel']
#CAT_MULTIPLE_FEATS = ['Phase', 'StdAge', 'CollaboratorClass', 'LeadSponsorClass', 'ConditionBrowseBranchAbbrev','InterventionType','ArmGroupType','BaselineDenomCountGroupId','BaselineMeasureDispersionType']
CAT_MULTIPLE_FEATS = ['Phase', 'StdAge', 'CollaboratorClass', 'LeadSponsorClass', 'ConditionBrowseBranchAbbrev','InterventionType','ArmGroupType']
CAT_MULTIPLE_TOP_FEATS1 = ['LocationCountry', 'LocationCity', 'LocationFacility','OrgFullName','LeadSponsorName', 'CollaboratorName']
CAT_MULTIPLE_TOP_FEATS2 = ['Condition', 'ConditionAncestorTerm','Keyword']
NUM_FEATS = ['EnrollmentCount']
AGE_FEATS =['MaximumAge','MinimumAge']
TO_COUNT_FEATS = ['Condition', 'ConditionAncestorTerm', 'CollaboratorClass', 'EligibilityCriteria', 'LocationFacility', 'LocationCity', 'LocationCountry', 'ArmGroupLabel']
TIME_FEAT = ['StartDate']
#TEXTUAL_FEATS1 = ['InterventionName', 'OrgFullName','LeadSponsorName', 'CollaboratorName', 'EligibilityCriteria', 'ArmGroupDescription', 'ArmGroupInterventionName', 'ArmGroupLabel']
TEXTUAL_FEATS1 = ['InterventionName', 'EligibilityCriteria', 'OfficialTitle', 'BriefSummary']
#TEXTUAL_FEATS2 = ['EventsTimeFrame', 'FlowDropWithdrawType', 'FlowGroupDescription', 'FlowGroupTitle', 'FlowMilestoneType', 'FlowPeriodTitle', 'FlowRecruitmentDetails', 'BaselineCategoryTitle', 'BaselineClassTitle', 'BaselineGroupDescription', 'BaselineGroupTitle', 'BaselineMeasureTitle', 'BaselineMeasureUnitOfMeasure']
TEXTUAL_FEATS2 = ['ArmGroupDescription', 'ArmGroupInterventionName', 'ArmGroupLabel']
WORLDRANK_FEATS = ['LocationFacility']
REGIONAL_FEATS = ['LocationCity', 'LocationState', 'MaximumAge', 'MinimumAge']
POPULATION_FEAT = ['LocationPopulationDensity']

ALL_FEATURES = list(set(UNIQUE_FEATS + CAT_SINGLE_FEATS + CAT_MULTIPLE_FEATS + CAT_MULTIPLE_TOP_FEATS1 +  CAT_MULTIPLE_TOP_FEATS2 + NUM_FEATS + TO_COUNT_FEATS + TEXTUAL_FEATS1 + TEXTUAL_FEATS2 + AGE_FEATS  + TIME_FEAT + POPULATION_FEAT + WORLDRANK_FEATS + REGIONAL_FEATS))

""" Logging config """
logging.basicConfig(
    filename = "logs/web.log",
    level = logging.INFO,
    filemode ='w',
    format = "%(asctime)s [%(levelname)s] %(message)s",
    datefmt = '%d.%m.%Y %H:%M:%S'
)

""" Import configuration """
with open("config/conf.json", "r") as conf_file:
    configuration = json.load(conf_file)
logging.info(f"Configuration:\n{configuration}")

""" Start MongoClients """
try:
    client = MongoClient(f"mongodb+srv://{configuration['mongo_client_1']['username']}:{configuration['mongo_client_1']['password']}@{configuration['mongo_client_1']['url']}")
    db = client['clinical-trials']
    client2 = MongoClient(f"mongodb+srv://{configuration['mongo_client_2']['username']}:{configuration['mongo_client_2']['password']}@{configuration['mongo_client_2']['url']}")
    db2 = client2['clinical-trials']
except Exception as err:
    logging.error(f"Not possible to access MongoDB instances - {err}")
    exit(1)

""" Functions """
class Debug(TransformerMixin, BaseEstimator):
    """
    Can be used as an intermediate step to check the status of the dataframe in the pipeline.
    
    Parameters
    --------
        debug
            String describing the debug step in pipeline.

    Attributes
    --------

    Notes
    --------
    - this transformer is used by developers to understand what is happening inside the pipeline
    """

    def __init__(self, debug, logging):
        #print(f"{debug} initiated")
        self.name = debug
        self.logging = logging

    def fit(self, X, y=None):
        self.logging.info(f"{self.name} - Fit executed")
        #print("Dataframe Type: ", type(X))
        return self

    def transform(self, X):
        self.logging.info(f"{self.name} - Transform executed")
        #print("Dataframe Type: ", type(X))
        #X.to_csv(f"dataframe_{self.name}_step.csv", sep=";")
        #print(list(X.columns))
        #display(X.head(5))
        return X.copy()

def retrieve_clinical_trials():
    logging.info("Retrieving clinical trials...")
    
    criteria = {
        '$and': [
        {'LocationCountry': { '$not': {'$size': 0}}},                                           # at least one country
        {'LocationFacility': { '$not': {'$size': 0}}},                                          # at least one facility
        {'Condition': { '$not': {'$size': 0}}},                                                 # at least one condition
        {'EnrollmentCount': {'$ne': 0}},                                                        # Number of patients > 0
        {'EnrollmentCount': {'$ne': None}},                                                     # EnrollmentCount != Null (None) - Actually this feature doesn't have any missing value
        {'EligibilityCriteria': {'$ne': None}},
        {'EligibilityCriteria': {'$ne': []}},
        {'EnrollmentDuration': {'$ne': 0}},                                                     # Duration > 0 (month)
        {'EnrollmentDuration': {'$ne': None}},                                                  # EnrollmentDuration != Null (None)
        {'Phase': { '$ne': ['Phase 1'], '$ne': ['Early Phase 1'], '$ne': ['Not Applicable']}},  # Not accepting phase 1, early phase 1 and not applicable. Accepting multiple-phase studies
        {'NCTId': {'$ne': 'NCT00001132'}}
        ]}

    projectionTrials = {'_id': 0,
                #"NCTId": 1,
                "OrgClass": 1,
                "Condition": 1,
                "LeadSponsorClass": 1,
                "CollaboratorClass": 1,
                "EligibilityCriteria": 1,
                "EnrollmentType": 1,
                "EnrollmentCount": 1,
                "Phase": 1,
                "HealthyVolunteers": 1,
                "Gender": 1,
                "StdAge": 1,
                "LocationFacility": 1,
                "LocationCity": 1,
                "LocationCountry": 1,
                "IsFDARegulatedDrug": 1,
                "IsFDARegulatedDevice": 1,
                "ArmGroupLabel": 1,
                "EnrollmentDuration": 1 ,
                "InterventionName": 1,
                "StartDate" : 1,
                "DesignPrimaryPurpose" : 1,
                "MaximumAge" : 1,
                "MinimumAge" : 1,
                "OrgFullName": 1,
                "ConditionAncestorTerm": 1,
                "ConditionBrowseBranchAbbrev":1,
                "ConditionMeshId":1,
                "LocationState": 1,
                "LocationZip": 1,
                "LocationPopulationDensity": 1,
                "LeadSponsorName": 1,
                
                # Newly added features
                  "InterventionType": 1,
                  "CollaboratorName": 1,
                  "ArmGroupType" : 1,
                  "BaselineDenomCountGroupId": 1,
                  "BaselineMeasureDispersionType" : 1,
                  "DesignAllocation" : 1,
                  "DesignInterventionModel" : 1,
                  "Keyword" : 1,

                  "EventsTimeFrame": 1,
                  "FlowDropWithdrawType": 1,
                  "FlowGroupDescription": 1,
                  "FlowGroupTitle": 1,
                  "FlowMilestoneType": 1,
                  "FlowPeriodTitle": 1,
                  "FlowRecruitmentDetails": 1,
                  "ArmGroupDescription": 1,
                  "ArmGroupInterventionName": 1,
                  "ArmGroupLabel": 1,
                  "BaselineCategoryTitle": 1,
                  "BaselineClassTitle": 1,
                  "BaselineGroupDescription": 1,
                  "BaselineGroupTitle": 1,
                  "BaselineMeasureTitle": 1,
                  "BaselineMeasureUnitOfMeasure": 1,

                  "OfficialTitle": 1,
                  "BriefSummary": 1
                }

    df_raw = pd.DataFrame(list(db.trials.find(filter=criteria, projection=projectionTrials).limit(3000)))
    #df_raw = pd.DataFrame(list(db.trials.find(filter=criteria, projection=projectionTrials)))
    logging.info(f"Number of trials retrieved: {len(df_raw)}")
    return df_raw

def cleanup_clinical_trials():
    global df_raw

    logging.info("Cleaning up clinical trials...")
    indexis = set()
    inc_counter, loc_counter, con_counter = 0, 0, 0
    for index, row in df_raw.iterrows():
        if len(row.LocationFacility) != len(row.LocationCity) or len(row.LocationFacility) != len(row.LocationCountry):
            indexis.add(index)
            inc_counter += 1
    
        for loc in row.LocationFacility:
            if loc.startswith("For additional information regarding investigative sites for this trial,"):
                indexis.add(index)
                loc_counter += 1
        
        if row.EligibilityCriteria[0].startswith("Please contact site for information"):
            indexis.add(index)
            con_counter += 1

    df_raw.drop(indexis, inplace=True)
    logging.info(f"Location Facility/City/Country Inconsistency: {inc_counter}")
    logging.info(f"Location Facility Inconsistency: {loc_counter}")
    logging.info(f"Condition Inconsistency: {con_counter}")
    logging.info(f"Number of trials after removing inconsistencies: {len(df_raw)}")

def retrieve_country_data():
    logging.info("Retrieving country data...")
    countryProjection = {"_id": 0, 
                "urbanPopulation" : 1,
                "countryName": 1,
                "population": 1,
                "density": 1, 
                "sizeInKm2": 1, 
                "lifeExpectancy": 1, 
                "GDP": 1, 
                "migrantsNet": 1, 
                "worldshare": 1,
                "unemploymentRate": 1,
                "hospitalBed": 1,
                "healthExpenditure": 1,
                "fertilityRate": 1,
                "medianAge": 1
    }

    dbcountry = db.country.find(filter={},projection=countryProjection)
    df_dbcountry = pd.DataFrame(list(dbcountry))
    return df_dbcountry

def retrieve_hospital_data():
    logging.info("Retrieving hospital data...")
    try:
        hospitaldata = pd.DataFrame(list(db.hospitals.find(filter={})))
    except Exception as err:
        logging.error(f"Not possible to retrieve hospital data - {err}")
        exit(1)
    
    hospitalcolumns = ['Name', 'World Rank']
    df_hospital = hospitaldata[hospitalcolumns]
    df_hospital.columns = ["Name", "WorldRank"]
    return df_hospital

def retrieve_regional_data():  
    logging.info("Retrieving regional data...")  
    df_regional = pd.DataFrame(list(db2.regional.find(filter={})))
    df_regional["Youth"]= pd.to_numeric(df_regional["Youth"])
    df_regional["Working"]= pd.to_numeric(df_regional["Working"])
    df_regional["Elderly"]= pd.to_numeric(df_regional["Elderly"])
    df_regional = df_regional.drop_duplicates(subset=["Area"])

    todrop = []
    for i,row in df_regional.iterrows():
        if math.isnan(row["Youth"])==True: todrop.append(i)
    df_regional = df_regional.drop(todrop)
    return df_regional

def get_country_list():
    global df_regional
    return df_regional['Area']

def retrieve_facility_ranking():
    logging.info("Retrieving facility data...")
    df_worldrank = pd.DataFrame(list(db2.worldrank.find(filter={})))
    worldrank_cols = ["Facility", "WorldRank"]
    df_worldrank = df_worldrank[worldrank_cols]
    return df_worldrank

def retrieve_university_data():
    logging.info("Retrieving university data...")
    # Import the University Ranking
    df_uni = pd.DataFrame(list(db2.universities.find(filter={})))
    unicolumns = ["Name", "WorldRank"]
    df_uni = df_uni[unicolumns]
    
    # TODO check 
    # Create Facility List to search through
    #frames=[hospital, df_uni]
    #facility = pd.concat(frames, ignore_index=True)

    #todrop=[]
    #for i, fac in facility.iterrows():
    #    if str(fac["WorldRank"]).isdigit() == False: todrop.append(i)
  
    #facility = facility.drop(todrop)
    #facility["WorldRank"]=pd.to_numeric(facility["WorldRank"])

    return df_uni

@lru_cache()
def run_pipeline():
    global df_raw
    global df_worldrank
    global countrylist
    global df_regional
    
    pipeline = Pipeline([
        ('features', FeatureUnion([
            ('target', Pipeline([
                ('extract', FeatureSelector(TARGET))
            ])),
            ('targetcondition', Pipeline([ 
                ('debug', Debug("TARGET", logging)),
                ('extract', FeatureSelector('Condition')),
                ('target_Condition',TargetConditionTransformer()),
                ('excluder', FeatureExcluder('Condition'))
                ])),
            ('categoricals_single', Pipeline([   
               ('debug', Debug("CAT SINGLE", logging)),     
               ('extract', FeatureSelector(CAT_SINGLE_FEATS)),
               ('cat_fill', MissingStringsTransformer(strategy='most_frequent')),
               ('single_one_hot_encoding', SingleOneHotEncoder()),
               ('excluder', FeatureExcluder(CAT_SINGLE_FEATS))
            ])),
            ('categoricals_multiple', Pipeline([
                ('debug', Debug("CAT MULTI", logging)), 
                ('extract', FeatureSelector(CAT_MULTIPLE_FEATS)),
                ('multiple_one_hot_encoding', MultipleOneHotEncoder()),
                ('excluder', FeatureExcluder(CAT_MULTIPLE_FEATS))
            ])),
            ('categoricals_top1', Pipeline([
                ('debug', Debug("CAT MULTI TOP", logging)), 
                ('extract', FeatureSelector(CAT_MULTIPLE_TOP_FEATS1)),
                ('multiple_one_hot_encoding', MultipleTopOneHotEncoder(strategie="top", top=20)),
                ('excluder', FeatureExcluder(CAT_MULTIPLE_TOP_FEATS1))
            ])),
            ('categoricals_top2', Pipeline([
                ('debug', Debug("CAT MULTI TOP 2", logging)), 
                ('extract', FeatureSelector(CAT_MULTIPLE_TOP_FEATS2)),
                ('multiple_one_hot_encoding', MultipleTopOneHotEncoder(strategie="top", top=40)),
                ('excluder', FeatureExcluder(CAT_MULTIPLE_TOP_FEATS2))
            ])),
            ('startMonth', Pipeline([
                ('debug', Debug("START MONTH", logging)), 
                ('extract', FeatureSelector(TIME_FEAT)),      
                ('startMonth', StartMonthTransformer()),
                ('excluder', FeatureExcluder(TIME_FEAT))
            ])),
            ('startYear', Pipeline([
                ('debug', Debug("START YEAR", logging)),
                ('extract', FeatureSelector(TIME_FEAT)),      
                ('startYear', StartYearTransformer()),
                ('excluder', FeatureExcluder(TIME_FEAT))
            ])),
            ('Age_features',Pipeline([
                ('debug', Debug("AGE FEATS", logging)),
                ('extract', FeatureSelector(AGE_FEATS)),
                ('cat_fill', MissingStringsTransformer(strategy='most_frequent')),
                ('toYear', ToYearTransformer()),
                ('excluder', FeatureExcluder(AGE_FEATS))                      
            ])),
            ('counting_features', Pipeline([
                ('debug', Debug("COUNTING FEATS", logging)),
                ('extract', FeatureSelector(TO_COUNT_FEATS)),
                ('counter', DistinctCounter()),
                ('excluder', FeatureExcluder(TO_COUNT_FEATS))
            ])),
            ('textual_features', Pipeline([
                ('debug', Debug("TEXT FEATS", logging)),
                ('extract', FeatureSelector(TEXTUAL_FEATS1)),
                ('counter', TextualFeatureTransformer( n_keywords = 25)),
                ('excluder', FeatureExcluder(TEXTUAL_FEATS1))
            ])),
            ('textual_features', Pipeline([
                ('debug', Debug("TEXT FEATS 2", logging)),
                ('extract', FeatureSelector(TEXTUAL_FEATS2)),
                ('counter', TextualFeatureTransformer( n_keywords = 10)),
                ('excluder', FeatureExcluder(TEXTUAL_FEATS2))
            ])),
            ('numerics', Pipeline([
                ('debug', Debug("NUMERICS", logging)),
                ('extract', FeatureSelector(NUM_FEATS)),
                ('nr_fill', MissingValuesTransformer(imputer = 'KNNImputer', n_neighbors = 5, weights = "uniform")),
                ('excluder', FeatureExcluder(NUM_FEATS))
            ])),
            ('special', Pipeline([
                ('debug', Debug("MESH", logging)),
                ('extract', FeatureSelector(UNIQUE_FEATS)),
                ('meshid_transformer', MeshIDTransformer()),
                ('excluder', FeatureExcluder(UNIQUE_FEATS))
            ])),
             ('worldrank', Pipeline([
                 ('debug', Debug("WORLDRANK", logging)),
                 ('extract', FeatureSelector(WORLDRANK_FEATS)),
                 ('worldrank_transformer', WorldRankTransformer(worldrankData = df_worldrank)),
                 ('excluder', FeatureExcluder(WORLDRANK_FEATS))
             ])),
            ('regional', Pipeline([
                ('debug', Debug("REGIONAL", logging)),
                ('extract', FeatureSelector(REGIONAL_FEATS)),
                ('regional_transformer', RegionalAgeTransformer(countrylist=countrylist, df_regional=df_regional)),
                ('excluder', FeatureExcluder(REGIONAL_FEATS))
            ])),
            ('addFeatures', Pipeline([
                ('extract', FeatureSelector(ALL_FEATURES))
            ]))
            
        ])),
        ('patients', Debug("PATIENTS", logging)),
        ('patients_distribution', PatientsDistributionTransformer()),
        ('location', Debug("LOCATION", logging)),
        # Below is the combination of all alternative transformers. parameters including:
        # - "transformer" one of three: => 'totalCombine', 'perCity', 'perCountry'
        # - "strategy": => 'simple', 'weighted'
        # - "mean": => 'trials', 'worldwide'
        # Strategy and mean are only need if transformer = 'totalCombine'
        ('location_transformation', LocationDataTransformer(df_dbcountry, transformer='totalCombine', strategy='weighted', mean='worldwide')),
        ('excluder', FeatureExcluder(ALL_FEATURES))
    ])

    new_data = pipeline.fit_transform(df_raw[list(set(TARGET + ALL_FEATURES + CAT_SINGLE_FEATS ))]) 
    if "index" in new_data.columns: new_data.drop(columns= ['index'], inplace=True)
    #joblib.dump(pipeline, 'model.joblib')
    return pipeline, new_data

def load_pipeline():
    global df_raw
    
    pipeline = joblib.load("model.joblib") 
    new_data = pipeline.fit_transform(df_raw[list(set(TARGET + ALL_FEATURES))])
    return pipeline, new_data

def prepare_values():
    global df_raw

    values = {}
    for attr in configuration["fields_to_retrieve"]:
        logging.info(f"Checking {attr}")
        if isinstance(df_raw[attr][0], list):
            value_set = set()
            for entry in df_raw[attr]:
                for value in entry:
                    if isinstance(value,np.integer):
                        tmp_value = int(value)
                    elif isinstance(value,np.floating):
                        tmp_value = float(value)
                    else:
                        tmp_value = str(value)
                    value_set.add(tmp_value)
            values[attr] = list(value_set)[:100]
        else:
            values[attr] = [ str(value) for value in list(df_raw[attr].unique())][:100]
    #logging.info(f"Values: {values}")
    return values

def create_mapping():
    global df_raw

    mapping = {}
    count = 0
    for facility_list, city_list, state_list, country_list, density_list in zip(df_raw['LocationFacility'], df_raw['LocationCity'], df_raw['LocationState'], df_raw['LocationCountry'], df_raw['LocationPopulationDensity']):
        if len(facility_list) == len(city_list) and len(city_list) == len(state_list) and len(state_list) == len(country_list) and len(country_list) == len(density_list):
            for facility, city, state, country, density in zip(facility_list, city_list, state_list, country_list, density_list):
                if country not in mapping.keys():
                    mapping[country] = {}
                if state not in mapping[country].keys():
                    mapping[country][state] = {}
                if city not in mapping[country][state].keys():
                    mapping[country][state][city] = {}
                if "density" not in mapping[country][state][city].keys():
                    mapping[country][state][city]["density"] = density
                if "facilities" not in mapping[country][state][city].keys():
                    mapping[country][state][city]["facilities"] = []
                if facility not in mapping[country][state][city]["facilities"]:
                    mapping[country][state][city]["facilities"].append(facility)
        else:
            if len(state_list) < len(facility_list):
                if country not in mapping.keys():
                    mapping[country] = {}
                if "none" not in mapping[country].keys():
                    mapping[country]["none"] = {}
                if city not in mapping[country]["none"].keys():
                    mapping[country]["none"][city] = {}
                if "density" not in mapping[country]["none"][city].keys():
                    mapping[country]["none"][city]["density"] = density
                if "facilities" not in mapping[country]["none"][city].keys():
                    mapping[country]["none"][city]["facilities"] = []
                if facility not in mapping[country]["none"][city]["facilities"]:
                    mapping[country]["none"][city]["facilities"].append(facility)
            else:
                count += 1
                logging.info(f"Inconsistency: \n\t -Facility: {facility_list}\n\t -City: {city_list}\n\t -State: {state_list}\n\t -Country: {country_list}\n\t -Density: {density_list}")
    
    collections.OrderedDict(sorted(mapping.items()))

    logging.info(f"{count} studies with different lengths")
    #logging.info(f"Mapping:\n{json.dumps(mapping, indent=2)}")
    return mapping

def apply_optional_steps(X, y):
    global params
    global scaler_instance
    
    joined = pd.concat([X, y], axis=1)
    logging.info(f"Size of dataset before time outlier remover: {len(joined)}")
    if params['timeoutlierremover_lgb'] == 1:
        outlier_instance = TimeOutlierRemover(2000, 2018)
    elif params['timeoutlierremover_lgb'] == 2:
        outlier_instance = TimeOutlierRemover(2005, 2018)
    elif params['timeoutlierremover_lgb'] == 3:
        outlier_instance = TimeOutlierRemover(2000, 2016)
    elif params['timeoutlierremover_lgb'] == 4:
        outlier_instance = TimeOutlierRemover(2005, 2016)
    else:
        outlier_instance = TimeOutlierRemover(1995, 2018)
    new_joined = outlier_instance.fit_transform(joined)
    logging.info(f"Size of dataset after time outlier remover: {len(new_joined)}")
    
    logging.info(f"Size of dataset before enrollment count outlier remover: {len(new_joined)}")
    if params['enrollmentoutlierremover'] == "MAD":
        outlier_instance = EnrollmentOutlierRemover(strategy = "MAD")
        new_joined = outlier_instance.fit_transform(new_joined)
    elif params['enrollmentoutlierremover'] == "IQR":
        outlier_instance = EnrollmentOutlierRemover(strategy = "IQR")
        new_joined = outlier_instance.fit_transform(new_joined)
    logging.info(f"Size of dataset after enrollment count outlier remover: {len(new_joined)}")
    
    new_y = new_joined['EnrollmentDuration']
    new_X = new_joined.drop(columns=['EnrollmentDuration'], axis=1)
    
    logging.info("Select relevant features")
    logging.info(f"Number of columns before selection: {len(new_X.columns)}")
    if params['feature_selector'] == 1:
        if len(params['features']): new_X = new_X.copy()[params["features"]]
    logging.info(f"Number of columns after selection: {len(new_X.columns)}")    

    logging.info("Scaling features")
    try:
        if params['scale'] == 1:
            logging.info("Starting StandardScaler")
            scaler_instance = StandardScaler()
            train_scaled = scaler_instance.fit_transform(new_X)
            new_X = pd.DataFrame(train_scaled, index=new_X.index, columns=new_X.columns)
        elif params['scale'] == 2:
            scaler_instance = MinMaxScaler()
            logging.info("Starting MinMaxScaler")
            train_scaled = scaler_instance.fit_transform(new_X)
            new_X = pd.DataFrame(train_scaled, index=df_train_X.index, columns=new_X.columns)
        elif params['scale'] == 3:
            logging.info("Starting Normalizer")
            scaler_instance = Normalizer()
            train_scaled = scaler_instance.fit_transform(new_X)
            new_X = pd.DataFrame(train_scaled, index=new_X.index, columns=new_X.columns)
    except Exception as err:
        logging.error(f"Scaling not working - {err}")
        scaler_instance = None
    
    return new_X, new_y

def create_enrollment_distribution(enrollmentCount, enrollmentDuration, startdate, countries):
    global df_dbcountry

    logging.info("## Starting enrollment distribution")
    logging.info(f"Data:\n enrollmentcount - {enrollmentCount}\n enrollmentduration - {enrollmentDuration}\n startdate - {startdate}\n countries - {countries}")

    duration = int(enrollmentDuration)   
    
    tmp = duration/2
    x = np.linspace(-tmp, tmp, num=duration) # np.linspace function will return a sequence of evenly spaced values on that interval

    # monthly distribution
    startdate = datetime.strptime(startdate, '%B %Y')
    enddate = startdate + relativedelta(months=+(int(enrollmentDuration)-1)) 
    label = pd.date_range(startdate,enddate, freq='MS').strftime("%B %Y").tolist()
    
    # Calculate a factor for the worldshare of a country
    df_dbcountry['worldshareFactor'] = 0.0
    for index, row in df_dbcountry.iterrows():
        if row['worldshare'] < 0.01 or row['worldshare'] == 0.0:
            df_dbcountry.at[index, 'worldshareFactor'] = 0.2
        if row['worldshare'] < 0.05 and row['worldshare']  >= 0.01:
            df_dbcountry.at[index, 'worldshareFactor'] = 0.4
        if row['worldshare'] < 0.2 and row['worldshare']  >= 0.05:
            df_dbcountry.at[index, 'worldshareFactor'] = 0.6
        if row['worldshare'] < 1 and row['worldshare']  >= 0.2:
            df_dbcountry.at[index, 'worldshareFactor'] = 0.8
        if row['worldshare'] >= 1:
            df_dbcountry.at[index, 'worldshareFactor'] = 1.0
    
    countryFrame = pd.DataFrame()
    countryFrame['Country'] = countries
    countryFrame['Worldshare'] = 0.0
    for index, row in countryFrame.iterrows():
        for indexC, rowC in df_dbcountry.iterrows():
            if row['Country'] == rowC['countryName']:
                countryFrame.at[index, 'Worldshare'] = rowC['worldshareFactor']    
                
    totalShare = countryFrame['Worldshare'].sum()
    
    countryFrame['patients'] = 0
    for index, row in countryFrame.iterrows():
        countryFrame.at[index, 'patients'] = round((row['Worldshare']/totalShare) * enrollmentCount)
    
    plotFrame = pd.DataFrame()
    plotFrame['Duration'] = label
    z = {}
    for index, row in countryFrame.iterrows():
        L = row['patients']
        col = "#"+''.join([random.choice('0123456789ABCDEF') for j in range(6)])
        plotFrame[row['Country']] = L/(1 + np.exp(-x)) # logistic sigmoid function
        plt.plot( 'Duration', row['Country'], data=plotFrame, marker='', color=col, linewidth=2)
    
    new_label = []
    for i in range(len(label)):
        if i == 0:
            new_label.append(label[i])
        elif 'January' in label[i]:
            new_label.append(label[i])
        elif i == len(label)-1:
            new_label.append(label[i])
        else: 
            new_label.append('')
    
    plt.legend()
    plt.xticks(rotation=90)
    plt.title('Enrollment count per country')
    plt.xlabel("Duration") 
    plt.ylabel("Sigmoid(EnrollmentCount per Country)") 
    plt.xticks(np.arange(len(label)), new_label)
    ax = plt.subplot(111)    
    ax.spines["top"].set_visible(False)     
    ax.spines["right"].set_visible(False)  
    plt.grid(b=True, which='major', color='#D3D3D3', linestyle='-')
    plt.savefig('images/sigmoid.png', edgecolor='none')
    plt.clf()

""" Routes """
application = Flask(__name__)
CORS(application)

@application.route('/', methods=['GET'])
def get_main():
    return "<html><body><h1>Welcome to Forecasting Patient Enrollment for Clinical Trials REST API.</h1></body></html>"

@application.route('/allvalues', methods=['GET'])
def get_allvalues():
    return jsonify({'result': clinical_trials_values})

@application.route('/values', methods=['GET'])
def get_values():
    # /values?attr=OrgClass
    attr = request.args.get('attr')
    logging.info(f"Requesting attr: {attr}")
    if attr in clinical_trials_values.keys():
        return jsonify({'result': clinical_trials_values[attr]})
    else:
        return jsonify({'result': []})

@application.route('/prediction', methods=['POST'])
def make_prediction():
    # /prediction?type=XGB OR /prediction?type=LGBM
    prediction_type = request.args.get('type')
    if prediction_type is None or prediction_type not in ["LGBM", "XGB"]:
        return jsonify({'result': "bad request", 'results': ''})
    
    content = request.get_json(force=True)
    logging.info(f"Body: {content}")
    
    # Check values
    for attr in configuration["mandatory_fields_for_prediction"]:
        if attr not in content:
            logging.info(f"{attr} not in request")
            return jsonify({'prediction': 'bad request', 'mandatory': configuration["mandatory_fields_for_prediction"]})
    
    # Convert to dataframe
    df_predict = pd.DataFrame.from_dict(content, orient='index').transpose()
    df_predict["EnrollmentDuration"] = np.nan                           # pipeline is expecting this feature
    if "index" in df_predict.columns: df_predict.drop(columns= ['index'], inplace=True)
    logging.info(f"To predict: \n{df_predict.head()}")
    df_predict.to_csv("output/df_predict.csv", index=False, sep=";")

    # Apply transformation
    df_transformed = pipeline.transform(df_predict)
    df_transformed.drop(columns=["EnrollmentDuration"], axis=1, inplace=True)
    if "index" in df_transformed.columns: df_transformed.drop(columns= ['index'], inplace=True)
    logging.info(f"To predict transformed: \n{df_transformed.head()}")
    df_transformed.to_csv("output/df_transformed.csv", index=False, sep=";")

    # Make prediction
    if prediction_type == "XGB":
        logging.info("## Starting to apply XGB on new data...")
        try:
            result = xgb_model.predict(df_transformed)
            logging.info(f"Prediction of XGB: {str(result)}")
            logging.info(f"Results of XGB: {str(xgb_results)}")

            # create sigmoid image
            create_enrollment_distribution(df_predict['EnrollmentCount'].iloc[0], result[0], df_predict['StartDate'].iloc[0], countries=df_predict['LocationCountry'].iloc[0])

            return jsonify({'prediction': float(result), 'result': xgb_results})
        except Exception as err:
            logging.error(f"Error while predicting with XGB - {err}")
            return jsonify({'prediction': 'not possible to predict (internal error)', 'result': ''})
    
    elif prediction_type == "LGBM":
        logging.info("## Starting to apply LGBM on new data...")
        try:
            # Apply Scaler
            if scaler_instance != None:
                transformed = scaler_instance.fit_transform(df_transformed)
                df_transformed = pd.DataFrame(transformed, index=df_transformed.index, columns=df_transformed.columns)
                df_transformed.to_csv("output/df_scaled.csv", index=False, sep=";")

            result = lgbm_model.predict(df_transformed)
            logging.info(f"Prediction of LGBM: {str(result)}")
            logging.info(f"Results of LGBM: {str(lgbm_results)}")

            # create sigmoid image
            create_enrollment_distribution(df_predict['EnrollmentCount'].iloc[0], result[0], df_predict['StartDate'].iloc[0], countries=df_predict['LocationCountry'].iloc[0])

            return jsonify({'prediction': float(result), 'result': lgbm_results})
        except Exception as err:
            logging.error(f"Error while predicting with LGBM - {err}")
            return jsonify({'prediction': 'not possible to predict (internal error)', 'result': ''})
    else:
        logging.error("Bad request")   
        return jsonify({'prediction': 'bad request', 'result': ''})

@application.route('/sigmoid_img', methods=['GET'])
def get_sigmoid_img():
    logging.info(f"Requesting sigmoid image")

    filename = 'images/sigmoid.png'
    return send_file(filename, mimetype='image/png')

if __name__ == '__main__':

    logging.info("## Retrieve and cleanup clinical trials ##")
    df_raw = retrieve_clinical_trials()
    cleanup_clinical_trials()

    logging.info("## Retrieve further data ##")
    df_dbcountry = retrieve_country_data()
    df_hospital = retrieve_hospital_data()
    df_regional = retrieve_regional_data()
    countrylist = df_regional['Area']
    df_worldrank = retrieve_facility_ranking()
    df_uni = retrieve_university_data()

    # Export data for analysis
    df_raw.to_csv("output/df_raw.csv", index=False, sep=";")
    # df_dbcountry.to_csv("output/df_dbcountry.csv", index=False, sep=";")
    # df_hospital.to_csv("output/df_hospital.csv", index=False, sep=";")
    # df_regional.to_csv("output/df_regional.csv", index=False, sep=";")
    # df_worldrank.to_csv("output/df_worldrank.csv", index=False, sep=";")
    # df_uni.to_csv("output/df_uni.csv", index=False, sep=";")

    logging.info("Saving possible values for each attr into dict")
    clinical_trials_values = prepare_values()
    clinical_trials_values["Locations_mapping"] = create_mapping()

    logging.info("## Preprocessing ##")
    if os.path.isfile('model.joblib'):
        logging.info("Loading pipeline...")
        pipeline, new_data = load_pipeline()
        logging.info(f"Length of new_data: {len(new_data)}")
        logging.info(f"Number of features: {len(new_data.columns)}")
    else:
        logging.info("Running pipeline...")
        logging.info(f"Length of df_raw before pipeline: {len(df_raw)}")
        pipeline, new_data = run_pipeline()
        logging.info(f"Length of new_data: {len(new_data)}")
        logging.info(f"Number of features: {len(new_data.columns)}")

    new_data.to_csv("output/new_data_preprocessed.csv", index=False, sep=";")

    logging.info("## Use Time Series Split ##")
    X = new_data.sort_values(by="StartYear").copy()

    X=X[X["StartYear"]>1995]
    X=X[X["StartYear"]<2018]

    logging.info(f"Size of dataset: {len(X)} records")

    Y= X["EnrollmentDuration"]
    X = X.drop("EnrollmentDuration", axis=1)

    ############################################################
    logging.info("## Run LGBM with optimized hyperparameters ##")
    # read params
    with open(configuration["lgbm_params_file"]) as f:
        params = json.load(f)
    logging.info(f"Best params for LGBM: {params}")

    # apply lgbm optional steps
    logging.info("Applying optional steps for LGBM")
    scaler_instance = None
    X_new, y_new = apply_optional_steps(X, Y)

    # First split
    train_size = int(len(X_new) * 0.9)
    df_train_X, df_test_X = X_new[0:train_size], X_new[train_size:len(X_new)]
    df_train_y, df_test_y = y_new[0:train_size], y_new[train_size:len(y_new)]
    logging.info("# 1. Split Summary:")
    logging.info(f"Length of Train Data: {len(df_train_X)}")
    logging.info(f"Length of Test Data: {len(df_test_X)}")

    #Info about the train and test set
    logging.info(f"Train Set Time Range: from {df_train_X['StartYear'].min()} to {df_train_X['StartYear'].max()}")
    logging.info(f"Test Set Time Range: from {df_test_X['StartYear'].min()} to {df_test_X['StartYear'].max()}")
    logging.info(f"Median Duration Training set: {df_train_y.median()}")
    logging.info(f"Median Duration Test set: {df_test_y.median()}")
    
    # Time Series Split for the Validation Set as well (second split, df_train => df_train + df_validation)
    train_size = int(len(df_train_X) * 0.8)
    df_train_X, df_val_X = df_train_X[0:train_size], df_train_X[train_size:len(df_train_X)]
    df_train_y, df_val_y = df_train_y[0:train_size], df_train_y[train_size:len(df_train_y)]
    logging.info("# 2. Split Summary:")
    logging.info(f"Length of Train Data: {len(df_train_X)}")
    logging.info(f"Length of Validation Data: {len(df_val_X)}")
    logging.info(f"Length of Test Data: {len(df_test_X)}")

    # Define LGBM
    import lightgbm as lgb
    lgbm_model = lgb.LGBMRegressor(
        boosting_type = params['boosting_type'],
        colsample_bytree = params['colsample_bytree'],
        learning_rate = params['learning_rate'],
        #max_bin = params['max_bin'],
        max_depth = params['max_depth'],
        min_child_weight = params['min_child_weight'],
        min_split_gain = params['min_split_gain'],
        n_estimators = params['n_estimators'],
        num_leaves = params['num_leaves'],
        objective = params['objective'],
        reg_alpha = params['reg_alpha'],
        reg_lambda = params['reg_lambda'],
        subsample_for_bin = params['subsample_for_bin'],
        subsample = params['subsample'])
    
    # Apply model on validation data
    try:
        lgbm_model.fit(df_train_X,df_train_y)
        y_pred = lgbm_model.predict(df_val_X)
    except Exception as err:
        logging.error(f"Not possible to fit and predict regression lgbm_model on validation data - {err}")
        exit(1)
    
    # Model scores on validation data
    val_scores = cross_val_score(lgbm_model, df_val_X, df_val_y , scoring='r2')
    val_score_avg = float(val_scores.mean())
    val_r2 = float(r2_score(df_val_y, y_pred))
    val_mae = float(mean_absolute_error(df_val_y , y_pred))
    val_mse = float(mean_squared_error(df_val_y , y_pred))
    val_rmse = float(sqrt(mean_squared_error(df_val_y , y_pred)))

    # Apply model on test data
    try:
        y_pred = lgbm_model.predict(df_test_X)
    except Exception as err:
        logging.error(f"Not possible to fit and predict regression model on test data - {err}")
        exit(1)

    # Model scores on test data
    test_scores = cross_val_score(lgbm_model, df_test_X, df_test_y , scoring='r2')
    test_score_avg = float(test_scores.mean())
    test_r2 = float(r2_score(df_test_y, y_pred))
    test_mae = float(mean_absolute_error(df_test_y , y_pred))
    test_mse = float(mean_squared_error(df_test_y , y_pred))
    test_rmse = float(sqrt(mean_squared_error(df_test_y , y_pred)))

    lgbm_results = {
        "validation_results": {
            "R2 Average": f"{val_score_avg:0.2f}", "R2": f"{val_r2:0.2f}", "MAE": f"{val_mae:0.2f}", "MSE": f"{val_mse:0.2f}", "RMSE": f"{val_rmse:0.2f}"
        },
        "test_results": {
            "R2 Average": f"{test_score_avg:0.2f}", "R2": f"{test_r2:0.2f}", "MAE": f"{test_mae:0.2f}", "MSE": f"{test_mse:0.2f}", "RMSE": f"{test_rmse:0.2f}"
        }
    }
    logging.info(f"LGBM results: {lgbm_results}")
    
    ############################################################
    logging.info("## Run XGB with optimized hyperparameters ##")
    # read params
    with open(configuration["xgb_params_file"]) as f:
        params = json.load(f)
    logging.info("Best params for XGB: ", params)

    # First split
    train_size = int(len(X) * 0.9)
    df_train_X, df_test_X = X[0:train_size], X[train_size:len(X)]
    df_train_y, df_test_y = Y[0:train_size], Y[train_size:len(Y)]
    logging.info("# 1. Split Summary:")
    logging.info(f"Length of Train Data: {len(df_train_X)}")
    logging.info(f"Length of Test Data: {len(df_test_X)}")

    # Time Series Split for the Validation Set as well (second split, df_train => df_train + df_validation)
    train_size = int(len(df_train_X) * 0.8)
    df_train_X, df_val_X = df_train_X[0:train_size], df_train_X[train_size:len(df_train_X)]
    df_train_y, df_val_y = df_train_y[0:train_size], df_train_y[train_size:len(df_train_y)]
    logging.info("# 2. Split Summary:")
    logging.info(f"Length of Train Data: {len(df_train_X)}")
    logging.info(f"Length of Validation Data: {len(df_val_X)}")
    logging.info(f"Length of Test Data: {len(df_test_X)}")

    # Define XGB
    import xgboost as xgb
    xgb_model = xgb.XGBRegressor(
        base_score = params['base_score'],
        booster = params['booster'],
        colsample_bylevel = params['colsample_bylevel'],
        colsample_bynode = params['colsample_bynode'],
        colsample_bytree = params['colsample_bytree'],
        eval_metric = params['eval_metric'],
        gamma = params['gamma'],
        importance_type = params['importance_type'],
        learning_rate = params['learning_rate'],
        max_delta_step = params['max_delta_step'],
        max_depth = params['max_depth'],
        min_child_weight = params['min_child_weight'],
        n_estimators = params['n_estimators'],
        n_jobs = params['n_jobs'],
        objective = params['objective'],
        random_state = params['random_state'],
        reg_alpha = params['reg_alpha'],
        reg_lambda = params['reg_lambda'],
        sampling_method = params['sampling_method'],
        #scale_pos_weight = params['scale_pos_weight'],
        subsample = params['subsample'],
        tree_method = params['tree_method'],
        verbosity = params['verbosity']) 
    
    # Apply model on validation data
    try:
        xgb_model.fit(df_train_X,df_train_y)
        y_pred = xgb_model.predict(df_val_X)
    except Exception as err:
        logging.error(f"Not possible to fit and predict regression xgb_model on validation data - {err}")
        exit(1)
    
    # Model scores on validation data
    val_scores = cross_val_score(xgb_model, df_val_X, df_val_y , scoring='r2')
    val_score_avg = float(val_scores.mean())
    val_r2 = float(r2_score(df_val_y, y_pred))
    val_mae = float(mean_absolute_error(df_val_y , y_pred))
    val_mse = float(mean_squared_error(df_val_y , y_pred))
    val_rmse = float(sqrt(mean_squared_error(df_val_y , y_pred)))

    # Apply model on test data
    try:
        y_pred = xgb_model.predict(df_test_X)
    except Exception as err:
        logging.error(f"Not possible to fit and predict regression model on test data - {err}")
        exit(1)

    # Model scores on test data
    test_scores = cross_val_score(xgb_model, df_test_X, df_test_y , scoring='r2')
    test_score_avg = float(test_scores.mean())
    test_r2 = float(r2_score(df_test_y, y_pred))
    test_mae = float(mean_absolute_error(df_test_y , y_pred))
    test_mse = float(mean_squared_error(df_test_y , y_pred))
    test_rmse = float(sqrt(mean_squared_error(df_test_y , y_pred)))

    xgb_results = {
        "validation_results": {
            "R2 Average": f"{val_score_avg:0.2f}", "R2": f"{val_r2:0.2f}", "MAE": f"{val_mae:0.2f}", "MSE": f"{val_mse:0.2f}", "RMSE": f"{val_rmse:0.2f}"
        },
        "test_results": {
            "R2 Average": f"{test_score_avg:0.2f}", "R2": f"{test_r2:0.2f}", "MAE": f"{test_mae:0.2f}", "MSE": f"{test_mse:0.2f}", "RMSE": f"{test_rmse:0.2f}"
        }
    }
    logging.info(f"XGB results: {xgb_results}")

    # release memory space
    del df_raw
    del new_data

    # Start API
    logging.info("## Start API ##")
    application.run(host='0.0.0.0', port=configuration["api_service"]["port"])