#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Source: Feature Engineering - Custom Transformers and Pipeline.ipynb (check GitHub Repository: https://github.com/CaroHolt/ClinicalTrialsEnrollment)
"""

from operator import itemgetter
import collections
from datetime import datetime
import random
import time
import statsmodels.api as sm
import json
import Levenshtein
import string
import gc
from itertools import chain
import re
from functools import reduce

from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.model_selection import train_test_split
from sklearn.metrics.pairwise import cosine_similarity
from sklearn.feature_extraction.text import CountVectorizer
from sklearn.feature_extraction import DictVectorizer
from sklearn.preprocessing import FunctionTransformer, StandardScaler, RobustScaler, MultiLabelBinarizer
from sklearn.impute import SimpleImputer, KNNImputer

from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize 
import nltk

import pandas as pd
import numpy as np

class FeatureSelector(TransformerMixin, BaseEstimator):
    """
    Returns a dataframe with selected features.
    
    Parameters
    --------
        cols
            columns to be selected.

    Attributes
    --------

    Notes
    --------
    """

    def __init__(self, cols):
        self.cols = cols

    def fit(self, X, y=None):
        """ Do nothing and return the estimator unchanged
        
        Parameters
        ----------
        X : pandas dataframe, shape [n_samples, n_features]

        y : Ignored
        """
        return self

    def transform(self, X):
        """ Selects defined features
        
        Parameters
        ----------
        X : pandas dataframe, shape [n_samples, n_features]
        
        Returns
        -------
        X_new : pandas dataframe with selected features
        """
        X_new = X[self.cols] # or: X_new = X.loc[:, self.cols].copy()
        return X_new

class FeatureExcluder(TransformerMixin, BaseEstimator):
    """
    Excludes the defined features.
    
    Parameters
    --------
        cols
            columns to be excluded.

    Attributes
    --------
        raw_features
            columns to be excluded.

    Notes
    --------
    """

    def __init__(self, cols):
        self.raw_features = list(cols)

    def fit(self, X, y=None):
        """ Do nothing and return the estimator unchanged.
        
        Parameters
        ----------
        X : pandas dataframe, shape [n_samples, n_features]
        
        y : Ignored
        """
        return self

    def transform(self, X):
        """ Excludes defined features.
        
        Parameters
        ----------
        X : pandas dataframe, shape [n_samples, n_features]
        
        Returns
        -------
        X_new : pandas dataframe without raw features
        """
        tmp = [set(self.raw_features), set(list(X.columns))]
        schnitt = set.intersection(*tmp)
        X_new = X.drop(list(schnitt), axis=1)

        for attr in X.columns:
          if "_x" in attr or "_y" in attr or "Condition1" in attr or "Condition2" in attr:
              X_new.drop(attr, axis=1, inplace=True)

        return X_new

class FeatureUnion(TransformerMixin, BaseEstimator):
    """
    Applies fit and transform on multiple transformers and merges their outputs.
    
    Parameters
    --------
        transformer_list
            list of transformers

    Attributes
    --------

    Notes
    --------
    """
    
    def __init__(self, transformer_list):
        self.transformer_list = transformer_list

    def fit(self, X, y=None):
        """ Fit all transformers.
        
        Parameters
        ----------
        X : pandas dataframe, shape [n_samples, n_features]
        y : Ignored
        """
        for (name, transformer) in self.transformer_list:
            transformer.fit(X, y)
        return self
      
    def transform(self, X):
        """ Run transform on all transformers and merge dataframes into a single one.

        Parameters
        ----------
        X : pandas dataframe, shape [n_samples, n_features]
        
        Returns
        -------
        X_new : merged pandas dataframe
        """
        Xts = [transformer.transform(X) for _, transformer in self.transformer_list]
        X_new = reduce(lambda X1, X2: pd.merge(X1, X2, left_index=True, right_index=True), Xts)
        return X_new

"""##Transformers for single categorical values"""

class MissingStringsTransformer(TransformerMixin, BaseEstimator):
    """
    Replaces missing values using SimpleImputer and returns a pandas dataframe.

    Parameters
    --------       
        strategy : string, default = 'most_frequent'
            String value defining the strategy of the simple imputer. Is only used for the simple imputer
            One can decide between: 
            - constant -> filling in a constant value
            - most_frequent -> calculate the most frequent value and inserting that value
        
        fill_value : string, default = ''
            Value defining the constant value that will be inserted when the Simple imputation strategy "constant" is selected

    Attributes
    --------
        imp 
          Placeholder to contain the created Imputer instance of the fit method.

    Notes
    --------
    https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
    
    """
    
    def __init__(self, 
                 strategy = 'most_frequent', 
                 fill_value= "",
                 n_neighbors = 2,
                 weights = "uniform"):
        self.strategy = strategy
        self.fill_value = fill_value    # in case of strategy 'constant'
        self.imp = None

    def fit(self, X, y=None):
        if self.strategy == 'constant':
            self.imp = SimpleImputer(strategy=self.strategy, fill_value=self.fill_value, missing_values=None)
        elif self.strategy == 'most_frequent':
            self.imp = SimpleImputer(strategy=self.strategy, fill_value='Missing', missing_values=None)
        else:
            raise Exception("Please define one strategy - 'most_frequent' or 'constant'")
        self.imp.fit(X)
        return self

    def transform(self, X):
        X_imp = self.imp.transform(X)
        X_new = pd.DataFrame(X_imp, index=X.index, columns=X.columns)
        return X_new

class SingleOneHotEncoder(TransformerMixin, BaseEstimator):
    """
    Applies One Hot Encoding on features with only single values (no lists).

    Parameters
    --------

    Attributes
    --------
        dv 
          Placeholder to contain the created DictVectorizer instance of the fit method.
        
        nan_cols
          Placeholder to contain empty columns to be excluded. Is just set once to make sure that always the same columns are removed.

    Notes
    --------
    https://scikit-learn.org/stable/modules/generated/sklearn.feature_extraction.DictVectorizer.html
    
    """

    def __init__(self):
        self.dv = None
        self.nan_cols = None

    def fit(self, X, y=None):
        X_dict = X.to_dict('records')
        self.dv = DictVectorizer(sparse=False)
        self.dv.fit(X_dict)
        return self

    def transform(self, X):
        X_dict = X.to_dict('records')
        Xt = self.dv.transform(X_dict)
        cols = self.dv.get_feature_names()
        X_new = pd.DataFrame(Xt, index=X.index, columns=cols)
        
        if not self.nan_cols:
            self.nan_cols = [c for c in cols if '=' not in c]
        
        X_new = X_new.drop(self.nan_cols, axis=1)
        return X_new

"""## Transformers for list of categorical values"""

class MultipleOneHotEncoder(TransformerMixin, BaseEstimator):
    """ Applies One Hot Encoding on features with lists as values.

    Parameters
    --------

    Attributes
    --------
        features 
          Placeholder to contain a dictionary consisting of feature name and possible values of each feature.

    Notes
    --------
    
    """

    def __init__(self):
        self.features = {}

    def fit(self, X, y=None):
        for feature in list(X.columns):
            distinct_values = set()
            for index, value_list in zip(X.index, X[feature]):
                for value in value_list: 
                    if value not in distinct_values: distinct_values.add(value)
            self.features[feature] = list(distinct_values)
        return self

    def transform(self, X):
        X_new = X.copy()
        
        for feature in self.features.keys():
            for value in self.features[feature]:
                X_new[f"{feature}={value}"] = 0
                for index, feature_list in zip(X_new.index, X_new[feature]):
                    if value in feature_list: X_new.at[index, f"{feature}={value}"] = 1
        return X_new

class MultipleTopOneHotEncoder(TransformerMixin, BaseEstimator):
    """ Applies One Hot Encoding on features with lists as values taking only the top X Values.

    Parameters
    --------
        strategy : string, default = 'top'
            String value defining the strategy. One can decide between: 
            - top : one hot encodes top X values of all possible values according to the occurrence in trials
            - min_value : one hot encodes all values, with a minimum of X occurences in trials
        
        top : integer, default = 20
            Number of top values to take for strategy 'top'

        min_value : integer, default = None
            Number of minimum occurences in order to be considered top, used for stratefy 'min_value'

    Attributes
    --------
        mlb 
          Placeholder to contain the created MultipleOneHotEncoder instance of the fit method.

    Notes
    --------
    
    """

    def __init__(self, strategie="top", top=20, min_value=None):
        self.mlb = None
        self.strategie = strategie
        self.top = top
        self.min_value = min_value

    def fit(self, X, y=None):
        values = {}
        for feature in list(X.columns):
            stats = {}
            for value_list in X[feature]:
                for value in set(value_list):
                    if value in stats: stats[value] += 1
                    else: stats[value] = 1
            if self.strategie == "min_value" and self.min_value is not None:
                values[feature] = {k: v for k, v in sorted(stats.items(),reverse=True, key=lambda item: item[1]) if v > self.min_value}
            elif self.strategie == "top" and self.top is not None:
                values[feature] = {k: v for k, v in sorted(stats.items(),reverse=True, key=lambda item: item[1])[:self.top]}
            else:
                raise Exception("Please define one strategy - 'top' or 'min_value'") 
        self.values = values
        #print(f"Ranking: ")
        #print(json.dumps(values, indent=2))

        X_tmp = X.copy()
        self.mlb = MultipleOneHotEncoder()
        new_columns = []
        for feature in self.values.keys():
            new_columns.append(f"{feature}_top")
            X_tmp[f"{feature}_top"] = [[x for x in set(value) if x in list(self.values[feature].keys())] for value in X[feature]]
        self.mlb.fit(X_tmp[new_columns])
        return self

    def transform(self, X):
        X_tmp = X.copy()
        new_columns = []
        for feature in self.values.keys():
            new_columns.append(f"{feature}_top")
            X_tmp[f"{feature}_top"] = [[x for x in set(value) if x in list(self.values[feature].keys())] for value in X[feature]]
        
        X_new_partial = self.mlb.transform(X_tmp[new_columns])
        X_new_partial.drop(new_columns, axis=1, inplace=True)
        X_new = X.merge(X_new_partial, left_index=True, right_index=True)
        return X_new

"""##Transformers for numerical values"""

class MissingValuesTransformer(TransformerMixin, BaseEstimator):
    """
    Replaces missing values using SimpleImputer, KNNImputer or IterativeImputer and returns a pandas dataframe.

    Parameters
    --------

        imputer : string, default = 'SimpleImputer'
            String value defining the impution strategy that should be applied.
            One can decide between:
            - SimpleImputer = impute values in the i-th feature dimension using only non-missing values 
                              in that feature dimension
            - KNNImputer = missing values are imputed using the mean value from n_neighbors nearest neighbors
            - IterativeImputer = multivariate imputation algorithm using entire set of available feature 
                                 dimensions to estimate the missing values
            
        strategy : string, default = 'mean'
            String value defining the strategy of the simple imputer. Is only used for the simple imputer
            One can decide between: 
            - mean -> calculate the mean value and inserting that value
            - median -> calculate the mean value and inserting that value
        
        fill_value : string, default = ''
            Value defining the constant value that will be inserted when the Simple imputation strategy "constant" is selected

        n_neighbors : integer, default = 2
            Integer value of the number of neighbors that is taken into account to determine the most frequent or average value of the k nearest neighbors.

        weights : string, default = "uniform"
            Parameter that defines how the values of the k nearest neighbors are weighted. 
            One can choose between:
            - uniform -> all neighbors are equally important
            - distance -> closer neighbors are higher weighted than distanced ones
        
        max_iter : int
            Maximum number of imputation rounds to perform before returning the imputations.
            
        initial_strategy : str, default=’mean’
            Which strategy to use to initialize the missing values in iterative imputer. 
            One can decide between: 
            - mean -> calculate the mean value and inserting that value
            - median -> calculate the mean value and inserting that value

    Attributes
    --------
        imp 
          Placeholder to contain the created Imputer instance of the fit method.

    Notes
    --------

    https://scikit-learn.org/stable/modules/generated/sklearn.impute.SimpleImputer.html
    https://levelup.gitconnected.com/scikit-learn-python-6-useful-tricks-for-data-scientists-1a0a502a6aa3
    
    """
    
    def __init__(self, 
                 imputer = 'SimpleImputer',
                 strategy = 'mean', 
                 fill_value= "",
                 n_neighbors = 2,
                 weights = "uniform",
                 max_iter = 10,
                 initial_strategy = 'mean'):
        self.imputer = imputer
        self.strategy = strategy
        self.fill_value = fill_value    # in case of strategy 'constant'
        self.n_neighbors = n_neighbors
        self.weights = weights
        self.max_iter = max_iter
        self.initial_strategy = initial_strategy
        self.imp = None

    def fit(self, X, y=None):
        if self.imputer == 'SimpleImputer':
            if self.strategy == 'mean':
                self.imp = SimpleImputer(strategy=self.strategy)
            elif self.strategy == 'median':
                self.imp = SimpleImputer(strategy=self.strategy)
            else:
                raise Exception("Please define one strategy - 'mean' or 'median'")
        elif self.imputer == 'KNNImputer':
            if self.weights not in ['uniform', 'distance']:
                raise Exception("Please provide a valid weighting: 'uniform' or 'distance'")
            if not isinstance(self.n_neighbors, (int, float, complex)):
                raise Exception("Please provide a valid number for n_neighbors")
            self.imp = KNNImputer(n_neighbors=self.n_neighbors, weights=self.weights) 
        #elif self.imputer == 'IterativeImputer':
        #    if self.initial_strategy not in ['mean', 'median']:
        #        raise Exception("Please define one strategy - 'mean' or 'median'")
        #    if not isinstance(self.max_iter, (int, float, complex)):
        #        raise Exception("Please provide a valid number for max_iter")
        #    self.imp = IterativeImputer(max_iter=self.max_iter, random_state=0, initial_strategy=self.initial_strategy)
        else: 
            raise Exception("Please define one imputer - 'SimpleImputer', 'KNNImputer' or 'IterativeImputer'")
        self.imp.fit(X)
        return self

    def transform(self, X):
        X_imp = self.imp.transform(X)
        new_columns = [f"{col}_new" for col in X.columns]
        X_new = pd.DataFrame(X_imp, index=X.index, columns=new_columns)
        return X_new

"""## Transformers for age values"""

class ToYearTransformer(TransformerMixin, BaseEstimator):
    """ Replaces different time frame with a number, save it as factors then multiply them,
      returns a pandas dataframe """
    def __init__(self, conversion_factors= {
                                'Years': 1,
                                'Year': 1,
                                'Months': 1/12,
                                'Month': 1/12,
                                'Weeks': 1/52,
                                'Week': 1/52,
                                'Days': 1/365 ,
                                'Day': 1/365,
                                'Hours': 1/8760,
                                'Hour': 1/8760
                                }):

        self._conversion_factors = conversion_factors
    def fit(self,X,y = None):
        return self
    
    def transform(self, X):

        for feature in X.columns:
            X_new = X.copy()
            X_new[feature] = X_new[feature].astype(str)
            for i in range(len(X_new)):
                for string, factor in self._conversion_factors.items(): 
                    for n in range(len(X.columns)):
                        if string in (X_new.iloc[i][n]):              
                            X_new.iloc[i][n] = (X_new.iloc[i][n]).replace(string, '')
                            X_new.iloc[i][n] = (X_new.iloc[i][n]).strip()
                            X_new.iloc[i][n] = float(X_new.iloc[i][n]) * factor
                            X_new.iloc[i][n] = str(X_new.iloc[i][n])


            X_new["MaximumAgeValue"] = X_new['MaximumAge'].astype(float)
            X_new["MinimumAgeValue"] = X_new['MinimumAge'].astype(float)

        return X_new

"""## Transformer for StartMonth"""

class StartMonthTransformer(TransformerMixin, BaseEstimator):
    """
    Class that extracts the month from the start date and creates a new column "StartMonth" in the data set containing
    the extracted month as float value.
    
    """
    def __init__(self):
        pass
    
    def fit(self,X,y = None):
        return self
    
    def transform(self, X):
        # Copy the passed data frame
        X_new = X.copy()
        # Create a new column "StartMonth" and remove the year from the StartDate feature
        X_new['StartMonth'] = X_new['StartDate'].str.replace('0|1|2|3|4|5|6|7|8|9| |  ','')
        
        # Transform each month to a float value
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('January','1')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('February','2')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('March','3')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('April','4')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('May','5')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('June','6')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('July','7')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('August','8')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('September','9')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('October','10')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('November','11')
        X_new['StartMonth'] = X_new['StartMonth'].str.replace('December','12')
        X_new['StartMonth'] = X_new['StartMonth'].astype(float)
        return X_new

"""## Transformer for StartYear"""

class StartYearTransformer(TransformerMixin, BaseEstimator):
    """
    Class that extracts the year from the start date and creates a new column "StartYear" in the data set containing
    the extracted startyear.
    
    """
    def __init__(self):
          pass
    
    def fit(self,X,y = None):
        return self
    
    def transform(self, X):
        # Copy the passed data frame
        X_new = X.copy()
        
        # Remove the month from the StartDate feature
        X_new['StartYear'] = X_new['StartDate'].str.replace('January|February|March|April|May|June|July|August|September|October|November|December', '')
        
        # Save the year as float value
        X_new['StartYear'] = (X_new['StartYear']).astype(float)
        X_new = X_new.drop(columns=['StartDate'])
        return X_new

class DistinctCounter(TransformerMixin, BaseEstimator):
    """ Creates new features with the number of items of features containing a list. "EligibilityCriteria" is an exception where the number of characters is counted.

    Parameters
    --------

    Attributes
    --------

    Notes
    --------
    
    """
    
    def __init__(self):
        pass
    
    def fit(self, X, y=None):
        return self
    
    def transform(self, X):
        X_new = X.copy()
        for feature in X.columns:
            if feature == "EligibilityCriteria":
                values = []
                for eligi in X["EligibilityCriteria"]:
                    if len(eligi): values.append(len(eligi[0]))
                    else: values.append(0)
                X_new["#EligiCriteria"] = values
            else:
                name = f"#Diff{feature}"
                values = []
                for value_list in X[feature]:
                    if len(value_list): values.append(len(value_list))
                    else: values.append(0)
                X_new[name] = values
        return X_new

class PatientsDistributionTransformer(TransformerMixin, BaseEstimator):
    """ Distributes the number of patients over number of facilities and number of countries.

    Parameters
    --------

    Attributes
    --------

    Notes
    --------
    
    """
    def __init__(self):
        pass

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        X_new = X.copy()
        features = ['LocationFacility', 'LocationCountry']
        for feature in features:
            name = f"#Pts/#Diff{feature}"
            s = pd.Series(X['EnrollmentCount']/[len(set(value)) for value in X[feature]])
            X_new[name] = pd.to_numeric(s, errors='coerce').fillna(0, downcast='infer')
        return X_new

class LocationDataTransformer(TransformerMixin, BaseEstimator):
    """ This transformer contains all alternative transformers relating to location (CountryDataTransformer - CityPopulation - PerCountryTransformer - PerCityTransformer)
        transformer:
            totalCombine: a combination between CountryDataTransformer and CityPopulationTransformer (Basically just add one more feature to CountryDataTransformer) => default
            perCity: divide a study into multiple rows, equivalent to list of cities in the study
            perCountry: divide a study into multiple rows, equaivalent to list of country in the study
        strategy:
            simple = just calculates the normal average of each country attribute of all different countries involved / average population density for each city => default
            weighted = calculates the average of each country attribute according to the number of occurences / average population density in the list of distinct cities
        mean:
            trials = uses the mean value of all trials to fill missing country values
            worldwide = uses the worldwide mean for this specific attribute
    """
    def __init__(self, country_data, transformer='totalCombine', strategy='simple', mean='trials', debug=False):
        self.transformer = transformer
        self.country_data = country_data.fillna(0)
        self.strategy = strategy
        self.mean = mean


    def fit(self, X, y=None):
        return self

    def most_frequent(self, List): # Returning array of most frequent elements in a list
        obj = {}
        arr = []
        for item in List:
            obj[item] = List.count(item)
        maxNum = obj[max(obj)]
        for key, value in obj.items():
            if value == maxNum:
                arr.append(key)
        return arr

    def transform(self, X):
        X_new = X.reset_index(drop=True)

        if self.transformer == 'totalCombine':           

            special = {
                'Former Yugoslavia': 'Serbia',
                'Former Serbia and Montenegro': 'Serbia',
                'Holy See (Vatican City State)': 'Italy',
                'Macedonia, The Former Yugoslav Republic of': 'North Macedonia',
                'Swaziland': 'Eswatini',
                'Netherlands Antilles': 'Curacao'
            }

            # creating columns for each country attribute
            columns = self.country_data.drop('countryName', axis=1).columns
            for attr in columns:
                X_new[f"avg_{attr}"], X_new[f"min_{attr}"], X_new[f"max_{attr}"], X_new[f"main_country_{attr}"] = None, None, None, None
                X_new[f"avg_{attr}"] = X_new[f"avg_{attr}"].astype(np.float64)
                X_new[f"min_{attr}"] = X_new[f"min_{attr}"].astype(np.float64)
                X_new[f"max_{attr}"] = X_new[f"max_{attr}"].astype(np.float64)
                X_new[f"main_country_{attr}"] = X_new[f"main_country_{attr}"].astype(np.float64)

            
            X_new['avg_city_population'], X_new['min_city_population'], X_new['max_city_population'] = 0, 0, 0
            for index, locationCity, locationCountry, locationPopulationDensity in zip(X_new.index, X_new['LocationCity'], X_new['LocationCountry'], X_new['LocationPopulationDensity']):

                # Add main country data
                if len(set(locationCountry)) == 1:
                    main_country = locationCountry[0]
                    for attr in columns:
                        if main_country in special.keys():
                            main_country = special[main_country]
                        X_new.at[index, f"main_country_{attr}"] = self.country_data[self.country_data.countryName == main_country][attr]
                        
                else: # If there are more than one country have highest number of facilities, calculate average of country features
                    mostFrequentCountry = self.most_frequent(locationCountry)
                    mainCountryDataTemp = 0
                    
                    for country in mostFrequentCountry:
                        if country in special.keys():
                            country = special[country]
                        for attr in columns:
                            mainCountryDataTemp += self.country_data[self.country_data.countryName == country][attr]
                            X_new.at[index, f"main_country_{attr}"] = mainCountryDataTemp / len(mostFrequentCountry)

                cityList = []
                for i in range(len(locationCity)):
                    cityList.append(f"{locationCity[i]} --- {locationCountry[i]}")

                if self.strategy == 'weighted':
                    tmp_countries = list(locationCountry)
                    tmp_cities = list(cityList) # Get list of involved city
                else:
                    tmp_countries = list(set(locationCountry))
                    tmp_cities = list(set(cityList)) # Get unique list of city
                    
                # iterate over cities and get density information
                density_sum = 0
                density_min, density_max = None, None
                for city in tmp_cities:
                    id = locationCity.index(city.split(" --- ")[0])
                    tmp_density = locationPopulationDensity[id]
                    density_sum += tmp_density
                    if density_min:
                        if density_min > tmp_density: density_min = tmp_density
                    else: density_min = tmp_density
                    if density_max:
                        if density_max < tmp_density: density_max = tmp_density
                    else: density_max = tmp_density

                X_new.at[index, "avg_city_population"] = density_sum/len(cityList)
                X_new.at[index, "min_city_population"] = density_min
                X_new.at[index, "max_city_population"] = density_max

                # iterate over countries and get country data
                data_avg, data_min, data_max = {}, {}, {}
                countries_not_found = []
                for country in tmp_countries:
                    if country in self.country_data.values:
                        tmp = self.country_data[self.country_data.countryName == country]
                        for attr in columns:
                            if attr in data_avg.keys(): data_avg[attr] += float(tmp[attr])
                            else: data_avg[attr] = float(tmp[attr])

                            if attr in data_min.keys():
                                if data_min[attr] > float(tmp[attr]): data_min[attr] = float(tmp[attr])
                            else: data_min[attr] = float(tmp[attr])

                            if attr in data_max.keys():
                                if data_max[attr] < float(tmp[attr]): data_max[attr] = float(tmp[attr])
                            else: data_max[attr] = float(tmp[attr])
                    elif country in special.keys():
                        tmp = self.country_data[self.country_data.countryName == special[country]]
                        for attr in columns:
                            if attr in data_avg.keys(): data_avg[attr] += float(tmp[attr])
                            else: data_avg[attr] = float(tmp[attr])

                            if attr in data_min.keys():
                                if data_min[attr] > float(tmp[attr]): data_min[attr] = float(tmp[attr])
                            else: data_min[attr] = float(tmp[attr])

                            if attr in data_max.keys():
                                if data_max[attr] < float(tmp[attr]): data_max[attr] = float(tmp[attr])
                            else: data_max[attr] = float(tmp[attr])
                    else:
                        countries_not_found.append(country)
                        tmp_countries.remove(country)

                for attr in data_avg.keys():
                    X_new.at[index, f"avg_{attr}"] = data_avg[attr]/len(tmp_countries)
                    X_new.at[index, f"min_{attr}"] = data_min[attr]
                    X_new.at[index, f"max_{attr}"] = data_max[attr]
                
            # fill empty values in other attributes
            for attr in data_avg.keys():
                #print(f"Avg {attr}: ", X_new[f"avg_{attr}"].isnull().sum())
                #print(f"Min {attr}: ", X_new[f"min_{attr}"].isnull().sum())
                #print(f"Max {attr}: ", X_new[f"max_{attr}"].isnull().sum())
                if self.mean == 'worldwide':
                    X_new[f"avg_{attr}"] = X_new[f"avg_{attr}"].fillna(self.country_data[attr].mean())
                    X_new[f"min_{attr}"] = X_new[f"min_{attr}"].fillna(self.country_data[attr].min())
                    X_new[f"max_{attr}"] = X_new[f"max_{attr}"].fillna(self.country_data[attr].max())
                    X_new[f"main_country_{attr}"] = X_new[f"avg_{attr}"].fillna(self.country_data[attr].mean())
                else:
                    X_new[f"avg_{attr}"] = X_new[f"avg_{attr}"].fillna(X_new[f'avg_{attr}'].mean())
                    X_new[f"avg_{attr}"] = X_new[f"avg_{attr}"].astype(np.float64)
                    X_new[f"min_{attr}"] = X_new[f"min_{attr}"].fillna(X_new[f'min_{attr}'].min())
                    X_new[f"min_{attr}"] = X_new[f"min_{attr}"].astype(np.float64)
                    X_new[f"max_{attr}"] = X_new[f"max_{attr}"].fillna(X_new[f'max_{attr}'].max())
                    X_new[f"max_{attr}"] = X_new[f"max_{attr}"].astype(np.float64)
                    X_new[f"main_country_{attr}"] = X_new[f"avg_{attr}"].fillna(X_new[f'avg_{attr}'].mean())
                    X_new[f"main_country_{attr}"] = X_new[f"avg_{attr}"].astype(np.float64)

            if len(countries_not_found): print("Countries not found: ", countries_not_found) # Print not found countries

        return X_new

class TextualFeatureTransformer( TransformerMixin, BaseEstimator ):
    """
    Class for performing the feature transformation for textual features. 
    After performing several NLP preprocessing steps, the n most frequently mentioned keywords are identified 
    and a one-hot coding is performed for them. 

    Parameters
    --------

        remove : array of strings
            Array of strings defining the NLP preprocessing steps that should be carried out 
            before the keyword extraction can take place.
            You can choose from the following steps: 
                - 'upper'
                - 'numbers'
                - 'special'
                
            By default, all of them are applied
        n_keywords : integer
            Integer defining the number of keywords that should be extracted
        stop_words : list or set
            List of stopwords that are to be eliminated prior to the keyword extraction.
        

    Attributes
    --------
    
    features : dict
        Dictionary of the feature subset that is to be transformed
    top_keywords: dict
        Dictionary to store the top n identified keywords of each feature
    df_transformedFeatures : dataframe
        Dataframe of the transformed features to apply one hot encoding on with regard to the preprocessed entries
     """
    
    #Class constructor method that takes in a list of values as its argument
    def __init__(self, 
                 remove = ['upper', 'numbers', 'special'],
                 n_keywords = 20, 
                 stop_words = set(stopwords.words('english'))):
        self._remove = remove
        self._n_keywords = n_keywords
        self._stop_words = stop_words
        self.features = {}
        self.top_keywords = {}
        self.df_transformedFeatures = pd.DataFrame()

    #Helper function to remove numbers 
    def remove_numbers(self, list):
        """ Removes all numbers in the list """
        pattern = '[0-9]'
        list = [re.sub(pattern, '', i) for i in list] 
        return list

    #Helper function to remove special characters
    def remove_special(self, list): 
        """ Removes all special characters in the list """
        pattern = '[^A-Za-z0-9]+'
        list = [re.sub(pattern, ' ', i) for i in list] 
        return list
    
    #Helper function to remove uppercase letter and replace with lower case
    def remove_upper(self, obj):
        """ Converts all letters to lower case in the list """
        obj = list(map(str.lower, obj))
        return obj
    
    #Helper function to create one hot encoding
    def one_hot (self, row, entry, attr):
        if str(entry) in row[attr]:
            return 1
        else:
            return 0
        
    def fit( self, X, y = None ):
        # Apply NLP preprocessing
        self.features = X.to_dict()
        for attr in X.columns:
            if isinstance(X[attr][0], list):    
                for spec in self._remove:
                    for k, v in self.features[attr].items():
                        exec("self.features['{}'][{}] = self.remove_{}({})".format(attr, k, spec, v ) )
            else:
                for spec in self._remove:
                    for k, v in self.features[attr].items():
                        if self.features[attr][k]:
                            if spec == 'upper':
                                self.features[attr][k] = v.lower()
                            if spec == 'numbers':
                                self.features[attr][k] = ''.join([i for i in self.features[attr][k] if not i.isdigit()])
                            if spec == 'special':
                                exclude = set(string.punctuation)
                                self.features[attr][k] = ''.join(ch for ch in self.features[attr][k] if ch not in exclude)
            
            # Convert list of strings or strings into list of tokens consisting of only one word
            for k, v in self.features[attr].items():
                tokens = []
                if isinstance(v, list):
                    for entry in v:
                        partial = entry.split()
                        tokens = tokens + partial
                else:
                    tokens = str(v).split(' ')
                tokens = [w for w in tokens if not w in self._stop_words]
                self.features[attr][k] = tokens
        
        # extract keywords
        self.df_transformedFeatures = pd.DataFrame.from_dict(self.features)
        for attr in self.df_transformedFeatures.columns:
            self.top_keywords[attr] = self.df_transformedFeatures[attr].explode().value_counts()
            self.top_keywords[attr].drop(['mg', 'g', '&', 'kg', 'b', None, 'None', ' ', ''], 
                                         inplace = True, 
                                         errors='ignore')
            self.top_keywords[attr] = self.top_keywords[attr].nlargest(20)
        #print(self.top_keywords)
        return self

    def transform(self, X , y = None ):
        X_new = self.df_transformedFeatures.copy()
        # Apply one hot according to keywords                       
        for attr in X.columns:
            for entry in self.top_keywords[attr].index:
                columnName = attr + '_' + str(entry)
                X_new[columnName] = X_new.apply(lambda row: self.one_hot(row, entry, attr), axis=1)

        X_new.drop(columns = attr)
        return X_new

class RegionalAgeTransformer( BaseEstimator, TransformerMixin ):
    def __init__( self, countrylist, df_regional, debug=False):
        self._debug = debug
        self._countrylist = countrylist # new
        self._df_regional = df_regional

    def fit( self, X, y = None ):
        return self 
    
    def transform(self, X , y = None ):

        X_new = pd.DataFrame(X)
        
        #Fill empty values with minimal and maximal age
        X_new["MinimumAge"].loc[X_new["MinimumAge"].isna()==True]="0 Years"
        X_new["MaximumAge"].loc[X_new["MaximumAge"].isna()==True]="100 Years"
        
        #Define Conversion Factors
        conversion_factors= {
                                'Years': 1,
                                'Year': 1,
                                'Months': 1/12,
                                'Month': 1/12,
                                'Weeks': 1/52,
                                'Week': 1/52,
                                'Days': 1/365 ,
                                'Day': 1/365,
                                'Hours': 1/8760,
                                'Hour': 1/8760
                                }
        
        #Convert time string to float
        for i in range(len(X_new["MinimumAge"])):
                    for string, factor in conversion_factors.items():   
                        if string in (X_new["MinimumAge"].iloc[i]):
                            X_new["MinimumAge"].iloc[i] = (X_new["MinimumAge"].iloc[i]).replace(string, '')
                            X_new["MinimumAge"].iloc[i] = (X_new["MinimumAge"].iloc[i]).strip()
                            X_new["MinimumAge"].iloc[i] = float(X_new["MinimumAge"].iloc[i]) * factor
                            X_new["MinimumAge"].iloc[i] = str(X_new["MinimumAge"].iloc[i] )

        X_new["MinimumAge"] = X_new["MinimumAge"].astype(float)

        for i in range(len(X_new["MaximumAge"])):
                    for string, factor in conversion_factors.items():   
                        if string in (X_new["MaximumAge"].iloc[i]):
                            X_new["MaximumAge"].iloc[i] = (X_new["MaximumAge"].iloc[i]).replace(string, '')
                            X_new["MaximumAge"].iloc[i] = (X_new["MaximumAge"].iloc[i]).strip()
                            X_new["MaximumAge"].iloc[i] = float(X_new["MaximumAge"].iloc[i]) * factor
                            X_new["MaximumAge"].iloc[i] = str(X_new["MaximumAge"].iloc[i] )
                            
        X_new["MaximumAge"] = X_new["MaximumAge"].astype(float)
        
        #Create Age column
        X_new["Age"]=np.empty((len(X_new), 0)).tolist()

        for index, min_value, max_value in zip(X_new.index, X_new["MinimumAge"], X_new["MaximumAge"]):
            if min_value <= 16: X_new.at[index, "Age"].append("Youth")
            if min_value < 65 and max_value > 16: X_new.at[index, "Age"].append("Working")
            if max_value >= 65: X_new.at[index, "Age"].append("Elderly")

        #Add additional local attributes to dataframe
        X_new["LocalAge"]=np.empty((len(X_new), 0)).tolist()
        X_new["AvgLocalAge"]=0
        
        for i, row in X_new.iterrows():
            for j,city in enumerate(row["LocationCity"]):
                if city in self._countrylist.unique():
                    if "Youth" in row["Age"]:
                        row["LocalAge"].append(int(self._df_regional["Youth"].loc[self._df_regional["Area"]==city]))
                    if "Working" in row["Age"]:
                        row["LocalAge"].append(int(self._df_regional["Working"].loc[self._df_regional["Area"]==city]))
                    if "Elderly" in row["Age"]:
                        row["LocalAge"].append(int(self._df_regional["Elderly"].loc[self._df_regional["Area"]==city]))
                else:
                    if len(row["LocationCity"])==len(row["LocationState"]):
                        if row["LocationState"][j] in self._countrylist.unique():
                            if "Youth" in row["Age"]:
                                row["LocalAge"].append(int(self._df_regional["Youth"].loc[self._df_regional["Area"]==row["LocationState"][j]]))
                            if "Working" in row["Age"]:
                                row["LocalAge"].append(int(self._df_regional["Working"].loc[self._df_regional["Area"]==row["LocationState"][j]]))
                            if "Elderly" in row["Age"]:
                                row["LocalAge"].append(int(self._df_regional["Elderly"].loc[self._df_regional["Area"]==row["LocationState"][j]]))
            
            #Take the sum of all matching population
            if len(X_new["LocalAge"][i])>0:
                X_new["AvgLocalAge"][i]=np.sum(X_new["LocalAge"][i])
                #X_new.at[i, "AvgLocalAge"] = np.sum(X_new.at[i, "LocalAge"])
                #X_new["AvgLocalAge"].iloc[i] = np.sum(X_new.at[i, "LocalAge"])
                
        todropfeatures = ["LocationCity", "LocationState", "MinimumAge", "MaximumAge", "Age", "LocalAge"]
        X_new=X_new.drop(todropfeatures, axis=1)

        return X_new

class WorldRankTransformer( BaseEstimator, TransformerMixin ):
    """
    Class that matches the LocationFacility with a data set that ranks Universitites and Hospitals 
    by their research activities.

    Parameters
    --------

        worldrankData : dataframe
            A dataframe containing information about the worldrank of the facilities.
            
        stopwords: set
            Set of stopwords that should be excluded when preprocessing the facility names.
        
        debug: boolean
            A boolean value defining whether or not the transformer execution should be debugged.

    Notes
    --------
        - Required columns = LocationFacility
        - Required information = Worldrank data set
        - Output columns = AvgFacilityRank for all Facilities involved
        
        """

    def __init__( self, 
                 worldrankData = pd.DataFrame(),
                 stopwords = set(stopwords.words('english')),
                 debug=False):
        self._worldrankData = worldrankData.copy()
        self._debug = debug
        self._stopwords = stopwords

    def fit( self, X, y = None ):
        return self 
    
    #Define Preprocessing Function
    def clean_string(self, text):
        text = ''.join([word for word in text if word not in string.punctuation])
        text = text.lower()
        text = ' '.join([word for word in text.split() if word not in self._stopwords])
        return text
    
    def transform(self, X , y = None ):
        #Copy df
        X_new = pd.DataFrame(X)
        
        #Create emtpy list columns
        X_new["FacilityWorldRank"]=np.empty((len(X_new), 0)).tolist()
        X_new["floatFacilityWorldRank"]=np.empty((len(X_new), 0)).tolist()
        X_new["CleanFacility"]=np.empty((len(X_new), 0)).tolist()
        X_new["AvgFacilityRank"]=0
        

        for index, row in X_new.iterrows():
            rowFacilities = []

            #Preprocess data
            for fac in row["LocationFacility"]:
                row["CleanFacility"].append(self.clean_string(fac))

            #Match facilities with worldrank data set
            for element in row['CleanFacility']:
                for fac in self._worldrankData.index:
                    if element == fac:
                        rowFacilities.append(self._worldrankData.loc[fac]['WorldRank'])
            X_new.at[index, 'FacilityWorldRank'] = rowFacilities
            
        # Calculate the mean world rank of all facilities a worldrank could be identified for
        for i, facility_worldrank in zip(X_new.index, X_new["FacilityWorldRank"]):
            if facility_worldrank:
                X_new.at[i, "AvgFacilityRank"] = sum(facility_worldrank) / len(facility_worldrank)
            
        
        # Duplicated 'LocationFacility'
        X_new.drop(columns=['LocationFacility', 'floatFacilityWorldRank', 'CleanFacility', 'FacilityWorldRank'], inplace = True)

        return X_new

class MeshIDTransformer( TransformerMixin, BaseEstimator ):
    def __init__( self, debug=False):
        self._debug = debug

    def fit( self, X, y = None ):
        return self 
    
    def transform(self, X , y = None ):
        #X_new = X.copy()
        X_new = pd.DataFrame(X)
        
        X_new["MeshID1"]=0
        X_new["MeshID2"]=0
        X_new["MeshID3"]=0

        for i, row in X_new.iterrows():
            if len(X_new["ConditionMeshId"][i])==1:
                X_new.at[i, "MeshID1"] = int(X_new["ConditionMeshId"][i][0].strip("D"))
            if len(X_new["ConditionMeshId"][i])==2:
                X_new.at[i, "MeshID1"] = int(X_new["ConditionMeshId"][i][0].strip("D"))
                X_new.at[i, "MeshID2"] = int(X_new["ConditionMeshId"][i][1].strip("D"))
            if len(X_new["ConditionMeshId"][i])==3:
                X_new.at[i, "MeshID1"] = int(X_new["ConditionMeshId"][i][0].strip("D"))
                X_new.at[i, "MeshID2"] = int(X_new["ConditionMeshId"][i][1].strip("D"))
                X_new.at[i, "MeshID3"] = int(X_new["ConditionMeshId"][i][2].strip("D"))
        
        X_new = X_new.drop("ConditionMeshId", axis=1)
        return X_new

class StandardScalerTransformer(TransformerMixin):
    """ Applies StandardScaler on data and returns a dataframe """

    def __init__(self):
        self.ss = None
        self.mean_ = None
        self.scale_ = None

    def fit(self, X, y=None):
        self.ss = StandardScaler()
        self.ss.fit(X)
        self.mean_ = pd.Series(self.ss.mean_, index=X.columns)
        self.scale_ = pd.Series(self.ss.scale_, index=X.columns)
        return self

    def transform(self, X):
        Xss = self.ss.transform(X)
        Xscaled = pd.DataFrame(Xss, index=X.index, columns=X.columns)
        return Xscaled
