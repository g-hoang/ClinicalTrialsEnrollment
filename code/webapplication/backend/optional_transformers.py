#!/usr/bin/env python3.6
# -*- coding: utf-8 -*-
"""
Source: Hyperopt and Models.ipynb (check GitHub Repository: https://github.com/CaroHolt/ClinicalTrialsEnrollment)
"""

from sklearn.base import BaseEstimator, TransformerMixin
import pandas as pd
import numpy as np
import statsmodels.api as sm
from sklearn.preprocessing import MinMaxScaler, Normalizer, StandardScaler

class TimeOutlierRemover( BaseEstimator, TransformerMixin ):
    #Class constructor method that takes in a list of values as its argument
    def __init__(self, startYear = 1995, endYear = 2018):
        self._startYear = startYear
        self._endYear = endYear

    def fit( self, X, y = None ):
        return self
    
    def transform(self, X , y = None ):
        X_new = X.copy()
        X_new = X_new[(X_new['StartYear'] >= self._startYear) & (X_new['StartYear'] <= self._endYear)]
        X_new = X_new.reset_index(drop=True)
        return X_new

class EnrollmentOutlierRemover( BaseEstimator, TransformerMixin ):
    #Class constructor method that takes in a list of values as its argument
    def __init__(self, strategy = "IQR"):
        self._strategy = strategy
        self._range = {}
        self._phases = ['phase1', 'phase1_2', 'phase2', 'phase2_3', 'phase3']
        self._phase_series = {}

    def fit(self, X, y = None ):
        # Get data of the different phases
        
        self._phase_series['phase1'] = X.loc[(X['Phase=Phase 1'] == 1) & (X["Phase=Phase 2"] == 0)]['EnrollmentCount_new']
        self._phase_series['phase1_2'] = X.loc[(X['Phase=Phase 1'] == 1) & (X["Phase=Phase 2"] == 1)]['EnrollmentCount_new']
        self._phase_series['phase2'] = X.loc[(X['Phase=Phase 1'] == 0) & (X["Phase=Phase 2"] == 1) & (X["Phase=Phase 3"] == 0)]['EnrollmentCount_new']
        self._phase_series['phase2_3'] = X.loc[ (X["Phase=Phase 2"] == 1) & (X["Phase=Phase 3"] == 1)]['EnrollmentCount_new']
        self._phase_series['phase3'] = X.loc[ (X["Phase=Phase 2"] == 0) & (X["Phase=Phase 3"] == 1)]['EnrollmentCount_new']
        
        # Calculcate outlier ranges
        if self._strategy == "IQR":
            for i in self._phases:
                IQR = self._phase_series[i].quantile(0.75) - self._phase_series[i].quantile(0.25)
                self._range[i] = [self._phase_series[i].median() - (1.5 * IQR), self._phase_series[i].median() + (2 * IQR)]
                
                
        if self._strategy == "MAD":
            for i in self._phases:
                MAD = self._phase_series[i].mad()
                self._range[i] = [self._phase_series[i].median() - (2 * MAD), self._phase_series[i].median() + (2 * MAD)]
        return self

    
    def transform(self, X , y = None ):
        X_new = X.copy()
        for index, row in X_new.iterrows():
            if row['Phase=Phase 1'] == 1 and row['Phase=Phase 2'] == 0:
                if row['EnrollmentCount_new'] < self._range['phase1'][0] or row['EnrollmentCount_new'] > self._range['phase1'][1]:
                    X_new.drop([index], inplace = True)
            if row['Phase=Phase 1'] == 1 and row['Phase=Phase 2'] == 1:
                if row['EnrollmentCount_new'] < self._range['phase1_2'][0] or row['EnrollmentCount_new'] > self._range['phase1_2'][1]:
                    X_new.drop([index], inplace = True)
            if row['Phase=Phase 1'] == 0 and row['Phase=Phase 2'] == 1 and row['Phase=Phase 3'] == 0:
                if row['EnrollmentCount_new'] < self._range['phase2'][0] or row['EnrollmentCount_new'] > self._range['phase2'][1]:
                    X_new.drop([index], inplace = True)
            if row['Phase=Phase 2'] == 1 and row['Phase=Phase 3'] == 1:
                if row['EnrollmentCount_new'] < self._range['phase2_3'][0] or row['EnrollmentCount_new'] > self._range['phase2_3'][1]:
                    X_new.drop([index], inplace = True)
            if row['Phase=Phase 2'] == 0 and row['Phase=Phase 3'] == 1:
                if row['EnrollmentCount_new'] < self._range['phase3'][0] or row['EnrollmentCount_new'] > self._range['phase3'][1]:
                    X_new.drop([index], inplace = True)
        return X_new

class TargetConditionTransformer( TransformerMixin, BaseEstimator ):
    def __init__( self, debug=False):
        self._debug = debug

    def fit( self, X, y = None ):
        return self 
    
    def transform(self, X , y = None ):

        X_new = pd.DataFrame(X)
        X_new=X_new.reset_index()
        
        X_new["Condition1"]=0
        X_new["Condition2"]=0

        for i, row in X_new.iterrows():
          X_new["Condition1"].iloc[i]= X_new["Condition"].iloc[i][0]
          if len(X_new["Condition"].iloc[i])>1:
            X_new["Condition2"].iloc[i]= X_new["Condition"].iloc[i][1]
          else:
            X_new["Condition2"].iloc[i]= "No second Condition"
        return X_new