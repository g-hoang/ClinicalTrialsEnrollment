"""
Description: This file contains configurable data
"""

# connection configuration for server development (production)
mongoinstance = "mongodb+srv://sovanta:Si8T8TtsViHYenjx@clinicaltrials-exomh.mongodb.net/test?retryWrites=true&w=majority"

# when testing, change it to True, so that there is a limit when requesting the information - only 10 trials instead of all
deblimit = 10

csvname = "data/clinicaltrials.csv"
dbname = "data/clinicaltrials.db"
logname = "logs/data_gathering_clinicaltrials.log"

ggPlaceURL = "https://maps.googleapis.com/maps/api/place/findplacefromtext/json"

GgPlaceParams = {
    # 'key': 'AIzaSyAS-tPZOE6w-OXEimN-J6eqTbpp_dIKaUU', # Lukas's key
    'key': 'AIzaSyBQo2nUei85kArWNEkF9CVADojvqKnOE6U', # Giang's key
    'input': '',
    'inputtype': 'textquery',
    'fields': 'business_status,formatted_address,name,rating,place_id,types,user_ratings_total',
    'language':'en', # There are some none english data as well,
    'region': '',
    'locationbias': ''
}

studyFilters = [
    "OverallStatus=Completed",
    "InterventionType=drug",
    "StudyType=Interventional",
    "Phase=Phase 2,Phase 3,Phase 4"
]

columns = [
    "NCTId",
    "OrgFullName",
    "OrgClass",
    "BriefTitle",
    "OfficialTitle",
    "BriefSummary",
    "StudyType",
    "OverallStatus",
    "Phase",
    
    "StartDate",
    "StartDateType",
    "StatusVerifiedDate",
    "CompletionDate",
    "CompletionDateType",
    "TargetDuration",

    "Condition",
    "ConditionAncestorId",
    "ConditionAncestorTerm",
    "ConditionBrowseBranchAbbrev",
    "ConditionBrowseLeafName",
    "ConditionBrowseLeafRelevance",
    "ConditionMeshId",
    "ConditionMeshTerm",

    "LeadSponsorName",
    "LeadSponsorClass",
    "CollaboratorName",
    "CollaboratorClass",

    "StudyPopulation",
    "EligibilityCriteria",
    "EnrollmentCount",
    "EnrollmentType",
    "HealthyVolunteers",
    "Gender",
    "StdAge",
    "MinimumAge",
    "MaximumAge",
    "LocationFacility",
    "LocationCity",
    "LocationState",
    "LocationStatus",
    "LocationZip",
    "LocationCountry",

    "InterventionType",
    "InterventionName",

    "IsFDARegulatedDevice",
    "IsFDARegulatedDrug",

    "EventsFrequencyThreshold",
    "EventsTimeFrame",

    "FlowAchievementComment",
    "FlowAchievementGroupId",
    "FlowAchievementNumSubjects",
    "FlowAchievementNumUnits",
    "FlowDropWithdrawComment",
    "FlowDropWithdrawType",
    "FlowGroupDescription",
    "FlowGroupId",
    "FlowGroupTitle",
    "FlowMilestoneComment",
    "FlowMilestoneType",
    "FlowPeriodTitle",
    "FlowPreAssignmentDetails",
    "FlowReasonComment",
    "FlowReasonGroupId",
    "FlowReasonNumSubjects",
    "FlowReasonNumUnits",
    "FlowRecruitmentDetails",
    "FlowTypeUnitsAnalyzed",

    "ArmGroupDescription",
    "ArmGroupInterventionName",
    "ArmGroupLabel",
    "ArmGroupType",

    "AvailIPDType",
    "AvailIPDURL",
    "BaselineCategoryTitle",
    "BaselineClassDenomCountGroupId",
    "BaselineClassDenomCountValue",
    "BaselineClassDenomUnits",
    "BaselineClassTitle",
    "BaselineDenomCountGroupId",
    "BaselineDenomCountValue",
    "BaselineDenomUnits",
    "BaselineGroupDescription",
    "BaselineGroupId",
    "BaselineGroupTitle",
    "BaselineMeasureCalculatePct",
    "BaselineMeasureDenomCountGroupId",
    "BaselineMeasureDenomCountValue",
    "BaselineMeasureDenomUnits",
    "BaselineMeasureDenomUnitsSelected",
    "BaselineMeasureDescription",
    "BaselineMeasureDispersionType",
    "BaselineMeasureParamType",
    "BaselineMeasurePopulationDescription",
    "BaselineMeasureTitle",
    "BaselineMeasureUnitOfMeasure",
    "BaselineMeasurementComment",
    "BaselineMeasurementGroupId",
    "BaselineMeasurementLowerLimit",
    "BaselineMeasurementSpread",
    "BaselineMeasurementUpperLimit",
    "BaselineMeasurementValue",
    "BaselinePopulationDescription",
    "BaselineTypeUnitsAnalyzed",
    "BioSpecDescription",

    "DesignAllocation",
    "DesignInterventionModel",
    "DesignInterventionModelDescription",
    "DesignPrimaryPurpose",
    "DesignTimePerspective",

    "PrimaryOutcomeTimeFrame",
    "ResultsFirstPostDate",
    "ResultsFirstSubmitDate",
    "OutcomeMeasureTimeFrame",
    "Keyword"
]
