"""
Description: This file contains all functions used to retrieve the data
"""

""" Create Select String """
def createSelectString(list_of_columns):
    selectString = '&fields='
    lengthColumns = len(list_of_columns)
    for i in range(lengthColumns):
        if(i == 0):
            selectString = selectString + list_of_columns[i]
        else:
            selectString = selectString  + '%2C' + list_of_columns[i]
    return selectString

""" Create filter String """
def createFilterString(list_of_filters, expression=""):
    filterString = '?expr=' + expression
    studyFilterString = 'SEARCH[Study]('

    # Create complete Filter String for Study Filters
    lengthStudyFilters = len(list_of_filters)
    for i in range(lengthStudyFilters):
        partial = list_of_filters[i].split('=')
        #Test if OR Argument is needed
        if(i == 0):
            studyFilterString = studyFilterString + 'AREA[' + partial[0] + '] ' + partial[1]
        if ("," in partial[1]):
            partialValues = partial[1].split(",")
            studyFilterString = studyFilterString + ' AND ' + '(AREA[' + partial[0] + '] ' + partialValues[0] + ' OR ' + partialValues[1] + ')'
        else:
            studyFilterString = studyFilterString + ' AND ' + 'AREA[' + partial[0] + '] ' + partial[1]

    studyFilterString = studyFilterString + ')'

    # Create Filters for the location
    #locationFilters = []
    locationFilterString = ''

    # Create the full filter String
    filterString = filterString + studyFilterString + locationFilterString

    return filterString

""" Functions to clean up retrieved data, especially open text """
def cleanUp(uString):
    return uString.replace("'","''").replace(";",",").replace("\"","").replace("\n","\\")

def returnSingleValue(uValue):
    if len(uValue):
        return cleanUp(uValue[0])
    else:
        return ""
