import datetime
from datetime import date

import requests
from dateutil.relativedelta import relativedelta

import gSheetReader


def main():
    # Read File and save start/ end date, lat, long, field name, and number of fields
    lat = "38.88156"
    long = "-121.980048"
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    sheetID = "10Z-xcBQSGMPsle-p09e4jyygBuTdoZm1IJ7XfDme08A"

    fieldName = "S"

    # Authenticate and get credentials
    creds = gSheetReader.getCreds(SCOPES, creds)
    service = gSheetReader.getService(creds)

    print("Running Algorithm")

    sixthYearStr = "2015"
    fifthYearStr = "2016"
    fourthYearStr = "2017"
    thirdYearStr = "2018"
    lastYearStr = "2019"
    currentYearStr = "2020"

    # Get todays date
    todayDate = date.today()
    today = '{0}-0{1}-{2}'.format(todayDate.year, todayDate.month, todayDate.day)

    # Get Five years date
    fiveYearDate = datetime.datetime.now() - relativedelta(years=4, months=9)
    fiveYear = '{0}-0{1}-{2}'.format(fiveYearDate.year, fiveYearDate.month, fiveYearDate.day)

    dateDict, dateList, valueList = getCimisHistoricalET(lat, long, fiveYear, today)
    print("Field: " + fieldName)

    sixYear, fiveYear, fourYear, threeYear, \
    lastYear, currentYear, sixYearVal, \
    fiveYearVal, fourYearVal, thirdYearVal, \
    lastYearVal, currentYearVal = splitYears(dateList, valueList, sixthYearStr, fifthYearStr, fourthYearStr,
                                             thirdYearStr, lastYearStr, currentYearStr)

    fiveYearVal = correctYears(fiveYear, fiveYearVal)
    fourYearVal = correctYears(fourYear, fourYearVal)
    thirdYearVal = correctYears(threeYear, thirdYearVal)
    lastYearVal = correctYears(lastYear, lastYearVal)
    currentYearVal = correctYears(currentYear, currentYearVal)

    # Get service using credentials
    print("Writing to Sheet")
    rangeName = fieldName
    tabID = gSheetReader.getServiceTabID(rangeName, sheetID, service)

    writeHistoricalETHeaders(creds, currentYearStr, fifthYearStr, fourthYearStr, lastYearStr, rangeName, sheetID,
                             thirdYearStr, service)

    writeHistoricalETData(creds, currentYear, currentYearVal, fieldName, fiveYear, fiveYearVal, fourYear, fourYearVal,
                          lastYear, lastYearVal, sheetID, thirdYearVal, threeYear, service)
    startRowIndex = 1
    endRowIndex = 366
    startColumnIndex = 8
    endColumnIndex = 9
    formula = "=AVERAGE(B2,D2,F2,H2)"
    gSheetReader.getServiceWriteFormula(sheetID, startRowIndex, endRowIndex, startColumnIndex,
                                        endColumnIndex, formula, tabID, service)


def writeAverageHistoricalET(creds, tabName, sheetID, length, service):
    for rowNum in range(2, length):
        rangeName = tabName + "!I" + str(rowNum)
        valueList = [["=Average(B" + str(rowNum) + ", D" + str(rowNum) + ", F" + str(rowNum) +
                      ", H" + str(rowNum) + ")"]]
        gSheetReader.getServiceWriteColumns(rangeName, valueList, sheetID, service)


def writeHistoricalETHeaders(creds, currentYearStr, fifthYearStr, fourthYearStr, lastYearStr, rangeName, sheetID,
                             thirdYearStr, service):
    headerList = [[fifthYearStr, "ET", fourthYearStr, "ET", thirdYearStr, "ET", lastYearStr, "ET",
                   "Average", "", currentYearStr, "ET"]]
    gSheetReader.getServiceWriteRows(rangeName, headerList, sheetID, service)


def writeHistoricalETData(creds, currentYear, currentYearVal, fieldName, fiveYear, fiveYearVal, fourYear, fourYearVal,
                          lastYear, lastYearVal, sheetID, thirdYearVal, threeYear, service):
    rangeName = fieldName + "!A2"
    valueList = [fiveYear, fiveYearVal, fourYear, fourYearVal, threeYear, thirdYearVal,
                 lastYear, lastYearVal, [""], [""], currentYear, currentYearVal]
    gSheetReader.getServiceWriteColumns(rangeName, valueList, sheetID, service)


def correctYears(dateList, valueList):
    length = len(dateList)
    fixedValueList = []
    for x in range(1, length):
        dateSplit = dateList[x].split("-")
        day = int(dateSplit[2])
        month = int(dateSplit[1])
        prevDateSplit = dateList[x - 1].split("-")
        prevDay = int(prevDateSplit[2])
        prevMonth = int(prevDateSplit[1])

        if (day - prevDay) > 1 and month == prevMonth:
            fixedValueList.append("0")
        else:
            fixedValueList.append(valueList[x])

    return fixedValueList


def splitYears(dateList, valueList, sixthYearStr, fifthYearStr, fourthYearStr, thirdYearStr, lastYearStr,
               currentYearStr):
    length = len(dateList)
    sixthYear = []
    sixthYearVal = []
    fifthYear = []
    fifthYearVal = []
    fourthYear = []
    fourthYearVal = []
    thirdYear = []
    thirdYearVal = []
    secondYear = []
    secondYearVal = []
    firstYear = []
    firstYearVal = []

    for x in range(length):
        dateSplit = dateList[x].split("-")
        year = dateSplit[0]
        day = dateSplit[2]
        month = dateSplit[1]
        if day == "29" and month == "02":
            continue
        elif year == sixthYearStr:
            sixthYear.append(dateList[x])
            sixthYearVal.append(float(valueList[x]))
        elif year == fifthYearStr:
            fifthYear.append(dateList[x])
            fifthYearVal.append(float(valueList[x]))
        elif year == fourthYearStr:
            fourthYear.append(dateList[x])
            fourthYearVal.append(float(valueList[x]))
        elif year == thirdYearStr:
            thirdYear.append(dateList[x])
            thirdYearVal.append(float(valueList[x]))
        elif year == lastYearStr:
            secondYear.append(dateList[x])
            secondYearVal.append(float(valueList[x]))
        elif year == currentYearStr:
            firstYear.append(dateList[x])
            firstYearVal.append(float(valueList[x]))

    return sixthYear, fifthYear, fourthYear, thirdYear, secondYear, firstYear, sixthYearVal, fifthYearVal, fourthYearVal, thirdYearVal, secondYearVal, firstYearVal


def splitYearBackslash(dateList, valueList, sixthYearStr, fifthYearStr, fourthYearStr, thirdYearStr, lastYearStr,
                       currentYearStr):
    length = len(dateList)
    sixthYear = []
    sixthYearVal = []
    fifthYear = []
    fifthYearVal = []
    fourthYear = []
    fourthYearVal = []
    thirdYear = []
    thirdYearVal = []
    secondYear = []
    secondYearVal = []
    firstYear = []
    firstYearVal = []

    for x in range(length):
        dateSplit = dateList[x].split("/")
        year = dateSplit[2]
        day = dateSplit[1]
        month = dateSplit[0]
        if day == "29" and month == "2":
            continue
        elif year == sixthYearStr:
            sixthYear.append(dateList[x])
            sixthYearVal.append(float(valueList[x]))
        elif year == fifthYearStr:
            fifthYear.append(dateList[x])
            fifthYearVal.append(float(valueList[x]))
        elif year == fourthYearStr:
            fourthYear.append(dateList[x])
            fourthYearVal.append(float(valueList[x]))
        elif year == thirdYearStr:
            thirdYear.append(dateList[x])
            thirdYearVal.append(float(valueList[x]))
        elif year == lastYearStr:
            secondYear.append(dateList[x])
            secondYearVal.append(float(valueList[x]))
        elif year == currentYearStr:
            firstYear.append(dateList[x])
            firstYearVal.append(float(valueList[x]))

    return sixthYear, fifthYear, fourthYear, thirdYear, secondYear, firstYear, sixthYearVal, fifthYearVal, fourthYearVal, thirdYearVal, secondYearVal, firstYearVal


def getCimisHistoricalET(latitude, longitude, sDate, eDate):
    # initialize variables
    dateDict = {}
    dateList = []
    valueList = []

    jsonData = getCimisService(eDate, latitude, longitude, sDate)

    # Verify zip code
    dateSplit = sDate.split("-")
    startYear = int(dateSplit[0])
    startMonth = int(dateSplit[1])
    startDay = int(dateSplit[2])
    dateSplit = eDate.split("-")
    endYear = int(dateSplit[0])
    endMonth = int(dateSplit[1])
    endDay = int(dateSplit[2])

    # Get num of days between two dates
    f_date = date(startYear, startMonth, startDay)
    l_date = date(endYear, endMonth, endDay)
    delta = l_date - f_date
    numDays = delta.days

    # Save Date and Et into dictionary for the past 4 years and 9 months
    for x in range(0, numDays):
        dateCheck = jsonData["Data"]["Providers"][0]["Records"][x]["Date"]
        dateSplit = dateCheck.split("-")
        checkDay = dateSplit[2]
        checkMonth = dateSplit[1]
        checkYear = dateSplit[0]
        yesterday = endDay - 1

        yesterday = int(yesterday)
        checkDay = int(checkDay)
        checkMonth = int(checkMonth)
        checkYear = int(checkYear)

        # Saves Data if it falls within the start and end date
        if checkDay == endDay and checkMonth == endMonth and checkYear == endYear:
            dateL = jsonData["Data"]["Providers"][0]["Records"][x]["Date"]
            ET = jsonData["Data"]["Providers"][0]["Records"][x]["DayAsceEto"]["Value"]
            dateDict[dateL] = ET
            dateList.append(dateL)
            valueList.append(ET)
            break
        elif checkDay == yesterday and checkMonth == endMonth and checkYear == endYear:
            dateL = jsonData["Data"]["Providers"][0]["Records"][x]["Date"]
            ET = jsonData["Data"]["Providers"][0]["Records"][x]["DayAsceEto"]["Value"]
            dateDict[dateL] = ET
            dateList.append(dateL)
            valueList.append(ET)
            break
        else:
            dateL = jsonData["Data"]["Providers"][0]["Records"][x]["Date"]
            ET = jsonData["Data"]["Providers"][0]["Records"][x]["DayAsceEto"]["Value"]
            dateDict[dateL] = ET
            dateList.append(dateL)
            valueList.append(ET)

    # Return the dates and ET
    return dateDict, dateList, valueList


def getCimisService(eDate, latitude, longitude, sDate):
    print("Running Cimis API Call")
    cimisAPIKey = "dafc4a37-cdb3-4b58-a73f-d8990eef4321"

    # Start and End date for Cimis
    startDate = sDate
    endDate = eDate
    lat = latitude
    long = longitude
    dataItems = "day-asce-eto"

    # Set URL's for google and cimis api's
    url = "http://et.water.ca.gov/api/data?"

    zipCode = getGoogleZipCode(lat, long)

    # Set rest requests for cimis using zip code
    resp = requests.get(
        url + "appKey=" + cimisAPIKey + "&targets=" + zipCode + "&startDate=" + startDate + "&endDate=" + endDate
        + "&dataItems=" + dataItems)

    jsonResp = resp.json()
    # s1 = json.dumps(jsonResp)
    # jsonObj = json.loads(s1)
    # jsonPrint = {'headers': '', 'body': jsonObj}
    # pprint.pprint(jsonPrint)
    # print(jsonResp)
    print("End Cimis API Call")
    return jsonResp


def getGoogleZipCode(lat, long):
    print("Running Google API Call")
    googleAPIKey = "AIzaSyCAXS9uDgxEq4G3eTHwYa0tLGMhN3NhVPA"
    googleUrl = "https://maps.googleapis.com/maps/api/geocode/json?"
    # Set rest requests for google using lat and long
    try:
        gResp = requests.get(googleUrl + "latlng=" + lat + "," + long + "&key=" + googleAPIKey)
        gJson = gResp.json()
        # Save ZipCode of lat and long
        # Find Zip Code Index
        zipCodeIndex = 99999
        addressList = (gJson["results"][0]["address_components"])
        for index, x in enumerate(addressList, 0):
            if x["types"][0] == "postal_code":
                zipCodeIndex = index
        zipCode = gJson["results"][0]["address_components"][zipCodeIndex]["long_name"]

    except IndexError:
        print("No zipcode found")
        zipCode = lat + "," + long

    print("End Google API Call")
    return zipCode


def splitLatLong(targets):
    targetSplit = targets.split(",")
    lat = targetSplit[0]
    long = targetSplit[1]
    return lat, long


if __name__ == '__main__':
    main()
