import GSheetCredentialSevice
import cimisHistoricalET
import gSheetReader


def main():
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    creds = None
    # ID for sheet 1-50
    sheetID = "1sEssCBZA6I2pnWCFGssokbf5zilvzxUxosZLIO-fS5U"
    # ID for sheet 51-100
    # sheetID = "1bBDQVSQdmP9LzEjwnFvKsyhCUjBVD65diE0rOhFX4R0"
    # ID for sheet 101-150
    # sheetID = "1AaQOEjFkXFty8IkhRHwU-CEU5TMNzwlpeYr5Qjl04bY"
    # ID for sheet 151-200
    # sheetID = "1nO5pV2YetXM1vaVCILcwLjgnT-TkKqMpAlqyOMoHHco"
    # ID for sheet 201-250
    # sheetID = "1JvL1cqfp05auQ1Gb-rnwXlGp9OCSuCMCB0l6nwnwECU"
    # ID for sheet 251-262
    # sheetID = "1-_8c561IkWmZiQ2fzcogt_GhG8YdtsrCoZhBgl6pqwA"

    inputList = input("Enter Cimis Station #")
    rangeName = inputList

    print("Getting Cred")
    # Authenticate and get credentials
    gSheet = GSheetCredentialSevice.GSheetCredentialSevice()

    service = gSheet.getService()

    tabID = gSheetReader.getServiceTabID(rangeName, sheetID, service)

    # Delete Header Rows Before Processing information
    startRowIndex = 0
    endRowIndex = 1
    gSheetReader.getServiceDeleteRows(sheetID, startRowIndex, endRowIndex, tabID, service)

    print("Getting Service")
    # Get service using credentials
    etRead = gSheetReader.getServiceRead(rangeName, sheetID, service)
    print("Running Algorithm")

    sixthYearStr = "2016"
    fifthYearStr = "2017"
    fourthYearStr = "2018"
    thirdYearStr = "2019"
    lastYearStr = "2020"
    currentYearStr = "2021"
    dateList = []
    valueList = []

    # Save the Dates and Value information from the sheet
    serviceJSON = etRead["valueRanges"][0]["values"]

    # For each row save the dates and values it's own list
    for i in serviceJSON:
        dateList.append(i[0])
        valueList.append(i[1])

    # Split the dates and values into it's corresponding year
    sixYear, fiveYear, fourYear, threeYear, \
    lastYear, currentYear, sixYearVal, \
    fiveYearVal, fourYearVal, thirdYearVal, \
    lastYearVal, currentYearVal = cimisHistoricalET.splitYearBackslash(dateList, valueList,
                                                                       sixthYearStr,
                                                                       fifthYearStr, fourthYearStr,
                                                                       thirdYearStr,
                                                                       lastYearStr, currentYearStr)

    # Write to sheet
    print("Writing to Sheet")
    stationName = inputList
    # Writes the Headers for the historical ET sheet
    cimisHistoricalET.writeHistoricalETHeaders(creds, currentYearStr, fifthYearStr, fourthYearStr, lastYearStr, stationName,
                                               sheetID,
                                               thirdYearStr, service)
    # Writes the data of the historical ET sheet
    cimisHistoricalET.writeHistoricalETData(creds, currentYear, currentYearVal, stationName, fiveYear, fiveYearVal, fourYear,
                                            fourYearVal,
                                            lastYear, lastYearVal, sheetID, thirdYearVal, threeYear, service)

    # Write Average of 5 years to sheet
    startRowIndex = 1
    endRowIndex = 366
    startColumnIndex = 8
    endColumnIndex = 9
    formula = "=AVERAGE(B2,D2,F2,H2)"
    gSheetReader.getServiceWriteFormula(sheetID, startRowIndex, endRowIndex, startColumnIndex,
                                        endColumnIndex, formula, tabID, service)
    # Delete Excess Rows
    startRowIndex = 366
    endRowIndex = 1540
    gSheetReader.getServiceDeleteRows(sheetID, startRowIndex, endRowIndex, tabID, service)
    print("Formatting Finished")


if __name__ == '__main__':
    main()
