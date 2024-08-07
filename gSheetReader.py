import GSheetCredentialSevice


#
# def main():
# creds = None
# sheetID = "1AnLa_zJSAo4eI2QflzmcDBT1pPNGRCQwhaLB-gD8PDc"
# # rangeName = ["S-Field Info"]
# rangeName = ["Copy of Garlic I5"]
# print("Getting Service")
# # Get service using credentials
# service = GSheetCredentialSevice.getService(creds)
# sheetInfo = getServiceRead(rangeName, sheetID, service)
# print("Running Algorithm")
#
# fieldName = "OP-G-NE"
# # Returns row searched keyword
# # fieldInfoDictList, rowIndexes = getRowValues(fieldName, sheetInfo)
# # column = 'B'
# # rowIndex = rowIndexes[0] + 1
# # print(fieldInfoDictList["Date"])
# # print(column + str(rowIndex))

def get_service():
    # http = credentials.authorize(httplib2.Http())
    # discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
    #                 'version=v4')
    # service = discovery.build('sheets', 'v4', http=http,
    #                           discoveryServiceUrl=discoveryUrl)
    gsheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = GSheetCredentialSevice.getService()
    return service


def getRowValuesDict(searchTarget, sheetInfo, header):
    # Initialize variables
    rowIndexes = getRow(searchTarget, sheetInfo, header)
    fieldInfoDict = {}
    serviceJSON = sheetInfo["valueRanges"][0]["values"]

    # Get columns
    columns = serviceJSON[0]

    for i, header in enumerate(columns, 0):
        fieldInfoDict[header] = []
    # Loop through
    for i, header in enumerate(columns, 0):
        for r in rowIndexes:
            try:
                fieldInfoDict[header].append(serviceJSON[r][i])
            except IndexError:
                fieldInfoDict[header].append(None)
    return fieldInfoDict


def getRow(target, sheetInfo, header):
    targetRow = []
    serviceJSON = sheetInfo["valueRanges"][0]["values"]

    rowLength = len(serviceJSON)

    columnIndex = getColumnHeader(header, serviceJSON)

    for i in range(1, rowLength):
        try:
            if serviceJSON[i][columnIndex] == target:
                targetRow.append(i)
        except IndexError:
            continue
    return targetRow


def getServiceRead(rangeName, sheetID, service):
    spreadSheetID = sheetID
    newrange = "'" + rangeName + "'"
    result = service.spreadsheets().values().batchGet(
        spreadsheetId=spreadSheetID, ranges=newrange)
    # print(result)
    response = result.execute()
    return response


def getServiceWriteColumns(rangeName, valueList, sheetID, service):
    body = {
        "majorDimension": "Columns",
        "values": valueList
    }
    service.spreadsheets().values().update().execute()


def getServiceWriteRows(rangeName, valueList, sheetID, service):
    body = {
        "majorDimension": "Rows",
        "values": valueList
    }
    service.spreadsheets().values().update().execute()


def getServiceWriteFormula(sheetID, startRow, endRow, startColumn, endColumn, formula, tabID, service):
    body = {
        "requests": [
            {
                "repeatCell": {
                    "range": {
                        "sheetId": tabID,
                        "startRowIndex": startRow,
                        "endRowIndex": endRow,
                        "startColumnIndex": startColumn,
                        "endColumnIndex": endColumn
                    },
                    "cell": {
                        "userEnteredValue": {
                            "formulaValue": formula
                        }
                    },
                    "fields": "userEnteredValue"
                }
            }]}
    service.spreadsheets().batchUpdate(spreadsheetId=sheetID, body=body).execute()


def getServiceDeleteRows(sheetID, startRow, endRow, tabID, service):
    body = {
        "requests": [
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": tabID,
                        "dimension": "ROWS",
                        "startIndex": startRow,
                        "endIndex": endRow
                    }
                }
            },
        ],
    }
    service.spreadsheets().batchUpdate(spreadsheetId=sheetID, body=body).execute()


def getServiceTabID(rangeName, sheetID, service):
    requestGridData = service.spreadsheets().get(spreadsheetId=sheetID, ranges=rangeName).execute()
    return requestGridData["sheets"][0]["properties"]["sheetId"]


def getColumnHeader(header, serviceJSON):
    headerRowNames = []
    columnIndex = 0
    result = serviceJSON
    # result = serviceJSON["valueRanges"][0]["values"]

    columnLength = len(result[0])

    for i in range(0, columnLength):
        try:
            headerRowNames.append(result[0][i])
            cleaned_up_header = headerRowNames[i].rstrip()
            if cleaned_up_header == header:
                columnIndex = i
            elif headerRowNames[i] == "":
                continue
        except IndexError:
            continue

    return columnIndex

def write_target_cell(target_cell, value, sheetID, service):
    spreadsheet_id = sheetID
    # new_range = "'" + target_cell + "'"
    new_value = [[value]]
    request_body = {
        'values': new_value,
    }
    response = service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=target_cell,
        valueInputOption='USER_ENTERED',
        body=request_body
    )
    # print(result)
    response = response.execute()
    return response


# if __name__ == '__main__':
#     main()
