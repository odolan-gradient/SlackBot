from __future__ import print_function

from GSheetCredentialSevice import GSheetCredentialSevice


####################################################################
# Base Class for writing Decagon sensor data to Gsheets            #
# Expand this class for changes to what is written                 #
####################################################################
class GSheetWriter(object):
    gSheetCredentialService = GSheetCredentialSevice()

    def __init__(self):

        self.gSheetCredentialService = GSheetCredentialSevice()

    def get_service(self):
        # http = credentials.authorize(httplib2.Http())
        # discoveryUrl = ('https://sheets.googleapis.com/$discovery/rest?'
        #                 'version=v4')
        # service = discovery.build('sheets', 'v4', http=http,
        #                           discoveryServiceUrl=discoveryUrl)
        service = self.gSheetCredentialService.getService()
        return service

    def create_new_spreadsheet(self, service, name, type):
        if type == 0:
            data = {'properties': {'title': name + ' t0'}}
        else:
            data = {'properties': {'title': name + ' t' + str(type)}}
        res = service.spreadsheets().create(body=data).execute()
        SHEET_ID = res['spreadsheetId']
        return SHEET_ID

    def create_new_sheet(self, service, spreadsheet, name, position):
        requests = [{
            'addSheet': {
                "properties": {
                    "title": name
                },
            }
        }]
        # Add sheet titled "Hi"

        self.update_sheet(service, spreadsheet, requests)

        return None

    def update_sheet(self, service, sheet, range, info):
        body = {
            'values': info
        }
        service.spreadsheets().values().update(spreadsheetId=sheet,
                                               range=range, body=body, valueInputOption='RAW').execute()
        # response = service.spreadsheets().batchUpdate(spreadsheetId=sheet, body=body).execute()
        return None

    def append_to_sheet(self, service, sheet, range, data):
        # TODO Try batch for writing  because  its preferred according to Jesus
        result = service.spreadsheets().values().append(spreadsheetId=sheet,
                                                        range=range, body=data,
                                                        valueInputOption='USER_ENTERED').execute()

        return result

    def read_sheet(self, service, sheet, range):
        result = service.spreadsheets().values().get(
            spreadsheetId=sheet, range=range).execute()
        return result

    def delete_last_row(self, service, sheetID, row_number, spreadsheetID):
        requests = [{
            'deleteDimension': {
                'range': {
                    'sheetId': sheetID,
                    'dimension': "ROWS",
                    'startIndex': row_number - 1,
                    'endIndex': row_number
                }
            }
        }]

        body = {
            'requests': requests
        }
        # print(body)
        response = service.spreadsheets().batchUpdate(spreadsheetId=spreadsheetID,
                                                      body=body).execute()

        return None

    def updateByFind(self, search, replace, spreadSheetID, service):
        requests = [{
            'findReplace': {
                'find': search,
                'replacement': replace,
                'allSheets': True
            }
        }]

        body = {
            'requests': requests
        }
        response = service.spreadsheets().batchUpdate(
            spreadsheetId=spreadSheetID,
            body=body).execute()
        find_replace_response = response.get('replies')[1].get('findReplace')
        print('{0} replacements made.'.format(
            find_replace_response.get('occurrencesChanged')))

    def writeToCell(self, service, spreadsheetID, rangeName, newCell):
        values = [
            [
                newCell
            ],
        ]
        body = {
            'values': values
        }
        result = service.spreadsheets().values().update().execute()
        # print('{0} cells updated.'.format(result.get('updatedCells')))

    def clearSheet(self, service, sheetID, rangeName):
        clear_values_request_body = {
        }
        request = service.spreadsheets().values().clear(spreadsheetId=sheetID, range=rangeName,
                                                        body=clear_values_request_body)
        response = request.execute()

    def deleteRangeRows(self, service, sheetID, tabID, startRow, endRow):
        # response = service.spreadsheets().get(spreadsheetId=sheetID, ranges= tabName)
        # sheet = response.execute()
        # print(sheet)
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
                }
            ],
        }
        response = service.spreadsheets().batchUpdate(spreadsheetId=sheetID, body=body).execute()
