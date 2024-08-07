class PepperKC(object):
    def __init__(self):
        None

    def get_kc(self, fieldName, specificDate):
        # TEMPORARY FORCED KC-----
        return 0.25
        #------------------------


        # today = specificDate
        # # today = newDate
        # print('Looking for Pepper kc...')
        # # print(' Planting Date: ' + str(planting_date))
        # # print(' Check Date: ' + str(check_date))
        # # print(' Days since start of year: ' + str(days))
        #
        # sheetID = '1kVfUBRWkN5mPNcknz9Onyrwp5IeePjTF_uBFS2gsPV0'
        #
        # rangeName = "FS"
        #
        # gSheet = GSheetCredentialSevice.GSheetCredentialSevice()
        #
        # service = gSheet.getService()
        #
        # result = gSheetReader.getServiceRead(rangeName, sheetID, service)
        # rowResult = result['valueRanges'][0]['values']
        # kc = 0.25
        #
        # for row, x in enumerate(rowResult):
        #     # Loop through fields
        #     # print(x)
        #     if row == 0:
        #         # find field to determine KC using date
        #         for j in x:
        #             # print(j)
        #             if j == fieldName:
        #                 fieldNameHeader = gSheetReader.getColumnHeader(fieldName, rowResult)
        #                 # print(fieldNameHeader)
        #     else:
        #         date = x[fieldNameHeader]
        #         kc = x[fieldNameHeader + 1]
        #         if date == str(today):
        #             print("Date: " + date)
        #             print("KC: " + kc)
        #
        # return float(kc)


#
# almondKC = AlmondKC()
# newDate = datetime.datetime(2021, 12, 22)
# kc = almondKC.get_kc()
# print(kc)
