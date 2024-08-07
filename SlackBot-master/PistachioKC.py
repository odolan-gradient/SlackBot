class PistachioKC(object):
    def __init__(self):
        None

    def get_kc(self, specificDate):
        today = specificDate
        # today = newDate
        print('Looking for Pistachio kc...')
        # print(' Planting Date: ' + str(planting_date))
        # print(' Check Date: ' + str(check_date))
        # print(' Days since start of year: ' + str(days))

        if 1 == today.month and today.day <= 15:
            kc = 0.0
        elif 1 == today.month and today.day > 15:
            kc = 0.0
        elif 2 == today.month and today.day <= 15:
            kc = 0.0
        elif 2 == today.month and today.day > 15:
            kc = 0.0
        elif 3 == today.month and today.day <= 15:
            kc = 0.0
        elif 3 == today.month and today.day > 15:
            kc = 0.0
        elif 4 == today.month and today.day <= 15:
            kc = 0.25
        elif 4 == today.month and today.day > 15:
            kc = 0.25
        elif 5 == today.month and today.day <= 15:
            kc = 0.71
        elif 5 == today.month and today.day > 15:
            kc = 0.71
        elif 6 == today.month and today.day <= 15:
            kc = 1.13
        elif 6 == today.month and today.day > 15:
            kc = 1.13
        elif 7 == today.month and today.day <= 15:
            kc = 1.19
        elif 7 == today.month and today.day > 15:
            kc = 1.19
        elif 8 == today.month and today.day <= 15:
            kc = 1.15
        elif 8 == today.month and today.day > 15:
            kc = 1.15
        elif 9 == today.month and today.day <= 15:
            kc = 0.95
        elif 9 == today.month and today.day > 15:
            kc = 0.95
        elif 10 == today.month and today.day <= 15:
            kc = 0.6
        elif 10 == today.month and today.day > 15:
            kc = 0.6
        elif 11 == today.month and today.day <= 15:
            kc = 0.0
        elif 11 == today.month and today.day > 15:
            kc = 0.0
        elif 12 == today.month and today.day <= 15:
            kc = 0.0
        elif 12 == today.month and today.day > 15:
            kc = 0.0
        else:
            kc = -999
        # print('KC found: ' + str(kc))
        # print()
        return kc


#
# almondKC = AlmondKC()
# newDate = datetime.datetime(2021, 12, 22)
# kc = almondKC.get_kc()
# print(kc)
