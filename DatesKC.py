from datetime import date

class DatesKC(object):
    def __init__(self):
        None

    def get_kc(self, specificDate = None):
        if specificDate != None:
            today = specificDate
        else:
            today = date.today()
        # print('Looking for Dates kc...')
        # print(' Planting Date: ' + str(planting_date))
        # print(' Check Date: ' + str(check_date))
        # print(' Days since start of year: ' + str(days))

        if 1 == today.month:
            kc = 0.65
        elif 2 == today.month:
            kc = 0.65
        elif 3 == today.month:
            kc = 0.65
        elif 4 == today.month:
            kc = 0.65
        elif 5 == today.month:
            kc = 0.65
        elif 6 == today.month:
            kc = 0.65
        elif 7 == today.month:
            kc = 0.65
        elif 8 == today.month:
            kc = 0.65
        elif 9 == today.month:
            kc = 0.65
        elif 10 == today.month:
            kc = 0.65
        elif 11 == today.month:
            kc = 0.6
        elif 12 == today.month:
            kc = 0.6
        else:
            kc = None
        # print('KC found: ' + str(kc))
        # print()
        return kc