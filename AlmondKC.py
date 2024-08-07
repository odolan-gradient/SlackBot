class AlmondKC(object):
    def __init__(self):
        None

    def get_kc(self, specific_date):

        if 1 == specific_date.month and specific_date.day <= 15:
            kc = 0.40
        elif 1 == specific_date.month and specific_date.day > 15:
            kc = 0.40
        elif 2 == specific_date.month and specific_date.day <= 15:
            kc = 0.41
        elif 2 == specific_date.month and specific_date.day > 15:
            kc = 0.41
        elif 3 == specific_date.month and specific_date.day <= 15:
            kc = 0.62
        elif 3 == specific_date.month and specific_date.day > 15:
            kc = 0.62
        elif 4 == specific_date.month and specific_date.day <= 15:
            kc = 0.80
        elif 4 == specific_date.month and specific_date.day > 15:
            kc = 0.80
        elif 5 == specific_date.month and specific_date.day <= 15:
            kc = 0.94
        elif 5 == specific_date.month and specific_date.day > 15:
            kc = 0.94
        elif 6 == specific_date.month and specific_date.day <= 15:
            kc = 1.05
        elif 6 == specific_date.month and specific_date.day > 15:
            kc = 1.05
        elif 7 == specific_date.month and specific_date.day <= 15:
            kc = 1.11
        elif 7 == specific_date.month and specific_date.day > 15:
            kc = 1.11
        elif 8 == specific_date.month and specific_date.day <= 15:
            kc = 1.11
        elif 8 == specific_date.month and specific_date.day > 15:
            kc = 1.11
        elif 9 == specific_date.month and specific_date.day <= 15:
            kc = 1.06
        elif 9 == specific_date.month and specific_date.day > 15:
            kc = 1.06
        elif 10 == specific_date.month and specific_date.day <= 15:
            kc = 0.92
        elif 10 == specific_date.month and specific_date.day > 15:
            kc = 0.92
        elif 11 == specific_date.month and specific_date.day <= 15:
            kc = 0.69
        elif 11 == specific_date.month and specific_date.day > 15:
            kc = 0.69
        elif 12 == specific_date.month and specific_date.day <= 15:
            kc = 0.43
        elif 12 == specific_date.month and specific_date.day > 15:
            kc = 0.43
        else:
            kc = -999
        # print('KC found: ' + str(kc))
        # print()
        return kc