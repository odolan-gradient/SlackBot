from datetime import date


class TomatoKC(object):
    def __init__(self):
        None

    def get_kc(self, planting_date, check_date):
        # print(planting_date)
        if check_date.year == date.today().year:
            days = self.days_since_start_of_year(check_date)
            march = date(date.today().year, 3, 1)
            april = date(date.today().year, 4, 1)
            may = date(date.today().year, 5, 1)
            june = date(date.today().year, 6, 1)
            july = date(date.today().year, 7, 1)
            august = date(date.today().year, 8, 1)

        else:
            days = self.days_since_start_of_year_historical(check_date)
            march = date(date.today().year - 1, 3, 1)
            april = date(date.today().year - 1, 4, 1)
            may = date(date.today().year - 1, 5, 1)
            june = date(date.today().year - 1, 6, 1)
            july = date(date.today().year - 1, 7, 1)
            august = date(date.today().year - 1, 8, 1)

        # march = date(date.today().year, 3, 1)
        # april = date(date.today().year, 4, 1)
        # may = date(date.today().year, 5, 1)
        # june = date(date.today().year, 6, 1)
        # july = date(date.today().year, 7, 1)

        # print()
        # print('Looking for kc...')
        # print(' Planting Date: ' + str(planting_date))
        # print(' Check Date: ' + str(check_date))
        # print(' Days since start of year: ' + str(days))

        if march <= planting_date < april:
            # print('  Planting date between March and April')
            if 0 <= days <= 70:
                kc = 0.15
            elif 70 < days <= 80:
                kc = 0.17
            elif 80 < days <= 90:
                kc = 0.17
            elif 90 < days <= 100:
                kc = 0.19
            elif 100 < days <= 110:
                kc = 0.2
            elif 110 < days <= 120:
                kc = 0.3
            elif 120 < days <= 130:
                kc = 0.4
            elif 130 < days <= 140:
                kc = 0.6
            elif 140 < days <= 150:
                kc = 0.9
            elif 150 < days <= 170:
                kc = 1.05
            elif 170 < days <= 180:
                kc = 1.08
            elif 180 < days:
                kc = 1.1

        elif april <= planting_date < may:
            # print('  Planting date between April and May')
            if 0 <= days <= 100:
                kc = 0.1
            elif 100 < days <= 110:
                kc = 0.15
            elif 110 < days <= 120:
                kc = 0.17
            elif 120 < days <= 130:
                kc = 0.2
            elif 130 < days <= 140:
                kc = 0.3
            elif 140 < days <= 150:
                kc = 0.6
            elif 150 < days <= 160:
                kc = 0.9
            elif 160 < days <= 170:
                kc = 1.05
            elif 170 < days <= 180:
                kc = 1.08
            elif 180 < days:
                kc = 1.1

        elif may <= planting_date < june:
            # print('  Planting date between May and June')
            if 0 <= days <= 120:
                kc = 0.1
            elif 120 < days <= 130:
                kc = 0.15
            elif 130 < days <= 140:
                kc = 0.17
            elif 140 < days <= 150:
                kc = 0.25
            elif 150 < days <= 160:
                kc = 0.3
            elif 160 < days <= 170:
                kc = 0.6
            elif 170 < days <= 180:
                kc = 0.8
            elif 180 < days <= 190:
                kc = 1.05
            elif 190 < days <= 200:
                kc = 1.07
            elif 200 < days:
                kc = 1.1

        elif june <= planting_date < august:
            # print('  Planting date between June and July')
            if 0 <= days <= 140:
                kc = 0.1
            elif 140 < days <= 150:
                kc = 0.15
            elif 150 < days <= 160:
                kc = 0.17
            elif 160 < days <= 170:
                kc = 0.25
            elif 170 < days <= 180:
                kc = 0.3
            elif 180 < days <= 190:
                kc = 0.6
            elif 190 < days <= 200:
                kc = 0.8
            elif 200 < days <= 210:
                kc = 1.05
            elif 210 < days <= 220:
                kc = 1.07
            elif 220 < days:
                kc = 1.1

        else:
            kc = -999
        # print('KC found: ' + str(kc))
        # print()
        return kc

    def days_since_start_of_year(self, check_date):
        current_year = date.today().year
        start_of_year = date(current_year, 1, 1)
        delta = check_date - start_of_year
        return delta.days + 1

    def days_since_start_of_year_historical(self, check_date):
        current_year = date.today().year - 1
        start_of_year = date(current_year, 1, 1)
        delta = check_date - start_of_year
        return delta.days + 1

#
# tomatokc = TomatoKC()
# # tomatokc.days_since_start_of_year(date.today())
# kc = tomatokc.get_kc(date(2020,3,5), date.today())
# print(kc)
