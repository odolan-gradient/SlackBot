from datetime import date


class CropCoefficient(object):
    def __init__(self):
        None

    def get_kc(self, crop, specific_date, planting_date=None):
        return {
            'tomato': self.get_tomato_kc(specific_date, planting_date),
            'tomatoes': self.get_tomato_kc(specific_date, planting_date),
            'almond': self.get_almond_kc(specific_date),
            'almonds': self.get_almond_kc(specific_date),
            'pistachio': self.get_pistachio_kc(specific_date),
            'pistachios': self.get_pistachio_kc(specific_date),
            'pepper': self.get_pepper_kc(),
            'peppers': self.get_pepper_kc(),
            'date': self.get_date_kc(specific_date),
            'dates': self.get_date_kc(specific_date),
            'lemon': self.get_lemon_kc(),
            'lemons': self.get_lemon_kc(),
            'squash': self.get_squash_kc(specific_date, planting_date),
        }.get(crop, None)

    def get_tomato_kc(self, specific_date, planting_date):

        kc = None
        if planting_date is None:
            return None

        days = self.days_since_start_of_year(specific_date)

        march = 3 #date(specific_date.year, 3, 1)
        april = 4 #date(specific_date.year, 4, 1)
        may = 5 #date(specific_date.year, 5, 1)
        june = 6 #date(specific_date.year, 6, 1)
        july = 7 #date(specific_date.year, 7, 1)
        august = 8 #date(specific_date.year, 8, 1)

        if march <= planting_date.month < april:
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

        elif april <= planting_date.month < may:
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

        elif may <= planting_date.month < june:
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

        elif june <= planting_date.month < august:
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

        if kc is not None:
            adjustment_value = 0.228
            kc = kc + adjustment_value
            if kc > 1.1:
                kc = 1.1

        return kc

    def get_almond_kc(self, specific_date):
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
        return kc

    def get_pistachio_kc(self, specific_date):

        if 1 == specific_date.month and specific_date.day <= 15:
            kc = 0.0
        elif 1 == specific_date.month and specific_date.day > 15:
            kc = 0.0
        elif 2 == specific_date.month and specific_date.day <= 15:
            kc = 0.0
        elif 2 == specific_date.month and specific_date.day > 15:
            kc = 0.0
        elif 3 == specific_date.month and specific_date.day <= 15:
            kc = 0.0
        elif 3 == specific_date.month and specific_date.day > 15:
            kc = 0.0
        elif 4 == specific_date.month and specific_date.day <= 15:
            kc = 0.25
        elif 4 == specific_date.month and specific_date.day > 15:
            kc = 0.25
        elif 5 == specific_date.month and specific_date.day <= 15:
            kc = 0.71
        elif 5 == specific_date.month and specific_date.day > 15:
            kc = 0.71
        elif 6 == specific_date.month and specific_date.day <= 15:
            kc = 1.13
        elif 6 == specific_date.month and specific_date.day > 15:
            kc = 1.13
        elif 7 == specific_date.month and specific_date.day <= 15:
            kc = 1.19
        elif 7 == specific_date.month and specific_date.day > 15:
            kc = 1.19
        elif 8 == specific_date.month and specific_date.day <= 15:
            kc = 1.15
        elif 8 == specific_date.month and specific_date.day > 15:
            kc = 1.15
        elif 9 == specific_date.month and specific_date.day <= 15:
            kc = 0.95
        elif 9 == specific_date.month and specific_date.day > 15:
            kc = 0.95
        elif 10 == specific_date.month and specific_date.day <= 15:
            kc = 0.6
        elif 10 == specific_date.month and specific_date.day > 15:
            kc = 0.6
        elif 11 == specific_date.month and specific_date.day <= 15:
            kc = 0.0
        elif 11 == specific_date.month and specific_date.day > 15:
            kc = 0.0
        elif 12 == specific_date.month and specific_date.day <= 15:
            kc = 0.0
        elif 12 == specific_date.month and specific_date.day > 15:
            kc = 0.0
        else:
            kc = -999
        # print('KC found: ' + str(kc))
        # print()
        return kc

    def get_pepper_kc(self):
        # TEMPORARY FORCED KC-----
        return 0.25
        # ------------------------

    def get_date_kc(self, specific_date = None):
        if specific_date != None:
            today = specific_date
        else:
            today = date.today()

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
        return kc

    def get_lemon_kc(self):
        # TEMPORARY FORCED KC-----
        return 0.69
        # ------------------------

    def get_squash_kc(self, specific_date, planting_date):
        kc = None
        if planting_date is None:
            return None

        days_since_planted = self.days_since_planted(planting_date, specific_date)

        kc_initial = 0.5
        kc_mid = 1.0
        kc_late = 0.75

        if days_since_planted <= 55:
            kc = kc_initial
        elif days_since_planted <= 75:
            kc = kc_mid
        elif days_since_planted <= 90:
            kc = kc_late
        else:
            kc = kc_late

        return kc

    def days_since_start_of_year(self, check_date):
        data_year = check_date.year
        start_of_year = date(data_year, 1, 1)
        delta = check_date - start_of_year
        return delta.days + 1

    def days_since_planted(self, planting_date, check_date):
        delta = check_date - planting_date
        return delta.days + 1


