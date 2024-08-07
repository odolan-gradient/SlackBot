import datetime


class IrrigationRecommendationExpert(object):
    """
    Class to model the irrigation system expert to start AI

    """

    def __init__(self):
        pass

    def make_recommendation(self, psi, field_capacity, wilting_point, vwc_1, vwc_2, vwc_3, crop='Tomatoes', date=None,
                            planting_date=None, harvest_date=None):
        """
        Call phases in order
        Phase 1 - PSI
        Phase 2 - VWC

        :return:
        """

        # Add weights to each phase? This would make it easier to adjust phases while testing

        if crop.lower() == "tomatoes" or crop.lower() == "tomato":
            crop_stage = self.get_crop_stage(date, harvest_date, planting_date)
            crop_stage_number, crop_stage_description = crop_stage

            recommendation = Recommendation(crop_stage=crop_stage)

            phase1_adjustment = self.phase1(psi, crop_stage=crop_stage_number)
            recommendation.add_rec_adjustment(phase1_adjustment)

            phase2_adjustment = self.phase2(field_capacity, wilting_point, vwc_1, vwc_2, vwc_3, crop_stage=crop_stage_number)
            recommendation.add_rec_adjustment(phase2_adjustment)

            return recommendation
        return None

    def get_crop_stage(self, date, harvest_date, planting_date):
        crop_stage = (None, None)
        if harvest_date is not None and planting_date is not None:
            delta = harvest_date - planting_date
            total_crop_days = delta.days

            one_fourth_chunk = total_crop_days // 4

            planting_plus_30 = planting_date + datetime.timedelta(days=30)
            harvest_minus_30 = harvest_date - datetime.timedelta(days=30)
            harvest_minus_14 = harvest_date - datetime.timedelta(days=14)

            stage_one_start = planting_date
            stage_one_end = planting_plus_30
            stage_two_start = planting_plus_30
            stage_two_end = harvest_minus_30
            stage_three_start = harvest_minus_30
            stage_three_end = harvest_minus_14
            stage_four_start = harvest_minus_14
            stage_four_end = harvest_date


            # print(f'Stage 1: {stage_one_start}  --  {stage_one_end}')
            # print(f'Stage 2: {stage_two_start}  --  {stage_two_end}')
            # print(f'Stage 3: {stage_three_start}  --  {stage_three_end}')
            # print(f'Stage 4: {stage_four_start}  --  {stage_four_end}')

            # if stage_one_start <= date <= stage_one_end:
            if stage_one_start <= date < stage_one_end:
                crop_stage = (1, '1: First 30 days of the season')
            elif stage_two_start <= date < stage_two_end:
                crop_stage = (2, '2: Majority of the growth season')
            elif stage_three_start <= date < stage_three_end:
                crop_stage = (3, '3: Last 30 Days before harvest')
            elif stage_four_start <= date <= stage_four_end:
                crop_stage = (4, '4: Last 14 Days before harvest')
            else:
                print('Error in get crop stage:')
                print(f'Trying to fit date: {date}')
                print(f'Planting date: {planting_date}')
                print(f'Harvest date: {harvest_date}')

            _, crop_stage_desc = crop_stage
            # print(f'Crop Date: {date}  ->  in Crop Stage: {crop_stage_desc}')
        return crop_stage

    def apply_recommendations(self, base, recommendation):
        maximum_daily_hours_possible = 12
        maximum_daily_hours_possible_clay = 10
        recommendations_applied = []
        recom = base
        for rec in recommendation.recommendation_info:
            if recommendation.recommendation_info[rec] is None:
                recommendations_applied.append(recom)
            else:
                recom = recom * recommendation.recommendation_info[rec]
                if recom > maximum_daily_hours_possible:
                    recom = maximum_daily_hours_possible
                recom = round(recom * 2) / 2
                recommendations_applied.append(recom)
        final = recommendations_applied[-1]
        return final, recommendations_applied

    def phase1(self, psi, crop_stage=''):
        """

        :param crop_stage:
        :return: Modification value for phase 3
        """
        if crop_stage is None or psi is None:
            return 1
        elif crop_stage == 1:
            return 1
        elif crop_stage == 2:
            if 0 <= psi < 0.2:
                return 0.5
            elif 0.2 <= psi < 1:
                return 1
            elif 1 <= psi < 1.5:
                return 1.25
            elif 1.5 <= psi < 2:
                return 1.5
            elif 2 <= psi < 2.2:
                return 1.5
            elif 2.2 <= psi < 2.5:
                return 0.90
            else:
                return 1
        elif crop_stage == 3:
            if 0 <= psi < 0.5:
                return 0
            elif 0.5 <= psi < 1:
                return 1
            elif 1 <= psi < 1.9:
                return 1
            elif 1.9 <= psi < 2:
                return 1.20
            elif 2 <= psi < 2.2:
                return 1.25
            elif 2.2 <= psi < 2.5:
                return 0
            else:
                return 1
        elif crop_stage == 4:
            return 0

    def phase2(self, field_capacity, wilting_point, vwc_1, vwc_2, vwc_3, crop_stage=''):
        """
        VWC

        :param crop_stage:
        :param field_capacity:
        :param wilting_point:
        :param vwc:
        :return: Modification value for phase 4
        """
        if field_capacity is None or wilting_point is None:
            return 1
        if vwc_1 is None and vwc_2 is None and vwc_3 is None:
            return 1

        soil_type = self.soil_type_lookup(field_capacity, wilting_point)
        if soil_type == 'Invalid':
            return 1

        soil_type_class = self.soil_type_class_lookup(soil_type)
        if soil_type_class == 99:
            return 1

        recommendation_adjustment = self.soil_class_adjustment(soil_type_class, vwc_1, vwc_2, vwc_3, crop_stage=crop_stage)
        return recommendation_adjustment

    def soil_type_class_lookup(self, soil_type):
        """
        Lookup soil type classification based on soil type

        :param soil_type: String
        :return: Int
        """
        choices = {
            'Sand':1,
            'Loamy Sand':1,
            'Sandy Loam':2,
            'Sandy Clay Loam':3,
            'Loam':3,
            'Silt Loam':4,
            'Silt':4,
            'Sandy Clay':5,
            'Clay Loam':5,
            'Silty Clay Loam':5,
            'Silty Clay':6,
            'Clay':6
        }
        return choices.get(soil_type, 99)

    def soil_type_lookup(self, field_capacity, wilting_point):
        """
        Lookup soil type based on field capacity and wilting point

        :param field_capacity: Int
        :param wilting_point: Int
        :return: String
        """
        if field_capacity == 10:
            return 'Sand'
        elif field_capacity == 12:
            return 'Loamy Sand'
        elif field_capacity == 18:
            return 'Sandy Loam'
        elif field_capacity == 27:
            return 'Sandy Clay Loam'
        elif field_capacity == 28:
            return 'Loam'
        elif field_capacity == 36:
            if wilting_point == 25:
                return 'Sandy Clay'
            elif wilting_point == 22:
                return 'Clay Loam'
        elif field_capacity == 31:
            return 'Silt Loam'
        elif field_capacity == 30:
            return 'Silt'
        elif field_capacity == 38:
            return 'Silty Clay Loam'
        elif field_capacity == 41:
            return 'Silty Clay'
        elif field_capacity == 42:
            return 'Clay'
        else:
            return 'Invalid'

    def soil_class_adjustment(self, soil_type_class, vwc_1, vwc_2, vwc_3, crop_stage=''):
        """
        Get adjustment % needed based on soil type class and vwc level

        :param soil_type_class: Int
        :param vwc: Float
        :return: Float
        """
        max = 10        #Recommended adjustment in case of worst case scenerio - 10*

        if crop_stage is None:
            return 1
        elif crop_stage == 1:
            vwc = self.check_vwc_data(vwc_1, vwc_2)
            adjustment = self.soil_class_adjustment_crop_stage_1(soil_type_class, vwc, max)
            return adjustment
        elif crop_stage == 2:
            vwc = self.check_vwc_data(vwc_2, vwc_3)
            adjustment = self.soil_class_adjustment_crop_stage_2(soil_type_class, vwc, max)
            return adjustment
        elif crop_stage == 3:
            vwc = self.check_vwc_data(vwc_2, vwc_3)
            adjustment = self.soil_class_adjustment_crop_stage_3(soil_type_class, vwc, max)
            return adjustment
        elif crop_stage == 4:
            adjustment = 0
            return adjustment


    def check_vwc_data(self, vwc_1, vwc_2):
        if vwc_1 is None and vwc_2 is None:
            vwc = None
        if vwc_1 is None:
            vwc = vwc_2
        elif vwc_2 is None:
            vwc = vwc_1
        else:
            vwc = (vwc_1 + vwc_2) / 2
        return vwc

    def soil_class_adjustment_crop_stage_1(self, soil_type_class, vwc, max):
        if soil_type_class == 1:
            if vwc < 5:
                return max         # < 10 gives MAX?
            elif vwc < 10:
                return 1.25
            elif vwc < 12:
                return 1
            elif vwc < 16:
                return 0.95
            elif vwc < 25:
                return 0.9
            elif vwc > 25:
                return 0
        if soil_type_class == 2:
            if vwc < 13:
                return max
            elif vwc < 16:
                return 1.5
            elif vwc < 20:
                return 1
            elif vwc < 25:
                return 0.9
            elif vwc > 25:
                return 0
        if soil_type_class == 3:
            if vwc < 23:
                return max
            elif vwc < 26:
                return 1.5
            elif vwc < 30:
                return 1
            elif vwc < 35:
                return 0.75
            elif vwc > 35:
                return 0
        if soil_type_class == 4:
            if vwc < 15:
                return max
            elif vwc < 20:
                return 1.75
            elif vwc < 25:
                return 1.5
            elif vwc < 30:
                return 1.25
            elif vwc < 35:
                return 1
            elif vwc < 40:
                return 0.75
            elif vwc > 40:
                return 0
        if soil_type_class == 5:
            if vwc < 25:
                return max
            elif vwc < 30:
                return 1.75
            elif vwc < 35:
                return 1.5
            elif vwc < 38:
                return 1.25
            elif vwc < 42:
                return 1
            elif vwc > 42:
                return 0
        if soil_type_class == 6:
            if vwc < 25:
                return max
            elif vwc < 30:
                return 1.75
            elif vwc < 35:
                return 1.50
            elif vwc < 38:
                return 1.25
            elif vwc < 45:
                return 1
            elif vwc > 45:
                return 0

    def soil_class_adjustment_crop_stage_2(self, soil_type_class, vwc, max):
        if soil_type_class == 1:
            if 0 < vwc <= 6:
                return max
            elif 6 < vwc <= 10:
                return 1.75
            elif 10 < vwc <= 12:
                return 1
            elif 12 < vwc <= 16:
                return 0.90
            elif 16 < vwc <= 25:
                return 0.85
            elif 25 < vwc:
                return 0
        if soil_type_class == 2:
            if 0 < vwc <= 10:
                return max
            elif 10 < vwc <= 13:
                return 1.75
            elif 13 < vwc <= 16:
                return 1.5
            elif 16 < vwc <= 20:
                return 1
            elif 20 < vwc <= 25:
                return 0.9
            elif 25 < vwc:
                return 0
        if soil_type_class == 3:
            if 0 < vwc <= 20:
                return max
            elif 20 < vwc <= 23:
                return 1.75
            elif 23 < vwc <= 26:
                return 1.5
            elif 26 < vwc <= 30:
                return 1
            elif 30 < vwc <= 35:
                return 0.9
            elif 35 < vwc:
                return 0
        if soil_type_class == 4:
            if 0 < vwc <= 15:
                return max
            elif 15 < vwc <= 20:
                return 1.75
            elif 20 < vwc <= 30:
                return 1.5
            elif 30 < vwc <= 35:
                return 1
            elif 35 < vwc <= 40:
                return 0.9
            elif 40 < vwc:
                return 0
        if soil_type_class == 5:    #Be more aggressive on cutting water with heavier soil types
            if 0 < vwc <= 20:
                return max
            elif 20 < vwc <= 25:
                return 1.75
            elif 25 < vwc <= 30:
                return 1.5
            elif 30 < vwc <= 35:
                return 1.25
            elif 35 < vwc <= 38:
                return 1
            elif 38 < vwc <= 42:
                return 0.85
            elif 42 < vwc:
                return 0
        if soil_type_class == 6:
            if 0 < vwc <= 25:
                return max
            elif 25 < vwc <= 35:
                return 1.75
            elif 35 < vwc <= 38:
                return 1.25
            elif 38 < vwc <= 44:
                return 1
            elif 44 < vwc:
                return 0

    def soil_class_adjustment_crop_stage_3(self, soil_type_class, vwc, max):
        if soil_type_class == 1:
            if 0 < vwc <= 3:
                return 1.25
            elif 3 < vwc <= 6:
                return 1.2
            elif 6 < vwc <= 10:
                return 1
            elif 10 < vwc <= 12:
                return 0.8
            elif 12 < vwc <= 16:
                return 0.75
            elif vwc > 16:
                return 0
        if soil_type_class == 2:
            if 0 < vwc <= 6:
                return 1.25
            elif 6 < vwc <= 10:
                return 1.2
            elif 10 < vwc <= 13:
                return 1
            elif 13 < vwc <= 16:
                return 0.8
            elif 16 < vwc <= 20:
                return 0.75
            elif vwc > 20:
                return 0
        if soil_type_class == 3:
            if 0 < vwc <= 15:
                return 1.2
            elif 15 < vwc <= 20:
                return 1.15
            elif 20 < vwc <= 23:
                return 1
            elif 23 < vwc <= 26:
                return 0.85
            elif 26 < vwc <= 30:
                return 0.8
            elif vwc > 30:
                return 0
        if soil_type_class == 4:
            if 0 < vwc <= 10:
                return 1.20
            elif 10 < vwc <= 15:
                return 1.15
            elif 15 < vwc <= 20:
                return 1
            elif 20 < vwc <= 30:
                return 0.85
            elif 30 < vwc <= 35:
                return 0.80
            elif vwc > 35:
                return 0
        if soil_type_class == 5:
            if 0 < vwc <= 15:
                return 1.15
            elif 15 < vwc <= 25:
                return 1
            elif 25 < vwc <= 30:
                return 0.9
            elif 30 < vwc <= 35:
                return 0.8
            elif 35 < vwc <= 38:
                return 0.7
            elif vwc > 38:
                return 0
        if soil_type_class == 6:
            if 0 < vwc <= 30:
                return 1
            elif 30 < vwc <= 35:
                return 0.9
            elif 35 < vwc <= 38:
                return 0.8
            elif vwc > 38:
                return 0

class Recommendation(object):
    def __init__(self, crop_stage=None):
        """

        """
        self.recommendation_info = {}
        self.num_of_adjustments = 0
        self.crop_stage = crop_stage

    def add_rec_adjustment(self, adjustment):
        """
        Add a recommendation adjustment to the total recommendations dictionary

        :param adjustment:
        :return:
        """
        self.recommendation_info[self.num_of_adjustments] = adjustment
        self.num_of_adjustments += 1