import csv
import uuid
from datetime import datetime
from datetime import timedelta
from itertools import zip_longest

import numpy as numpy

from DBWriter import DBWriter
from IrrigationRecommendationExpert import IrrigationRecommendationExpert
from Soils import Soil


class CwsiProcessor(object):
    def __init__(self):
        pass

    def convert_farenheit_list_to_celsius_list(self, fahrenheit: list[float]) -> list[float] or None:
        """
        Function to convert Fahrenheit to Celsius
        :param fahrenheit: Fahrenheit in float
        :return: celsius in float
        """
        celsius_list = []
        for fahrenheit_data_point in fahrenheit:
            if fahrenheit_data_point is None:
                celsius_list.append(None)
            else:
                c = (fahrenheit_data_point - 32) * 5 / 9
                celsius_list.append(c)
        return celsius_list

    def get_highest_and_lowest_temperature_indexes(self, all_results_converted, mute_prints = False):
        """
        Function to loop through the converted results and return the indexes corresponding to
        the hottest air temperature each day. If there are multiple hottest air temperatures with the same
        value, check their corresponding vpd's and pick the one with the higher vpd

        :param all_results_converted:
        :return:
        """
        i = 0

        day_data_points = 0  # New variable needed to check how many data points are in each day before checking for the hottest time
        # If there are not enough data points, it will ignore that day
        data_points_required_for_full_day = 10  # Data points required for processing. 10 by default
        highest_temp_values_indexes = []
        highest_temp_values = []
        lowest_temp_values_indexes = []
        lowest_temp_values = []
        day_break_indexes = [0]
        length_of_list = len(all_results_converted['dates'])

        while i < length_of_list:
            # Loop through list of ambient temps from start until we find value that isn't None
            # This was needed because occasionally the first value of the ambient temp list is None
            if all_results_converted['ambient temperature'][i] is None:
                highest_temp_value = 0
                lowest_temp_value = 999
            else:
                highest_temp_value = all_results_converted['ambient temperature'][i]
                lowest_temp_value = all_results_converted['ambient temperature'][i]

            highest_temp_index = lowest_temp_index = i

            if i == length_of_list - 1:
                if day_data_points > data_points_required_for_full_day:  # There are at least 10 data points in that day
                    if not mute_prints:
                        print('Data points: ' + str(day_data_points))
                    highest_temp_index = i
                    lowest_temp_index = i
                    highest_temp_values_indexes.append(int(highest_temp_index))
                    lowest_temp_values_indexes.append(int(lowest_temp_index))
                    if all_results_converted['ambient temperature'][highest_temp_index] is not None:
                        highest_temp_values.append(
                            round(all_results_converted['ambient temperature'][highest_temp_index], 1)
                        )
                    else:
                        highest_temp_values.append(None)
                    if all_results_converted['ambient temperature'][lowest_temp_index] is not None:
                        lowest_temp_values.append(
                            round(all_results_converted['ambient temperature'][lowest_temp_index], 1)
                        )
                    else:
                        lowest_temp_values.append(None)
                else:
                    if not mute_prints:
                        print('\t\tMRID ISSUES - Only ' + str(day_data_points) + ' data points')
                highest_temp_value = all_results_converted['ambient temperature'][i]

                break
            while all_results_converted['dates'][i].day == \
                    all_results_converted['dates'][i + 1].day:
                day_data_points = day_data_points + 1
                next_ambient_temp = all_results_converted['ambient temperature'][i + 1]
                if next_ambient_temp is not None:
                    if next_ambient_temp > highest_temp_value:
                        highest_temp_value = next_ambient_temp
                        highest_temp_index = i + 1

                    if next_ambient_temp < lowest_temp_value:
                        lowest_temp_value = next_ambient_temp
                        lowest_temp_index = i + 1

                i += 1
                if i == length_of_list - 1:
                    break
            i += 1
            if i < length_of_list - 1:
                # print(f'New day: {all_results_converted["dates"][i]}')
                day_break_indexes.append(i)
            if day_data_points > data_points_required_for_full_day:  # There are at least 10 data points in that day
                if not mute_prints:
                    print('\t\tData points: ' + str(day_data_points))
                highest_temp_values_indexes.append(int(highest_temp_index))
                lowest_temp_values_indexes.append(int(lowest_temp_index))
                if all_results_converted['ambient temperature'][highest_temp_index] is not None:
                    highest_temp_values.append(
                        round(all_results_converted['ambient temperature'][highest_temp_index], 1)
                    )
                else:
                    highest_temp_values.append(None)
                if all_results_converted['ambient temperature'][lowest_temp_index] is not None:
                    lowest_temp_values.append(
                        round(all_results_converted['ambient temperature'][lowest_temp_index], 1))
                else:
                    lowest_temp_values.append(None)
            else:
                if not mute_prints:
                    print('\t\tMRID ISSUES - Only ' + str(day_data_points) + ' data points')
            day_data_points = 0
        # print(f'New day: {all_results_converted["dates"][len(all_results_converted["dates"]) - 1]}')
        day_break_indexes.append(len(all_results_converted["dates"]) - 1)
        if not mute_prints:
            print()
            print('\t\tResults from Hottest / Coldest Temps Alg')
            print(f'\t\t\tHighest Temp Val Ind: {highest_temp_values_indexes}')
            print(f'\t\t\tHighest Temp Val: {highest_temp_values}')
            print(f'\t\t\tLowest Temp Val Ind: {lowest_temp_values_indexes}')
            print(f'\t\t\tLowest Temp Val: {lowest_temp_values}')
            print()
        return highest_temp_values_indexes, lowest_temp_values_indexes, day_break_indexes

    def get_gallons(self, all_results_converted, prev_gallons):
        i = 0
        daily_gallons = []
        previous_gallons = prev_gallons
        day_gallons = 0
        length_of_list = len(all_results_converted['dates'])

        day_break_points = [0]
        j = 0
        while i < length_of_list - 1:
            if all_results_converted['dates'][i].day == \
                    all_results_converted['dates'][i + 1].day:
                i += 1
            else:
                day_break_points.append(i + 1)
                i += 1
        if i != day_break_points[len(day_break_points) - 1]:
            day_break_points.append(i)
        while j < len(day_break_points) - 1:
            my_list = all_results_converted['daily gallons'][day_break_points[j]:day_break_points[j + 1]]
            day_gallons = sum(my_list)
            daily_gallons.append(day_gallons)
            j += 1

        return daily_gallons, previous_gallons


    def update_irrigation_ledger(self, all_results_converted, irrigation_ledger):
        # Todo -support measurement configurations other than 1 Hour. Currently, if the measurement configuration is
        #  not 1 Hour, the values for the switch come totaled for w/e the measurement configuration is. Meaning, if
        #  the measurement configuration is 15 minutes, a switch that is on would show 15 minutes for each 15 minute
        #  interval.
        dates_list = all_results_converted['dates']
        switch_minutes_list = all_results_converted['daily switch']

        for date, switch_minutes in zip(dates_list, switch_minutes_list):
            date_key = date.date()

            if date_key not in irrigation_ledger:
                irrigation_ledger[date_key] = [None] * 24

            hour_index = date.hour

            if irrigation_ledger[date_key][hour_index] is None:
                irrigation_ledger[date_key][hour_index] = switch_minutes


    def update_irrigation_ledger_2(self, all_results_converted, irrigation_ledger):
        # Todo -support measurement configurations other than 1 Hour. Currently, if the measurement configuration is
        #  not 1 Hour, the values for the switch come totaled for w/e the measurement configuration is. Meaning, if
        #  the measurement configuration is 15 minutes, a switch that is on would show 15 minutes for each 15 minute
        #  interval.
        dates_list = all_results_converted['dates']
        switch_minutes_list = all_results_converted['daily switch']

        for date, switch_minutes in zip(dates_list, switch_minutes_list):
            date_key = date.date()

            if date_key not in irrigation_ledger:
                irrigation_ledger[date_key] = [None] * 96

            hour_index = date.hour
            minute_index = 0
            if date.minute == 15:
                minute_index = 1
            elif date.minute == 30:
                minute_index = 2
            elif date.minute == 45:
                minute_index = 3

            data_index = hour_index * 4 + minute_index

            if irrigation_ledger[date_key][data_index] is None:
                irrigation_ledger[date_key][data_index] = switch_minutes
        print()


    def update_irrigation_ledger_3(self, all_results_converted, irrigation_ledger):
        """
        Function to handle irrigation ledger regardless of measurement configuration window.
        It does that by using the lowest measurement window option to set up the ledger and fits the data to that ledger
        regardless of actual config. Since it's using the lowest option, this should accommodate any config with no
        problem.
        This function sets up the ledger to have 288 buckets for each day, 1 bucket for every 5 minutes, which is the
        lowest measurement configuration available from Meter. This ledger then gets filled with data regardless of
        the config window size

        :param all_results_converted:
        :param irrigation_ledger:
        """
        dates_list = all_results_converted['dates']
        switch_minutes_list = all_results_converted['daily switch']

        # This is the lowest we can set the data interval in Zentra.
        # As of 7-29, Zentra supports 5min, 10min, 15min, 20min, 30min
        # 60min, 90min, 120min, 240min

        lowest_data_interval_option = 5        # Cannot be 0, used for division as the denominator

        # Caution dividing by lowest_data_interval_option if it doesn't lead to a float that can be easily cast
        # to int because we use that value to set the size of a list
        num_of_cubbies_per_date = int(round(24 * (60 / lowest_data_interval_option), 0))

        for date, switch_minutes in zip(dates_list, switch_minutes_list):
            date_key = date.date()

            if date_key not in irrigation_ledger:
                irrigation_ledger[date_key] = [None] * num_of_cubbies_per_date

            hour_index = date.hour
            minute_index_adjustment = int(date.minute / lowest_data_interval_option)

            data_index = int(hour_index * (60 / lowest_data_interval_option) + minute_index_adjustment)

            if irrigation_ledger[date_key][data_index] is None:
                irrigation_ledger[date_key][data_index] = switch_minutes

        # Expected data points per hour
        # data_points_per_hour = []
        # for date in dates_list:
        #     no_none_switch_values = [val for val in irrigation_ledger[date.date()] if val is not None]
        #     if len(no_none_switch_values) not in data_points_per_hour:
        #         data_points_per_hour.append(len(no_none_switch_values))
        #
        # print()


    def clean_irrigation_ledger(self, irrigation_ledger):
        """
        Function to remove old dates from the irrigation ledger that are no longer relevant and may have been left
        behind from previous manual or automatic runs
        :param irrigation_ledger:
        """
        # Calculate the cutoff date
        cutoff_date = datetime.now().date() - timedelta(days=15)

        # Grab keys older than cutoff date to be removed
        dates_to_remove = [date for date in irrigation_ledger if date < cutoff_date]

        for key in dates_to_remove:
            del irrigation_ledger[key]



    def get_switch(self, all_results_converted, prev_switch):
        i = 0
        daily_switch = []
        daily_switch2 = []
        previous_switch = prev_switch
        day_switch = 0
        length_of_list = len(all_results_converted['dates'])
        print('\tProcessing Switch')
        print(f'\t\tPrevious switch: {str(prev_switch)}')

        day_data_points = 0
        left_over_switch = prev_switch
        # day_break_points = [0]
        # j = 0
        water = 0
        # These following lines are causing us to double count the irrigation hours that overlaps in
        #   the API call returns
        # if all_results_converted['daily switch'][0]:
        #     water = all_results_converted['daily switch'][0]

        for index, switchData in enumerate(all_results_converted['daily switch']):
            if index == length_of_list - 1:
                # Only append value if the last value's day is different from the previous
                # Fixes issue on initialization
                daily_switch.append(water)
                # if all_results_converted['dates'][index].day != all_results_converted['dates'][index - 1].day:
                #     daily_switch.append(water)
                break

            today = all_results_converted['dates'][index].day
            next_day = all_results_converted['dates'][index + 1].day
            switch_today_data = all_results_converted['daily switch'][index]
            next_day_switch_data = all_results_converted['daily switch'][index + 1]

            if today == next_day:
                day_data_points = day_data_points + 1
                if next_day_switch_data is None or next_day_switch_data == 'None':
                    water = water + 0
                else:
                    water = water + next_day_switch_data
            else:
                if next_day_switch_data is None or next_day_switch_data == 'None':
                    water = 0
                else:
                    daily_switch.append(water)
                    water = next_day_switch_data
                    day_data_points = 0

        if not len(daily_switch) == 0 and left_over_switch > 0:
            print('\t\tModifying daily switch[0] to compensate overnight irrigation from previous night')
            print('\t\tfrom: ' + str(daily_switch[0]))
            print(
                '\t\tto: ' + str(daily_switch[0]) + ' + ' + str(left_over_switch) + ' = ' + str(
                    daily_switch[0] + left_over_switch
                )
            )
            # should the index here be -2 instead of 0?
            daily_switch[0] = daily_switch[0] + left_over_switch
            previous_switch = 0

        if len(daily_switch) > 1 and day_data_points < 10:
            print('\t\tAssigning previous switch to: ' + str(daily_switch[-1]))
            previous_switch = daily_switch[-1]
            del daily_switch[-1]

        return daily_switch, previous_switch

    def final_results(self, all_results_converted, highest_temp_values_index, lowest_temp_values_index, logger):
        """
        Function to assemble final results dict that contains only info for hottest time of the day for each day
        This dict will be used to populate the Google Sheets
        Use for multiple days of processing info

        :param logger:
        :param lowest_temp_values_index:
        :param all_results_converted:
        :param highest_temp_values_index:
        :return:
        """
        final_results_converted = {
            "dates": [],
            "canopy temperature": [],
            "ambient temperature": [],
            "lowest ambient temperature": [],
            "gdd": [],
            "crop stage": [],
            "vpd": [],
            "vwc_1": [],
            "vwc_2": [],
            "vwc_3": [],
            "vwc_1_ec": [],
            "vwc_2_ec": [],
            "vwc_3_ec": [],
            "daily gallons": [],
            "daily switch": [],
            "cwsi": [],
            "sdd": [],
            "rh": []
        }

        # print()
        # print('\tAll Results Converted -> Before Processing: ')
        # for key, values in all_results_converted.items():
        #     print('\t', key, " : ", values)

        for index, i in enumerate(highest_temp_values_index):
            # if all_results_converted["dates"][i].year == this_year:
            date = all_results_converted["dates"][i]
            tc = all_results_converted["canopy temperature"][i]
            ta = all_results_converted["ambient temperature"][i]
            lta = all_results_converted["ambient temperature"][lowest_temp_values_index[index]]
            rh = all_results_converted["rh"][i]
            vpd = all_results_converted["vpd"][i]
            vwc_1 = all_results_converted["vwc_1"][i]
            vwc_2 = all_results_converted["vwc_2"][i]
            vwc_3 = all_results_converted["vwc_3"][i]
            vwc_1_ec = all_results_converted["vwc_1_ec"][i]
            vwc_2_ec = all_results_converted["vwc_2_ec"][i]
            vwc_3_ec = all_results_converted["vwc_3_ec"][i]

            '''
                Calling functions to calculate SDD, and CWSI with each specific days values
                '''
            sdd = self.get_sdd(tc, ta)
            cwsi = self.get_cwsi(tc, vpd, ta, logger.crop_type, rh=rh)
            gdd = self.get_gdd(ta, lta, logger.crop_type)

            logger.update_ir_consecutive_data(cwsi, sdd, date)

            final_results_converted["dates"].append(date)
            final_results_converted["ambient temperature"].append(ta)
            final_results_converted["lowest ambient temperature"].append(lta)
            final_results_converted["gdd"].append(gdd)
            # final_results_converted["crop stage"].append(crop_stage)
            final_results_converted["vpd"].append(vpd)
            final_results_converted["vwc_1"].append(vwc_1)
            final_results_converted["vwc_2"].append(vwc_2)
            final_results_converted["vwc_3"].append(vwc_3)
            final_results_converted["vwc_1_ec"].append(vwc_1_ec)
            final_results_converted["vwc_2_ec"].append(vwc_2_ec)
            final_results_converted["vwc_3_ec"].append(vwc_3_ec)
            final_results_converted["rh"].append(rh)

            # If logger.ir_active is False

            if not logger.ir_active or logger.crop_type.lower() in ['almonds', 'almond', 'pistachios', 'pistachio']:
                date_to_check = date.date()
                logger.ir_active = logger.should_ir_be_active(date_to_check=date_to_check)
                print(f'\t\t\t{logger.ir_active}')

            if logger.ir_active:
                final_results_converted["canopy temperature"].append(tc)
                final_results_converted["sdd"].append(sdd)
                final_results_converted["cwsi"].append(cwsi)
            elif logger.field.field_type == 'R&D':
                final_results_converted["canopy temperature"].append(tc)
                final_results_converted["sdd"].append(sdd)
                final_results_converted["cwsi"].append(None)
            else:
                final_results_converted["canopy temperature"].append(None)
                final_results_converted["sdd"].append(None)
                final_results_converted["cwsi"].append(None)

        # NEW WAY OF GETTING SWITCH DATA
        for date in final_results_converted["dates"]:
            if date.date() in logger.irrigation_ledger:
                no_none_switch_values = [val for val in logger.irrigation_ledger[date.date()] if val is not None]
                irrigation_minutes = sum(no_none_switch_values)
                final_results_converted['daily switch'].append(irrigation_minutes)

                if None not in logger.irrigation_ledger[date.date()]:
                    del logger.irrigation_ledger[date.date()]

        return final_results_converted

    def get_sdd(self, canopy_temp, ambient_temp):
        """
        Function to calculate SDD given Canopy Temperature
        and Ambient Temperature

        :param canopy_temp:
        :param ambient_temp:
        :return:
        """
        if ambient_temp is None or canopy_temp is None:
            return None
        return canopy_temp - ambient_temp

    def get_rh(self, vpd, ta):
        """
        Function to calculate Relative Humidity given VPD
        and Ambient Temperature

        :param vpd:
        :param ta:
        :return:
        """
        rh = 100 - ((100 * vpd) / (
                0.001 * (610.7 * 10 ** ((7.5 * ((5 * (ta - 32)) / 9)) / (237.7 + ((5 * (ta - 32)) / 9))))))
        return rh

    def get_gdd(self, high_temp, low_temp, crop_type, algorithm='base'):
        if crop_type.lower() == 'tomatoes' or crop_type.lower() == 'tomato':
            if algorithm == 'base':
                gdd = self.get_tomato_gdd_base(high_temp, low_temp)
                return gdd
            elif algorithm == 'limited':
                gdd = self.get_tomato_gdd_limited(high_temp, low_temp)
                return gdd
            elif algorithm == 'limited2':
                gdd = self.get_tomato_gdd_limited_2(high_temp, low_temp)
                return gdd
        else:
            return None

    def get_tomato_gdd_base(self, high_temp, low_temp):
        TOMATO_BASE_VALUE = 50
        if high_temp is None or low_temp is None:
            return 0
        gdd = ((high_temp + low_temp) / 2) - TOMATO_BASE_VALUE
        if gdd < 0:
            return 0
        return gdd

    def get_tomato_gdd_limited(self, high_temp, low_temp):
        TOMATO_BASE_VALUE = 50
        TOMATO_HIGH_TEMP_LIMIT = 86
        if high_temp is None or low_temp is None:
            return 0
        if high_temp > TOMATO_HIGH_TEMP_LIMIT:
            high_temp = TOMATO_HIGH_TEMP_LIMIT
        if low_temp < TOMATO_BASE_VALUE:
            low_temp = TOMATO_BASE_VALUE
        gdd = ((high_temp + low_temp) / 2) - TOMATO_BASE_VALUE
        if gdd < 0:
            return 0
        return gdd

    def get_tomato_gdd_limited_2(self, high_temp, low_temp):
        TOMATO_BASE_VALUE = 50
        TOMATO_HIGH_TEMP_LIMIT = 86
        if high_temp is None or low_temp is None:
            return 0
        average_temp = (high_temp + low_temp) / 2
        if average_temp < TOMATO_HIGH_TEMP_LIMIT and average_temp > TOMATO_BASE_VALUE:
            gdd = average_temp - TOMATO_BASE_VALUE
        elif average_temp >= TOMATO_HIGH_TEMP_LIMIT:
            gdd = TOMATO_HIGH_TEMP_LIMIT - TOMATO_BASE_VALUE
        elif average_temp <= TOMATO_BASE_VALUE:
            gdd = 0
        if gdd < 0:
            return 0
        return gdd

    def get_cwsi(self, tc, vpd, ta, cropType, rh=0, return_negative=False, with_adjustment=True):
        """
        Function to calculate Crop Water Stress Index given
        SDD, RH, VPD, and TA

        :param tc:
        :param vpd:
        :param ta:
        :param cropType:
        :param rh:
        :param return_negative:
        :return:
        """
        if tc is None or ta is None or vpd is None or cropType is None or tc < -50:
            return None

        ta_celsius = (ta - 32) * 5 / 9
        tc_celsius = (tc - 32) * 5 / 9
        sdd = tc - ta
        sdd_celsius = tc_celsius - ta_celsius

        # Converting cropType to all lowercase to avoid case isssues
        if cropType.lower() == 'tomatoes' or cropType.lower() == 'tomato':
            # Using Farenheit sdd this 2021 year, changing to Celsius in the future
            # Using old school formula for psi for 2021 that goes from 0-2.2, switching to new formula in the future
            cwsi = self.get_old_tomatoes_cwsi(sdd, rh, vpd, ta, with_adjustment)
            # cwsi = self.get_tomatoes_cwsi(sdd, vpd)
        elif cropType.lower() == 'almonds':
            cwsi = self.get_almonds_cwsi(sdd_celsius, vpd)
        elif cropType.lower() == 'pistachios':
            cwsi = self.get_pistachios_cwsi(sdd_celsius, vpd)
        elif cropType.lower() == 'grapes':
            cwsi = self.get_grapes_cwsi(sdd_celsius, vpd)
        elif cropType.lower() == 'garlic':
            cwsi = self.get_garlic_cwsi(sdd_celsius, vpd)
        elif cropType.lower() == 'lemons':
            cwsi = self.get_lemons_cwsi(sdd_celsius, vpd)
        elif cropType.lower() == 'tangerines':
            cwsi = self.get_tangerines_cwsi(sdd_celsius, vpd)
        else:
            # print("Crop Type: " + str(cropType) + ' not supported')
            return None

        if cwsi is not None and cwsi < 0:
            if return_negative:
                pass
            else:
                cwsi = 0

        return cwsi

    def get_old_tomatoes_cwsi(self, sdd, rh, vpd, ta, with_adjustment=True):
        if sdd is None or ta is None or rh is None or vpd is None or sdd < -50:
            return None

        # if rh > 0 and ta > 0:
        # if ta < 85:
        #     cwsi = ((sdd + (ta - 85) + (sdd * (rh / 100))) - ((-2.5086 * vpd) + 0.8639)) / (
        #             2.7 - ((-2.5086 * vpd) + 0.8639)) + 2
        # else:
        #     cwsi = ((sdd + (sdd * (rh / 100))) - ((-2.5086 * vpd) + 0.8639)) / (
        #             2.7 - ((-2.5086 * vpd) + 0.8639)) + 2

        # OLD FORMULA without adjustments
        # cwsi = ((sdd ) - ((-2.5086 * vpd) + 0.8639)) / (2.7 - ((-2.5086 * vpd) + 0.8639)) + 2

        water_stress_baseline = 2.7
        non_water_stress_baseline = ((-2.5086 * vpd) + 0.8639)
        if with_adjustment:
            if rh > 0 and ta > 0:
                if ta < 85:
                    temp_adjustment = ta - 85
                    rh_adjustment = sdd * (rh / 100)
                    adjustments = temp_adjustment + rh_adjustment

                    top = (sdd + adjustments - non_water_stress_baseline)
                    bot = (water_stress_baseline - non_water_stress_baseline)

                    cwsi = top / bot + 2
                else:
                    temp_adjustment = 0
                    rh_adjustment = sdd * (rh / 100)
                    adjustments = temp_adjustment + rh_adjustment

                    top = (sdd + adjustments - non_water_stress_baseline)
                    bot = (water_stress_baseline - non_water_stress_baseline)

                    cwsi = top / bot + 2
            else:
                cwsi = None

        else:
            top = (sdd - non_water_stress_baseline)
            bot = (water_stress_baseline - non_water_stress_baseline)

            cwsi = top / bot + 2

        return cwsi

    def get_tomatoes_cwsi(self, sdd, vpd):
        '''
        CWSI = SDD - NWSB / WSB - NWSB

        :param sdd:
        :param vpd:
        :return:
        '''
        water_stress_baseline = 2.7
        non_water_stress_baseline = ((-2.5086 * vpd) + 0.8639)

        top = (sdd - non_water_stress_baseline)
        bot = (water_stress_baseline - non_water_stress_baseline)

        cwsi = top / bot

        return cwsi

    def get_almonds_cwsi(self, sdd_celsius, vpd):
        '''
        CWSI = SDD - NWSB / WSB - NWSB

        :param sdd:
        :param vpd:
        :return:
        '''

        # =(J25 - ((-2.8311 * L25) + 1.5071)) / (1.5 - ((-2.8311 * L25) + 1.5071))
        # Need to get WSB and NWSB from 2020 data using Celsius and update below
        # since we are using all Celsius info for almonds going forward
        # Current algo using Farenheit for WSB, NWSB and vpd
        water_stress_baseline = 1.5
        non_water_stress_baseline = ((-2.8311 * vpd) + 1.5071)

        top = (sdd_celsius - non_water_stress_baseline)
        bot = (water_stress_baseline - non_water_stress_baseline)

        cwsi = top / bot

        return cwsi
        # Turned on 3/28/2022
        # After the season CWSI for almonds doesn't make sense because there are no leaves to read the IR for
        # so disabling it for now
        # return None

    def get_pistachios_cwsi(self, sdd_celsius, vpd):
        '''
        CWSI = SDD - NWSB / WSB - NWSB

        :param sdd:
        :param vpd:
        :return:
        '''

        # Algo still in development
        water_stress_baseline = 0.9
        non_water_stress_baseline = ((-2.4216 * vpd) + 0.7034)

        top = (sdd_celsius - non_water_stress_baseline)
        bot = (water_stress_baseline - non_water_stress_baseline)

        cwsi = top / bot

        return cwsi
        # return None

    def get_grapes_cwsi(self, sdd_celsius, vpd):
        '''
        CWSI = SDD - NWSB / WSB - NWSB

        :param sdd:
        :param vpd:
        :return:
        '''

        # Algo still in development
        # water_stress_baseline = 2.7
        # non_water_stress_baseline = ((-2.5086 * vpd) + 0.8639)
        #
        # top = (sdd_celsius - non_water_stress_baseline)
        # bot = (water_stress_baseline - non_water_stress_baseline)
        #
        # cwsi = top / bot
        #
        # return cwsi
        return None

    def get_garlic_cwsi(self, sdd_celsius, vpd):
        '''
        CWSI = SDD - NWSB / WSB - NWSB

        :param sdd:
        :param vpd:
        :return:
        '''

        # Algo still in development
        # water_stress_baseline = 2.7
        # non_water_stress_baseline = ((-2.5086 * vpd) + 0.8639)
        #
        # top = (sdd_celsius - non_water_stress_baseline)
        # bot = (water_stress_baseline - non_water_stress_baseline)
        #
        # cwsi = top / bot
        #
        # return cwsi
        return None

    def get_lemons_cwsi(self, sdd_celsius, vpd):
        '''
                CWSI = SDD - NWSB / WSB - NWSB

                :param sdd:
                :param vpd:
                :return:
                '''

        # Algo still in development
        # water_stress_baseline = 0.8
        # non_water_stress_baseline = ((-1.1938 * vpd) + 2.8598)
        #
        # top = (sdd_celsius - non_water_stress_baseline)
        # bot = (water_stress_baseline - non_water_stress_baseline)
        #
        # cwsi = top / bot
        #
        # return cwsi
        return None

    def get_tangerines_cwsi(self, sdd_celsius, vpd):
        '''
                CWSI = SDD - NWSB / WSB - NWSB

                :param sdd:
                :param vpd:
                :return:
                '''

        # Algo still in development
        # water_stress_baseline = 0.9
        # non_water_stress_baseline = ((-1.8129 * vpd) + 1.0770)
        #
        # top = (sdd_celsius - non_water_stress_baseline)
        # bot = (water_stress_baseline - non_water_stress_baseline)
        #
        # cwsi = top / bot
        #
        # return cwsi
        return None

    def prep_data_for_writting_db(self, final_results, logger, db_dates):
        final_results_no_dups = self.remove_duplicates_already_in_logger_db(db_dates, final_results)

        gpm = 0
        logger_model = logger.model
        logger_id = logger.id
        # logger_direction = logger.loggerDirection

        field_capacity = logger.soil.field_capacity
        wilting_point = logger.soil.wilting_point

        psi_critical, psi_threshold = self.get_psi_thresholds(logger.crop_type)

        final_results_db = {"logger_id": [], "date": [], "time": [], "canopy_temperature": [], "canopy_temperature_celsius": [],
                            "ambient_temperature": [], "ambient_temperature_celsius": [], "vpd": [],
                            "vwc_1": [], "vwc_2": [], "vwc_3": [],
                            "field_capacity": [], "wilting_point": [], "daily_gallons": [], "daily_switch": [],
                            "daily_hours": [], "daily_pressure": [], "daily_inches": [], "psi": [], "psi_threshold": [],
                            "psi_critical": [], "sdd": [], "sdd_celsius": [], "rh": [], 'eto': [], 'kc': [], 'etc': [], 'et_hours': [],
                            "phase1_adjustment": [], "phase1_adjusted": [], "phase2_adjustment": [],
                            "phase2_adjusted": [], "phase3_adjustment": [], "phase3_adjusted": [], "vwc_1_ec": [],
                            "vwc_2_ec": [], "vwc_3_ec": [],
                            "lowest_ambient_temperature": [], "lowest_ambient_temperature_celsius":[], "gdd": [], "crop_stage": [], "id": [],
                            "planting_date": [], "variety": []}
        if hasattr(logger, 'gpm'):
            if type(logger.gpm) == str:
                gpm = logger.gpm.replace(',', '')
            else:
                gpm = logger.gpm
            if logger.gpm == None:
                gpm = 0
            i = 0
        else:
            gpm = 0
        if hasattr(logger, 'irrigation_set_acres'):
            if type(logger.irrigation_set_acres) == str:
                acres = logger.irrigation_set_acres.replace(',', '')
            else:
                acres = logger.irrigation_set_acres
            if logger.irrigation_set_acres == 0:
                acres = 1
        else:
            acres = 1

        for ind, val in enumerate(final_results_no_dups["dates"]):
            final_results_db["logger_id"].append(logger_id)
            # final_results_db["logger_direction"].append(logger_direction)
            final_results_db["field_capacity"].append(field_capacity)
            final_results_db["wilting_point"].append(wilting_point)
            final_results_db["psi_threshold"].append(psi_threshold)
            final_results_db["psi_critical"].append(psi_critical)
            final_results_db["date"].append(val.strftime("%Y-%m-%d"))
            final_results_db["time"].append(val.strftime("%I:%M %p"))
            final_results_db["id"].append(uuid.uuid4())
            if logger.planting_date:
                final_results_db["planting_date"].append(logger.planting_date)
            if ind < len(final_results_no_dups["daily switch"]):
                final_results_db["daily_hours"].append(round(final_results_no_dups["daily switch"][ind] / 60, 1))
                final_results_db["daily_inches"].append(
                    round((final_results_no_dups["daily switch"][ind] * float(gpm)) / (float(acres) * 27154), 1)
                )
        final_results_db["canopy_temperature"] = final_results_no_dups["canopy temperature"]
        final_results_db["canopy_temperature_celsius"] = self.convert_farenheit_list_to_celsius_list(final_results_no_dups["canopy temperature"])
        final_results_db["ambient_temperature"] = final_results_no_dups["ambient temperature"]
        final_results_db["ambient_temperature_celsius"] = self.convert_farenheit_list_to_celsius_list(final_results_no_dups["ambient temperature"])
        final_results_db["lowest_ambient_temperature"] = final_results_no_dups["lowest ambient temperature"]
        final_results_db["lowest_ambient_temperature"] = self.convert_farenheit_list_to_celsius_list(final_results_no_dups["lowest ambient temperature"])
        final_results_db["gdd"] = final_results_no_dups["gdd"]
        final_results_db["vpd"] = final_results_no_dups["vpd"]
        final_results_db["crop_stage"] = final_results_no_dups['crop stage']
        # while len(final_results_no_dups["daily switch"]) > len(final_results_no_dups["dates"]):
        if len(final_results_no_dups["daily switch"]) > len(final_results_no_dups["dates"]):
            print('\t---Mismatch length of dates and switch arrays---')
            print('\t# of Date values: ' + str(len(final_results_no_dups["dates"])))
            print('\t# of Switch values: ' + str(len(final_results_no_dups["daily switch"])))
            # if final_results_no_dups["daily switch"][-1] == 0:
            # del final_results_no_dups["daily switch"][-1]
            # elif final_results_no_dups["daily switch"][-2] == 0:
            #     del final_results_no_dups["daily switch"][-2]
            # else:
            #     del final_results_no_dups["daily switch"][-1]
        final_results_db["daily_switch"] = final_results_no_dups["daily switch"]
        final_results_db["psi"] = final_results_no_dups["cwsi"]
        final_results_db["sdd"] = final_results_no_dups["sdd"]
        final_results_db["sdd_celsius"] = self.convert_farenheit_list_to_celsius_list(final_results_no_dups["sdd"])
        final_results_db["rh"] = final_results_no_dups["rh"]
        if "kc" in final_results_no_dups:
            final_results_db["kc"] = final_results_no_dups["kc"]
        if "eto" in final_results_no_dups:
            final_results_db["eto"] = final_results_no_dups["eto"]
        if "etc" in final_results_no_dups:
            final_results_db["etc"] = final_results_no_dups["etc"]
        if "et_hours" in final_results_no_dups:
            final_results_db["et_hours"] = final_results_no_dups["et_hours"]

        final_results_db["vwc_1"] = final_results_no_dups["vwc_1"]
        final_results_db["vwc_2"] = final_results_no_dups["vwc_2"]
        final_results_db["vwc_3"] = final_results_no_dups["vwc_3"]
        final_results_db["vwc_1_ec"] = final_results_no_dups["vwc_1_ec"]
        final_results_db["vwc_2_ec"] = final_results_no_dups["vwc_2_ec"]
        final_results_db["vwc_3_ec"] = final_results_no_dups["vwc_3_ec"]

        filename = 'data.csv'
        print(f'\tWriting data to csv file: {filename}')
        with open(filename, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(final_results_db.keys())
            # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
            # This will add full null rows for any additional daily_switch list values
            writer.writerows(zip_longest(*final_results_db.values()))

        return None

    def remove_duplicates_already_in_logger_db(self, db_dates, final_results):
        print('\t\tChecking for and removing duplicate data')
        final_results_dates = [date.date() for date in final_results['dates']]
        no_db_repeat_final_results = {}

        for key in final_results:
            no_db_repeat_final_results[key] = []

        found_duplicates = False
        duplicate_dates = []
        for results_date in final_results_dates:
            if results_date not in db_dates:
                final_results_date_index = final_results_dates.index(results_date)
                for key in final_results:
                    if key != 'daily gallons' and len(final_results[key]) > final_results_date_index:
                        no_db_repeat_final_results[key].append(final_results[key][final_results_date_index])
            else:
                found_duplicates = True
                duplicate_dates.append(results_date)

        if found_duplicates:
            print(f'\t\tFound some duplicates and skipped from writing those to db - {duplicate_dates}')
            #TODO Check duplicate values and take the one that has the hottest ambient temp

        return no_db_repeat_final_results

    def process_data_for_writing_db_portal(
            self,
            final_results,
            field,
            logger_name='',
            logger_direction='',
            logger_lat=0,
            logger_long=0
    ):
        """

        :param logger_long: Logger longitude for location
        :param logger_lat: Logger latitude for location
        :param logger_direction:
        :param logger_name:
        :param final_results: {"dates": [], "canopy temperature": [], "ambient temperature": [], "vpd": [],
        #                            "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [],
        #                            "vwc_3_ec": [],
        #                            "daily gallons": [], "daily switch": [], "cwsi": [], "sdd": [], "rh": []}
        :param field: Field object
        :return:
        """
        individual_logger = False
        if len(logger_name) > 0:
            individual_logger = True
        vwc_1 = final_results['vwc_1']
        vwc_2 = final_results['vwc_2']
        vwc_3 = final_results['vwc_3']
        si_num = final_results['cwsi']
        if si_num is not None:
            si_num = round(si_num, 1)
        crop_type = field.loggers[0].crop_type
        planting_date = field.loggers[0].planting_date
        field_name = field.nickname
        report_url = field.report_url
        preview_url = field.preview_url

        # Check to make work in progress be the crop image for the individual loggers portal else it would be the crop
        # type image
        if individual_logger and preview_url == 'https://i.imgur.com/04UdmBH.png':
            crop_image = 'https://i.imgur.com/04UdmBH.png'
        else:
            crop_image = self.get_crop_image(crop_type)

        field_capacities = []
        wilting_points = []

        for logger in field.loggers:
            if logger.active:
                if not individual_logger:
                    field_capacities.append(float(logger.soil.field_capacity))
                    wilting_points.append(float(logger.soil.wilting_point))
                else:
                    if logger.name == logger_name or logger.nickname == logger_name:
                        field_capacity_avg = logger.soil.field_capacity
                        wilting_point_avg = logger.soil.wilting_point
        if not individual_logger:
            field_capacity_avg = numpy.mean(field_capacities)
            wilting_point_avg = numpy.mean(wilting_points)

        # Adding in an extra parameter field_name to calculate_portal_soil_moisture_num() because of a special case
        # requested by Ex 5/5/2024 for Barrios 25W. Details are explained in the function
        soil_moisture_num = self.calculate_portal_soil_moisture_num(
            vwc_1,
            vwc_2,
            vwc_3,
            planting_date,
            crop_type,
            field_name=field.name
        )
        # Custom fix for Ex and DOugherty soil type
        dougherty = False
        if field.name in ['Dougherty Bros9', 'Dougherty Bros5', 'Dougherty Bros2N', 'Dougherty Bros2S', 'Dougherty BrosS7', 'Dougherty BrosD3']:
            dougherty = True

        soil_moisture_desc = self.calculate_portal_soil_moisture_desc(
            soil_moisture_num,
            field_capacity_avg,
            wilting_point_avg,
            dougherty=dougherty,
        )
        si_desc = self.calculate_portal_si_desc(crop_type, si_num)

        order = self.calculate_portal_order(crop_type, soil_moisture_desc, si_desc)

        final_results_portal_db = {"order": order, "field": field_name, "crop_type": crop_type,
                                   "crop_image": crop_image,
                                   "soil_moisture_num": soil_moisture_num, "soil_moisture_desc": soil_moisture_desc,
                                   "si_num": si_num, "si_desc": si_desc, "report": report_url, "preview": preview_url}

        if not field.active:
            # Writing uninstall in field portal
            final_results_portal_db = {
                "order": 999, "field": field_name, "crop_type": crop_type, "crop_image": crop_image,
                "soil_moisture_num": 'null', "soil_moisture_desc": "Uninstalled",
                "si_num": 'null', "si_desc": "Uninstalled", "report": report_url, "preview": preview_url
            }

        # Adding logger name when processing individual logger data
        if individual_logger:
            final_results_portal_db['logger_name'] = logger_name
            final_results_portal_db['logger_direction'] = logger_direction
            if logger_lat != 0 and logger_long != 0:
                gmap_location = f'https://www.google.com/maps/search/?api=1&query={float(logger_lat)},{float(logger_long)}'
                final_results_portal_db['location'] = gmap_location
            else:
                final_results_portal_db['location'] = None

        return final_results_portal_db

    def get_psi_thresholds(self, crop_type):
        if crop_type.lower() == "tomato" or crop_type.lower() == "tomatoes":
            psi_threshold = 1.6
            psi_critical = 2.2
        elif crop_type.lower() == "almond" or crop_type.lower() == "almonds" \
                or crop_type.lower() == 'pistachio' or crop_type.lower() == 'pistachios':
            psi_threshold = 0.5
            psi_critical = 1.0
        else:
            psi_threshold = None
            psi_critical = None
        return psi_critical, psi_threshold

    def calculate_portal_soil_moisture_num(self, vwc_1, vwc_2, vwc_3, planting_date, crop_type, field_name=''):
        vwc_list = []
        if crop_type.lower() == 'tomato' or crop_type.lower() == 'tomatoes':
            today = datetime.today().date()
            day_delta = today - planting_date

            # Special conditional for Barrios 25W that wanted to average vwc_1 and vwc_2 always and avoid vwc_3 because
            # vwc_3 is in a sand streak which will never go up in moisture, and using that is bringing down their
            # portal irrigation description to 'below optimum' instead of 'optimum' and they "don't like to see that"
            #########################
            if field_name == 'Barrios Farms25W':
                if vwc_1 is None and vwc_2 is None:
                    return None
                elif vwc_1 is None:
                    return round(vwc_2, 1)
                elif vwc_2 is None:
                    return round(vwc_1, 1)
                vwc_list.append(vwc_1)
                vwc_list.append(vwc_2)
                soil_moisture_num = numpy.mean(vwc_list)
                return round(soil_moisture_num, 1)
            ###########################

            if day_delta.days > 30:
                if vwc_2 is None and vwc_3 is None:
                    return None
                elif vwc_2 is None:
                    return round(vwc_3, 1)
                elif vwc_3 is None:
                    return round(vwc_2, 1)
                vwc_list.append(vwc_2)
                vwc_list.append(vwc_3)
                soil_moisture_num = numpy.mean(vwc_list)
                return round(soil_moisture_num, 1)

        # crop isn't tomato or its tomato within 30 days after planting

        if vwc_1 is None and vwc_2 is None:
            return None
        elif vwc_1 is None:
            return round(vwc_2, 1)
        elif vwc_2 is None:
            return round(vwc_1, 1)
        vwc_list.append(vwc_1)
        vwc_list.append(vwc_2)
        soil_moisture_num = numpy.mean(vwc_list)
        return round(soil_moisture_num, 1)

    def calculate_portal_soil_moisture_desc(self, soil_moisture_num: float, field_capacity_avg: float,
                                            wilting_point_avg: float, dougherty: bool = False) -> str:
        """
        Create a soil with the field capacity and wilting point we were given and then get the appropriate
        description for the soil_moisture_num based on that soil type's vwc ranges (Function already provided for
        in the Soil class)

        :param soil_moisture_num: Float of the soil moisture value we want to calculate the description for
        :param field_capacity_avg: Float of the field capacity for the soil type
        :param wilting_point_avg: Float of the wilting point for the soil type
        :return: String of the appropriate description
        """
        soil = Soil(field_capacity=field_capacity_avg, wilting_point=wilting_point_avg)
        if dougherty:
            soil = Soil('Dougherty')
        description = soil.find_vwc_range_description(soil_moisture_num)
        return description

    def calculate_portal_si_desc(self, crop_type, si_num):
        optimum = 'Optimum'
        low = 'Low'
        medium = 'Medium'
        high = 'High'
        very_high = 'Very High'
        no_si = 'No Stress Index'
        if si_num is None:
            return no_si
        if crop_type.lower() == 'tomato' or crop_type.lower() == 'tomatoes':
            if 0 <= si_num < 0.6:
                return optimum
            elif 0.6 <= si_num < 1.2:
                return low
            elif 1.2 <= si_num < 1.6:
                return medium
            elif 1.6 <= si_num < 2.2:
                return high
            elif 2.2 <= si_num:
                return very_high
        else:
            if 0 <= si_num < 0.2:
                return optimum
            elif 0.2 <= si_num < 0.4:
                return low
            elif 0.4 <= si_num < 0.6:
                return medium
            elif 0.6 <= si_num < 0.8:
                return high
            elif 0.8 <= si_num:
                return very_high

    def calculate_portal_order(self, crop_type, soil_moisture_desc, si_desc):
        order = 0
        special_start = 100
        tomato_start = 90
        almond_start = 80
        pistachio_start = 70

        # Crop type start
        if crop_type.lower() == 'tomato' or crop_type.lower() == 'tomatoes':
            order = tomato_start
        elif crop_type.lower() == 'almond' or crop_type.lower() == 'almonds':
            order = almond_start
        elif crop_type.lower() == 'pistachio' or crop_type.lower() == 'pistachios':
            order = pistachio_start

        # VWC Level
        '''
        VERY_LOW = 'Very Low Moisture'
        LOW = 'Low Moisture Levels'
        BELOW_OPT = 'Below Optimum'
        OPTIMUM = 'Optimum Moisture'
        HIGH = 'High Soil Moisture'
        VERY_HIGH = 'Very High Soil Moisture'
        INCORRECT = 'Incorrect Value'
        '''
        if soil_moisture_desc == 'Optimum Moisture':
            order = order + 0
        elif soil_moisture_desc == 'Below Optimum':
            order = order + 0.5
        elif soil_moisture_desc == 'Low Moisture Levels' or soil_moisture_desc == 'High Soil Moisture':
            order = order + 1
        elif soil_moisture_desc == 'Very Low Moisture' or soil_moisture_desc == 'Very High Soil Moisture':
            order = order + 2

        # Stress Level
        if si_desc == 'Optimum':
            order = order + 0
        elif si_desc == 'Low':
            order = order + 0.5
        elif si_desc == 'Medium':
            order = order + 1
        elif si_desc == 'High':
            order = order + 1.5
        elif si_desc == 'Very High':
            order = order + 2

        return order

    def get_crop_image(self, crop_type):
        # Currently hosting off of Imgur
        crop_type = crop_type.lower()

        defUrl = 'https://i.imgur.com/'
        tomato = defUrl + 'yqTglkD.png'
        almond = defUrl + '2wkD3SR.png'
        pistachio = defUrl + 'Gl3UuEe.png'
        lemon = defUrl + 'NFkOMUu.png'
        grape = defUrl + '7bRDO3t.png'
        pepper = defUrl + 'h5aOCK3.png'
        garlic = defUrl + 'hPjyLfU.png'
        date = defUrl + '5yyz4dJ.png'
        hemp = defUrl + 'o0Ss80G.png'
        tangerines = defUrl + 'Fs8edNK.png'
        squash = defUrl + 'siBofu6.png'
        cherry = defUrl + 'LqbA0Ie.png'
        old_onion = defUrl + '8wCFjgF.png'
        onion = defUrl + 'H0r7Ezb.png'
        watermelon = defUrl + 'V9V5jAN.png'
        corn = defUrl + '0XBmvsz.png'
        seedling = defUrl + 'yiNUwJV.png'
        asparagus = defUrl + 'INJg1oa.png'
        cantaloupe = defUrl + 'PbTGD1X.png'
        # default = defUrl + 'B2coKxO.png'  # pumpkin image
        default = defUrl + 'yiNUwJV.png'  # seedling as new default

        return {
            'tomato': tomato,
            'tomatoes': tomato,
            'almond': almond,
            'almonds': almond,
            'pistachio': pistachio,
            'pistachios': pistachio,
            'lemon': lemon,
            'lemons': lemon,
            'grape': grape,
            'grapes': grape,
            'pepper': pepper,
            'peppers': pepper,
            'garlic': garlic,
            'date': date,
            'dates': date,
            'hemp': hemp,
            'tangerines': tangerines,
            'squash': squash,
            'cherry': cherry,
            'cherries': cherry,
            'onion': onion,
            'onions': onion,
            'corn': corn,
            'watermelon': watermelon,
            'watermelons': watermelon,
            'asparagus': asparagus,
            'cantaloupe': cantaloupe,
            'seedling': seedling
        }.get(crop_type, default)

    def irrigation_ai_processing(self, data, logger):

        # print(f'Grabbing data from {dataset} for logger {logger_id} - ')
        # dml = 'SELECT *' \
        #       'FROM `stomato.' + dataset + '.' + logger_id + '` ' \
        #                                                      'WHERE et_hours is not NULL ORDER BY date DESC'
        #
        # dbwriter = DBWriter()
        expertSys = IrrigationRecommendationExpert()
        # result = dbwriter.run_dml(dml)

        # FORMAT OF DATA
        # final_results_converted = {"dates": [], "canopy temperature": [], "ambient temperature": [], "vpd": [],
        #                            "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [],
        #                            "vwc_3_ec": [],
        #                            "daily gallons": [], "daily switch": [], "cwsi": [], "sdd": [], "rh": [], "kc": []}

        # applied_finals = {}
        ai_results = {"logger_id": [], "date": [], "time": [], "canopy_temperature": [], "ambient_temperature": [],
                      "vpd": [], "vwc_1": [], "vwc_2": [], "vwc_3": [], "field_capacity": [], "wilting_point": [],
                      "daily_gallons": [], "daily_switch": [], "daily_hours": [], "daily_pressure": [],
                      "daily_inches": [], "psi": [], "psi_threshold": [], "psi_critical": [],
                      "sdd": [], "rh": [], 'eto': [], 'kc': [], 'etc': [], 'et_hours': [],
                      "phase1_adjustment": [], "phase1_adjusted": [], "phase2_adjustment": [], "phase2_adjusted": []}

        # applied_finals['date'] = []
        # applied_finals['base'] = []
        # applied_finals['final_rec'] = []
        # applied_finals['adjustment_values'] = []
        # applied_finals['adjustment_steps'] = []

        planting_date = logger.planting_date
        # growers = open_pickle()
        # for grower in growers:
        #     for field in grower.fields:
        #         field_name = dbwriter.remove_unwanted_chars_for_db(field.name)
        #         if dataset in field_name:
        #             for logger in field.loggers:
        #                 planting_date = logger.planting_date

        # harvest_date = result[-1][1]
        harvest_date = '2022-06-06'

        # final_results_converted = {"dates": [], "canopy temperature": [], "ambient temperature": [], "vpd": [],
        #                            "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [],
        #                            "vwc_3_ec": [],
        #                            "daily gallons": [], "daily switch": [], "cwsi": [],
        #                            "sdd": [], "rh": [], "kc": []}

        field_capacity = logger.soil.field_capacity
        wilting_point = logger.soil.wilting_point
        ai_results_phase1_adjustment = []
        ai_results_phase1_adjusted = []
        ai_results_phase2_adjustment = []
        ai_results_phase2_adjusted = []

        for data_point in data:
            date = data_point['date']
            vwc_1 = data_point['vwc_1']
            vwc_2 = data_point['vwc_2']
            vwc_3 = data_point['vwc_3']
            psi = data_point['cwsi']
            et_hours = data_point['et_hours']

            rec = expertSys.make_recommendation(
                psi, field_capacity, wilting_point, vwc_1, vwc_2, vwc_3,
                crop='Tomatoes', date=date, planting_date=planting_date,
                harvest_date=harvest_date
            )
            applied_final, applied_steps = expertSys.apply_recommendations(et_hours, rec)

            ai_results_phase1_adjustment.append(rec.recommendation_info[0])
            ai_results_phase1_adjusted.append(applied_steps[0])
            ai_results_phase2_adjustment.append(rec.recommendation_info[1])
            ai_results_phase2_adjusted.append(applied_steps[1])

        data['phase1_adjustment'] = ai_results_phase1_adjustment
        data['phase1_adjusted'] = ai_results_phase1_adjusted
        data['phase2_adjustment'] = ai_results_phase2_adjustment
        data['phase2_adjusted'] = ai_results_phase2_adjusted

        # print(f'Adding AI info to DB {dataset} - {logger_id} ')
        #
        # filename = 'ai_data.csv'
        # print('\t- writing data to csv')
        # with open(filename, "w", newline='') as outfile:
        #     writer = csv.writer(outfile)
        #     writer.writerow(ai_results.keys())
        #     # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        #     # This will add full null rows for any additional daily_switch list values
        #     writer.writerows(zip_longest(*ai_results.values()))
        # print('...Done - file: ' + filename)

        # schema = [
        #     bigquery.SchemaField("logger_id", "STRING"),
        #     bigquery.SchemaField("date", "DATE"),
        #     bigquery.SchemaField("time", "STRING"),
        #     bigquery.SchemaField("canopy_temperature", "FLOAT"),
        #     bigquery.SchemaField("ambient_temperature", "FLOAT"),
        #     bigquery.SchemaField("vpd", "FLOAT"),
        #     bigquery.SchemaField("vwc_1", "FLOAT"),
        #     bigquery.SchemaField("vwc_2", "FLOAT"),
        #     bigquery.SchemaField("vwc_3", "FLOAT"),
        #     bigquery.SchemaField("field_capacity", "FLOAT"),
        #     bigquery.SchemaField("wilting_point", "FLOAT"),
        #     bigquery.SchemaField("daily_gallons", "FLOAT"),
        #     bigquery.SchemaField("daily_switch", "FLOAT"),
        #     bigquery.SchemaField("daily_hours", "FLOAT"),
        #     bigquery.SchemaField("daily_pressure", "FLOAT"),
        #     bigquery.SchemaField("daily_inches", "FLOAT"),
        #     bigquery.SchemaField("psi", "FLOAT"),
        #     bigquery.SchemaField("psi_threshold", "FLOAT"),
        #     bigquery.SchemaField("psi_critical", "FLOAT"),
        #     bigquery.SchemaField("sdd", "FLOAT"),
        #     bigquery.SchemaField("rh", "FLOAT"),
        #     bigquery.SchemaField("eto", "FLOAT"),
        #     bigquery.SchemaField("kc", "FLOAT"),
        #     bigquery.SchemaField("etc", "FLOAT"),
        #     bigquery.SchemaField("et_hours", "FLOAT"),
        #     bigquery.SchemaField('phase1_adjustment', 'FLOAT'),
        #     bigquery.SchemaField('phase1_adjusted', 'FLOAT'),
        #     bigquery.SchemaField('phase2_adjustment', 'FLOAT'),
        #     bigquery.SchemaField('phase2_adjusted', 'FLOAT'),
        #     bigquery.SchemaField('phase3_adjustment', 'FLOAT'),
        #     bigquery.SchemaField('phase3_adjusted', 'FLOAT'),
        #     bigquery.SchemaField('vwc_1_ec', 'FLOAT'),
        #     bigquery.SchemaField('vwc_2_ec', 'FLOAT'),
        #     bigquery.SchemaField('vwc_3_ec', 'FLOAT'),
        # ]
        # dbwriter.write_to_table_from_csv(dataset, logger_id, filename, schema, overwrite=True)
        print()

        print('Fully Done')
        return ai_results

    def temp_irrigation_ai(self, logger):

        """
            Apply the AI Irrigation recommendation to a specific logger's data

            :type logger: Logger object
            :param logger: Logger object
            """
        dataset = logger.field.name
        logger_name = logger.name

        print(f'Grabbing data from {dataset} for logger {logger_name} - ')
        dml = 'SELECT date, cwsi, vwc_1, vwc_2, vwc_3, et_hours' \
              'FROM `stomato.' + dataset + '.' + logger_name + '` ' \
                                                               'WHERE et_hours is not NULL' \
                                                               ' AND WHERE phase1_adjustment is NULL ORDER BY date DESC'

        dbwriter = DBWriter()
        expertSys = IrrigationRecommendationExpert()
        result = dbwriter.run_dml(dml, project='stomato')
        applied_finals = {}
        ai_results = {"logger_id": [], "date": [], "time": [], "canopy_temperature": [], "ambient_temperature": [],
                      "vpd": [], "vwc_1": [], "vwc_2": [], "vwc_3": [], "field_capacity": [], "wilting_point": [],
                      "daily_gallons": [], "daily_switch": [], "daily_hours": [], "daily_pressure": [],
                      "daily_inches": [], "psi": [], "psi_threshold": [], "psi_critical": [],
                      "sdd": [], "rh": [], 'eto': [], 'kc': [], 'etc': [], 'et_hours': [],
                      "phase1_adjustment": [], "phase1_adjusted": [], "phase2_adjustment": [], "phase2_adjusted": []}
        applied_finals['date'] = []
        applied_finals['base'] = []
        applied_finals['final_rec'] = []
        applied_finals['adjustment_values'] = []
        applied_finals['adjustment_steps'] = []

        planting_date = logger.planting_date

        # harvest_date = result[-1][1]
        harvest_date = planting_date + timedelta(days=120)

        field_capacity = logger.soil.field_capacity
        wilting_point = logger.soil.wilting_point

        for r in result:
            date = r[1]
            psi = r[2]
            vwc_1 = r[3]
            vwc_2 = r[4]
            vwc_3 = r[5]
            et_hours = r[6]

            rec = expertSys.make_recommendation(
                psi, field_capacity, wilting_point, vwc_1, vwc_2, vwc_3,
                crop='Tomatoes', date=date, planting_date=planting_date,
                harvest_date=harvest_date
            )
            applied_final, applied_steps = expertSys.apply_recommendations(et_hours, rec)

            phase1_adjustment = rec.recommendation_info[0]
            phase1_adjusted = applied_steps[0]
            phase2_adjustment = rec.recommendation_info[1]
            phase2_adjusted = applied_steps[1]

            print(f'Got AI Results:')
            print(f'Date: {date}   ET: {et_hours}')
            print(f'Phase 1 Results - Adjustment: {phase1_adjustment}  Adjusted: {phase1_adjusted}')
            print(f'Phase 2 Results - Adjustment: {phase2_adjustment}  Adjusted: {phase2_adjusted}')
            print()
            print(f'Adding AI info to DB {dataset} - {logger_name} ')

            dml = 'UPDATE `stomato.' + dataset + '.' + logger_name + '` ' \
                                                                     'SET  phase1_adjustment = ' + phase1_adjustment + ', ' \
                                                                                                                       'phase1_adjusted = ' + phase1_adjusted + ', ' \
                                                                                                                                                                'phase2_adjustment = ' + phase2_adjustment + ', ' \
                                                                                                                                                                                                             'phase2_adjusted = ' + phase2_adjusted + ', ' \
                                                                                                                                                                                                                                                      'WHERE et_hours is not NULL' \
                                                                                                                                                                                                                                                      ' AND WHERE phase1_adjustment is NULL' \
                                                                                                                                                                                                                                                      ' AND WHERE date = ' + date

            result = dbwriter.run_dml(dml, project='stomato')

        # filename = 'ai_data.csv'
        # print('\t- writing data to csv')
        # with open(filename, "w", newline='') as outfile:
        #     writer = csv.writer(outfile)
        #     writer.writerow(ai_results.keys())
        #     # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        #     # This will add full null rows for any additional daily_switch list values
        #     writer.writerows(zip_longest(*ai_results.values()))
        # print('...Done - file: ' + filename)

        # schema = [
        #     bigquery.SchemaField("logger_id", "STRING"),
        #     bigquery.SchemaField("date", "DATE"),
        #     bigquery.SchemaField("time", "STRING"),
        #     bigquery.SchemaField("canopy_temperature", "FLOAT"),
        #     bigquery.SchemaField("ambient_temperature", "FLOAT"),
        #     bigquery.SchemaField("vpd", "FLOAT"),
        #     bigquery.SchemaField("vwc_1", "FLOAT"),
        #     bigquery.SchemaField("vwc_2", "FLOAT"),
        #     bigquery.SchemaField("vwc_3", "FLOAT"),
        #     bigquery.SchemaField("field_capacity", "FLOAT"),
        #     bigquery.SchemaField("wilting_point", "FLOAT"),
        #     bigquery.SchemaField("daily_gallons", "FLOAT"),
        #     bigquery.SchemaField("daily_switch", "FLOAT"),
        #     bigquery.SchemaField("daily_hours", "FLOAT"),
        #     bigquery.SchemaField("daily_pressure", "FLOAT"),
        #     bigquery.SchemaField("daily_inches", "FLOAT"),
        #     bigquery.SchemaField("psi", "FLOAT"),
        #     bigquery.SchemaField("psi_threshold", "FLOAT"),
        #     bigquery.SchemaField("psi_critical", "FLOAT"),
        #     bigquery.SchemaField("sdd", "FLOAT"),
        #     bigquery.SchemaField("rh", "FLOAT"),
        #     bigquery.SchemaField("eto", "FLOAT"),
        #     bigquery.SchemaField("kc", "FLOAT"),
        #     bigquery.SchemaField("etc", "FLOAT"),
        #     bigquery.SchemaField("et_hours", "FLOAT"),
        #     bigquery.SchemaField('phase1_adjustment', 'FLOAT'),
        #     bigquery.SchemaField('phase1_adjusted', 'FLOAT'),
        #     bigquery.SchemaField('phase2_adjustment', 'FLOAT'),
        #     bigquery.SchemaField('phase2_adjusted', 'FLOAT'),
        #     bigquery.SchemaField('phase3_adjustment', 'FLOAT'),
        #     bigquery.SchemaField('phase3_adjusted', 'FLOAT'),
        #     bigquery.SchemaField('vwc_1_ec', 'FLOAT'),
        #     bigquery.SchemaField('vwc_2_ec', 'FLOAT'),
        #     bigquery.SchemaField('vwc_3_ec', 'FLOAT'),
        # ]
        # dbwriter.write_to_table_from_csv(dataset, logger_id, filename, schema, overwrite=True)
        print()

        print('Fully Done')

        pass

    def get_crop_stage_level(self, accumulated_gdds):
        # Preliminary Crop Stage GDD ranges from
        # "Predicitng Phenological Event of California Tomatoes" by Zalom and Wilson (1999)
        if accumulated_gdds == 0:
            crop_stage_level = 0
        elif 0 < accumulated_gdds <= 185:
            crop_stage_level = 1
        elif 185 < accumulated_gdds <= 427:
            crop_stage_level = 2
        elif 427 < accumulated_gdds <= 495:
            crop_stage_level = 3
        elif 495 < accumulated_gdds <= 557:
            crop_stage_level = 4
        elif 557 < accumulated_gdds <= 770:
            crop_stage_level = 5
        elif 770 < accumulated_gdds <= 909:
            crop_stage_level = 6
        elif 909 < accumulated_gdds <= 996:
            crop_stage_level = 7
        elif 996 < accumulated_gdds <= 1101:
            crop_stage_level = 8
        elif 1101 < accumulated_gdds <= 1214:
            crop_stage_level = 9
        elif 1214 < accumulated_gdds:
            crop_stage_level = 10
        else:
            crop_stage_level = accumulated_gdds

        return crop_stage_level

    def get_crop_stage(self, accumulated_gdds):
        crop_stage_level = self.get_crop_stage_level(accumulated_gdds)

        # Preliminary Crop Stage GDD ranges from
        # "Predicitng Phenological Event of California Tomatoes" by Zalom and Wilson (1999)
        return {
            0: 'NA',
            1: 'Bloom',
            2: '0.5-0.75 inch Green',
            3: '1.25-1.5 inch Green',
            4: 'Mature Green Fruit',
            5: 'Pink Fruit',
            6: '10-30% Red',
            7: '30-50% Red',
            8: '50-75% Red',
            9: '75-90% Red',
            10: '90-100% Red',
        }.get(crop_stage_level, 'Error - GDDs:' + str(crop_stage_level))
