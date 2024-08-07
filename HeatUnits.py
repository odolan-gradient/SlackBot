import os
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import numpy as np
import pandas as pd
import scipy.integrate as spi
from scipy.optimize import curve_fit

import Decagon


def main(force_re_download: bool = False):
    pickle_file_name = 'weather_stations_2023.pickle'
    pickle_file_path = 'H:\\Shared drives\\Stomato\\HeatUnits\\'
    weather_stations = Decagon.open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)
    print()
    # df = pd.DataFrame(columns=weather_stations_names_and_dates)
    writer = pd.ExcelWriter('weather_station_gdds.xlsx', engine='xlsxwriter')
    print()
    for station in weather_stations:
        print(f'Processing {station.name}...')
        # final_results = process_station_data(station)
        final_results = process_station_data(station, force_re_download=True)
        df = fill_dataframe(station, final_results)
        df.to_excel(writer, sheet_name=station.name, index=False)
    writer.save()
    print()


def process_station_data(station, force_re_download: bool = False):

    # Download data
    dxd_save_location = 'H:\\Shared drives\\Stomato\\HeatUnits\\'
    file_path = dxd_save_location + station.name + '.dxd'
    response = False
    # If dxd already exists, don't re-download
    if not os.path.isfile(file_path) or force_re_download:
        print("Downloading data. Calling API")
        response = station.get_logger_data(specific_mrid=0, dxd_save_location=dxd_save_location, file_name=station.name)
    else:
        print('Found dxd')
        response = True

    # Process data
    specific_year = 2023
    if response:
        raw_dxd = station.read_dxd(dxd_save_location=dxd_save_location, file_name=station.name)
        raw_data = station.get_all_ports_information_weather_stations(raw_dxd, specific_year=specific_year)
        clean_data = cleanup_data(station, raw_data)
        # print(clean_data)
        highest_temp_values_ind, lowest_temp_values_ind, day_break_indexes = \
            station.cwsi_processor.get_highest_and_lowest_temperature_indexes(clean_data, mute_prints=True)
        # print(highest_temp_values_ind)
        final_results = calculate_gdds(station, clean_data, highest_temp_values_ind, lowest_temp_values_ind, day_break_indexes)
        print(final_results)
        return final_results


def fill_dataframe(station, final_results):
    date = f"Dates"
    gdd = f"GDD"
    accum_gdd = f"Accum GDD"
    gdd_crop_stage = f"GDD Crop Stage"
    gdd_limited = f"GDD Limited"
    accum_gdd_limited = f"Accum GDD Limited"
    gdd_limited_crop_stage = f"GDD Limited Crop Stage"
    gdd_limited_2 = f"GDD Limited 2"
    accum_gdd_limited_2 = f"Accum GDD Limited 2"
    gdd_limited_2_crop_stage = f"GDD Limited 2 Crop Stage"
    gdd_trapezoidal_date = f"GDD Trapezoidal Date"
    gdd_trapezoidal = f"GDD Trapezoidal"
    accum_gdd_trapezoidal = f"Accum GDD Trapezoidal"
    gdd_trapezoidal_crop_stage = f"GDD Trapezoidal Crop Stage"
    gdd_sinusoidal_integrated_date = f"GDD Sinusoidal Date"
    gdd_sinusoidal_integrated = f"GDD Sinusoidal Integrated"
    accum_gdd_sinusoidal_integrated = f"Accum GDD Sinusoidal Integrated"
    gdd_sinusoidal_integrated_crop_stage = f"GDD Sinusoidal Integrated Crop Stage"
    gdd_fourier_integrated_date = f"GDD Fourier Integrated Date"
    gdd_fourier_integrated = f"GDD Fourier Integrated"
    accum_gdd_fourier_integrated = f"Accum GDD Fourier Integrated"
    gdd_fourier_integrated_crop_stage = f"GDD Fourier Integrated Crop Stage"
    high_temp = f"High Temp"
    low_temp = f"Low Temp"

    df = pd.DataFrame(columns=[
        date,
        gdd,
        accum_gdd,
        gdd_crop_stage,
        gdd_limited,
        accum_gdd_limited,
        gdd_limited_crop_stage,
        gdd_limited_2,
        accum_gdd_limited_2,
        gdd_limited_2_crop_stage,
        gdd_trapezoidal_date,
        gdd_trapezoidal,
        accum_gdd_trapezoidal,
        gdd_trapezoidal_crop_stage,
        gdd_sinusoidal_integrated_date,
        gdd_sinusoidal_integrated,
        accum_gdd_sinusoidal_integrated,
        gdd_sinusoidal_integrated_crop_stage,
        gdd_fourier_integrated_date,
        gdd_fourier_integrated,
        accum_gdd_fourier_integrated,
        gdd_fourier_integrated_crop_stage,
        high_temp,
        low_temp
    ])

    for ind, date_val in enumerate(final_results['dates']):
        try:
            df = df.append({
                date: date_val,
                gdd: final_results['gdd'][ind],
                accum_gdd: final_results['gdd accum'][ind],
                gdd_crop_stage: final_results['gdd crop stage'][ind],
                gdd_limited: final_results['gdd limited'][ind],
                accum_gdd_limited: final_results['gdd limited accum'][ind],
                gdd_limited_crop_stage: final_results['gdd limited crop stage'][ind],
                gdd_limited_2: final_results['gdd limited 2'][ind],
                accum_gdd_limited_2: final_results['gdd limited 2 accum'][ind],
                gdd_limited_2_crop_stage: final_results['gdd limited 2 crop stage'][ind],
                gdd_trapezoidal_date: final_results['gdd trapezoidal date'][ind],
                gdd_trapezoidal: final_results['gdd trapezoidal'][ind],
                accum_gdd_trapezoidal: final_results['gdd trapezoidal accum'][ind],
                gdd_trapezoidal_crop_stage: final_results['gdd trapezoidal crop stage'][ind],
                gdd_sinusoidal_integrated_date: final_results['gdd sinusoidal integrated date'][ind],
                gdd_sinusoidal_integrated: final_results['gdd sinusoidal integrated'][ind],
                accum_gdd_sinusoidal_integrated: final_results['gdd sinusoidal integrated accum'][ind],
                gdd_sinusoidal_integrated_crop_stage: final_results['gdd sinusoidal integrated crop stage'][ind],
                gdd_fourier_integrated_date: final_results['gdd fourier integrated date'][ind],
                gdd_fourier_integrated: final_results['gdd fourier integrated'][ind],
                accum_gdd_fourier_integrated: final_results['gdd fourier integrated accum'][ind],
                gdd_fourier_integrated_crop_stage: final_results['gdd fourier integrated crop stage'][ind],
                low_temp: final_results["low air temperature"][ind],
                high_temp: final_results["high air temperature"][ind]
            }, ignore_index=True)
        except Exception as e:
            print()
            print(e)

    # for column in df.columns:
    #     if column.lower() in final_results:
    #         df[column] = final_results[column.lower()]

    # print()
    # print(df)
    return df


def calculate_gdds(station, clean_data, highest_temp_values_ind, lowest_temp_values_ind, day_break_indexes):
    final_results = {"dates": [], "solar radiation": [], "precipitation": [], "lightning activity": [],
                     "lightning distance": [], "wind direction": [], "wind speed": [], "gust speed": [],
                     "air temperature": [], "relative humidity": [], "atmospheric pressure": [], "x axis level": [],
                     "y axis level": [], "vpd": [],
                     "gdd": [], "gdd accum": [], "gdd crop stage": [],
                     "gdd limited": [], "gdd limited accum": [], "gdd limited crop stage": [],
                     "gdd limited 2": [], "gdd limited 2 accum": [], "gdd limited 2 crop stage": [],
                     "gdd trapezoidal date": [], "gdd trapezoidal": [], "gdd trapezoidal accum": [], "gdd trapezoidal crop stage": [],
                     "gdd sinusoidal integrated date": [], "gdd sinusoidal integrated": [], "gdd sinusoidal integrated accum": [], "gdd sinusoidal integrated crop stage": [],
                     "gdd fourier integrated date": [], "gdd fourier integrated": [], "gdd fourier integrated accum": [], "gdd fourier integrated crop stage": [],
                     "high air temperature": [], "low air temperature": []}
    print(f'Station: {station.name}')
    gdd_accum = 0
    gdd_limited_accum = 0
    gdd_limited_2_accum = 0
    for index, i in enumerate(highest_temp_values_ind):
        final_results["dates"].append(clean_data["dates"][i].date())
        final_results["solar radiation"].append(clean_data["solar radiation"][i])
        final_results["precipitation"].append(clean_data["precipitation"][i])
        final_results["lightning activity"].append(clean_data["lightning activity"][i])
        final_results["lightning distance"].append(clean_data["lightning distance"][i])
        final_results["wind direction"].append(clean_data["wind direction"][i])
        final_results["wind speed"].append(clean_data["wind speed"][i])
        final_results["gust speed"].append(clean_data["gust speed"][i])
        final_results["air temperature"].append(clean_data["air temperature"][i])
        final_results["relative humidity"].append(clean_data["relative humidity"][i])
        final_results["atmospheric pressure"].append(clean_data["atmospheric pressure"][i])
        final_results["x axis level"].append(clean_data["x axis level"][i])
        final_results["y axis level"].append(clean_data["y axis level"][i])
        final_results["vpd"].append(clean_data["vpd"][i])

        air_temp_max = clean_data["air temperature"][i]
        air_temp_min = clean_data["air temperature"][lowest_temp_values_ind[index]]
        gdd = station.cwsi_processor.get_gdd(air_temp_max, air_temp_min, station.crop_type)
        gdd_accum += gdd
        gdd_crop_stage = get_crop_stage(gdd_accum)

        gdd_limited = station.cwsi_processor.get_gdd(air_temp_max, air_temp_min, station.crop_type, algorithm="limited")
        gdd_limited_accum += gdd_limited
        gdd_limited_crop_stage = get_crop_stage(gdd_limited_accum)

        gdd_limited_2 = station.cwsi_processor.get_gdd(air_temp_max, air_temp_min, station.crop_type, algorithm="limited2")
        gdd_limited_2_accum += gdd_limited_2
        gdd_limited_2_crop_stage = get_crop_stage(gdd_limited_2_accum)

        final_results["gdd"].append(gdd)
        final_results["gdd accum"].append(gdd_accum)
        final_results["gdd crop stage"].append(gdd_crop_stage)
        final_results["gdd limited"].append(gdd_limited)
        final_results["gdd limited accum"].append(gdd_limited_accum)
        final_results["gdd limited crop stage"].append(gdd_limited_crop_stage)
        final_results["gdd limited 2"].append(gdd_limited_2)
        final_results["gdd limited 2 accum"].append(gdd_limited_2_accum)
        final_results["gdd limited 2 crop stage"].append(gdd_limited_2_crop_stage)

        final_results["high air temperature"].append(clean_data["air temperature"][i])
        final_results["low air temperature"].append(clean_data["air temperature"][lowest_temp_values_ind[index]])

    # Work in progress
    final_results = calculate_gdds_trapezoidal(final_results, clean_data, day_break_indexes)

    final_results = calculate_gdds_sinusoidal(final_results, clean_data, day_break_indexes)

    final_results = calculate_gdds_fourier(final_results, clean_data, day_break_indexes)

    return final_results


def calculate_gdds_sinusoidal(final_results, clean_data, day_break_indexes):
    # This is the sinusoidal function we want to fit to our data.
    def sinusoidal_func(x, a, b, c, d):
        return a * np.sin(b * (x - np.radians(c))) + d

    # For each day
    sinusoidal_gdd_accum = 0
    for ind, day_index in enumerate(day_break_indexes):
        if ind == len(day_break_indexes) - 1:
            break
        # print()
        # try:
        #     print(f'Day {final_results["dates"][ind]}')
        # except IndexError:
        #     break
        #     print(f'IndexError: {ind}')
        hours = []
        temperatures = []
        # If we don't have enough data points for a whole day, skip it
        if (day_break_indexes[ind + 1] - 1) - day_index < 22:
            final_results["gdd sinusoidal integrated date"].append(0)
            final_results["gdd sinusoidal integrated"].append(0)
            final_results["gdd sinusoidal integrated accum"].append(sinusoidal_gdd_accum)
            sinusoidal_gdd_crop_stage = get_crop_stage(sinusoidal_gdd_accum)
            final_results["gdd sinusoidal integrated crop stage"].append(sinusoidal_gdd_crop_stage)
            continue

        # Get the hours and temperatures for each day
        for i in range(day_index, day_break_indexes[ind + 1] - 1):
            hours.append(clean_data["dates"][i].hour)
            temperatures.append(clean_data["air temperature"][i])
        hours = np.array(hours)
        temperatures = np.array(temperatures)

        # sinusoidal ------------------------------------------------------------
        # This is the actual curve fitting step.
        try:
            params, params_covariance = curve_fit(sinusoidal_func, hours, temperatures, p0=[1, 2 * np.pi / 24, 0, 20], maxfev=5000)
        except Exception as error:
            print(error)
            final_results["gdd sinusoidal integrated date"].append(0)
            final_results["gdd sinusoidal integrated"].append(0)
            final_results["gdd sinusoidal integrated accum"].append(sinusoidal_gdd_accum)
            sinusoidal_gdd_crop_stage = get_crop_stage(sinusoidal_gdd_accum)
            final_results["gdd sinusoidal integrated crop stage"].append(sinusoidal_gdd_crop_stage)
            continue

        # print("Fitted parameters: ", params)

        # Now we can use these parameters to plot the fitted function.
        # plt.figure(figsize=(6, 4))
        # plt.scatter(hours, temperatures, label='Data')
        # plt.plot(hours, sinusoidal_func(hours, params[0], params[1], params[2], params[3]), label='Fitted function')
        # #
        # plt.legend(loc='best')
        # plt.show()
        # sinusoidal ------------------------------------------------------------

        # Set base temperature
        base_temp = 50

        # Define the function that we'll be integrating
        def func_to_integrate(x, a, b, c, d):
            temp = sinusoidal_func(x, a, b, c, d)
            return temp - base_temp if temp > base_temp else 0

        # Perform the integration for each day (you might need to adjust the limits of integration based on your data)
        gdd, error = spi.quad(func_to_integrate, 0, 24, args=tuple(params))
        gdd = gdd / 24
        # print("Growing Degree Days (GDD): ", gdd)
        sinusoidal_gdd_accum += gdd
        final_results["gdd sinusoidal integrated date"].append(clean_data["dates"][day_index].date())
        final_results["gdd sinusoidal integrated"].append(gdd)
        final_results["gdd sinusoidal integrated accum"].append(sinusoidal_gdd_accum)
        sinusoidal_gdd_crop_stage = get_crop_stage(sinusoidal_gdd_accum)
        final_results["gdd sinusoidal integrated crop stage"].append(sinusoidal_gdd_crop_stage)

    return final_results


def calculate_gdds_fourier(final_results, clean_data, day_break_indexes):
    # This is the Fourier series function we want to fit to our data.
    # It includes a constant term (a0) and two series, one for cosine and one for sine.
    # The series go up to the third harmonic, but you could extend this if necessary.
    def fourier_func(x, a0, a1, b1, a2, b2, a3, b3):
        return (a0 +
                a1 * np.cos((2 * np.pi * 1 / 24) * x + b1) +
                a2 * np.cos((2 * np.pi * 2 / 24) * x + b2) +
                a3 * np.cos((2 * np.pi * 3 / 24) * x + b3))

    # For each day
    fourier_gdd_accum = 0
    for ind, day_index in enumerate(day_break_indexes):
        if ind == len(day_break_indexes) - 1:
            break
        # print()
        # try:
        #     print(f'Day {final_results["dates"][ind]}')
        # except IndexError:
        #     break
        #     print(f'IndexError: {ind}')
        hours = []
        temperatures = []
        # If we don't have enough data points for a whole day, skip it
        if (day_break_indexes[ind + 1] - 1) - day_index < 22:
            final_results["gdd fourier integrated date"].append(0)
            final_results["gdd fourier integrated"].append(0)
            final_results["gdd fourier integrated accum"].append(fourier_gdd_accum)
            fourier_gdd_crop_stage = get_crop_stage(fourier_gdd_accum)
            final_results["gdd fourier integrated crop stage"].append(fourier_gdd_crop_stage)
            continue
        for i in range(day_index, day_break_indexes[ind + 1] - 1):
            hours.append(clean_data["dates"][i].hour)
            temperatures.append(clean_data["air temperature"][i])
        hours = np.array(hours)
        temperatures = np.array(temperatures)

        # fourier ------------------------------------------------------------
        params, params_covariance = curve_fit(fourier_func, hours, temperatures, p0=[20, 0, 0, 0, 0, 0, 0])

        # print("Fitted parameters: ", params)

        # Plot the fitted function.
        # plt.figure(figsize=(6, 4))
        # plt.scatter(hours, temperatures, label='Data')
        # plt.plot(hours, fourier_func(hours, *params), label='Fitted function')
        #
        # plt.legend(loc='best')
        # plt.show()
        # fourier ------------------------------------------------------------

        # Set base temperature
        base_temp = 50

        # Define the function that we'll be integrating
        def func_to_integrate(x, a0, a1, b1, a2, b2, a3, b3):
            temp = fourier_func(x, a0, a1, b1, a2, b2, a3, b3)
            return temp - base_temp if temp > base_temp else 0

        # Perform the integration for each day (you might need to adjust the limits of integration based on your data)
        gdd, error = spi.quad(func_to_integrate, 0, 24, args=tuple(params))
        gdd = gdd / 24
        # print("Growing Degree Days (GDD): ", gdd)
        fourier_gdd_accum += gdd
        final_results["gdd fourier integrated date"].append(clean_data["dates"][day_index].date())
        final_results["gdd fourier integrated"].append(gdd)
        final_results["gdd fourier integrated accum"].append(fourier_gdd_accum)
        fourier_gdd_crop_stage = get_crop_stage(fourier_gdd_accum)
        final_results["gdd fourier integrated crop stage"].append(fourier_gdd_crop_stage)

    return final_results


def calculate_gdds_trapezoidal(final_results, clean_data, day_break_indexes):
    trapezoidal_gdd_accum = 0
    for ind, day_index in enumerate(day_break_indexes):
        if ind == len(day_break_indexes) - 1:
            break
        # print(f"Day: {clean_data['dates'][day_index].date()}")
        # print(f"From: {clean_data['dates'][day_index]} -> {clean_data['dates'][day_break_indexes[ind + 1]]}")
        total_trap_area = 0
        # print(f'Range: {day_index} -> {day_break_indexes[ind + 1] - 1}')
        # Doing day_break_indexes[ind + 1] - 1 because day_break_indexes have the index of the first hour of every day
        #  so we want to stop at the 23rd hour of the previous day, not the first of a new day
        for i in range(day_index, day_break_indexes[ind + 1] - 1):
            height_1 = clean_data["air temperature"][i]
            height_2 = clean_data["air temperature"][i + 1]
            # Limiting the height of the trapezoid to 86, so if the temperature is higher than that, we just use 86
            # if height_1 > 86:
            #     height_1 = 86
            # if height_2 > 86:
            #     height_2 = 86

            # Limiting the height of the trapezoid to 50, so if the temperature is lower than that, we just use 50
            if height_1 < 50:
                height_1 = 50
            if height_2 < 50:
                height_2 = 50
            height_1 = height_1 - 50
            height_2 = height_2 - 50
            trap_area = (height_1 + height_2) / 2
            total_trap_area += trap_area
            # print(f'Trapezoid H1: {height_1:.2f} H2: {height_2:.2f} A: {trap_area:.1f}')
        total_trap_area = total_trap_area / 24
        trapezoidal_gdd_accum += total_trap_area
        trapezoidal_gdd_crop_stage = get_crop_stage(trapezoidal_gdd_accum)
        # print(f'Total Trapezoid Area: {total_trap_area:.1f}')
        # print()
        final_results["gdd trapezoidal date"].append(clean_data["dates"][day_index].date())
        final_results["gdd trapezoidal"].append(total_trap_area)
        final_results["gdd trapezoidal accum"].append(trapezoidal_gdd_accum)
        final_results["gdd trapezoidal crop stage"].append(trapezoidal_gdd_crop_stage)
    return final_results


def get_crop_stage(accumulated_gdds):
    '''

    :param accumulated_gdds:
    :return:
    '''

    '''
        0: 'NA',
        1: 'Bloom',                     0 - 185
        2: '0.5-0.75 inch Green',       186 - 427
        3: '1.25-1.5 inch Green',       428 - 495
        4: 'Mature Green Fruit',        496 - 557
        5: 'Pink Fruit',                558 - 770
        6: '10-30% Red',                771 - 909
        7: '30-50% Red',                910 - 996
        8: '50-75% Red',                997 - 1101
        9: '75-90% Red',                1102 - 1200
        10: '90-100% Red',              1201 - 1300
    '''
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


def cleanup_data(logger, raw_data):
    install_date = logger.install_date
    uninstall_date = logger.uninstall_date
    # print(install_date)
    # print(uninstall_date)

    clean_dict = {key: [] for key in raw_data.keys()}
    for ind, dp_date in enumerate(raw_data['dates']):
        if dp_date is None:
            print('----------------------NONE DATE')
        if raw_data['air temperature'][ind] != 'None':
            if install_date <= dp_date.date() <= uninstall_date:
                for key in clean_dict:
                    if key != 'daily gallons':
                        # print(key)
                        clean_dict[key].append(raw_data[key][ind])
            # print()
    clean_dict['ambient temperature'] = clean_dict['air temperature']
    return clean_dict

if __name__ == "__main__":
    main(force_re_download=True)