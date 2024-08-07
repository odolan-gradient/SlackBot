import csv
import datetime
import os
import statistics

import matplotlib.pyplot as plt
import plotly.graph_objs as go
import plotly.io as pio
import pytz
from astral import LocationInfo
from astral.sun import sun
from scipy.stats import linregress

import Decagon


def analyze_year(
        pickle_file_name: str,
        pickle_file_path: str,
        specific_year: int,
        analyze_ambient_temp: bool = False,
        analyze_rh: bool = False,
        analyze_vpd: bool = False,
        analyze_vwc: bool = False,
        analyze_canopy_temp: bool = False,
        analyze_psi: bool = False,
        write_csv_tables: bool = False,
        graph_data: bool = False,
        save_new_pickle: bool = False,
        grab_data_from_pickle: bool = False
):
    """
    Take a pickle and analyze data for each of its growers->fields->loggers
    This method will take a pickle filename and path, as well as a specific year, and go through that pickle calling
    analyze_logger() on each logger and compiling/aggregating the results. The results can then be printed or output to a csv
    depending on use case.

    :param grab_data_from_pickle:
    :param save_new_pickle:
    :param graph_data:
    :param analyze_psi:
    :param write_csv_tables:
    :param analyze_canopy_temp: Boolean - True if you want to perform this analysis
    :param analyze_vwc: Boolean - True if you want to perform this analysis
    :param analyze_vpd: Boolean - True if you want to perform this analysis
    :param analyze_rh: Boolean - True if you want to perform this analysis
    :param analyze_ambient_temp: Boolean - True if you want to perform this analysis
    :param pickle_file_name: String of the pickle filename
    :param pickle_file_path: String of the pickle filepath
    :param specific_year: int of the specific year you want to data from
    """

    # Take out Lucero fields

    if grab_data_from_pickle:
        all_data_pickle_file_name = 'all_2022_analysis.pickle'
        all_data_pickle_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Pickles\\'
        all_data = Decagon.open_pickle(filename=all_data_pickle_file_name, specific_file_path=all_data_pickle_file_path)
        # for datapoint in all_data:
        #     print(f'Length of {datapoint}: {len(all_data[datapoint])}')
    else:
        all_data = {
            'logger': [],
            'field': [],
            'grower': [],
            'soil type': [],
            'field capacity': [],
            'wilting point': [],
            'net yield': [],
            'paid yield': [],
            'ambient temp hours in opt with sun': [],
            'ambient temp hours in opt without sun': [],
            'vwc 1 in optimum hours': [],
            'vwc 2 in optimum hours': [],
            'vwc 3 in optimum hours': [],
            'vwc 1 in optimum %': [],
            'vwc 2 in optimum %': [],
            'vwc 3 in optimum %': [],
            'vwc 1_2 in optimum hours': [],
            'vwc 2_3 in optimum hours': [],
            'vwc 1_2 in optimum %': [],
            'vwc 2_3 in optimum %': [],
            'vwc total datapoints': [],
            'psi average': [],
            'psi first 3 values': [],
            'psi first 3 dates': [],
            'psi days below 05': [],
            'psi days 05 to 1': [],
            'psi days 1 to 16': [],
            'psi days 16 to 20': [],
            'psi days above 20': [],
            'psi days below 05 %': [],
            'psi days 05 to 1 %': [],
            'psi days 1 to 16 %': [],
            'psi days 16 to 20 %': [],
            'psi days above 20 %': [],
            'psi total datapoints': [],
        }

        # Ambient Temp Variables
        all_hours_in_optimum_temp_with_sun = []
        all_hours_in_optimum_temp_without_sun = []

        # RH Variables
        all_avg_rh = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }
        all_min_rh = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }
        all_max_rh = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }
        all_mode_rh = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }

        # VPD Variables
        all_avg_vpd = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }
        all_min_vpd = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }
        all_max_vpd = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }
        all_mode_vpd = {
            77: [],
            78: [],
            79: [],
            80: [],
            81: [],
            82: [],
            83: [],
            84: [],
            85: [],
            86: [],
            87: [],
            88: [],
            89: [],
            90: [],
            91: [],
            92: [],
            93: [],
            94: [],
            95: []
        }

        # VWC Variables
        total_vwc_1_in_optimum_hours = 0
        total_vwc_2_in_optimum_hours = 0
        total_vwc_3_in_optimum_hours = 0
        total_vwc_1_2_in_optimum_hours = 0
        total_vwc_2_3_in_optimum_hours = 0
        total_logger_vwc_total_valid_datapoints = 0

        # PSI Variables
        all_psi_avg_values = []
        all_psi_days_below_05 = 0
        all_psi_days_05_to_1 = 0
        all_psi_days_1_to_16 = 0
        all_psi_days_16_to_20 = 0
        all_psi_days_above_20 = 0
        all_logger_psi_total_data_points = 0

        total_data_points = 0

        growers = Decagon.open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)
        for grower in growers:
            for field in grower.fields:
                if field.field_type != 'R&D':
                    for logger in field.loggers:
                        if logger.cropType in ['Tomato', 'Tomatoes', 'tomato', 'tomatoes'] and 'z6' in logger.id:
                            # if hasattr(logger, 'type')
                            # print(f'Logger Crop: {logger.cropType}')
                            # dxd_save_location = 'H:\\Shared drives\\Stomato\\2022\\Testing\\Dxd Files\\All Data\\'
                            dxd_save_location = f'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Dxd Files\\{str(specific_year)}\\'

                            result = analyze_logger(
                                logger,
                                dxd_save_location,
                                specific_year,
                                analyze_ambient_temp,
                                analyze_rh,
                                analyze_vpd,
                                analyze_vwc,
                                analyze_canopy_temp,
                                analyze_psi
                            )
                            print()
                            if result is not None:
                                all_data['logger'].append(logger.name)
                                all_data['field'].append(logger.field.name)
                                all_data['grower'].append(logger.grower.name)
                                all_data['soil type'].append(logger.soil.soil_type)
                                all_data['field capacity'].append(logger.soil.field_capacity)
                                all_data['wilting point'].append(logger.soil.wilting_point)
                                all_data['net yield'].append(logger.field.net_yield)
                                all_data['paid yield'].append(logger.field.paid_yield)

                                hours_in_opt_with_sun, hours_in_opt_without_sun, hours_rh_data, hours_vpd_data, hours_vwc_data, psi_data, logger_total_data_points = result
                                total_data_points = total_data_points + logger_total_data_points

                                if analyze_ambient_temp:
                                    if hours_in_opt_with_sun is not None and hours_in_opt_with_sun > 0:
                                        all_hours_in_optimum_temp_with_sun.append(hours_in_opt_with_sun)
                                    all_data['ambient temp hours in opt with sun'].append(hours_in_opt_with_sun)

                                    if hours_in_opt_without_sun is not None and hours_in_opt_without_sun > 0:
                                        all_hours_in_optimum_temp_without_sun.append(hours_in_opt_without_sun)
                                    all_data['ambient temp hours in opt without sun'].append(hours_in_opt_without_sun)

                                if analyze_rh:
                                    for key in hours_rh_data:
                                        avg_rh, min_rh, max_rh, mode_rh = hours_rh_data[key]
                                        if avg_rh is not None:
                                            all_avg_rh[key].append(avg_rh)
                                        if min_rh is not None:
                                            all_min_rh[key].append(min_rh)
                                        if max_rh is not None:
                                            all_max_rh[key].append(max_rh)
                                        if mode_rh is not None:
                                            all_mode_rh[key].append(mode_rh)

                                if analyze_vpd:
                                    for key in hours_vpd_data:
                                        avg_vpd, min_vpd, max_vpd, mode_vpd = hours_vpd_data[key]
                                        if avg_vpd is not None:
                                            all_avg_vpd[key].append(avg_vpd)
                                        if min_vpd is not None:
                                            all_min_vpd[key].append(min_vpd)
                                        if max_vpd is not None:
                                            all_max_vpd[key].append(max_vpd)
                                        if mode_vpd is not None:
                                            all_mode_vpd[key].append(mode_vpd)

                                if analyze_vwc:
                                    vwc_1_in_optimum_hours, vwc_2_in_optimum_hours, vwc_3_in_optimum_hours, vwc_1_2_in_optimum_hours, vwc_2_3_in_optimum_hours, logger_vwc_total_valid_datapoints = hours_vwc_data

                                    total_vwc_1_in_optimum_hours += vwc_1_in_optimum_hours
                                    total_vwc_2_in_optimum_hours += vwc_2_in_optimum_hours
                                    total_vwc_3_in_optimum_hours += vwc_3_in_optimum_hours
                                    total_vwc_1_2_in_optimum_hours += vwc_1_2_in_optimum_hours
                                    total_vwc_2_3_in_optimum_hours += vwc_2_3_in_optimum_hours
                                    total_logger_vwc_total_valid_datapoints += logger_vwc_total_valid_datapoints

                                    all_data['vwc 1 in optimum hours'].append(vwc_1_in_optimum_hours)
                                    all_data['vwc 2 in optimum hours'].append(vwc_2_in_optimum_hours)
                                    all_data['vwc 3 in optimum hours'].append(vwc_3_in_optimum_hours)
                                    all_data['vwc 1_2 in optimum hours'].append(vwc_1_2_in_optimum_hours)
                                    all_data['vwc 2_3 in optimum hours'].append(vwc_2_3_in_optimum_hours)
                                    if logger_vwc_total_valid_datapoints > 0:
                                        all_data['vwc 1 in optimum %'].append(
                                            round((vwc_1_in_optimum_hours / logger_vwc_total_valid_datapoints) * 100, 1))
                                        all_data['vwc 2 in optimum %'].append(
                                            round((vwc_2_in_optimum_hours / logger_vwc_total_valid_datapoints) * 100, 1))
                                        all_data['vwc 3 in optimum %'].append(
                                            round((vwc_3_in_optimum_hours / logger_vwc_total_valid_datapoints) * 100, 1))
                                        all_data['vwc 1_2 in optimum %'].append(
                                            round((vwc_1_2_in_optimum_hours / logger_vwc_total_valid_datapoints) * 100, 1))
                                        all_data['vwc 2_3 in optimum %'].append(
                                            round((vwc_2_3_in_optimum_hours / logger_vwc_total_valid_datapoints) * 100, 1))
                                    else:
                                        all_data['vwc 1 in optimum %'].append(None)
                                        all_data['vwc 2 in optimum %'].append(None)
                                        all_data['vwc 3 in optimum %'].append(None)
                                        all_data['vwc 1_2 in optimum %'].append(None)
                                        all_data['vwc 2_3 in optimum %'].append(None)
                                    all_data['vwc total datapoints'].append(logger_vwc_total_valid_datapoints)

                                if analyze_psi:
                                    psi_avg, psi_first_3_values, psi_first_3_dates, days_in_ranges = psi_data
                                    days_below_05, days_05_to_1, days_1_to_16, days_16_to_20, days_above_20, valid_psi_data_points = days_in_ranges
                                    all_psi_avg_values.append(psi_avg)
                                    all_data['psi average'].append(psi_avg)
                                    all_data['psi first 3 values'].append(psi_first_3_values)
                                    all_data['psi first 3 dates'].append(psi_first_3_dates)

                                    all_psi_days_below_05 += days_below_05
                                    all_psi_days_05_to_1 += days_05_to_1
                                    all_psi_days_1_to_16 += days_1_to_16
                                    all_psi_days_16_to_20 += days_16_to_20
                                    all_psi_days_above_20 += days_above_20
                                    all_logger_psi_total_data_points += valid_psi_data_points

                                    all_data['psi days below 05'].append(days_below_05)
                                    all_data['psi days 05 to 1'].append(days_05_to_1)
                                    all_data['psi days 1 to 16'].append(days_1_to_16)
                                    all_data['psi days 16 to 20'].append(days_16_to_20)
                                    all_data['psi days above 20'].append(days_above_20)

                                    if valid_psi_data_points > 0:
                                        all_data['psi days below 05 %'].append(
                                            round((days_below_05 / valid_psi_data_points) * 100, 1))
                                        all_data['psi days 05 to 1 %'].append(
                                            round((days_05_to_1 / valid_psi_data_points) * 100, 1))
                                        all_data['psi days 1 to 16 %'].append(
                                            round((days_1_to_16 / valid_psi_data_points) * 100, 1))
                                        all_data['psi days 16 to 20 %'].append(
                                            round((days_16_to_20 / valid_psi_data_points) * 100, 1))
                                        all_data['psi days above 20 %'].append(
                                            round((days_above_20 / valid_psi_data_points) * 100, 1))
                                    else:
                                        all_data['psi days below 05 %'].append(None)
                                        all_data['psi days 05 to 1 %'].append(None)
                                        all_data['psi days 1 to 16 %'].append(None)
                                        all_data['psi days 16 to 20 %'].append(None)
                                        all_data['psi days above 20 %'].append(None)


        # Final results processing
        if analyze_ambient_temp:
            average_hours_in_opt_with_sun = statistics.mean(all_hours_in_optimum_temp_with_sun)
            average_hours_in_opt_without_sun = statistics.mean(all_hours_in_optimum_temp_without_sun)

        if analyze_rh:
            total_rh_info = []
            for key in all_avg_rh:
                total_rh_info.append(
                    (
                        key,
                        round(statistics.mean(all_avg_rh[key]), 1),
                        round(statistics.mean(all_min_rh[key]), 1),
                        round(statistics.mean(all_max_rh[key]), 1),
                        round(statistics.mean(all_mode_rh[key]), 1)
                    )
                )

        if analyze_vpd:
            total_vpd_info = []
            for key in all_avg_vpd:
                total_vpd_info.append(
                    (
                        key,
                        round(statistics.mean(all_avg_vpd[key]), 1),
                        round(statistics.mean(all_min_vpd[key]), 1),
                        round(statistics.mean(all_max_vpd[key]), 1),
                        round(statistics.mean(all_mode_vpd[key]), 1)
                    )
                )

        if analyze_vwc:
            pass

        if analyze_psi:
            all_psi_average = statistics.mean(all_psi_avg_values)

        print()
        print('---------------------------------------------------------------------------------')
        print(f'Total Data Points: {total_data_points}')
        if analyze_ambient_temp:
            print(
                f'ALL AVG hours with sun: {average_hours_in_opt_with_sun} | without sun: {average_hours_in_opt_without_sun}')
        if analyze_rh:
            print(f'ALL AVG RH Info:')
            for datapoint in total_rh_info:
                temp, avg, minimum, maximum, mode = datapoint
                print(f'Temp: {temp} - Avg: {avg}, Min: {minimum}, Max: {maximum}, Mode: {mode}')
        if analyze_vpd:
            print(f'ALL AVG VPD Info:')
            for datapoint in total_vpd_info:
                temp, avg, minimum, maximum, mode = datapoint
                print(f'Temp: {temp} - Avg: {avg}, Min: {minimum}, Max: {maximum}, Mode: {mode}')
        if analyze_vwc:
            print(f'ALL VWC Info:')
            print(
                f'VWC 1 in optimum: {(total_vwc_1_in_optimum_hours / total_logger_vwc_total_valid_datapoints) * 100:.1f} %')
            print(
                f'VWC 2 in optimum: {(total_vwc_2_in_optimum_hours / total_logger_vwc_total_valid_datapoints) * 100:.1f} %')
            print(
                f'VWC 3 in optimum: {(total_vwc_3_in_optimum_hours / total_logger_vwc_total_valid_datapoints) * 100:.1f} %')
            print(
                f'VWC 1_2 in optimum: {(total_vwc_1_2_in_optimum_hours / total_logger_vwc_total_valid_datapoints) * 100:.1f} %')
            print(
                f'VWC 2_3 in optimum: {(total_vwc_2_3_in_optimum_hours / total_logger_vwc_total_valid_datapoints) * 100:.1f} %')
        if analyze_psi:
            print(f'PSI average: {all_psi_average}')
            # print(f'First 3 PSI values: {}')
            # print(f'First 3 PSI dates: {}')
        print('---------------------------------------------------------------------------------')
        print()

    if save_new_pickle and not grab_data_from_pickle:
        all_data_pickle_file_name = 'all_2022_analysis.pickle'
        all_data_pickle_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Pickles\\'
        Decagon.write_pickle(all_data, filename=all_data_pickle_file_name, specific_file_path=all_data_pickle_file_path)
        # pprint.pprint(all_data)

    if write_csv_tables:
        write_data_to_csv(total_rh_info, total_vpd_info)

    if graph_data:
        # graph_scatter_plot('paid yield', 'ambient temp hours in opt with sun', 'field', all_data)
        # graph_scatter_plot('paid yield', 'vwc 1 in optimum %', 'field', all_data)
        # graph_scatter_plot('paid yield', 'vwc 2 in optimum %', 'field', all_data)
        # graph_scatter_plot('paid yield', 'vwc 3 in optimum %', 'field', all_data)
        # graph_scatter_plot('paid yield', 'vwc 1_2 in optimum %', 'field', all_data)
        # graph_scatter_plot('paid yield', 'vwc 2_3 in optimum %', 'field', all_data)

        graph_scatter_plot('paid yield', 'psi average', 'field', all_data)
        graph_scatter_plot('paid yield', 'psi days below 05', 'field', all_data)
        graph_scatter_plot('paid yield', 'psi days 05 to 1', 'field', all_data)
        graph_scatter_plot('paid yield', 'psi days 1 to 16', 'field', all_data)
        graph_scatter_plot('paid yield', 'psi days 16 to 20', 'field', all_data)
        graph_scatter_plot('paid yield', 'psi days above 20', 'field', all_data)

        # graph_vwc_optimums_by_soil_type(all_data, 'vwc 1 in optimum %')
        # graph_scatter_by_soil_type(all_data, 'vwc 2 in optimum %')
        # graph_vwc_optimums_by_soil_type(all_data, 'vwc 3 in optimum %')
        # graph_scatter_by_soil_type(all_data, 'vwc 2_3 in optimum %')
        # graph_scatter_by_soil_type(all_data, 'paid yield')
        pass


def graph_scatter_by_soil_type(all_data, option):
    data = {
        'Sand': [],
        'Loamy Sand': [],
        'Sandy Loam': [],
        'Sandy Clay Loam': [],
        'Loam': [],
        'Sandy Clay': [],
        'Silt Loam': [],
        'Silt': [],
        'Clay Loam': [],
        'Silty Clay Loam': [],
        'Silty Clay': [],
        'Clay': []
    }
    associated_labels = {
        'Sand': [],
        'Loamy Sand': [],
        'Sandy Loam': [],
        'Sandy Clay Loam': [],
        'Loam': [],
        'Sandy Clay': [],
        'Silt Loam': [],
        'Silt': [],
        'Clay Loam': [],
        'Silty Clay Loam': [],
        'Silty Clay': [],
        'Clay': []
    }
    for ind, dp in enumerate(all_data['soil type']):
        data[dp].append(all_data[option][ind])
        associated_labels[dp].append(all_data['field'][ind])

    # print(data)
    # print(associated_labels)

    # Define a color scale for soil types
    color_scale = {
        'Sand': '#FFD700',  # Gold
        'Loamy Sand': '#FFA500',  # Orange
        'Sandy Loam': '#FF4500',  # OrangeRed
        'Sandy Clay Loam': '#DC143C',  # Crimson
        'Loam': '#8B0000',  # DarkRed
        'Sandy Clay': '#7CFC00',  # LawnGreen
        'Silt Loam': '#32CD32',  # LimeGreen
        'Silt': '#228B22',  # ForestGreen
        'Clay Loam': '#006400',  # DarkGreen
        'Silty Clay Loam': '#40E0D0',  # Turquoise
        'Silty Clay': '#20B2AA',  # LightSeaGreen
        'Clay': '#008080'  # Teal
    }

    # Create a scatter plot with hover tooltips
    fig = go.Figure()

    # Loop through each soil type and its associated data points
    for soil_type, y_values in data.items():
        if not y_values:
            continue
        x_value = soil_type
        labels = associated_labels[soil_type]
        color = color_scale[soil_type]

        # Create a scatter trace for each data point and set the hover text
        for i in range(len(y_values)):
            y_value = y_values[i]
            fig.add_trace(go.Scatter(
                x=[x_value],
                y=[y_value],
                mode='markers',
                marker=dict(size=10, color=color),
                text=[labels[i]],
                hovertemplate=f'Soil Type: {soil_type}<br>X: %{{x}}<br>Y: %{{y}}<br>Label: %{{text}}<extra></extra>',
                showlegend=False
            ))

    fig.update_layout(
        title=f'Scatter plot - Soil Type vs {option}',
        xaxis_title='Soil Type',
        yaxis_title=option
    )

    # Show the figure
    pio.show(fig)

def graph_scatter_plot(key_x, key_y, key_z, all_data):

    x_values = all_data[key_x]
    y_values = all_data[key_y]
    labels = all_data[key_z]
    new_labels = [(x + ' ' + y) for x, y in zip(all_data['field'], all_data['logger'])]
    # print(new_labels)

    # Filter out None values in x_values and y_values
    # filtered_data = [(x, y, label) for x, y, label in zip(x_values, y_values, labels) if x is not None and y is not None]
    filtered_data = [(x, y, label) for x, y, label in zip(x_values, y_values, new_labels) if
                     x is not None and y is not None]

    x_filtered = [x for x, _, _ in filtered_data]
    y_filtered = [y for _, y, _ in filtered_data]
    labels_filtered = [label for _, _, label in filtered_data]

    # Scatter plot
    scatter_trace = go.Scatter(
        x=x_filtered,
        y=y_filtered,
        mode='markers',
        marker=dict(size=8),
        text=labels_filtered,
        hoverinfo='text+x+y',
        name='Data Points'
    )

    # Calculate the best-fit line and R-squared using scipy.stats.linregress
    result = linregress(x_filtered, y_filtered)
    slope, intercept, r_value = result.slope, result.intercept, result.rvalue
    best_fit_line_y = [slope * x + intercept for x in x_filtered]

    best_fit_line_trace = go.Scatter(
        x=x_filtered,
        y=best_fit_line_y,
        mode='lines',
        marker=dict(color='red'),
        name='Best Fit Line'
    )

    data = [scatter_trace, best_fit_line_trace]

    layout = go.Layout(
        title=f'Scatter Plot - {key_x} vs {key_y}',
        xaxis=dict(title=f'{key_x} Values'),
        yaxis=dict(title=f'{key_y} Values'),
        hovermode='closest',
        annotations=[
            dict(
                x=0.05,  # Adjust these values to position the annotation
                y=0.98,
                xref='paper',
                yref='paper',
                text=f'R-squared: {r_value ** 2:.2f}',
                showarrow=False,
                font=dict(size=16)
            )
        ]
    )

    fig = go.Figure(data=data, layout=layout)
    pio.show(fig)
    # pyo.plot(fig, filename='scatter_plot_with_best_fit_line.html', auto_open=True)


def write_data_to_csv(total_rh_info: list[tuple], total_vpd_info: list[tuple]):
    """
    Method to output certain information passed to it to csv files

    :param total_rh_info: List of tuples defined as follows: [(temperature, average_rh, minimum_rh, maximum_rh),...]
    :param total_vpd_info: List of tuples defined as follows: [(temperature, average_vpd, minimum_vpd, maximum_vpd),...]
    """
    rh_file = '2022_rh_data.csv'
    vpd_file = '2022_vpd_data.csv'
    with open(rh_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Temperature', 'Average', 'Minimum', 'Maximum', 'Mode'])
        for temp, avg, minimum, maximum, mode in total_rh_info:
            writer.writerow([temp, avg, minimum, maximum, mode])
    with open(vpd_file, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['Temperature', 'Average', 'Minimum', 'Maximum', 'Mode'])
        for temp, avg, minimum, maximum, mode in total_vpd_info:
            writer.writerow([temp, avg, minimum, maximum, mode])


def data_from_install_to_uninstall(logger, raw_data):
    install_date = logger.install_date
    uninstall_date = logger.uninstall_date
    # print(install_date)
    # print(uninstall_date)

    clean_dict = {key: [] for key in raw_data.keys()}
    for ind, dp_date in enumerate(raw_data['dates']):
        if dp_date is None:
            print('----------------------NONE DATE')
        if install_date <= dp_date.date() <= uninstall_date:
            for key in clean_dict:
                if key != 'daily gallons':
                    # print(key)
                    clean_dict[key].append(raw_data[key][ind])
            # print()

    return clean_dict


def analyze_logger(
        logger,
        dxd_save_location: str,
        specific_year: int,
        analyze_ambient_temp: bool = False,
        analyze_rh: bool = False,
        analyze_vpd: bool = False,
        analyze_vwc: bool = False,
        analyze_canopy_temp: bool = False,
        analyze_psi:bool = False
):
    """
    Method to take in a logger object, a dxd_save_location and a specific_year integer and go and grab all that logger's
    data for that specific_year

    :param analyze_psi: Boolean - True if you want to perform this analysis
    :param analyze_canopy_temp: Boolean - True if you want to perform this analysis
    :param analyze_vwc: Boolean - True if you want to perform this analysis
    :param analyze_vpd: Boolean - True if you want to perform this analysis
    :param analyze_rh: Boolean - True if you want to perform this analysis
    :param analyze_ambient_temp: Boolean - True if you want to perform this analysis
    :param logger: Logger object we are processing
    :param dxd_save_location: String of the path for where we want to save dxd's
    :param specific_year: int of the year we want to grab data for
    :return: At the moment, returning several lists individually, will eventually clean up the return once I finish
    all the processing functions
    """
    print(f'Analyzing: {logger.field.name} - {logger.name} - {logger.id}...')
    print(f'Install: {logger.install_date.strftime("%m-%d")} \t Uninstall: {logger.uninstall_date.strftime("%m-%d")}')

    file_path = dxd_save_location + logger.id + '.dxd'
    response = False
    logger_total_data_points = 0
    if logger is not None:
        # If dxd already exists, don't re-download
        if os.path.isfile(file_path):
            print('Found dxd')
            response = True
        else:
            print("Didn't find dxd. Calling API")
            response = logger.get_logger_data(specific_mrid=0, dxd_save_location=dxd_save_location)
    # response = True

    # Loggers we want to skip due to issues during season:
    if logger.id == 'z6-11959' or logger.id == 'z6-12304':
        response = False

    if 'Lucero' in logger.grower.name:
        response = False

    if response:
        raw_dxd = logger.read_dxd(dxd_save_location=dxd_save_location)
        raw_data = logger.get_all_ports_information(raw_dxd, specific_year=specific_year)
        clean_data = data_from_install_to_uninstall(logger, raw_data)

        hours_in_opt_with_sun = None
        hours_in_opt_without_sun = None
        hours_rh_data = None
        hours_vpd_data = None
        hours_vwc_data = None
        psi_data = None
        logger_total_data_points = None
        # print(raw_data)
        # print(f'Field: {logger.field.name} - Logger: {logger.name}')
        if len(clean_data["dates"]) > 0:
            delta_between_install_uninstall = logger.uninstall_date - logger.install_date
            logger_total_data_points = len(clean_data["dates"])
            # print(f'Expected logger data points: {days_between_instanll_unintall.days * 24}')
            print(
                f'Data start: {clean_data["dates"][0].strftime("%m-%d")}\t| Data end: {clean_data["dates"][-1].strftime("%m-%d")}'
            )
            print(f'Data Points -> Expected  | \t Actual')
            print(f'    {(logger_total_data_points/(delta_between_install_uninstall.days * 24)) * 100:5.1f}%       {delta_between_install_uninstall.days * 24}    | \t  {logger_total_data_points}')
            # print(f'Total logger data points: {logger_total_data_points}')
            # print(f'Roughly {logger_total_data_points / 24:.1f} days')
            print(f'Yield -> Net: {str(logger.field.net_yield)} | Paid: {str(logger.field.paid_yield)}')

            all_year_sunrise_sunset_data = calculate_sunrise_sunset(specific_year, 'Los Angeles', 'USA')

            if analyze_ambient_temp:
                print(f'\tAnalyzing Ambient Temp...')
                hours_in_opt_with_sun, hours_in_opt_without_sun = ambient_temp_analysis(
                    clean_data, all_year_sunrise_sunset_data
                )
                print(f'\t\tTotal hours in optimal with sun: {hours_in_opt_with_sun}')
                print(f'\t\tTotal hours in optimal without sun: {hours_in_opt_without_sun}')
            else:
                print(f'\tNot Analyzing Ambient Temp')

            if analyze_rh:
                print(f'\tAnalyzing RH...')
                # Both hours_rh_data and hours_vpd_data are dictionaries of tuples with temperature from 77 - 95 as keys:
                # {77: (avg, min, max, mode), 78: (...), ..., 95: (...)}
                hours_rh_data = rh_analysis(clean_data, all_year_sunrise_sunset_data)
            else:
                print(f'\tNot Analyzing RH')

            if analyze_vpd:
                print(f'\tAnalyzing VPD...')
                # Both hours_rh_data and hours_vpd_data are dictionaries of tuples with temperature from 77 - 95 as keys:
                # {77: (avg, min, max, mode), 78: (...), ..., 95: (...)}
                hours_vpd_data = vpd_analysis(clean_data, all_year_sunrise_sunset_data)
            else:
                print(f'\tNot Analyzing VPD')

            if analyze_vwc:
                print(f'\tAnalyzing VWC...')
                hours_vwc_data = vwc_analysis(clean_data, logger)

            else:
                print(f'\tNot Analyzing VWC')

            if analyze_canopy_temp:
                print(f'\tAnalyzing Canopy Temp...')
                pass
            else:
                print(f'\tNot Analyzing Canopy Temp')

            if analyze_psi:
                print(f'\tAnalyzing PSI...')
                psi_data = psi_analysis(clean_data, logger)
            else:
                print(f'\tNot Analyzing PSI')

            return hours_in_opt_with_sun, hours_in_opt_without_sun, hours_rh_data, hours_vpd_data, hours_vwc_data, psi_data, logger_total_data_points
            # return None
    else:
        print('Response false')
        return None


def vwc_analysis(raw_data: dict, logger):

    planting_date = raw_data['dates'][0].date()
    harvest_date = raw_data['dates'][-1].date()

    vwc_optimum_lower = logger.soil.optimum_lower
    vwc_optimum_upper = logger.soil.optimum_upper

    vwc_1_in_optimum_hours = 0
    vwc_2_in_optimum_hours = 0
    vwc_3_in_optimum_hours = 0
    vwc_1_2_in_optimum_hours = 0
    vwc_2_3_in_optimum_hours = 0

    valid_vwc_1_values = 0
    valid_vwc_2_values = 0
    valid_vwc_3_values = 0
    valid_vwc_1_2_values = 0
    valid_vwc_2_3_values = 0

    total_data_points = len(raw_data['dates'])

    for ind, data_point in enumerate(raw_data['dates']):

        vwc_1 = raw_data['vwc_1'][ind]
        vwc_2 = raw_data['vwc_2'][ind]
        vwc_3 = raw_data['vwc_3'][ind]

        vwc_1_valid = vwc_1 is not None and vwc_1 != 'None'
        vwc_2_valid = vwc_2 is not None and vwc_2 != 'None'
        vwc_3_valid = vwc_3 is not None and vwc_3 != 'None'
        vwc_1_2_valid = False
        vwc_2_3_valid = False

        if vwc_1_valid and vwc_2_valid:
            vwc_1_2_valid = True
            vwc_1_2 = (vwc_1 + vwc_2) / 2

        if vwc_2_valid and vwc_3_valid:
            vwc_2_3_valid = True
            vwc_2_3 = (vwc_2 + vwc_3) / 2

        if vwc_1_valid:
            valid_vwc_1_values += 1
            if vwc_optimum_lower <= vwc_1 <= vwc_optimum_upper:
                vwc_1_in_optimum_hours += 1

        if vwc_2_valid:
            valid_vwc_2_values += 1
            if vwc_optimum_lower <= vwc_2 <= vwc_optimum_upper:
                vwc_2_in_optimum_hours += 1

        if vwc_3_valid:
            valid_vwc_3_values += 1
            if vwc_optimum_lower <= vwc_3 <= vwc_optimum_upper:
                vwc_3_in_optimum_hours += 1

        if vwc_1_2_valid:
            valid_vwc_1_2_values += 1
            if vwc_optimum_lower <= vwc_1_2 <= vwc_optimum_upper:
                vwc_1_2_in_optimum_hours += 1

        if vwc_2_3_valid:
            valid_vwc_2_3_values += 1
            if vwc_optimum_lower <= vwc_2_3 <= vwc_optimum_upper:
                vwc_2_3_in_optimum_hours += 1

    vwc_total_valid_data_points = min(valid_vwc_1_values, valid_vwc_2_values, valid_vwc_3_values, valid_vwc_1_2_values,
                                      valid_vwc_2_3_values)
    # print(f'\t\tVWC 1 valid values: {valid_vwc_1_values}/{total_data_points}')
    # print(f'\t\tVWC 2 valid values: {valid_vwc_2_values}/{total_data_points}')
    # print(f'\t\tVWC 3 valid values: {valid_vwc_3_values}/{total_data_points}')
    # print(f'\t\tVWC 1_2 valid values: {valid_vwc_1_2_values}/{total_data_points}')
    # print(f'\t\tVWC 2_3 valid values: {valid_vwc_2_3_values}/{total_data_points}')

    if vwc_total_valid_data_points > 0:
        print(f'\t\tVWC 1 in optimum: {(vwc_1_in_optimum_hours / vwc_total_valid_data_points) * 100:.1f} %')
        print(f'\t\tVWC 2 in optimum: {(vwc_2_in_optimum_hours / vwc_total_valid_data_points) * 100:.1f} %')
        print(f'\t\tVWC 3 in optimum: {(vwc_3_in_optimum_hours / vwc_total_valid_data_points) * 100:.1f} %')
        print(f'\t\tVWC 1_2 in optimum: {(vwc_1_2_in_optimum_hours / vwc_total_valid_data_points) * 100:.1f} %')
        print(f'\t\tVWC 2_3 in optimum: {(vwc_2_3_in_optimum_hours / vwc_total_valid_data_points) * 100:.1f} %')

    return vwc_1_in_optimum_hours, vwc_2_in_optimum_hours, vwc_3_in_optimum_hours, vwc_1_2_in_optimum_hours, vwc_2_3_in_optimum_hours, vwc_total_valid_data_points


def vpd_analysis(raw_data: dict, all_year_sunrise_sunset_data):
    """
    Method that goes through the raw_data and analyzes the critical vpd information

    :param raw_data: Dictionary with all the raw_data
    :return: optimum_temp_vpd_spread: Dictionary with keys for each temperature range with the analyzed vpd info for
    each. For example: {77: [average_vpd, minimum_vpd, maximum_vpd, mode_vpd], 78: [...], ...}
    """
    optimum_temp_vpd_spread = {
        77: [],
        78: [],
        79: [],
        80: [],
        81: [],
        82: [],
        83: [],
        84: [],
        85: [],
        86: [],
        87: [],
        88: [],
        89: [],
        90: [],
        91: [],
        92: [],
        93: [],
        94: [],
        95: []
    }

    for ind, data_point in enumerate(raw_data['dates']):
        month = data_point.month

        # Grab month, sunrise and sunset
        _, sunrise, sunset = all_year_sunrise_sunset_data[month - 1]

        # Since sunrise and sunset are values for the 15th of each month, replace the day part with the data_point day
        #   to be able to compare with < > since we only care about the time and month part
        sunrise = sunrise.replace(day=data_point.day)
        sunset = sunset.replace(day=data_point.day)

        ambient_temp_dp = raw_data['ambient temperature'][ind]

        utc = pytz.UTC
        if ambient_temp_dp != 'None' and ambient_temp_dp is not None:
            # Need to add utc info to be able to compare with sunrise and sunset
            data_point = data_point.replace(tzinfo=utc)

            # During sunlight
            if sunrise < data_point < sunset:
                # We want to grab each vpd value in each temperature range to then find averages, minimums, maximums
                # and frequency of vpd in each
                if 77 <= ambient_temp_dp < 78:
                    optimum_temp_vpd_spread[77].append(round(raw_data['vpd'][ind], 1))

                if 78 <= ambient_temp_dp < 79:
                    optimum_temp_vpd_spread[78].append(round(raw_data['vpd'][ind], 1))

                if 79 <= ambient_temp_dp < 80:
                    optimum_temp_vpd_spread[79].append(round(raw_data['vpd'][ind], 1))

                if 80 <= ambient_temp_dp < 81:
                    optimum_temp_vpd_spread[80].append(round(raw_data['vpd'][ind], 1))

                if 81 <= ambient_temp_dp < 82:
                    optimum_temp_vpd_spread[81].append(round(raw_data['vpd'][ind], 1))

                if 82 <= ambient_temp_dp < 83:
                    optimum_temp_vpd_spread[82].append(round(raw_data['vpd'][ind], 1))

                if 83 <= ambient_temp_dp < 84:
                    optimum_temp_vpd_spread[83].append(round(raw_data['vpd'][ind], 1))

                if 84 <= ambient_temp_dp < 85:
                    optimum_temp_vpd_spread[84].append(round(raw_data['vpd'][ind], 1))

                if 85 <= ambient_temp_dp < 86:
                    optimum_temp_vpd_spread[85].append(round(raw_data['vpd'][ind], 1))

                if 86 <= ambient_temp_dp < 87:
                    optimum_temp_vpd_spread[86].append(round(raw_data['vpd'][ind], 1))

                if 87 <= ambient_temp_dp < 88:
                    optimum_temp_vpd_spread[87].append(round(raw_data['vpd'][ind], 1))

                if 88 <= ambient_temp_dp < 89:
                    optimum_temp_vpd_spread[88].append(round(raw_data['vpd'][ind], 1))

                if 89 <= ambient_temp_dp < 90:
                    optimum_temp_vpd_spread[89].append(round(raw_data['vpd'][ind], 1))

                if 90 <= ambient_temp_dp < 91:
                    optimum_temp_vpd_spread[90].append(round(raw_data['vpd'][ind], 1))

                if 91 <= ambient_temp_dp < 92:
                    optimum_temp_vpd_spread[91].append(round(raw_data['vpd'][ind], 1))

                if 92 <= ambient_temp_dp < 93:
                    optimum_temp_vpd_spread[92].append(round(raw_data['vpd'][ind], 1))

                if 93 <= ambient_temp_dp < 94:
                    optimum_temp_vpd_spread[93].append(round(raw_data['vpd'][ind], 1))

                if 94 <= ambient_temp_dp < 95:
                    optimum_temp_vpd_spread[94].append(round(raw_data['vpd'][ind], 1))

                if 95 <= ambient_temp_dp < 96:
                    optimum_temp_vpd_spread[95].append(round(raw_data['vpd'][ind], 1))

    # plt.plot(optimum_temp_vpd_spread[77])
    # plt.xlabel("Index")
    # plt.ylabel("VPD")
    # plt.title("Plot of VPD values at temp of 77 F")
    #
    # plt.show()
    avg_vpd = None
    minimum_vpd = None
    maximum_vpd = None
    mode_vpd = None

    for key in optimum_temp_vpd_spread:
        if len(optimum_temp_vpd_spread[key]) > 0:
            avg_vpd = round(statistics.mean(optimum_temp_vpd_spread[key]), 1)
            minimum_vpd = min(optimum_temp_vpd_spread[key])
            maximum_vpd = max(optimum_temp_vpd_spread[key])
            mode_vpd = statistics.mode(optimum_temp_vpd_spread[key])

        # print('------------------------------------------------------')
        # print(f'Temp: {key} -- Avg VPD: {avg_vpd}, min: {minimum_vpd}, max: {maximum_vpd}, mode: {mode_vpd}')
        # print('------------------------------------------------------')

        optimum_temp_vpd_spread[key] = (avg_vpd, minimum_vpd, maximum_vpd, mode_vpd)
    return optimum_temp_vpd_spread


def rh_analysis(raw_data: dict, all_year_sunrise_sunset_data: list[tuple]):
    """

    :param all_year_sunrise_sunset_data:
    :param raw_data: Dictionary with all the raw_data
    :return: optimum_temp_rh_spread: Dictionary with keys for each temperature range with the analyzed rh info for
    each. For example: {77: [average_rh, minimum_rh, maximum_rh, mode_rh], 78: [...], ...}
    """
    optimum_temp_rh_spread = {
        77: [],
        78: [],
        79: [],
        80: [],
        81: [],
        82: [],
        83: [],
        84: [],
        85: [],
        86: [],
        87: [],
        88: [],
        89: [],
        90: [],
        91: [],
        92: [],
        93: [],
        94: [],
        95: []
    }

    for ind, data_point in enumerate(raw_data['dates']):
        month = data_point.month

        # Grab month, sunrise and sunset
        _, sunrise, sunset = all_year_sunrise_sunset_data[month - 1]

        # Since sunrise and sunset are values for the 15th of each month, replace the day part with the data_point day
        #   to be able to compare with < > since we only care about the time and month part
        sunrise = sunrise.replace(day=data_point.day)
        sunset = sunset.replace(day=data_point.day)

        ambient_temp_dp = raw_data['ambient temperature'][ind]

        utc = pytz.UTC
        if ambient_temp_dp != 'None' and ambient_temp_dp is not None:
            # Need to add utc info to be able to compare with sunrise and sunset
            data_point = data_point.replace(tzinfo=utc)

            # During sunlight
            if sunrise < data_point < sunset:
                # We want to grab each rh value in each temperature range to then find averages, minimums, maximums
                # and frequency of rh in each
                if 77 <= ambient_temp_dp < 78:
                    optimum_temp_rh_spread[77].append(round(raw_data['rh'][ind], 1))

                if 78 <= ambient_temp_dp < 79:
                    optimum_temp_rh_spread[78].append(round(raw_data['rh'][ind], 1))

                if 79 <= ambient_temp_dp < 80:
                    optimum_temp_rh_spread[79].append(round(raw_data['rh'][ind], 1))

                if 80 <= ambient_temp_dp < 81:
                    optimum_temp_rh_spread[80].append(round(raw_data['rh'][ind], 1))

                if 81 <= ambient_temp_dp < 82:
                    optimum_temp_rh_spread[81].append(round(raw_data['rh'][ind], 1))

                if 82 <= ambient_temp_dp < 83:
                    optimum_temp_rh_spread[82].append(round(raw_data['rh'][ind], 1))

                if 83 <= ambient_temp_dp < 84:
                    optimum_temp_rh_spread[83].append(round(raw_data['rh'][ind], 1))

                if 84 <= ambient_temp_dp < 85:
                    optimum_temp_rh_spread[84].append(round(raw_data['rh'][ind], 1))

                if 85 <= ambient_temp_dp < 86:
                    optimum_temp_rh_spread[85].append(round(raw_data['rh'][ind], 1))

                if 86 <= ambient_temp_dp < 87:
                    optimum_temp_rh_spread[86].append(round(raw_data['rh'][ind], 1))

                if 87 <= ambient_temp_dp < 88:
                    optimum_temp_rh_spread[87].append(round(raw_data['rh'][ind], 1))

                if 88 <= ambient_temp_dp < 89:
                    optimum_temp_rh_spread[88].append(round(raw_data['rh'][ind], 1))

                if 89 <= ambient_temp_dp < 90:
                    optimum_temp_rh_spread[89].append(round(raw_data['rh'][ind], 1))

                if 90 <= ambient_temp_dp < 91:
                    optimum_temp_rh_spread[90].append(round(raw_data['rh'][ind], 1))

                if 91 <= ambient_temp_dp < 92:
                    optimum_temp_rh_spread[91].append(round(raw_data['rh'][ind], 1))

                if 92 <= ambient_temp_dp < 93:
                    optimum_temp_rh_spread[92].append(round(raw_data['rh'][ind], 1))

                if 93 <= ambient_temp_dp < 94:
                    optimum_temp_rh_spread[93].append(round(raw_data['rh'][ind], 1))

                if 94 <= ambient_temp_dp < 95:
                    optimum_temp_rh_spread[94].append(round(raw_data['rh'][ind], 1))

                if 95 <= ambient_temp_dp < 96:
                    optimum_temp_rh_spread[95].append(round(raw_data['rh'][ind], 1))

    # plt.plot(optimum_temp_rh_spread[77])
    # plt.xlabel("Index")
    # plt.ylabel("RH")
    # plt.title("Plot of RH values at temp of 77 F")
    #
    # plt.show()
    avg_rh = None
    minimum_rh = None
    maximum_rh = None
    mode_rh = None
    for key in optimum_temp_rh_spread:
        if len(optimum_temp_rh_spread[key]) > 0:
            avg_rh = round(statistics.mean(optimum_temp_rh_spread[key]), 1)
            minimum_rh = min(optimum_temp_rh_spread[key])
            maximum_rh = max(optimum_temp_rh_spread[key])
            mode_rh = statistics.mode(optimum_temp_rh_spread[key])

        # print('------------------------------------------------------')
        # print(f'Temp: {key} -- Avg RH: {avg_rh}, min: {minimum_rh}, max: {maximum_rh}, mode: {mode_rh}')
        # print('------------------------------------------------------')

        optimum_temp_rh_spread[key] = (avg_rh, minimum_rh, maximum_rh, mode_rh)
    return optimum_temp_rh_spread


def ambient_temp_analysis(raw_data: dict, all_year_sunrise_sunset_data: list[tuple]):
    """
    Go through all the raw_data and analyze pertinent information. For example: How many hours are spent in optimum
    tomato production ranges: 77-95 during sunlight.

    :param all_year_sunrise_sunset_data:
    :param raw_data: Dictionary with all the raw_data
    :return: hours_in_optimal_temp_with_sun, hours_in_optimal_temp_without_sun: Integers for each with the total number
    of hours spent in each.
    """
    hours_in_optimal_temp_with_sun = 0
    hours_in_optimal_temp_without_sun = 0
    daily_hours_in_optimal_temp_with_sun = 0
    daily_hours_in_optimal_temp_without_sun = 0
    totals_in_optimal_with_sun = []
    totals_in_optimal_without_sun = []
    temp_in_optimum_with_sun_list = []
    temp_in_optimum_without_sun_list = []
    temp_out_of_optimum_list = []

    # for month, sunrise, sunset in all_year_sunrise_sunset_data:
    #     print(f'Month: {month} - Sunrise: {sunrise.strftime("%H:%M")} | Sunset: {sunset.strftime("%H:%M")}')

    for ind, data_point in enumerate(raw_data['dates']):
        if ind == len(raw_data['dates']) - 1:
            break
        month = data_point.month
        next_data_point = raw_data['dates'][ind + 1]
        ambient_temp_dp = raw_data['ambient temperature'][ind]

        # Grab month, sunrise and sunset
        _, sunrise, sunset = all_year_sunrise_sunset_data[month - 1]

        # Since sunrise and sunset are values for the 15th of each month, replace the day part with the data_point day
        #   to be able to compare with < > since we only care about the time and month part
        sunrise = sunrise.replace(day=data_point.day)
        sunset = sunset.replace(day=data_point.day)

        utc = pytz.UTC
        if ambient_temp_dp != 'None' and ambient_temp_dp is not None:
            # Need to add utc info to be able to compare with sunrise and sunset
            data_point = data_point.replace(tzinfo=utc)

            # During sunlight
            if sunrise < data_point < sunset:
                # print(f"Sunrise {sunrise.strftime('%m/%d %H:%M')} < Date {data_point.strftime('%m/%d %H:%M')} < Sunset {sunset.strftime('%m/%d %H:%M')}")
                if 77 < ambient_temp_dp < 95:
                    # print(ambient_temp_dp)
                    # print(
                    #     f"Sunrise {sunrise.strftime('%m/%d %H:%M')} < Date {data_point.strftime('%m/%d %H:%M')} < Sunset {sunset.strftime('%m/%d %H:%M')}  == Temp: {ambient_temp_dp}"
                    # )
                    hours_in_optimal_temp_with_sun += 1
                    daily_hours_in_optimal_temp_with_sun += 1
                    temp_in_optimum_with_sun_list.append(round(ambient_temp_dp, 1))
                    if data_point.day != next_data_point.day:
                        if daily_hours_in_optimal_temp_with_sun > 0:
                            totals_in_optimal_with_sun.append(
                                (data_point, daily_hours_in_optimal_temp_with_sun, temp_in_optimum_with_sun_list)
                            )
                            daily_hours_in_optimal_temp_with_sun = 0
                            temp_in_optimum_with_sun_list = []
                else:
                    if data_point.day != next_data_point.day:
                        if daily_hours_in_optimal_temp_with_sun > 0:
                            totals_in_optimal_with_sun.append(
                                (data_point, daily_hours_in_optimal_temp_with_sun, temp_in_optimum_with_sun_list)
                            )
                            daily_hours_in_optimal_temp_with_sun = 0
                            temp_in_optimum_with_sun_list = []

            # After sunset or before sunrise
            elif data_point > sunset or data_point < sunrise:
                if 77 < ambient_temp_dp < 95:
                    # print(ambient_temp_dp)
                    # print(
                    #     f"\tSunrise {sunrise.strftime('%m/%d %H:%M')} < Date {data_point.strftime('%m/%d %H:%M')} < Sunset {sunset.strftime('%m/%d %H:%M')}  == Temp: {ambient_temp_dp}"
                    # )
                    hours_in_optimal_temp_without_sun += 1
                    daily_hours_in_optimal_temp_without_sun += 1
                    temp_in_optimum_without_sun_list.append(round(ambient_temp_dp, 1))
                    if data_point.day != next_data_point.day:
                        if daily_hours_in_optimal_temp_without_sun > 0:
                            totals_in_optimal_without_sun.append(
                                (data_point, daily_hours_in_optimal_temp_without_sun, temp_in_optimum_with_sun_list)
                            )
                            daily_hours_in_optimal_temp_without_sun = 0
                            temp_in_optimum_without_sun_list = []
                else:
                    if data_point.day != next_data_point.day:
                        if daily_hours_in_optimal_temp_without_sun > 0:
                            totals_in_optimal_without_sun.append(
                                (data_point, daily_hours_in_optimal_temp_without_sun, temp_in_optimum_with_sun_list)
                            )
                            daily_hours_in_optimal_temp_without_sun = 0
                            temp_in_optimum_without_sun_list = []

            # Getf
            # or optimum temperatures
            # raw_data['ambient temperature'][ind]

    # print(f'Total hours in optimal with sun: {hours_in_optimal_temp_with_sun}')
    # print(f'Total hours in optimal without sun: {hours_in_optimal_temp_without_sun}')
    # print()
    return hours_in_optimal_temp_with_sun, hours_in_optimal_temp_without_sun


def psi_analysis(raw_data: dict, logger):
    logger.ir_active = False
    highest_temp_values_ind, lowest_temp_values_ind, _ = \
        logger.cwsi_processor.get_highest_and_lowest_temperature_indexes(raw_data, mute_prints=True)
    final_results, _ = logger.cwsi_processor.final_results(
                            raw_data, highest_temp_values_ind, lowest_temp_values_ind, logger
                        )
    average_psi = get_average_psi(final_results['cwsi'], remove_days_at_start=10, remove_days_at_end=15)

    first_3_not_none_psi_values, first_3_not_none_psi_value_dates = get_first_not_none_x_values_and_dates(final_results, 3)

    # Find the amount of time we spend in the optimum
    days_in_ranges = find_days_in_different_ranges_psi(final_results, logger)


    print(f'Average PSI: {average_psi:.2f}')
    # print(f'First 3 PSI Dates: {first_3_not_none_psi_value_dates}')
    # print(f'First 3 PSI Values: {first_3_not_none_psi_values}')
    print(f'Amount of time in different ranges: {days_in_ranges}')

    return average_psi, first_3_not_none_psi_values, first_3_not_none_psi_value_dates, days_in_ranges

def find_days_in_different_ranges_psi(data: dict, logger):
    days_below_05 = 0
    days_05_to_1 = 0
    days_1_to_16 = 0
    days_16_to_20 = 0
    days_above_20 = 0

    valid_psi_data_points = 0

    for dp in data['cwsi']:
        if dp is not None and dp != "None":
            valid_psi_data_points += 1
            if 0 <= dp < 0.5:
                days_below_05 += 1
            elif 0.5 <= dp < 1:
                days_05_to_1 += 1
            elif 1 <= dp < 1.6:
                days_1_to_16 += 1
            elif 1.6 <= dp < 2.0:
                days_16_to_20 += 1
            elif dp >= 2.0:
                days_above_20 += 1

    if valid_psi_data_points > 0:
        print(f'\t\tPsi < 0.5:          {(days_below_05 / valid_psi_data_points) * 100:.1f} %  ({days_below_05} days)')
        print(f'\t\tPsi in 0.5 - 1.0:   {(days_05_to_1 / valid_psi_data_points) * 100:.1f} %  ({days_05_to_1} days)')
        print(f'\t\tPsi in 1.0 - 1.6:   {(days_1_to_16 / valid_psi_data_points) * 100:.1f} %  ({days_1_to_16} days)')
        print(f'\t\tPsi in 1.6 - 2.0:   {(days_16_to_20 / valid_psi_data_points) * 100:.1f} %  ({days_16_to_20} days)')
        print(f'\t\tPsi > 2.0:          {(days_above_20 / valid_psi_data_points) * 100:.1f} %  ({days_above_20} days)')

    return days_below_05, days_05_to_1, days_1_to_16, days_16_to_20, days_above_20, valid_psi_data_points


def get_first_not_none_x_values_and_dates(data: dict, x_values: int) -> tuple[list[float], list[datetime.date]]:
    dates_list = data['dates']
    psi_list = data['cwsi']

    non_none_psi_values = []
    dates_subset = []

    for psi, date in zip(psi_list, dates_list):
        if psi is not None:
            non_none_psi_values.append(psi)
            # If we want datetimes
            dates_subset.append(date)

            # If we want strings of the dates
            # dates_subset.append(date.strftime('%Y-%m-%d'))

    non_none_psi_values = non_none_psi_values[:x_values]
    dates_subset = dates_subset[:x_values]

    return non_none_psi_values, dates_subset


def get_average_psi(psi_list: dict, remove_days_at_start: int = 0, remove_days_at_end: int = 0) -> float:
    psi_list_clean = [x for x in psi_list if x is not None]
    psi_list_after_removal = psi_list_clean[remove_days_at_start:len(psi_list_clean) - remove_days_at_end]
    if len(psi_list_after_removal) == 0:
        return 0
    else:
        average = sum(psi_list_after_removal) / len(psi_list_after_removal)
        return average


def calculate_sunrise_sunset(year: int, city: str, country: str):
    """
    Method to find the sunrise and sunset given the year, city and country

    :param year: Int of the year
    :param city: String of the city
    :param country: String of the country
    :return: all_year_data: List of tuples (month, sunrise, sunset) for each month in number format 1-12
    """
    # Set your location (you can replace these values with your desired location)
    city = LocationInfo(city, country)
    # Set the year you want to calculate the sunrise and sunset times for
    year = year
    # Loop through each month in the year
    all_year_data = []
    for month in range(1, 13):
        # Calculate the middle day of the month
        day = 15

        # Create a date object for the middle day of the month
        date = datetime.date(year, month, day)

        # Calculate the sunrise and sunset times for the middle day of the month
        sun_times = sun(city.observer, date)

        # Extract the sunrise and sunset times
        sunrise = sun_times["sunrise"]
        sunset = sun_times["sunset"]
        sunrise_string = sunrise.strftime("%H:%M")
        sunset_string = sunset.strftime("%H:%M")

        datapoint = (month, sunrise, sunset)
        all_year_data.append(datapoint)

        # Print the sunrise and sunset times for the month
        # print(f"{month}\t{sunrise}\t{sunset}")
    return all_year_data


def bar_graph_data(totals_in_optimal: list[tuple]):
    """
    Graph information in bar chart

    :param totals_in_optimal: List of tuples (dates, hours_of_opt, temp_values)
    """
    dates, hours_of_opt, temp_values = zip(*totals_in_optimal)
    # Create a plot
    plt.bar(dates, hours_of_opt)
    # Set the x-axis label
    plt.xlabel("Date")
    # Set the y-axis label
    plt.ylabel("Hours of Optimum")
    # Set the plot title
    plt.title("Date vs Hours of Optimum")
    # Show the plot
    plt.show()


def line_graph_data(totals_in_optimal: list[tuple]):
    """
    Graph information in line graph

    :param totals_in_optimal: List of tuples (dates, hours_of_opt, temp_values)
    """
    dates, hours_of_opt, temp_values = zip(*totals_in_optimal)
    # Create a plot
    plt.plot(dates, hours_of_opt)
    # Set the x-axis label
    plt.xlabel("Date")
    # Set the y-axis label
    plt.ylabel("Hours of Optimum")
    # Set the plot title
    plt.title("Date vs Hours of Optimum")
    # Show the plot
    plt.show()


# For 2022 data
pickle_file_name = '2022_pickle.pickle'
pickle_file_path = 'H:\\Shared drives\\Stomato\\2022\\Pickle\\'
#
# analyze_year(pickle_file_name, pickle_file_path, 2022, analyze_psi=True, save_new_pickle=True, graph_data=True)
# analyze_year(pickle_file_name, pickle_file_path, 2022, analyze_vwc=True, graph_data=True, save_new_pickle=True)


analyze_year(pickle_file_name, pickle_file_path, 2022, graph_data=True, grab_data_from_pickle=True)





# all_data_pickle_file_name = 'all_2022_analysis.pickle'
# all_data_pickle_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Pickles\\'
# data = Decagon.open_pickle(filename=all_data_pickle_file_name, specific_file_path=all_data_pickle_file_path)
#
# for ind, dp in enumerate(data['field']):
#     print()
    # print(f"{dp}\t{data['logger'][ind]}\t{data['psi first 3 dates'][ind]}\t{data['psi first 3 values'][ind]}")

# pickle_file_name = '2023_pickle.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\2023\\Pickle\\'
# Decagon.show_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)
# growers = Decagon.open_pickle(pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             logger.consecutive_ir_values = deque()
# # #             logger.crop_type = logger.cropType
# Decagon.write_pickle(growers, filename=pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     if 'Lucero' in grower.name:
#         for field in grower.fields:
#             print(field.name, field.net_yield, field.paid_yield)
        # grower.to_string()
#         for field in grower.fields:
#             print(field.name)
#             for logger in field.loggers:
#                 if logger.name == 'GR-1S-S':
#                     print(logger.name)
#                     print(logger.field_capacity)
#                     print(logger.wilting_point)
#                     logger.field_capacity = logger.fieldCapacity = 18
#
#                     logger.wilting_point = logger.wiltingPoint = 8
#                     print(logger.field_capacity)
#                     print(logger.wilting_point)
                #     install = datetime.date(2022, 8, 11)
                #     logger.install_date = install
                #     print(logger.name, logger.id)
                #     print(logger.install_date)
                #     print()
#
# Decagon.write_pickle(growers, filename=pickle_file_name, specific_file_path=pickle_file_path)

# growers = Decagon.open_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         if field.name == 'Lucero BakersfieldTowerline':
#             print('BEFORE---------------')
#             print(len(field.loggers))
#             field.to_string()
#             # field.loggers.pop(5)
#             # field.loggers.pop(3)
#             print()
#             print('AFTER-------------')
#             print(len(field.loggers))
#             field.to_string()
# Decagon.write_pickle(growers, filename=pickle_file_name, specific_file_path=pickle_file_path)


# logger_id_dict = {}
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             if not logger.rnd:
#                 if logger.id in logger_id_dict.keys():
#                     # if (str(logger.field.name + logger.name)) not in logger_id_dict[logger.id]:
#                         logger_id_dict[logger.id].append(str(logger.field.name + logger.name))
#                 else:
#                     logger_id_dict[logger.id] = [str(logger.field.name + logger.name)]
#         #
# for key in logger_id_dict:
#     if len(logger_id_dict[key]) > 1:
#         print(key, logger_id_dict[key])
#         print()
# pprint.pprint(logger_id_dict)

# pickle_file_name = '2023_pickle.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\2023\\Pickle\\'
# growers = Decagon.open_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             # logger.gpm = float(logger.gpm)
#
#             print(type(logger.gpm))
# Decagon.write_specific_pickle(growers, pickle_file_name, specific_file_path=pickle_file_path)
#
# pickle_file_name = '2023_pickle.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\2023\\Pickle\\'
# growers = Decagon.open_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             logger.uninstall_date = None
# Decagon.write_specific_pickle(growers, pickle_file_name, specific_file_path=pickle_file_path)

# all_data_pickle_file_name = 'all_2022_analysis.pickle'
# all_data_pickle_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Pickles\\'
# all_data = Decagon.open_specific_pickle(all_data_pickle_file_name, all_data_pickle_file_path)

# pickle_file_name = '2022_pickle.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\2022\\Pickle\\'
# all_data_pickle_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Dxd Files\\2022\\'
# growers = Decagon.open_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         if field.crop_type in ['Tomato', 'Tomatoes', 'tomato', 'tomatoes']:
#             for logger in field.loggers:
                # if logger.name in ['IRR-1-80-AI', 'IRR-1-80-Ctrl','IRR-2-60-AI','IRR-2-60-Ctrl','IRR-3-80-AI','IRR-3-80-Ctrl','IRR-4-60-AI','IRR-4-60-Ctrl','IRR-5-80-AI','IRR-5-80-Ctrl']:
                # if not logger.rnd:
                #     print(f' {logger.name} \t {logger.install_date} \t {logger.uninstall_date}')
# Decagon.write_specific_pickle(growers, pickle_file_name, specific_file_path=pickle_file_path)
#             if logger.name == 'HU-304_3-NW':
#                 raw_dxd = logger.read_dxd(dxd_save_location=all_data_pickle_file_path)
#                 raw_data = logger.get_all_ports_information(raw_dxd, specific_year=2022)
#
#                 dates = raw_data['dates']
#                 temp = raw_data['ambient temperature']
#                 vwc_1 = raw_data['vwc_1']
#                 for ind, val in enumerate(temp):
#                     date = raw_data['dates'][ind]
#                     vwc_1 = raw_data['vwc_1'][ind]
#                     b4 = datetime.datetime(2022, 8, 12)
#                     after = datetime.datetime(2022, 8, 17)
#                     if (b4 < date < after):
#                         print(raw_data['dates'][ind])
#                         print(val)
#                         print(vwc_1)
#                         print()
                # print(dates)
                # print(temp)
#
# Decagon.show_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)

# pickle_file_name = '2023_pickle.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\2023\\Pickle\\'
# growers = Decagon.open_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)

# Decagon.show_specific_pickle(pickle_file_name, specific_file_path=pickle_file_path)

# logger_id_dict = {}
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             if not logger.rnd:
#                 if logger.id in logger_id_dict.keys():
#                     if (str(logger.field.name + logger.name)) not in logger_id_dict[logger.id]:
#                         logger_id_dict[logger.id].append(str(logger.field.name + logger.name))
#                 else:
#                     logger_id_dict[logger.id] = [str(logger.field.name + logger.name)]
#         #
# for key in logger_id_dict:
#     if len(logger_id_dict[key]) > 1:
#         print(logger_id_dict[key])
#         print()
# pprint.pprint(logger_id_dict)


# print()

# for key in all_data.keys():
#     print(key)

# print('BOTTOM LEFT QUADRANT')
# for ind, dp in enumerate(all_data['logger']):
#     if all_data["paid yield"][ind] is not None and all_data["vwc 2_3 in optimum %"][ind] is not None:
#         if all_data["paid yield"][ind] < 20 and all_data["vwc 2_3 in optimum %"][ind] < 60:
#             print(f'{all_data["field"][ind]} - {all_data["paid yield"][ind]}')
#
# print()
#
# print('TOP RIGHT QUADRANT')
# for ind, dp in enumerate(all_data['logger']):
#     if all_data["paid yield"][ind] is not None and all_data["vwc 2_3 in optimum %"][ind] is not None:
#         if all_data["paid yield"][ind] > 60 and all_data["vwc 2_3 in optimum %"][ind] > 50:
#             print(f'{all_data["field"][ind]} - {all_data["paid yield"][ind]}')
#
# print()
#
# print('BOTTOM QUADRANT')
# for ind, dp in enumerate(all_data['logger']):
#     if all_data["paid yield"][ind] is not None and all_data["vwc 2_3 in optimum %"][ind] is not None:
#         if all_data["paid yield"][ind] < 60 and \
#                 all_data["paid yield"][ind] > 30 and \
#                 all_data["vwc 2_3 in optimum %"][ind] < 40:
#             print(f'{all_data["field"][ind]} - {all_data["paid yield"][ind]}')
#
# print()
#
# print('LUCERO')
# for ind, dp in enumerate(all_data['logger']):
#     if all_data["paid yield"][ind] is not None and all_data["vwc 2_3 in optimum %"][ind] is not None:
#         if 'Lucero' in all_data["grower"][ind]:
#             print(f'{all_data["field"][ind]} - {all_data["paid yield"][ind]}')

    # print(f'Yield: {all_data["net yield"][ind]}')
    # print(f'Days of vwc data: {round(dp / 24, 1)}')

# for key in all_data:
#     if key == 'vwc total datapoints':
#         print(key)
#         for dp, ind in enumerate(all_data[key]):
#             print(f'Yield: {all_data["net yield"][ind]}')
#             print(f'Days of vwc data: {round(dp/24, 1)}')
#         print()

# For 2021 data
# pickle_file_name = 'entry.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\2021\\Pickle\\'
# analyze_year(pickle_file_name, pickle_file_path, 2021)


# calculate_sunrise_sunset(2022, "Los Angeles", "USA")
# all_year_sunrise_sunset_data = calculate_sunrise_sunset(2022, 'Los Angeles', 'USA')
# month, sunrise, sunset = all_year_sunrise_sunset_data[0]
# print(f'Month: {month} - Sunrise: {sunrise.strftime("%H:%M")} | Sunset: {sunset.strftime("%H:%M")}')
# for month, sunrise, sunset in all_year_sunrise_sunset_data:
#     print(f'Month: {month} - Sunrise: {sunrise.strftime("%H:%M")} | Sunset: {sunset.strftime("%H:%M")}')

# analyze_logger(
#     'BO-F7-SW', '2022_pickle.pickle', 'H:\\Shared drives\\Stomato\\2022\\Pickle\\',
#     'H:\\Shared drives\\Stomato\\2023\\Testing\\Dxd Files\\'
#     )
