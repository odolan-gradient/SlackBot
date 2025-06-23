from datetime import date


class Thresholds(object):
    cwsi_threshold = 1.5
    vwc_upper_threshold = 40
    vwc_lower_threshold = 5

    weather_threshold = 100
    consecutive_temps = 3

    tomato_psi_danger = 2.0
    permanent_psi_danger = 0.8


    #Tech

    error_sdd_upper = -5
    error_temp_lower = 0
    error_temp_upper = 130
    error_rh_lower = 0
    error_rh_upper = 100
    error_vpd_lower = 0
    error_vpd_upper = 7
    error_vwc_lower = 1
    error_vwc_upper = 60

    battery_threshold = 30

    # PSI Dates
    # almond window: March 15 – September 30
    almond_start_date = date(date.today().year, 3, 14)  # March 14
    almond_end_date = date(date.today().year, 10, 1)  # October 1

    # pistachio window: April 15 – Nov 1
    pistachio_start_date = date(date.today().year, 4, 14)  # April 14
    pistachio_end_date = date(date.today().year, 11, 1)  # November 1
