"""
Main worker file

"""
import csv
import datetime
import json
import math
import pickle
import time
from datetime import date, timedelta
from datetime import datetime
from itertools import zip_longest
from os import path
from pathlib import Path
from shutil import copyfile

import matplotlib.pyplot as plt
import pandas as pd
from google.cloud import bigquery

import Technician
from CIMIS import CIMIS
from CimisStation import CimisStation
from CwsiProcessor import CwsiProcessor
from DBWriter import DBWriter
from Field import Field
from Grower import Grower
from IrrigationRecommendationExpert import IrrigationRecommendationExpert
from Logger import Logger, DXD_DIRECTORY
from Saulisms import Saulisms
from SwitchTestCase import SwitchTestCase
from Technician import Technician
from WeatherStation import WeatherStation

DIRECTORY_YEAR = "2024"
PICKLE_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Pickle\\"
BACKUP_PICKLE_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Pickle\\Backup\\"
PICKLE_NAME = DIRECTORY_YEAR + "_pickle.pickle"
PICKLE_PATH = PICKLE_DIRECTORY + PICKLE_NAME

NOTIFICATIONS_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Notifications"


def open_pickle(filename: str = PICKLE_NAME, specific_file_path: str = PICKLE_DIRECTORY):
    """
    Function to open a pickle and return its contents.

    :return:
        List fields
    """

    if path.exists(specific_file_path + filename):
        with open(specific_file_path + filename, 'rb') as f:
            content = pickle.load(f)
        return content


def write_pickle(data, filename: str = PICKLE_NAME, specific_file_path: str = PICKLE_DIRECTORY):
    """
    Function to write to a pickle.

    A pickle is a form of permanent storage used to store any data structure. In this case, it's storing
    the list of fields.

    :param specific_file_path:
    :param filename:
    :param data: List that you want to have writen
    :return:
    """

    # Backup the old pickle before writing to it
    # backup_pickle()

    if path.exists(specific_file_path):
        with open(specific_file_path + filename, 'wb') as f:
            pickle.dump(data, f)


def backup_pickle(specific_name=None):
    """

    :return:
    """
    now = datetime.today()
    if specific_name is not None:
        file_name = specific_name + "_pickle_backup_" + str(now.strftime("%m-%d-%y  %I_%M_%S %p")) + ".pickle"
    else:
        file_name = "pickle_backup_" + str(now.strftime("%m-%d-%y  %I_%M_%S %p")) + ".pickle"

    print('Backing up Pickle...')

    # Check if the pickle we want to copy exists and if it does, copy it
    if path.exists(PICKLE_PATH):
        copyfile(
            PICKLE_PATH,
            BACKUP_PICKLE_DIRECTORY + file_name
        )

    print('Pickle Backed Up - ', file_name)


def reset_notifications(technicians: list[Technician]):
    """
    Resets notifications for each technician

    :param technicians: List of Technician
    :return: None
    """
    for tech in technicians:
        # tech.all_notifications = AllNotifications()
        tech.all_notifications.clear_all_notifications()


def get_all_technicians(growers: list[Grower]) -> list[Technician]:
    """
    Get a list of all technicians

    :param growers: List of Growers
    :return: List of Technicians
    """
    all_technicians = []
    for grower in growers:
        if grower.technician not in all_technicians and grower.technician is not None:
            all_technicians.append(grower.technician)
    return all_technicians


def update_et_information(
        get_et: bool = False,
        write_to_db: bool = False,
        start_date=None,
        end_date=None,
        window: int = 10
):
    if get_et:
        yesterdayRaw = date.today() - timedelta(1)
        if start_date is None:
            start_date = yesterdayRaw
        if end_date is None:
            end_date = yesterdayRaw
        start_date = start_date - timedelta(days=window)
        try:
            all_et_data_dicts = pull_all_et_values(str(start_date), str(end_date))
        except Exception as error:
            print('ERROR in get_et')
            print(error)
    if get_et and write_to_db:
        try:
            write_all_et_values_to_db(all_et_data_dicts)
        except Exception as error:
            print('ERROR in write_et_to_db')
            print(error)


def update_information(get_weather: bool = False, get_data: bool = False, write_to_portal: bool = False,
                       write_to_db: bool = False, check_for_notifications: bool = False,
                       email_notifications: bool = False, all_params: bool = False):
    """
    Function to update information from each field with a trial.

    :return:
    """

    # OLD WAY OF SETTING THE CREDENTIALS FOR GOOGLE CLOUD
    # if os.path.exists('C:\\Users\\javie\\Projects\\S-TOMAto\\credentials.json'):
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/javie/Projects/S-TOMAto/credentials.json"
    # elif os.path.exists('C:\\Users\\javie\\PycharmProjects\\Stomato\\credentials.json'):
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/javie/PycharmProjects/Stomato/credentials.json"
    # elif os.path.exists('C:\\Users\\jsalcedo\\PycharmProjects\\Stomato\\credentials.json'):
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/jsalcedo/PycharmProjects/Stomato/credentials.json"
    # elif os.path.exists('C:\\Users\\jesus\\PycharmProjects\\Stomato\\credentials.json'):
    #     os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/jesus/PycharmProjects/Stomato/credentials.json"

    now = datetime.today()
    print(">>>>>>>>>>>>>>>>>>>S-TOMAto Program<<<<<<<<<<<<<<<<<<<")
    print("                                 - " + now.strftime("%m/%d/%y  %I:%M %p"))
    print()
    print()

    update_information_start_time = time.time()

    if all_params:
        get_data = True
        get_weather = True
        write_to_portal = True
        write_to_db = True
        check_for_notifications = True
        email_notifications = True

    # Get growers from pickle
    growers = open_pickle()

    # Backup current pickle
    backup_pickle()
    print()

    # Grab the cimis stations pickle to pass in to Logger for ET data updating
    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")

    # Iterate through growers and call update on each field which will in turn
    #  call update on each logger in each field
    for ind, grower in enumerate(growers):
        # print(ind)
        # try:
        grower.update(cimis_stations_pickle, get_weather=get_weather, get_data=get_data,
                      write_to_portal=write_to_portal, write_to_db=write_to_db,
                      check_for_notifications=check_for_notifications)
        # except Exception as e:
        # Adding so any
        # print("Error in " + str(g.name) + " . update")
        # print("Error type: " + str(e))
        # print("Writing growers")
        write_pickle(growers)

        if check_for_notifications:
            if hasattr(grower, 'technician'):
                technician = grower.technician
                # technician.all_notifications.write_all_notifications_to_txt(technician.name, g.name)
                # technician.all_notifications.write_all_notifications_to_html(technician.name, g.name)
                technician.all_notifications.write_all_notifications_to_html_v2(technician.name, grower.name)
                technician.all_notifications.clear_all_notifications()

    if check_for_notifications and email_notifications:
        all_technicians = get_all_technicians(growers)
        for tech in all_technicians:
            # list_of_notifcation_files.append(tech.notification_file_path)
            tech.all_notifications.email_all_notifications(tech.name, tech.email, file_type='html')

    # Write pickle with updated information after update
    print('Writing data to pickle-')
    write_pickle(growers)

    update_information_end_time = time.time()
    print("----------FINISHED----------")
    update_information_elapsed_time_seconds = update_information_end_time - update_information_start_time

    update_information_elapsed_time_hours = int(update_information_elapsed_time_seconds // 3600)
    update_information_elapsed_time_minutes = int((update_information_elapsed_time_seconds % 3600) // 60)
    update_information_elapsed_time_seconds = int(update_information_elapsed_time_seconds % 60)

    print(f"Update Information execution time: {update_information_elapsed_time_hours}:"
          + f"{update_information_elapsed_time_minutes}:"
          + f"{update_information_elapsed_time_seconds} (hours:minutes:seconds)")
    print()
    print()


def notifications_setup(growers, technicians, file_type='txt'):
    """
    Setup notifications files for each technician

    :param growers: List of growers
    :param technicians: List of technicians
    :return:
    """
    now = datetime.today()
    # Clear previous notifications
    growers[0].all_notifications.clear_all_notifications()

    notif_folder = Path(NOTIFICATIONS_DIRECTORY)

    saulisms = Saulisms()

    # Setup Tech Notifications
    for tech in technicians:
        technician_name = tech.name
        saying, saying_date = saulisms.get_random_saulism()

        # SENSOR ERROR
        sensor_error_notif_folder = Path.joinpath(notif_folder, 'Sensor Error')
        sensor_error_file_name = technician_name + "_sensor_error_notifications_" + str(
            now.strftime("%m-%d-%y")
        ) + "." + file_type
        sensor_error_file_path = sensor_error_notif_folder / sensor_error_file_name

        if file_type == 'txt':
            with open(sensor_error_file_path, 'a') as the_file:
                the_file.write("\n")
                the_file.write(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  SENSOR ERRORS  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
                )
                the_file.write(
                    "                                                      " + now.strftime("%m/%d/%y  %I:%M %p")
                )
        elif file_type == 'html':
            with open(sensor_error_file_path, 'a') as the_file:
                the_file.write("<!DOCTYPE html>\n")
                the_file.write("<html>\n")
                the_file.write("<head>\n")
                the_file.write("<title>Sensor Error Notifications</title>\n")
                the_file.write("</head>\n")
                the_file.write("<body>\n")
                the_file.write("<style>\n")
                the_file.write("table { table-layout: fixed; }\n")
                the_file.write("table, th, td {border: 1px solid black; border-collapse: collapse;}\n")
                the_file.write("th, td {padding: 15px;}\n")
                the_file.write("tr:nth-child(even) {background-color: #F0F8FF;}\n")
                the_file.write("</style>\n")
                the_file.write("<h2>SENSOR ERRORS</h2>\n")
                the_file.write(f"<h2>{now.strftime('%m/%d/%y  %I:%M %p')}</h2>\n")
                the_file.write(f"<h2 style='font-style: italic; font-size: 150%;'>\"{saying}\", {saying_date}</h2>\n")
                the_file.write("<hr>\n")

        # TECH WARNINGS
        # Disabling for the time being
        # tech_warning_notif_folder = Path.joinpath(notif_folder, 'Tech Warning')
        # tech_warning_file_name = technician_name + "_tech_warning_notifications_" + str(
        #     now.strftime("%m-%d-%y")
        # ) + ".txt"
        # tech_warning_file_path = tech_warning_notif_folder / tech_warning_file_name
        #
        # with open(tech_warning_file_path, 'a') as the_file:
        #     the_file.write("\n")
        #     the_file.write(
        #         ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  TECHNICIAN WARNINGS  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
        #     )
        #     the_file.write(
        #         "                                                      " + now.strftime("%m/%d/%y  %I:%M %p")
        #     )

        # LOGGER SETUPS
        logger_setups_notif_folder = Path.joinpath(notif_folder, 'Logger Setups')
        logger_setups_file_name = technician_name + "_logger_setups_notifications_" + str(
            now.strftime("%m-%d-%y")
        ) + "." + file_type
        logger_setups_file_path = logger_setups_notif_folder / logger_setups_file_name

        if file_type == 'txt':
            with open(logger_setups_file_path, 'a') as the_file:
                the_file.write("\n")
                the_file.write(
                    ">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>  NEW FIELDS CREATED  <<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<\n"
                )
                the_file.write(
                    "                                                      " + now.strftime("%m/%d/%y  %I:%M %p")
                )
        elif file_type == 'html':
            with open(logger_setups_file_path, 'a') as the_file:
                the_file.write("<!DOCTYPE html>\n")
                the_file.write("<html>\n")
                the_file.write("<head>\n")
                the_file.write("<title>Logger Setups Notifications</title>\n")
                the_file.write("</head>\n")
                the_file.write("<body>\n")
                the_file.write("<style>\n")
                the_file.write("table { table-layout: fixed; }\n")
                the_file.write("table, th, td {border: 1px solid black; border-collapse: collapse;}\n")
                the_file.write("th, td {padding: 15px;}\n")
                the_file.write("tr:nth-child(even) {background-color: #F0F8FF;}\n")
                the_file.write("</style>\n")
                the_file.write("<h2>LOGGER SETUPS</h2>\n")
                the_file.write(f"<h2>{now.strftime('%m/%d/%y  %I:%M %p')}</h2>\n")
                the_file.write(f"<h2 style='font-style: italic; font-size: 150%;'>\"{saying}\", {saying_date}</h2>\n")


def add_field_to_grower(growerName, field):
    """


    :param growerName:
    :param field:
    :return:
    """
    growers = open_pickle()
    grower = None
    for item in growers:
        if item.name == growerName:
            grower = item
            break
        else:
            grower = None

    field.grower = grower
    for logger in field.loggers:
        if logger.grower is None:
            logger.grower = grower
        if logger.field is None:
            logger.field = field
    grower.fields.append(field)
    write_pickle(growers)


def remove_field(grower_name, field_name, avoid_user_input=False):
    """
    Function to remove field from a grower

    :param grower_name:
    :param field_name:
    :param avoid_user_input:
    """
    growers = open_pickle()
    for g in growers:
        if g.name == grower_name:
            for ind, f in enumerate(g.fields):
                if f.name == field_name:
                    print('Field found:')
                    f.to_string()
                    print()
                    print('About to remove ' + g.name + ' - ' + f.name + ' at index ' + str(ind))

                    if avoid_user_input:
                        confirm = 'y'
                    else:
                        confirm = input('Confirm? (Y/N) ').lower().strip()

                    if confirm[:1] == 'y':
                        print('Confirmed - Field removed')
                        del g.fields[ind]
                        write_pickle(growers)
                    elif confirm[:1] == 'n':
                        print('Canceled')
                    else:
                        print('Invalid Input')
                # else:
                # print('Field not found')


def deactivate_grower(grower_name: str) -> bool:
    """
    Function to deactivate a grower to ensure his fields do not get updated

    :param grower_name:
    :return:
    """
    success = False
    growers = open_pickle()
    for g in growers:
        if g.name == grower_name:
            print('Grower {} found:'.format(g.name))
            if g.active:
                g.deactivate()
                success = True
    write_pickle(growers)
    return success


def deactivate_field(grower_name: str, field_name: str) -> bool:
    """
    Function to deactivate a field so that the loggers in it no longer update

    :param grower_name: String of the grower name
    :param field_name: String of the field name
    :return: success - Boolean of whether a field was successfully deactivate or not
    """
    success = False
    growers = open_pickle()
    for g in growers:
        if g.name == grower_name:
            for f in g.fields:
                if f.name == field_name:
                    print('Field {} found:'.format(f.name))
                    if f.active:
                        f.deactivate()
                        success = True
    write_pickle(growers)
    return success


def deactivate_logger(grower_name: str, field_name: str, logger_id: str) -> bool:
    """
    Function to deactivate a logger so its data no longer gets processed

    :param grower_name:
    :param field_name:
    :param logger_id:
    :return: success - Boolean of whether a logger was successfully deactivated or not
    """
    success = False
    growers = open_pickle()
    for g in growers:
        if g.name == grower_name:
            for f in g.fields:
                if f.name == field_name:
                    for logger in f.loggers:
                        if logger.id == logger_id:
                            print('Logger {} found:'.format(logger.id))
                            if logger.active:
                                logger.deactivate()
                                success = True
    write_pickle(growers)
    return success


def remove_grower(grower_name: str) -> None:
    """
    Deprecated function. We now 'deactivate' the grower instead to leave them in the pickle
    Function to remove a grower from the pickle

    :param grower_name: String of the grower name
    """
    growers = open_pickle()
    for ind, g in enumerate(growers):
        if g.name == grower_name:
            print('Grower found:')
            g.to_string()
            print()
            print('About to remove ' + g.name + ' at index ' + str(ind))

            confirm = input('Confirm? (Y/N) ').lower().strip()
            if confirm[:1] == 'y':
                print('Confirmed - Grower removed')
                del growers[ind]
                write_pickle(growers)
            elif confirm[:1] == 'n':
                print('Canceled')
            else:
                print('Invalid Input')
    print('Growers remaining:')
    for g in growers:
        print(g.name)


def remove_last_grower():
    """
    Function to remove the last grower in the pickle.
    Typically, used to remove a grower that was added by mistake

    """
    growers = open_pickle()
    del growers[-1]
    write_pickle(growers)


def removeLogger(growerName: str, fieldName: str, loggerID: str) -> None:
    """
    Function to remove a logger from a grower's field

    :param growerName: String of the grower name
    :param fieldName: String of the field name
    :param loggerID: String of the logger id
    """
    growers = open_pickle()
    for g in growers:
        if g.name == growerName:
            for f in g.fields:
                if f.name == fieldName:
                    for ind, l in enumerate(f.loggers):
                        if l.id == loggerID:
                            print('Logger found:')
                            l.to_string()
                            print()
                            print(
                                'About to remove ' + g.name + ' - ' + f.name + ' - ' + l.id + ' at index ' + str(
                                    ind
                                )
                            )

                            confirm = input('Confirm? (Y/N) ').lower().strip()
                            if confirm[:1] == 'y':
                                print('Confirmed - Field removed')
                                del f.loggers[ind]
                                write_pickle(growers)
                            elif confirm[:1] == 'n':
                                print('Canceled')
                            else:
                                print('Invalid Input')


def addGrowerToGrowers(grower: Grower) -> None:
    """
    Function to add a grower to the growers in the pickle

    :param grower:
    """
    growers = open_pickle()

    growers.append(grower)

    write_pickle(growers)


def show_pickle(filename: str = PICKLE_NAME, specific_file_path: str = PICKLE_DIRECTORY):
    """
        Function to print out the contents of the pickle.

        :return:
    """
    data = open_pickle(filename=filename, specific_file_path=specific_file_path)
    print("PICKLE CONTENTS")
    pickle_contents = ''
    for d in data:
        pickle_contents += d.to_string()
    print(pickle_contents)
    return pickle_contents


def only_certain_growers_update(
        growerNames: list[str],
        get_weather: bool = False,
        get_data: bool = False,
        write_to_portal: bool = False,
        write_to_db: bool = False,
        check_for_notifications: bool = False,
        subtract_from_mrid: int = 0
) -> None:
    """
    Function to only update certain growers

    :param subtract_from_mrid: Int used to subtract a specific amount from the logger MRIDs for API calls
    :param growerNames: List of grower names in string form
    :param get_et: Boolean, True if you want to get ET, False otherwise
    :param get_weather: Boolean, True if you want to get Weather, False otherwise
    :param get_data: Boolean, True if you want to get Data, False otherwise
    :param write_to_portal: Boolean, True if you want to write data to grower portal, False otherwise
    :param write_to_db: Boolean, True if you want to write to DB, False otherwise
    :param check_for_notifications: Boolean, True if you want to check for notifications, False otherwise
    """
    # Grab the cimis stations pickle to pass in to Logger for ET data updating
    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")

    print("Only updating: " + str(growerNames))
    allGrowers = open_pickle()
    for g in allGrowers:
        if g.name in growerNames:
            g.updated = False
            for field in g.fields:
                field.updated = False
                for logger in field.loggers:
                    logger.updated = False
            g.update(
                cimis_stations_pickle,
                get_weather=get_weather,
                get_data=get_data,
                write_to_portal=write_to_portal,
                write_to_db=write_to_db,
                check_for_notifications=check_for_notifications,
                subtract_from_mrid=subtract_from_mrid
            )
    write_pickle(allGrowers)


def only_certain_growers_field_update(
        grower_name: str,
        field_name: str,
        get_weather: bool = False,
        get_data: bool = False,
        write_to_portal: bool = False,
        write_to_db: bool = False,
        check_for_notifications: bool = False,
        subtract_from_mrid: int = 0
) -> None:
    """
    Function to only update a certain field for a grower

    :param subtract_from_mrid:
    :param grower_name: String of grower name
    :param field_name: String of field name
    :param get_et: Boolean, True if you want to get ET, False otherwise
    :param get_weather: Boolean, True if you want to get Weather, False otherwise
    :param get_data: Boolean, True if you want to get Data, False otherwise
    :param write_to_portal: Boolean, True if you want to write data to grower portal sheet, False otherwise
    :param write_to_db: Boolean, True if you want to write to DB, False otherwise
    :param check_for_notifications: Boolean, True if you want to check for notifications, False otherwise
    """
    allGrowers = open_pickle()
    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
    for g in allGrowers:
        if g.name == grower_name:
            for f in g.fields:
                if f.name == field_name:
                    f.updated = False
                    for logger in f.loggers:
                        logger.updated = False
                    f.update(
                        cimis_stations_pickle=cimis_stations_pickle,
                        get_weather=get_weather,
                        get_data=get_data,
                        write_to_portal=write_to_portal,
                        write_to_db=write_to_db,
                        check_for_notifications=check_for_notifications,
                        subtract_from_mrid=subtract_from_mrid
                    )
    write_pickle(allGrowers)


def only_certain_growers_fields_update(
        fields: list[str],
        get_weather: bool = False,
        get_data: bool = False,
        write_to_portal: bool = False,
        write_to_db: bool = False,
        check_for_notifications: bool = False,
        subtract_from_mrid: int = 0,
        specific_mrid=None
) -> None:
    """
    Function to only update certain fields

    :param fields: List of strings
    :param get_et: Boolean, True if you want to get ET, False otherwise
    :param get_weather: Boolean, True if you want to get Weather, False otherwise
    :param get_data: Boolean, True if you want to get Data, False otherwise
    :param write_to_portal: Boolean, True if you want to write data to grower portal sheet, False otherwise
    :param write_to_db: Boolean, True if you want to write to DB, False otherwise
    :param check_for_notifications: Boolean, True if you want to check for notifications, False otherwise
    """
    allGrowers = open_pickle()
    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
    for g in allGrowers:
        for f in g.fields:
            if f.name in fields:
                f.updated = False
                for l in f.loggers:
                    l.updated = False
                f.update(
                    cimis_stations_pickle=cimis_stations_pickle,
                    get_weather=get_weather,
                    get_data=get_data,
                    write_to_portal=write_to_portal,
                    write_to_db=write_to_db,
                    check_for_notifications=check_for_notifications,
                    subtract_from_mrid=subtract_from_mrid,
                    specific_mrid=specific_mrid
                )
    write_pickle(allGrowers)


def only_certain_growers_field_logger_update(
        grower_name: str,
        field_name: str,
        logger_name: str = '',
        logger_id: str = '',
        write_to_db: bool = False,
        check_for_notifications: bool = False,
        specific_mrid: float = None,
        subtract_from_mrid: float = 0
) -> None:
    """
    Function to update a specific Logger

    :param grower_name: String of grower name
    :param field_name: String of field name
    :param logger_name: String of logger ID
    :param write_to_db: Boolean, True if you want to write to DB, False otherwise
    :param check_for_notifications: Boolean, True if you want to check notifications
    :param specific_mrid: Float value if you want to call METER API with a specific MRID
    :param subtract_from_mrid: Float value if you want to subtract a certain amount from the MRID
                                before calling the METER API
    """
    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
    all_growers = open_pickle()
    logger_to_update = None
    for grower in all_growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    field.updated = False
                    for logger in field.loggers:
                        if len(logger_name) > 0 and logger.name == logger_name:
                            logger_to_update = logger
                        if len(logger_id) > 0 and logger.id == logger_id:
                            logger_to_update = logger
                        if logger_to_update is not None:
                            logger_to_update.updated = False
                            logger_to_update.update(
                                cimis_stations_pickle,
                                write_to_db=write_to_db,
                                check_for_notifications=check_for_notifications, specific_mrid=specific_mrid,
                                subtract_from_mrid=subtract_from_mrid
                            )
                            break
    write_pickle(all_growers)


def setup_grower(grower_name: str, technician_name: str, email: str = '', region: str = '',
                 active: bool = True) -> Grower:
    """
    Function to create a Grower object

    :param technician_name: 
    :param grower_name: String of the grower name
    :param email: String of the grower email
    :param region: String of the grower region
    :param active: Boolean indicating if the grower is active
    :return:
    """
    fields = []

    tech = None
    growers = open_pickle()
    for grower in growers:
        if grower.technician.name == technician_name:
            tech = grower.technician
            break
    if tech is None:
        tech = Technician(technician_name, '')

    new_grower = Grower(grower_name, fields, tech, email, region=region, active=active)
    growers.append(new_grower)
    write_pickle(growers)
    return new_grower


def setup_field(
        field_name,
        lat,
        long,
        cimis_station,
        acres,
        crop_type,
        grower=None,
        active=True,
        field_type='Commercial'
):
    """
    Function to create a Field object

    :param field_type:
    :param crop_type:
    :param acres:
    :param field_name:
    :param lat:
    :param long:
    :param cimis_station:
    :param grower:
    :param active:
    :return:
    """
    loggers = []
    field = Field(field_name, loggers, lat, long, cimis_station, acres, crop_type, grower=grower, active=active,
                  field_type=field_type)
    return field


def setup_logger(logger_id, password, name, crop_type, soil_type, gpm, acres, loggerDirection, lat, long, install_date,
                 planting_date=None, grower=None, field=None, rnd=False, active=True):
    """
    Function to set up a new Logger

    :param install_date:
    :param soil_type:
    :param name:
    :param lat:
    :param long:
    :param loggerDirection:
    :param logger_id:
    :param password:
    :param crop_type:
    :param gpm:
    :param acres:
    :param planting_date:
    :param grower:
    :param field:
    :param rnd:
    :param active:
    :return:
    """
    logger = Logger(logger_id, password, name, crop_type, soil_type, gpm, acres, loggerDirection, install_date, lat,
                    long, grower=grower, field=field,
                    planting_date=planting_date, rnd=rnd, active=active)
    return logger


def set_loggers_rnd(logger_ids):
    """

    :param logger_ids: 
    """
    logger_ids_list = logger_ids
    growers = open_pickle()
    for grower in growers:
        for field in grower.fields:
            for logger in field.loggers:
                for logger_id in logger_ids_list:
                    if logger.id == logger_id:
                        print("Found id: " + str(logger_id) + " and set R&D to TRUE")
                        logger.rnd = True
                        logger_ids_list.remove(logger_id)
                        print(" Remaining IDs:" + str(logger_ids_list))
                        logger.to_string()
    write_pickle(growers)


def get_grower(grower_name: str) -> Grower:
    """
    Function to get a grower object from the pickle

    :param grower_name: String of grower name
    :return: Grower object
    """
    growers = open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            return grower


def get_gpm(field_name: str, logger_name: str) -> float:
    """
    Function to get the GPM for a logger in a field

    :param field_name: String of the field name
    :param logger_name: String of the logger name
    :return: float of the Gallons per Minute
    """
    growers = open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field_name:
                for log in f.loggers:
                    if log.name == logger_name:
                        # print(log.gpm)
                        # print(type(log.gpm))
                        return log.gpm


def get_acres(field_name: str, logger_name: str) -> float:
    """
    Function to get the acres for a logger in a field

    :param logger_name: String of the logger name
    :param field_name: String of the field name
    :return: Float of the acres
    """
    growers = open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field_name:
                for log in f.loggers:
                    if log.name == logger_name:
                        # print(log.acres)
                        return log.irrigation_set_acres


def show_grower(grower_name: str) -> None:
    """
    Function to call a grower's to_string()

    :param grower_name: String of the grower name
    """
    g = get_grower(grower_name)
    if g is not None:
        g.to_string()
    else:
        print('Grower -' + grower_name + '- not found')


def get_field(field_name: str, grower_name: str = '') -> Field:
    """
    Function to get a field

    :param field_name: String for the field name
    :param grower_name: Optional parameter of the string for the grower name
    :return: Field object of the field
    """
    if grower_name:
        grower = get_grower(grower_name)
        for field in grower.fields:
            if field.name == field_name:
                return field
    else:
        growers = open_pickle()
        for grower in growers:
            for field in grower.fields:
                if field.name == field_name:
                    return field


def show_field(field_name: str, grower_name: str = '') -> None:
    """
    Function to pring out to console a field's information (to_string*())

    :param field_name: String of the field name
    :param grower_name: String of the grower name
    """
    f = get_field(field_name, grower_name)
    if f is not None:
        f.to_string()
    else:
        print('Field -' + field_name + '- not found')


def write_new_historical_et_to_db(table, filename, historicalET="Historical_ET", overwrite=False):
    """
    Function to write mew historical ET table to DB

    :param table:
    :param filename:
    :param historicalET:
    :param overwrite:
    """
    schema = [
        bigquery.SchemaField("Year4", "DATE"),
        bigquery.SchemaField("Year4ET", "FLOAT"),
        bigquery.SchemaField("Year3", "DATE"),
        bigquery.SchemaField("Year3ET", "FLOAT"),
        bigquery.SchemaField("Year2", "DATE"),
        bigquery.SchemaField("Year2ET", "FLOAT"),
        bigquery.SchemaField("Year1", "DATE"),
        bigquery.SchemaField("Year1ET", "FLOAT"),
        bigquery.SchemaField("Average", "FLOAT"),
    ]

    dbwriter = DBWriter()
    project = 'stomato-info'
    dbwriter.write_to_table_from_csv(historicalET, table, filename, schema, project, overwrite=overwrite)


def write_new_historical_et_to_db_2(dataset_id, table, data, filename="HistoricalET.csv", overwrite=False):
    """
    Function writes irr scheduling data into csv then creates a db table from csv given a data table of dates and etos

    :param dataset_id:
    :param table: The station number
    :param data: Dictionary of dates and etos
    :param filename:
    :param overwrite:
    """
    print('\t-Writing data to csv')
    with open(filename, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(data.keys())
        writer.writerows(zip_longest(*data.values()))
    print('\t...Done - file: ' + filename)
    keys_list = list(data.keys())
    schema = [
        bigquery.SchemaField(keys_list[0], "DATE", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[1], "Float", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[2], "DATE", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[3], "Float", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[4], "DATE", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[5], "Float", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[6], "DATE", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[7], "Float", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[8], "DATE", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[9], "Float", mode="NULLABLE"),
        bigquery.SchemaField(keys_list[10], "Float", mode="NULLABLE"),
    ]
    dbwriter = DBWriter()
    print("\tWriting Data to DB")
    project = 'stomato-info'
    dbwriter.write_to_table_from_csv(dataset_id, table, filename, schema, project, overwrite=overwrite)


def update_irr_scheduling(table, fieldName, data, filename="irrScheduling.csv", overwrite=False, logger=None):
    """
    Function writes irr scheduling data into csv then creates a db table from csv

    :param table:
    :param fieldName:
    :param data:
    :param filename:
    :param overwrite:
    """
    print('\tWriting data to csv...')
    with open(filename, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(data.keys())
        writer.writerows(zip_longest(*data.values()))
    print('\t<-Writing done - file: ' + filename)
    schema = [
        bigquery.SchemaField("current_date", "DATE", mode="REQUIRED"),
        bigquery.SchemaField("historical_eto", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("kc", "FLOAT", mode="REQUIRED"),
        bigquery.SchemaField("historical_etc", "FLOAT", mode="NULLABLE"),
        bigquery.SchemaField("historical_hours", "FLOAT", mode="NULLABLE")
    ]
    dbwriter = DBWriter()
    print("\tWriting Irrigation Scheduling Table to DB...")
    # project = 'stomato-' + DATABASE_YEAR
    project = dbwriter.get_db_project(logger.crop_type)
    dbwriter.write_to_table_from_csv(fieldName, table, filename, schema, project, overwrite=overwrite)


def get_all_current_cimis_stations():
    """
    Get all the cimis stations for where we have equipment currently in the pickle

    :return:
    """
    cimis_stations = []
    growers = open_pickle()
    for g in growers:
        for f in g.fields:
            if f.cimis_station not in cimis_stations:
                cimis_stations.append(f.cimis_station)
    print('Cimis Stations currently in pickle: ' + str(cimis_stations))
    return cimis_stations


def pull_all_et_values(start_date, end_date):
    """
    Function to grab all ET values from a given starDate through to a given endDate
    for all CIMIS stations in our current pickle

    :param start_date:
    :param end_date:
    :return:
    """
    cimis = CIMIS()

    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")

    all_cimis_station_et = cimis.get_all_stations_et_data(cimis_stations_pickle, start_date, end_date)

    write_pickle(cimis_stations_pickle, filename="cimisStation.pickle")
    return all_cimis_station_et


def update_all_eto_values(start_date: str, end_date: str):
    """
    Function to grab all ETo values from a given start_date through to a given end_date
    for all CIMIS stations in our current pickle and update them to the DB to ensure we have current/correct values

    :param start_date: str indicating first day of data to pull from CIMIS
    :param end_date: str indicating the last day of data to pull from CIMIS
    :return:
    """
    record_limit = 1750
    cimis = CIMIS()
    cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
    # Convert the string dates to datetime objects
    date1 = datetime.strptime(start_date, '%Y-%m-%d')
    date2 = datetime.strptime(end_date, '%Y-%m-%d')
    # Calculate the difference between the two dates
    difference = date2 - date1

    station_limit = math.floor(record_limit / difference.days)
    all_cimis_station_eto = {}
    for i in range(0, len(cimis_stations_pickle), station_limit):
        current_pairs = cimis_stations_pickle[i:i + station_limit]
        print(f'Pulling data from CIMIS for {len(current_pairs)} stations')
        some_stations = cimis.get_all_stations_et_data(current_pairs, start_date, end_date)
        all_cimis_station_eto.update(some_stations)

    print(f'Preparing to write CIMIS stations data to DB')
    write_all_et_values_to_db(all_cimis_station_eto, overwrite=True)


def remove_duplicates_already_in_et_db(db_dates, table_id):
    """
    Removes duplicate rows in the ET DB table so new rows on those dates can be inserted
    :param db_dates: list of dates to delete from the DB table at the dataset id
    :param table_id:
    :return:
    """
    # Remove the rows first then append the new lines
    project = 'stomato-info'
    dbwriter = DBWriter()
    print()
    print('\tChecking for and removing duplicate ET data')
    first_date = db_dates[0]
    last_date = db_dates[-1]
    dml_statement = (f"Delete From `{project}.ET.{table_id}`"
                     f"Where Date between '{first_date}' and '{last_date}'")
    dbwriter.run_dml(dml_statement)

    print('\t\t<-Duplicate ET data removal complete.')


def write_all_et_values_to_db(all_cimis_station_et, overwrite: bool = False):
    """
    Write all ET values for all CIMIS stations in our current pickle
    to their corresponding ET tables in the DB

    :return:
    """
    print('\n\tWriting ET values to DB...')
    dbwriter = DBWriter()
    project = 'stomato-info'
    dataset_id = 'ET'
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("eto", "FLOAT"),
    ]
    filename = 'all et.csv'

    for et_data_dict_key in all_cimis_station_et:
        if all_cimis_station_et is not None:
            db_dates_list = [date for date in all_cimis_station_et[et_data_dict_key]['dates']]
            # Remove any duplicates in the DB based on the dates if ET table exists
            if dbwriter.check_if_table_exists(dataset_id, et_data_dict_key, project=project):
                remove_duplicates_already_in_et_db(db_dates_list, et_data_dict_key)

        # Write to the ET table in DB
        print(f'\tWriting station {et_data_dict_key} data to csv')
        keys = ["dates", "eto"]
        just_et_data = {key: all_cimis_station_et[et_data_dict_key][key] for key in keys}

        with open(filename, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(just_et_data.keys())
            writer.writerows(zip_longest(*just_et_data.values()))
        print('\t\t<-csv Done')

        table_id = et_data_dict_key
        print('\tWriting to DB')
        dbwriter.write_to_table_from_csv(dataset_id, table_id, filename, schema, project, overwrite)


def write_et_values_specific_station(start_date: str, end_date: str, cimis_station: str) -> None:
    """
    Get et for a specific station for a date range and write those values to the DB

    :param start_date:
    :param end_date:
    :param cimis_station:
    """
    stations = cimis_station
    c = CIMIS()
    dicts = c.getDictForStation(stations, start_date, end_date)
    dbwriter = DBWriter()
    for etDataDict in dicts:
        # write_alletsdicts_to_db(etDataDict)
        if etDataDict is not None:
            dataset_id = 'ET'
            table_id = etDataDict['station']
            schema = [
                bigquery.SchemaField("date", "DATE"),
                bigquery.SchemaField("eto", "FLOAT"),
            ]
            filename = 'all et.csv'
            print('- writing data to csv')
            keys = ["dates", "eto"]
            justEtData = {key: etDataDict[key] for key in keys}

            with open(filename, "w", newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(justEtData.keys())
                writer.writerows(zip_longest(*justEtData.values()))
            print('...Done - file: ' + filename)
            #
            project = 'stomato-info'
            dbwriter.write_to_table_from_csv(dataset_id, table_id, filename, schema, project)


def calculate_mrid_subtraction_for_date(startDate):
    """
    Calculate the subtraction necessary make to MRID to get values from a specific start date

    :param startDate:
    :return:
    """
    # Turn date string into datetimes
    startDateDT = datetime.strptime(startDate, "%Y-%m-%d")
    endDateDT = datetime.today()
    delta = endDateDT - startDateDT + timedelta(days=1)
    days = delta.days - 1
    # print(days)
    subMridResult = (days * 24)
    # print(subMridResult)

    return subMridResult


def get_previous_data_field(grower_name, field_name, startDate,
                            write_to_db=False, specific_mrid=None):
    """
    Grab data for a field from a specific start date

    :param grower_name:
    :param field_name:
    :param startDate:
    :param write_to_db:
    :param specific_mrid:
    """
    growers = open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        print("Updating Logger: " + logger.id)
                        print("Converting date to MRID")
                        print("Start Date: " + startDate)
                        mridResult = calculate_mrid_subtraction_for_date(startDate)
                        print("Subtract from MRID: " + str(mridResult))
                        print("This is previous day switch: " + str(logger.prev_day_switch))
                        print("Assigning new prev day switch")
                        logger.prev_day_switch = 0
                        print("New prev day switch: " + str(logger.prev_day_switch))
                        write_pickle(growers)
                        only_certain_growers_field_logger_update(
                            grower_name, field_name, logger_id=logger.id,
                            write_to_db=write_to_db,
                            specific_mrid=specific_mrid,
                            subtract_from_mrid=mridResult
                        )


def failed_cimis_update_et_from_prev_day_eto():
    """
    Update et tables for all fields

    """
    growers = open_pickle()
    for grower in growers:
        for field in grower.fields:
            for logger in field.loggers:
                print(f'\tUpdating et values in Logger {logger.name} table...')
                try:
                    logger.merge_et_db_with_logger_db_values()
                except Exception as err:
                    print(f"ET Did not update for this logger {logger.name}")
                    print(err)


def reset_updated_all():
    """
    Reset the updated boolean for all growers, fields and loggers

    """
    print('Resetting updated on all growers')
    growers = open_pickle()
    for g in growers:
        g.updated = False
        for f in g.fields:
            f.updated = False
            for logger in f.loggers:
                logger.updated = False
    write_pickle(growers)
    cimisStationsPickle = CimisStation.open_cimis_station_pickle(CimisStation)
    for stations in cimisStationsPickle:
        stations.updated = False
    write_pickle(cimisStationsPickle, filename="cimisStation.pickle")


def set_planting_date_for_field(grower_name, field_name, year, month, day):
    """
    Set a planting date for a field

    :param grower_name:
    :param field_name:
    :param year:
    :param month:
    :param day:
    """
    growers = open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        print("Old Planting Date: " + str(logger.planting_date))
                        # print(type(logger.planting_date))
                        newPlantingDate = datetime(year, month, day).date()
                        logger.planting_date = newPlantingDate
                        print("New Planting Date: " + str(logger.planting_date))
    write_pickle(growers)


def update_prev_switch(logger_id, switch):
    """


    :param logger_id:
    :param switch:
    """
    growers = open_pickle()
    for grower in growers:
        for field in grower.fields:
            for logger in field.loggers:
                if logger.id in logger_id:
                    print("Logger ID: " + str(logger.id))
                    print("Prev Switch: " + str(logger.prev_day_switch))
                    logger.prev_day_switch = switch
                    print("New Switch: " + str(logger.prev_day_switch))
    write_pickle(growers)


def check_successful_updated_growers():
    """
    Check and print out the number of growers that updated successfully based on their updated boolean

    :return:
    """
    growers = open_pickle()
    active_growers = get_number_of_active_growers()
    successfulGrowers = 0
    for g in growers:
        if g.updated and g.active:
            successfulGrowers = successfulGrowers + 1
    if successfulGrowers == len(active_growers):
        print("\tAll growers successful! ")
        print("\t{0}/{1}".format(successfulGrowers, active_growers))
        return True
    else:
        print("\t{0}/{1} active growers updated successfully".format(successfulGrowers, active_growers))
        return False


def get_number_of_active_growers() -> tuple[float, float]:
    """
    Function to calculate the number of active growers

    :return active_growers: Number of active growers
            inactive_growers: Number of inactive growers
    """
    active_growers = 0
    inactive_growers = 0
    growers = open_pickle()
    for grower in growers:
        if grower.active:
            active_growers += 1
        else:
            inactive_growers += 1
    return active_growers, inactive_growers


def get_number_of_active_fields_for_grower(grower: Grower) -> tuple[float, float]:
    """
    Function to calculate the number of active fields for a grower

    :param grower: Grower object
    :return active_fields: Number of active fields
            inactive_fields: Number of inactive fields
    """
    active_fields, inactive_fields = grower.get_number_of_active_fields()
    return active_fields, inactive_fields


def get_number_of_active_loggers_for_field(field: Field) -> tuple[float, float]:
    """
    Function to calculate the number of active and inactive logger for a field

    :param field: Field object
    :return active_loggers: Number of active logger
            inactive_loggers: Number of inactive loggers
    """
    active_loggers, inactive_loggers = field.get_number_of_active_loggers()
    return active_loggers, inactive_loggers


def updated_run_report() -> bool:
    """
    Function to show a report on how many growers/fields/loggers got updated successfully

    :return:
    """
    print('\t--Updated Run Report--')
    growers = open_pickle()
    successfulGrowers = 0
    successfulFields = 0
    successfulLoggers = 0
    number_of_active_growers, number_of_inactive_growers = get_number_of_active_growers()
    for g in growers:
        if g.active:
            if g.updated:
                successfulGrowers = successfulGrowers + 1
            else:
                print("\tGrower: -{0}- was unsuccessful in updating".format(g.name))
                for f in g.fields:
                    if f.active:
                        if f.updated:
                            successfulFields = successfulFields + 1
                        else:
                            print("\t due to Field: {0}".format(f.name))
                            for logger in f.loggers:
                                if logger.active:
                                    if logger.updated:
                                        successfulLoggers = successfulLoggers + 1
                                    else:
                                        print("\t  > Logger: {0}".format(logger.name))
    if successfulGrowers == number_of_active_growers:
        print('\t Clean run! All active growers updated successfully!')
        return True
    else:
        return False


def set_cimis_station(field_name, cimisStation):
    """
    Set the cimis station for a field

    :param field_name:
    :param cimisStation:
    """
    growers = open_pickle()
    for grower in growers:
        for field in grower.fields:
            if field.name == field_name:
                field.cimis_station = str(cimisStation)
                print("New Cimis Station: " + field.cimis_station)
    write_pickle(growers)


def apply_ai_recommendation_to_logger(project, dataset, logger_name):
    """
    Apply the AI Irrigation recommendation to a specific logger's data

    :param project:
    :param dataset:
    :param logger_name:
    """
    print(f'Grabbing data from {project} - {dataset} for logger {logger_name} - ')
    dml = 'SELECT *' \
          'FROM `' + project + '.' + dataset + '.' + logger_name + '` ORDER BY date DESC'
    # 'WHERE et_hours is not NULL ORDER BY date DESC'

    dbwriter = DBWriter()
    expertSys = IrrigationRecommendationExpert()
    result = dbwriter.run_dml(dml, project=project)
    applied_finals = {}
    ai_results = {"logger_id": [], "date": [], "time": [], "canopy_temperature": [], "ambient_temperature": [],
                  "vpd": [], "vwc_1": [], "vwc_2": [], "vwc_3": [], "field_capacity": [], "wilting_point": [],
                  "daily_gallons": [], "daily_switch": [], "daily_hours": [], "daily_pressure": [],
                  "daily_inches": [], "psi": [], "psi_threshold": [], "psi_critical": [],
                  "sdd": [], "rh": [], 'eto': [], 'kc': [], 'etc': [], 'et_hours': [],
                  "phase1_adjustment": [], "phase1_adjusted": [], "phase2_adjustment": [], "phase2_adjusted": [],
                  "phase3_adjustment": [], "phase3_adjusted": [], "vwc_1_ec": [], "vwc_2_ec": [], "vwc_3_ec": [],
                  "lowest_ambient_temperature": [], "gdd": [], "crop_stage": [], "id": [], "planting_date": [],
                  "variety": []}

    applied_finals['date'] = []
    applied_finals['base'] = []
    applied_finals['final_rec'] = []
    applied_finals['adjustment_values'] = []
    applied_finals['adjustment_steps'] = []

    planting_date = None
    crop_type = ''
    growers = open_pickle()
    for grower in growers:
        for field in grower.fields:
            field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
            if dataset in field_name:
                for logger in field.loggers:
                    planting_date = logger.planting_date
                    crop_type = logger.crop_type

    # harvest_date = result[-1][1]
    harvest_date = planting_date + timedelta(days=126)
    prev_et_hours = 0

    for r in result:
        logger_id = r[0]
        date = r[1]
        time = r[2]
        canopy_temperature = r[3]
        ambient_temperature = r[4]
        vpd = r[5]
        vwc_1 = r[6]
        vwc_2 = r[7]
        vwc_3 = r[8]
        field_capacity = r[9]
        wilting_point = r[10]
        daily_gallons = r[11]
        daily_switch = r[12]
        daily_hours = r[13]
        daily_pressure = r[14]
        daily_inches = r[15]
        psi = r[16]
        psi_threshold = r[17]
        psi_critical = r[18]
        sdd = r[19]
        rh = r[20]
        eto = r[21]
        kc = r[22]
        etc = r[23]
        et_hours = r[24]
        lta = r[34]
        gdd = r[35]
        crop_stage = r[36]
        id = r[37]
        t_planting_date = r[38]
        variety = r[39]

        rec = expertSys.make_recommendation(
            psi, field_capacity, wilting_point, vwc_1, vwc_2, vwc_3,
            crop='Tomatoes', date=date, planting_date=planting_date,
            harvest_date=harvest_date
        )
        if et_hours is None:
            et_hours = prev_et_hours
        applied_final, applied_steps = expertSys.apply_recommendations(et_hours, rec)

        ai_results['logger_id'].append(logger_id)
        ai_results['date'].append(date)
        ai_results['time'].append(time)
        ai_results['canopy_temperature'].append(canopy_temperature)
        ai_results['ambient_temperature'].append(ambient_temperature)
        ai_results['vpd'].append(vpd)
        ai_results['vwc_1'].append(vwc_1)
        ai_results['vwc_2'].append(vwc_2)
        ai_results['vwc_3'].append(vwc_3)
        ai_results['field_capacity'].append(field_capacity)
        ai_results['wilting_point'].append(wilting_point)
        ai_results['daily_gallons'].append(daily_gallons)
        ai_results['daily_switch'].append(daily_switch)
        ai_results['daily_hours'].append(daily_hours)
        ai_results['daily_pressure'].append(daily_pressure)
        ai_results['daily_inches'].append(daily_inches)
        ai_results['psi'].append(psi)
        ai_results['psi_threshold'].append(psi_threshold)
        ai_results['psi_critical'].append(psi_critical)
        ai_results['sdd'].append(sdd)
        ai_results['rh'].append(rh)
        ai_results['eto'].append(eto)
        ai_results['kc'].append(kc)
        ai_results['etc'].append(etc)
        ai_results['et_hours'].append(et_hours)
        ai_results['phase1_adjustment'].append(rec.recommendation_info[0])
        ai_results['phase1_adjusted'].append(applied_steps[0])
        ai_results['phase2_adjustment'].append(rec.recommendation_info[1])
        ai_results['phase2_adjusted'].append(applied_steps[1])
        ai_results['lowest_ambient_temperature'].append(lta)
        ai_results['gdd'].append(gdd)
        ai_results['crop_stage'].append(crop_stage)
        ai_results['id'].append(id)
        ai_results['planting_date'].append(t_planting_date)
        ai_results['variety'].append(variety)

        if et_hours is not None:
            prev_et_hours = et_hours

    print(f'Adding AI info to DB {dataset} - {logger_name} ')

    filename = 'ai_data.csv'
    print('\t- writing data to csv')
    with open(filename, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(ai_results.keys())
        # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        # This will add full null rows for any additional daily_switch list values
        writer.writerows(zip_longest(*ai_results.values()))
    # print('...Done - file: ' + filename)

    schema = [
        bigquery.SchemaField("logger_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("time", "STRING"),
        bigquery.SchemaField("canopy_temperature", "FLOAT"),
        bigquery.SchemaField("ambient_temperature", "FLOAT"),
        bigquery.SchemaField("vpd", "FLOAT"),
        bigquery.SchemaField("vwc_1", "FLOAT"),
        bigquery.SchemaField("vwc_2", "FLOAT"),
        bigquery.SchemaField("vwc_3", "FLOAT"),
        bigquery.SchemaField("field_capacity", "FLOAT"),
        bigquery.SchemaField("wilting_point", "FLOAT"),
        bigquery.SchemaField("daily_gallons", "FLOAT"),
        bigquery.SchemaField("daily_switch", "FLOAT"),
        bigquery.SchemaField("daily_hours", "FLOAT"),
        bigquery.SchemaField("daily_pressure", "FLOAT"),
        bigquery.SchemaField("daily_inches", "FLOAT"),
        bigquery.SchemaField("psi", "FLOAT"),
        bigquery.SchemaField("psi_threshold", "FLOAT"),
        bigquery.SchemaField("psi_critical", "FLOAT"),
        bigquery.SchemaField("sdd", "FLOAT"),
        bigquery.SchemaField("rh", "FLOAT"),
        bigquery.SchemaField("eto", "FLOAT"),
        bigquery.SchemaField("kc", "FLOAT"),
        bigquery.SchemaField("etc", "FLOAT"),
        bigquery.SchemaField("et_hours", "FLOAT"),
        bigquery.SchemaField("phase1_adjustment", "FLOAT"),
        bigquery.SchemaField("phase1_adjusted", "FLOAT"),
        bigquery.SchemaField("phase2_adjustment", "FLOAT"),
        bigquery.SchemaField("phase2_adjusted", "FLOAT"),
        bigquery.SchemaField("phase3_adjustment", "FLOAT"),
        bigquery.SchemaField("phase3_adjusted", "FLOAT"),
        bigquery.SchemaField("vwc_1_ec", "FLOAT"),
        bigquery.SchemaField("vwc_2_ec", "FLOAT"),
        bigquery.SchemaField("vwc_3_ec", "FLOAT"),
        bigquery.SchemaField("lowest_ambient_temperature", "FLOAT"),
        bigquery.SchemaField("gdd", "FLOAT"),
        bigquery.SchemaField("crop_stage", "STRING"),
        bigquery.SchemaField("id", "STRING"),
        bigquery.SchemaField("planting_date", "DATE"),
        bigquery.SchemaField("variety", "STRING"),
    ]
    # project = 'stomato-' + DIRECTORY_YEAR
    project = dbwriter.get_db_project(crop_type)
    dbwriter.write_to_table_from_csv(dataset, logger_name, filename, schema, project, overwrite=True)
    print()

    print('Fully Done')


def apply_cwsi_to_whole_table(dataset, logger_name, crop_type):
    """
    Apply CWSI to a specific logger's data table in the DB

    :param dataset:
    :param logger_name:
    :param crop_type:
    """
    dbwriter = DBWriter()
    project = dbwriter.get_db_project(crop_type)
    print(f'Grabbing data from {project} - {dataset} for logger {logger_name} - ')
    dml = 'SELECT *' \
          'FROM `' + project + '.' + dataset + '.' + logger_name + '` ' \
                                                                   'WHERE et_hours is not NULL ORDER BY date DESC'

    cwsi_processor = CwsiProcessor()
    result = dbwriter.run_dml(dml, project=project)
    modified_data = {"logger_id": [], "date": [], "time": [], "canopy_temperature": [], "ambient_temperature": [],
                     "vpd": [], "vwc_1": [], "vwc_2": [], "vwc_3": [], "field_capacity": [], "wilting_point": [],
                     "daily_gallons": [], "daily_switch": [], "daily_hours": [], "daily_pressure": [],
                     "daily_inches": [], "psi": [], "psi_threshold": [], "psi_critical": [],
                     "sdd": [], "rh": [], 'eto': [], 'kc': [], 'etc': [], 'et_hours': [],
                     "phase1_adjustment": [], "phase1_adjusted": [], "phase2_adjustment": [], "phase2_adjusted": [],
                     "phase3_adjustment": [], "phase3_adjusted": [], "vwc_1_ec": [], "vwc_2_ec": [], "vwc_3_ec": []}

    for r in result:
        logger_name = r[0]
        date = r[1]
        time = r[2]
        canopy_temperature = r[3]
        ambient_temperature = r[4]
        vpd = r[5]
        vwc_1 = r[6]
        vwc_2 = r[7]
        vwc_3 = r[8]
        field_capacity = r[9]
        wilting_point = r[10]
        daily_gallons = r[11]
        daily_switch = r[12]
        daily_hours = r[13]
        daily_pressure = r[14]
        daily_inches = r[15]
        sdd = r[19]
        rh = r[20]
        eto = r[21]
        kc = r[22]
        etc = r[23]
        et_hours = r[24]

        # def get_cwsi(self, tc, vpd, ta, cropType, rh=0, return_negative=False):
        psi = cwsi_processor.get_cwsi(canopy_temperature, vpd, ambient_temperature, crop_type)
        psi_threshold = 0.5
        psi_critical = 1

        modified_data['logger_id'].append(logger_name)
        modified_data['date'].append(date)
        modified_data['time'].append(time)
        modified_data['canopy_temperature'].append(canopy_temperature)
        modified_data['ambient_temperature'].append(ambient_temperature)
        modified_data['vpd'].append(vpd)
        modified_data['vwc_1'].append(vwc_1)
        modified_data['vwc_2'].append(vwc_2)
        modified_data['vwc_3'].append(vwc_3)
        modified_data['field_capacity'].append(field_capacity)
        modified_data['wilting_point'].append(wilting_point)
        modified_data['daily_gallons'].append(daily_gallons)
        modified_data['daily_switch'].append(daily_switch)
        modified_data['daily_hours'].append(daily_hours)
        modified_data['daily_pressure'].append(daily_pressure)
        modified_data['daily_inches'].append(daily_inches)
        modified_data['psi'].append(psi)
        modified_data['psi_threshold'].append(psi_threshold)
        modified_data['psi_critical'].append(psi_critical)
        modified_data['sdd'].append(sdd)
        modified_data['rh'].append(rh)
        modified_data['eto'].append(eto)
        modified_data['kc'].append(kc)
        modified_data['etc'].append(etc)
        modified_data['et_hours'].append(et_hours)
        modified_data['phase1_adjustment'].append(None)
        modified_data['phase1_adjusted'].append(None)
        modified_data['phase2_adjustment'].append(None)
        modified_data['phase2_adjusted'].append(None)
        modified_data['phase3_adjustment'].append(None)
        modified_data['phase3_adjusted'].append(None)
        modified_data['vwc_1_ec'].append(None)
        modified_data['vwc_2_ec'].append(None)
        modified_data['vwc_3_ec'].append(None)

    print(f'Adding PSI modified info to DB {dataset} - {logger_name}')

    filename = 'psi_modified_data.csv'
    print('\t- writing data to csv')
    with open(filename, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(modified_data.keys())
        # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        # This will add full null rows for any additional daily_switch list values
        writer.writerows(zip_longest(*modified_data.values()))
    # print('...Done - file: ' + filename)

    schema = [
        bigquery.SchemaField("logger_id", "STRING"),
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("time", "STRING"),
        bigquery.SchemaField("canopy_temperature", "FLOAT"),
        bigquery.SchemaField("ambient_temperature", "FLOAT"),
        bigquery.SchemaField("vpd", "FLOAT"),
        bigquery.SchemaField("vwc_1", "FLOAT"),
        bigquery.SchemaField("vwc_2", "FLOAT"),
        bigquery.SchemaField("vwc_3", "FLOAT"),
        bigquery.SchemaField("field_capacity", "FLOAT"),
        bigquery.SchemaField("wilting_point", "FLOAT"),
        bigquery.SchemaField("daily_gallons", "FLOAT"),
        bigquery.SchemaField("daily_switch", "FLOAT"),
        bigquery.SchemaField("daily_hours", "FLOAT"),
        bigquery.SchemaField("daily_pressure", "FLOAT"),
        bigquery.SchemaField("daily_inches", "FLOAT"),
        bigquery.SchemaField("psi", "FLOAT"),
        bigquery.SchemaField("psi_threshold", "FLOAT"),
        bigquery.SchemaField("psi_critical", "FLOAT"),
        bigquery.SchemaField("sdd", "FLOAT"),
        bigquery.SchemaField("rh", "FLOAT"),
        bigquery.SchemaField("eto", "FLOAT"),
        bigquery.SchemaField("kc", "FLOAT"),
        bigquery.SchemaField("etc", "FLOAT"),
        bigquery.SchemaField("et_hours", "FLOAT"),
        bigquery.SchemaField('phase1_adjustment', 'FLOAT'),
        bigquery.SchemaField('phase1_adjusted', 'FLOAT'),
        bigquery.SchemaField('phase2_adjustment', 'FLOAT'),
        bigquery.SchemaField('phase2_adjusted', 'FLOAT'),
        bigquery.SchemaField('phase3_adjustment', 'FLOAT'),
        bigquery.SchemaField('phase3_adjusted', 'FLOAT'),
        bigquery.SchemaField('vwc_1_ec', 'FLOAT'),
        bigquery.SchemaField('vwc_2_ec', 'FLOAT'),
        bigquery.SchemaField('vwc_3_ec', 'FLOAT'),
    ]
    # project = 'stomato-' + DATABASE_YEAR
    project = dbwriter.get_db_project(crop_type)
    dbwriter.write_to_table_from_csv(dataset, logger_name, filename, schema, project, overwrite=True)
    print()

    print('Fully Done')


def get_crop_stage(data_point_date, harvest_date, planting_date):
    """
    Method to get the crop stage for the artificial intelligence irrigation system
     for a tomato crop based on current date, harvest date,
     and planting date. Grabs total crop days and divides that by 4 to get a chunk.
     Chunk 1 is stage 1, chunk 2 is stage 2, and chunk 3 + 4 is stage 3.

    :param data_point_date:
    :param harvest_date:
    :param planting_date:
    :return:
    """
    crop_stage = None
    if harvest_date is not None and planting_date is not None:
        delta = harvest_date - planting_date
        total_crop_days = delta.days

        one_fourth_chunk = total_crop_days // 4

        stage_one_start = planting_date
        stage_one_end = planting_date + timedelta(days=one_fourth_chunk)
        stage_two_start = stage_one_end
        stage_two_end = stage_one_end + timedelta(days=one_fourth_chunk * 2)
        stage_three_start = stage_two_end
        stage_three_end = harvest_date

        print('Stage 1 Start: {}'.format(stage_one_start))
        print('Stage 1 End: {}'.format(stage_one_end))
        print('Stage 2 Start: {}'.format(stage_two_start))
        print('Stage 2 End: {}'.format(stage_two_end))
        print('Stage 3 Start: {}'.format(stage_three_start))
        print('Stage 3 End: {}'.format(stage_three_end))

        # if stage_one_start <= date <= stage_one_end:
        if data_point_date <= stage_one_end:
            crop_stage = 'Stage 1'
        elif stage_two_start <= data_point_date < stage_two_end:
            crop_stage = 'Stage 2'
        elif stage_three_start <= data_point_date:
            crop_stage = 'Stage 3'

        print('Crop Stage: {}'.format(crop_stage))
    return crop_stage


def reassign_technician(old_technician_name: str, new_technician_name: str):
    """
    Change all growers with a technician to a different technician. Takes in the old technician name and the new 
    technician name and pulls up all growers that have the old technician. Then it looks for a technician with the 
    new technician name and if it finds one it uses that technician as the new technician for the growers 

    :param old_technician_name:
    :param new_technician_name:
    """
    growers = open_pickle()
    all_technicians = get_all_technicians(growers)
    new_technician = None
    for tech in all_technicians:
        if tech.name == new_technician_name:
            new_technician = tech
    for g in growers:
        if g.technician.name == old_technician_name:
            print(
                'Changing Technician for Grower {} \n from {} to {}'.format(
                    g.name, old_technician_name,
                    new_technician_name
                )
            )
            g.technician = new_technician
    write_pickle(growers)


def deactivate_growers_with_all_inactive_fields():
    """
    Method to check if all of a growers fields are inactive and if they are set the grower itself as inactive

    :return: None
    """
    growers = open_pickle()
    all_fields_inactive = True
    for g in growers:
        for f in g.fields:
            if f.active:
                all_fields_inactive = False
        if all_fields_inactive:
            g.deactivate()
        all_fields_inactive = True
    # for g in growers:
    #     if not g.active:
    #         g.to_string()
    write_pickle(growers)


def remove_inactive_growers_from_pickle():
    """
    Method that goes through the current pickle, picks out active growers, and overwrites the current pickle
    only with these active growers. There is a confirmation dialog to make sure we want to run this and a
    pickle backup is also created before overwriting it.
    """
    new_growers = []
    growers = open_pickle()
    print('Removing inactive growers from pickle will overwrite current pickle only leaving active growers')
    confirm = input('Are you sure you want to do this? (Y/N) ').lower().strip()

    if confirm[:1] == 'y':
        backup_pickle('before_removing_inactive')
        for grower in growers:
            if grower.active:
                print('Transferring > ', grower.name)
                new_growers.append(grower)
            else:
                print('Not Transferring >', grower.name)
        write_pickle(new_growers)


def remove_inactive_fields_from_growers_from_pickle():
    """
    Method that goes through the current pickle, picks out active growers, and overwrites the current pickle
    only with these active growers. There is a confirmation dialog to make sure we want to run this and a
    pickle backup is also created before overwriting it.
    """
    growers = open_pickle()
    print('Removing inactive fields from pickle will overwrite current pickle only leaving active fields')
    confirm = input('Are you sure you want to do this? (Y/N) ').lower().strip()

    if confirm[:1] == 'y':
        backup_pickle('before_removing_inactive_fields')
        for grower in growers:
            if grower.active:
                active_fields = []
                for field in grower.fields:
                    if field.active:
                        active_fields.append(field)
                    else:
                        print(f'Removing {field.nickname} from {grower.name}')
                grower.fields = active_fields
        write_pickle(growers)


def new_year_pickle_cleanup():
    """
    Convenience method to chain a couple of methods together. This should be run when we are ready to setup
    a pickle for the new year. This will first deactivate growers that have all their fields inactive and
    will then remove these growers from the current pickle
    """
    deactivate_growers_with_all_inactive_fields()
    remove_inactive_growers_from_pickle()


def get_technician(technician_name: str, growers=None):
    """
    Grab the technician object for a technician with technician_name

    :param technician_name:
    :return:
    """
    if growers is None:
        growers = open_pickle()
    technicians = get_all_technicians(growers)
    for technician in technicians:
        if technician.name == technician_name:
            print('Got Technician = ', technician_name, technician)
            return technician
    print("Technician - ", technician_name, ' - not found')
    return None


def check_technician_clones():
    growers = open_pickle()
    tech_dict = {'Vanessa': [], 'Exsaelth': [], 'Adriana': [], 'Development Test Tech': []}
    for grower in growers:
        # print(grower.name, ' - ', grower.technician.name, ' - ', id(grower.technician))
        if id(grower.technician) not in tech_dict[grower.technician.name]:
            tech_dict[grower.technician.name].append(id(grower.technician))
    for key, values in tech_dict.items():
        print(key, " : ", values)


def temp_ai_application():
    # AI Logger
    apply_ai_recommendation_to_logger('stomato-2023', 'Lucero_Dillard_RoadD3', 'DI-D3-SE')

    # Old Control
    # apply_ai_recommendation_to_logger('stomato-2023', 'Lucero_Dillard_RoadD3', 'DI-D3-NW')

    # New Control
    apply_ai_recommendation_to_logger('stomato-2023', 'Lucero_Dillard_RoadD4', 'DI-D4-W')


def check_for_new_cimis_stations():
    cimisStationsPickle = CimisStation.open_cimis_station_pickle(CimisStation)
    cimisStationList = []
    for stations in cimisStationsPickle:
        cimisStationList.append(stations.station_number)
    growers = open_pickle()
    for g in growers:
        for f in g.fields:
            if f.cimis_station not in cimisStationList:
                cimisStationList.append(f.cimis_station)
                print("Adding Cimis Station: ", f.cimis_station, " to pickle")
                x = input("ET for " + f.cimis_station + "\n")
                cimisStation = CimisStation(f.cimis_station, float(x))
                cimisStationsPickle.append(cimisStation)
    CimisStation.write_cimis_station_pickle(CimisStation, cimisStationsPickle)


def write_uninstallation_progress_to_db():
    dbwriter = DBWriter()
    schema = [
        bigquery.SchemaField("Grower", "STRING"),
        bigquery.SchemaField("Field", "STRING"),
        bigquery.SchemaField("Uninstalled_Date", "DATE"),
        bigquery.SchemaField("Acres", "FLOAT"),
        bigquery.SchemaField("Region", "STRING"),
        bigquery.SchemaField("Latt_Long", "STRING")
    ]
    uninstallDictNorth = {"Grower": [], "Field": [], "Uninstalled_Date": [], "Acres": [], "Region": [], "Latt_Long": []}
    uninstallDictSouth = {"Grower": [], "Field": [], "Uninstalled_Date": [], "Acres": [], "Region": [], "Latt_Long": []}

    growers = open_pickle()
    for g in growers:
        for f in g.fields:
            if g.region == 'North' and f.loggers[-1].crop_type == "Tomatoes":
                uninstallDictNorth["Grower"].append(g.name)
                uninstallDictNorth["Field"].append(f.nickname)
                lat_long = str(f.loggers[-1].lat) + "," + str(f.loggers[-1].long)
                uninstallDictNorth["Latt_Long"].append(str(lat_long))
            elif g.region == "South" and f.loggers[-1].crop_type == "Tomatoes":
                uninstallDictSouth["Grower"].append(g.name)
                uninstallDictSouth["Field"].append(f.nickname)
                lat_long = str(f.loggers[-1].lat) + "," + str(f.loggers[-1].long)
                uninstallDictSouth["Latt_Long"].append(str(lat_long))

    print(uninstallDictNorth)
    print(uninstallDictSouth)

    with open('uninstallation.csv', "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(uninstallDictNorth.keys())
        # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        # This will add full null rows for any additional daily_switch list values
        writer.writerows(zip_longest(*uninstallDictNorth.values()))
    project = 'stomato-info'
    dbwriter.write_to_table_from_csv(
        '1_uninstallation_progress', 'North', 'uninstallation.csv', schema, project
    )

    with open('uninstallation.csv', "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(uninstallDictSouth.keys())
        # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        # This will add full null rows for any additional daily_switch list values
        writer.writerows(zip_longest(*uninstallDictSouth.values()))

    dbwriter.write_to_table_from_csv(
        '1_uninstallation_progress', 'South', 'uninstallation.csv', schema, project
    )



def test_switch_cases():
    """
    Method to set up and loop through all switch test cases and record results.

    """

    # Setup test cases
    test_cases = []

    standard_data = {
        'dates': [datetime(2022, 6, 12, 0, 0), datetime(2022, 6, 12, 1, 0),
                  datetime(2022, 6, 12, 2, 0), datetime(2022, 6, 12, 3, 0),
                  datetime(2022, 6, 12, 4, 0), datetime(2022, 6, 12, 5, 0),
                  datetime(2022, 6, 12, 6, 0), datetime(2022, 6, 12, 7, 0),
                  datetime(2022, 6, 12, 8, 0), datetime(2022, 6, 12, 9, 0),
                  datetime(2022, 6, 12, 10, 0), datetime(2022, 6, 12, 11, 0),
                  datetime(2022, 6, 12, 12, 0), datetime(2022, 6, 12, 13, 0),
                  datetime(2022, 6, 12, 14, 0), datetime(2022, 6, 12, 15, 0),
                  datetime(2022, 6, 12, 16, 0), datetime(2022, 6, 12, 17, 0),
                  datetime(2022, 6, 12, 18, 0), datetime(2022, 6, 12, 19, 0),
                  datetime(2022, 6, 12, 20, 0), datetime(2022, 6, 12, 21, 0),
                  datetime(2022, 6, 12, 22, 0), datetime(2022, 6, 12, 23, 0),
                  datetime(2022, 6, 13, 0, 0), datetime(2022, 6, 13, 1, 0)],
        'canopy temperature': [76, 77, 88, 93, 78, 76, 88, 83, 76, 89, 79, 93, 84, 80, 81, 94, 85, 78, 80,
                               94, 89, 87, 76, 82, 93, 89],
        'ambient temperature': [93, 79, 103, 89, 94, 84, 102, 77, 96, 84, 95, 79, 77, 97, 74, 73, 88, 100,
                                82, 81, 96, 78, 93, 89, 94, 85],
        'rh': [31, 56, 46, 15, 15, 41, 39, 21, 16, 43, 34, 16, 55, 29, 33, 51, 28, 54, 23, 32, 58, 42, 24,
               59, 50, 58],
        'vpd': [2, 2, 2, 4, 5, 2, 5, 3, 1, 3, 0, 5, 3, 0, 2, 2, 0, 4, 2, 5, 1, 3, 1, 5, 2, 2],
        'vwc_1': [34, 22, 33, 23, 24, 21, 26, 31, 33, 22, 38, 35, 36, 30, 40, 23, 34, 36, 32, 35, 41, 33,
                  39, 27, 36, 39],
        'vwc_2': [43, 35, 20, 41, 35, 35, 25, 42, 40, 35, 30, 41, 32, 20, 29, 32, 40, 26, 25, 42, 38, 31,
                  29, 38, 40, 35],
        'vwc_3': [20, 25, 39, 20, 34, 32, 25, 39, 23, 31, 22, 25, 42, 26, 25, 21, 31, 42, 42, 37, 23, 42,
                  33, 20, 34, 35], 'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [0, 0, 0, 0, 0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}

    standard_data_2023 = {
        'dates': [datetime(2023, 3, 15, 0, 0, 15, 512237), datetime(2023, 3, 15, 1, 0, 15, 512237),
                  datetime(2023, 3, 15, 2, 0, 15, 512237), datetime(2023, 3, 15, 3, 0, 15, 512237),
                  datetime(2023, 3, 15, 4, 0, 15, 512237), datetime(2023, 3, 15, 5, 0, 15, 512237),
                  datetime(2023, 3, 15, 6, 0, 15, 512237), datetime(2023, 3, 15, 7, 0, 15, 512237),
                  datetime(2023, 3, 15, 8, 0, 15, 512237), datetime(2023, 3, 15, 9, 0, 15, 512237),
                  datetime(2023, 3, 15, 10, 0, 15, 512237), datetime(2023, 3, 15, 11, 0, 15, 512237),
                  datetime(2023, 3, 15, 12, 0, 15, 512237), datetime(2023, 3, 15, 13, 0, 15, 512237),
                  datetime(2023, 3, 15, 14, 0, 15, 512237), datetime(2023, 3, 15, 15, 0, 15, 512237),
                  datetime(2023, 3, 15, 16, 0, 15, 512237), datetime(2023, 3, 15, 17, 0, 15, 512237),
                  datetime(2023, 3, 15, 18, 0, 15, 512237), datetime(2023, 3, 15, 19, 0, 15, 512237),
                  datetime(2023, 3, 15, 20, 0, 15, 512237), datetime(2023, 3, 15, 21, 0, 15, 512237),
                  datetime(2023, 3, 15, 22, 0, 15, 512237), datetime(2023, 3, 15, 23, 0, 15, 512237),
                  datetime(2023, 3, 16, 0, 0, 15, 512237), datetime(2023, 3, 16, 1, 0, 15, 512237)],
        'canopy temperature': [90, 83, 93, 91, 94, 93, 81, 78, 90, 76, 92, 94, 88, 77, 91, 86, 84, 90, 94, 86, 85, 85,
                               77, 85, 84, 76],
        'ambient temperature': [104, 74, 76, 100, 99, 83, 92, 101, 81, 77, 76, 78, 82, 90, 96, 101, 100, 92, 89, 84, 97,
                                74, 98, 99, 93, 77],
        'rh': [52, 51, 40, 36, 42, 20, 16, 27, 29, 25, 20, 29, 35, 50, 47, 15, 39, 52, 52, 50, 52, 38, 56, 38, 31, 50],
        'vpd': [4, 1, 0, 1, 3, 2, 5, 2, 4, 1, 5, 5, 1, 1, 4, 5, 0, 1, 4, 3, 2, 4, 0, 5, 0, 4],
        'vwc_1': [38, 24, 41, 34, 30, 29, 41, 26, 23, 34, 27, 38, 31, 40, 39, 41, 30, 44, 39, 31, 31, 29, 23, 23, 29,
                  38],
        'vwc_2': [20, 25, 23, 24, 21, 22, 43, 38, 20, 30, 24, 40, 32, 42, 31, 42, 24, 33, 20, 36, 41, 36, 28, 22, 24,
                  35],
        'vwc_3': [27, 27, 20, 38, 34, 43, 20, 44, 27, 37, 27, 24, 34, 43, 26, 23, 36, 21, 26, 36, 32, 42, 38, 23, 41,
                  29], 'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 60, 60, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0, 0.0]}

    half_and_half = {
        'dates': [datetime(2023, 3, 15, 12, 0, 8, 544470), datetime(2023, 3, 15, 13, 0, 8, 544470),
                  datetime(2023, 3, 15, 14, 0, 8, 544470), datetime(2023, 3, 15, 15, 0, 8, 544470),
                  datetime(2023, 3, 15, 16, 0, 8, 544470), datetime(2023, 3, 15, 17, 0, 8, 544470),
                  datetime(2023, 3, 15, 18, 0, 8, 544470), datetime(2023, 3, 15, 19, 0, 8, 544470),
                  datetime(2023, 3, 15, 20, 0, 8, 544470), datetime(2023, 3, 15, 21, 0, 8, 544470),
                  datetime(2023, 3, 15, 22, 0, 8, 544470), datetime(2023, 3, 15, 23, 0, 8, 544470),
                  datetime(2023, 3, 16, 0, 0, 8, 544470), datetime(2023, 3, 16, 1, 0, 8, 544470),
                  datetime(2023, 3, 16, 2, 0, 8, 544470), datetime(2023, 3, 16, 3, 0, 8, 544470),
                  datetime(2023, 3, 16, 4, 0, 8, 544470), datetime(2023, 3, 16, 5, 0, 8, 544470),
                  datetime(2023, 3, 16, 6, 0, 8, 544470), datetime(2023, 3, 16, 7, 0, 8, 544470),
                  datetime(2023, 3, 16, 8, 0, 8, 544470), datetime(2023, 3, 16, 9, 0, 8, 544470),
                  datetime(2023, 3, 16, 10, 0, 8, 544470), datetime(2023, 3, 16, 11, 0, 8, 544470),
                  datetime(2023, 3, 16, 12, 0, 8, 544470)],
        'canopy temperature': [76, 76, 78, 88, 78, 90, 78, 91, 82, 83, 82, 91, 93, 80, 93, 90, 83, 78, 82, 79, 89, 87,
                               88, 81, 80],
        'ambient temperature': [90, 103, 85, 103, 88, 76, 86, 103, 88, 75, 98, 89, 74, 93, 74, 103, 91, 95, 100, 100,
                                78, 95, 79, 81, 95],
        'rh': [23, 37, 27, 31, 59, 37, 21, 43, 42, 31, 32, 19, 38, 54, 20, 48, 57, 42, 45, 59, 17, 44, 48, 51, 32],
        'vpd': [4, 3, 1, 3, 1, 4, 1, 5, 3, 5, 4, 3, 0, 3, 4, 4, 5, 4, 1, 2, 2, 0, 3, 3, 3],
        'vwc_1': [39, 23, 20, 20, 42, 43, 28, 40, 39, 25, 41, 28, 41, 29, 37, 25, 38, 30, 28, 21, 43, 37, 20, 36, 44],
        'vwc_2': [21, 42, 25, 32, 38, 31, 22, 39, 24, 28, 44, 25, 31, 29, 33, 23, 38, 38, 38, 24, 30, 24, 42, 44, 20],
        'vwc_3': [40, 33, 25, 30, 40, 26, 23, 23, 41, 44, 36, 29, 40, 23, 43, 43, 25, 24, 28, 37, 20, 31, 20, 39, 25],
        'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0]}

    multiple_days = {
        'dates': [datetime(2023, 3, 14, 0, 0, 36, 783492), datetime(2023, 3, 14, 1, 0, 36, 783492),
                  datetime(2023, 3, 14, 2, 0, 36, 783492), datetime(2023, 3, 14, 3, 0, 36, 783492),
                  datetime(2023, 3, 14, 4, 0, 36, 783492), datetime(2023, 3, 14, 5, 0, 36, 783492),
                  datetime(2023, 3, 14, 6, 0, 36, 783492), datetime(2023, 3, 14, 7, 0, 36, 783492),
                  datetime(2023, 3, 14, 8, 0, 36, 783492), datetime(2023, 3, 14, 9, 0, 36, 783492),
                  datetime(2023, 3, 14, 10, 0, 36, 783492), datetime(2023, 3, 14, 11, 0, 36, 783492),
                  datetime(2023, 3, 14, 12, 0, 36, 783492), datetime(2023, 3, 14, 13, 0, 36, 783492),
                  datetime(2023, 3, 14, 14, 0, 36, 783492), datetime(2023, 3, 14, 15, 0, 36, 783492),
                  datetime(2023, 3, 14, 16, 0, 36, 783492), datetime(2023, 3, 14, 17, 0, 36, 783492),
                  datetime(2023, 3, 14, 18, 0, 36, 783492), datetime(2023, 3, 14, 19, 0, 36, 783492),
                  datetime(2023, 3, 14, 20, 0, 36, 783492), datetime(2023, 3, 14, 21, 0, 36, 783492),
                  datetime(2023, 3, 14, 22, 0, 36, 783492), datetime(2023, 3, 14, 23, 0, 36, 783492),
                  datetime(2023, 3, 15, 0, 0, 36, 783492), datetime(2023, 3, 15, 1, 0, 36, 783492),
                  datetime(2023, 3, 15, 2, 0, 36, 783492), datetime(2023, 3, 15, 3, 0, 36, 783492),
                  datetime(2023, 3, 15, 4, 0, 36, 783492), datetime(2023, 3, 15, 5, 0, 36, 783492),
                  datetime(2023, 3, 15, 6, 0, 36, 783492), datetime(2023, 3, 15, 7, 0, 36, 783492),
                  datetime(2023, 3, 15, 8, 0, 36, 783492), datetime(2023, 3, 15, 9, 0, 36, 783492),
                  datetime(2023, 3, 15, 10, 0, 36, 783492), datetime(2023, 3, 15, 11, 0, 36, 783492),
                  datetime(2023, 3, 15, 12, 0, 36, 783492), datetime(2023, 3, 15, 13, 0, 36, 783492),
                  datetime(2023, 3, 15, 14, 0, 36, 783492), datetime(2023, 3, 15, 15, 0, 36, 783492),
                  datetime(2023, 3, 15, 16, 0, 36, 783492), datetime(2023, 3, 15, 17, 0, 36, 783492),
                  datetime(2023, 3, 15, 18, 0, 36, 783492), datetime(2023, 3, 15, 19, 0, 36, 783492),
                  datetime(2023, 3, 15, 20, 0, 36, 783492), datetime(2023, 3, 15, 21, 0, 36, 783492),
                  datetime(2023, 3, 15, 22, 0, 36, 783492), datetime(2023, 3, 15, 23, 0, 36, 783492),
                  datetime(2023, 3, 16, 0, 0, 36, 783492), datetime(2023, 3, 16, 1, 0, 36, 783492)],
        'canopy temperature': [90, 92, 88, 90, 76, 94, 80, 76, 94, 88, 91, 86, 92, 84, 79, 90, 89, 83, 88, 93, 85, 82,
                               94, 76, 93, 76, 89, 81, 83, 80, 82, 86, 78, 84, 89, 76, 93, 79, 76, 89, 92, 79, 82, 83,
                               84, 93, 88, 78, 94, 84],
        'ambient temperature': [101, 76, 75, 87, 81, 85, 99, 101, 99, 101, 97, 97, 90, 89, 91, 98, 89, 76, 87, 77, 87,
                                96, 97, 96, 90, 82, 99, 84, 90, 77, 76, 84, 95, 93, 72, 73, 84, 77, 77, 99, 77, 93, 80,
                                78, 87, 93, 80, 103, 90, 88],
        'rh': [57, 24, 39, 34, 46, 59, 19, 21, 57, 41, 17, 25, 45, 38, 15, 38, 42, 34, 25, 31, 51, 35, 16, 32, 31, 36,
               50, 48, 27, 38, 51, 41, 21, 24, 31, 45, 17, 34, 28, 16, 22, 35, 24, 19, 24, 23, 46, 49, 18, 20],
        'vpd': [5, 0, 5, 1, 1, 2, 4, 1, 4, 5, 2, 4, 5, 5, 4, 0, 5, 2, 3, 2, 1, 4, 4, 1, 0, 3, 2, 1, 4, 3, 4, 5, 0, 3, 4,
                3, 1, 2, 3, 2, 5, 5, 3, 1, 4, 5, 3, 1, 0, 0],
        'vwc_1': [29, 26, 32, 39, 37, 34, 26, 25, 25, 29, 24, 39, 26, 38, 37, 26, 26, 22, 29, 34, 43, 33, 33, 26, 28,
                  31, 36, 22, 25, 44, 43, 20, 31, 22, 23, 22, 43, 39, 21, 34, 40, 43, 24, 36, 22, 24, 36, 21, 30, 34],
        'vwc_2': [26, 33, 21, 38, 20, 20, 24, 33, 32, 30, 25, 26, 24, 21, 31, 31, 30, 28, 31, 32, 23, 38, 31, 35, 30,
                  21, 36, 37, 29, 29, 33, 43, 36, 29, 23, 35, 29, 23, 39, 31, 36, 25, 20, 37, 41, 36, 37, 28, 36, 38],
        'vwc_3': [22, 27, 44, 32, 42, 38, 42, 43, 31, 28, 40, 29, 41, 40, 33, 22, 27, 32, 35, 39, 24, 38, 21, 30, 44,
                  28, 26, 29, 34, 38, 26, 21, 39, 37, 21, 43, 41, 33, 28, 22, 25, 26, 28, 34, 25, 30, 41, 25, 44, 38],
        'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [0.0, 0.0, 60, 60, 60, 60, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0, 60, 60, 60, 60, 0.0]}

    small_edges = {
        'dates': [datetime(2023, 3, 15, 23, 0, 39, 744241), datetime(2023, 3, 16, 0, 0, 39, 744241),
                  datetime(2023, 3, 16, 1, 0, 39, 744241), datetime(2023, 3, 16, 2, 0, 39, 744241),
                  datetime(2023, 3, 16, 3, 0, 39, 744241), datetime(2023, 3, 16, 4, 0, 39, 744241),
                  datetime(2023, 3, 16, 5, 0, 39, 744241), datetime(2023, 3, 16, 6, 0, 39, 744241),
                  datetime(2023, 3, 16, 7, 0, 39, 744241), datetime(2023, 3, 16, 8, 0, 39, 744241),
                  datetime(2023, 3, 16, 9, 0, 39, 744241), datetime(2023, 3, 16, 10, 0, 39, 744241),
                  datetime(2023, 3, 16, 11, 0, 39, 744241), datetime(2023, 3, 16, 12, 0, 39, 744241),
                  datetime(2023, 3, 16, 13, 0, 39, 744241), datetime(2023, 3, 16, 14, 0, 39, 744241),
                  datetime(2023, 3, 16, 15, 0, 39, 744241), datetime(2023, 3, 16, 16, 0, 39, 744241),
                  datetime(2023, 3, 16, 17, 0, 39, 744241), datetime(2023, 3, 16, 18, 0, 39, 744241),
                  datetime(2023, 3, 16, 19, 0, 39, 744241), datetime(2023, 3, 16, 20, 0, 39, 744241),
                  datetime(2023, 3, 16, 21, 0, 39, 744241), datetime(2023, 3, 16, 22, 0, 39, 744241),
                  datetime(2023, 3, 16, 23, 0, 39, 744241), datetime(2023, 3, 17, 0, 0, 39, 744241)],
        'canopy temperature': [77, 81, 90, 77, 92, 93, 79, 87, 87, 81, 86, 78, 91, 88, 88, 76, 92, 94, 88, 79, 93, 80,
                               93, 90, 85, 94],
        'ambient temperature': [85, 92, 90, 104, 82, 80, 86, 102, 89, 74, 79, 84, 78, 102, 103, 98, 97, 103, 84, 88,
                                101, 83, 79, 73, 96, 83],
        'rh': [25, 37, 51, 53, 27, 49, 54, 49, 26, 38, 58, 35, 30, 38, 20, 28, 25, 44, 32, 49, 53, 41, 36, 26, 21, 35],
        'vpd': [1, 2, 5, 3, 1, 5, 3, 5, 3, 1, 2, 5, 1, 1, 4, 3, 2, 4, 5, 4, 5, 5, 3, 3, 5, 4],
        'vwc_1': [34, 37, 41, 41, 42, 33, 36, 21, 42, 31, 34, 28, 33, 39, 30, 38, 25, 22, 35, 24, 39, 23, 20, 33, 39,
                  44],
        'vwc_2': [28, 42, 24, 37, 21, 21, 39, 42, 29, 24, 44, 22, 37, 39, 40, 28, 21, 22, 39, 38, 21, 25, 26, 33, 31,
                  30],
        'vwc_3': [26, 30, 40, 34, 20, 27, 44, 28, 43, 36, 37, 36, 26, 36, 41, 22, 37, 42, 26, 25, 43, 35, 43, 36, 24,
                  34], 'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0]}

    lacking_full_days = {
        'dates': [datetime(2023, 3, 16, 16, 0, 32, 92243), datetime(2023, 3, 16, 17, 0, 32, 92243),
                  datetime(2023, 3, 16, 18, 0, 32, 92243), datetime(2023, 3, 16, 19, 0, 32, 92243),
                  datetime(2023, 3, 16, 20, 0, 32, 92243), datetime(2023, 3, 16, 21, 0, 32, 92243),
                  datetime(2023, 3, 16, 22, 0, 32, 92243), datetime(2023, 3, 16, 23, 0, 32, 92243),
                  datetime(2023, 3, 17, 0, 0, 32, 92243), datetime(2023, 3, 17, 1, 0, 32, 92243)],
        'canopy temperature': [86, 88, 88, 83, 76, 86, 91, 82, 80, 84],
        'ambient temperature': [95, 100, 86, 72, 94, 89, 90, 89, 86, 103],
        'rh': [28, 18, 38, 16, 21, 46, 53, 51, 47, 31], 'vpd': [5, 2, 3, 3, 2, 3, 5, 5, 4, 5],
        'vwc_1': [25, 40, 40, 35, 29, 21, 24, 41, 39, 34], 'vwc_2': [37, 23, 35, 26, 38, 36, 44, 32, 30, 34],
        'vwc_3': [23, 40, 23, 31, 29, 44, 38, 27, 37, 33], 'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [],
        'daily gallons': [], 'daily switch': [0.0, 0.0, 0.0, 60, 60, 60, 60, 60, 60, 0.0]}

    small_med_edges = {
        'dates': [datetime(2023, 3, 15, 21, 0, 23, 131219), datetime(2023, 3, 15, 22, 0, 23, 131219),
                  datetime(2023, 3, 15, 23, 0, 23, 131219), datetime(2023, 3, 16, 0, 0, 23, 131219),
                  datetime(2023, 3, 16, 1, 0, 23, 131219), datetime(2023, 3, 16, 2, 0, 23, 131219),
                  datetime(2023, 3, 16, 3, 0, 23, 131219), datetime(2023, 3, 16, 4, 0, 23, 131219),
                  datetime(2023, 3, 16, 5, 0, 23, 131219), datetime(2023, 3, 16, 6, 0, 23, 131219),
                  datetime(2023, 3, 16, 7, 0, 23, 131219), datetime(2023, 3, 16, 8, 0, 23, 131219),
                  datetime(2023, 3, 16, 9, 0, 23, 131219), datetime(2023, 3, 16, 10, 0, 23, 131219),
                  datetime(2023, 3, 16, 11, 0, 23, 131219), datetime(2023, 3, 16, 12, 0, 23, 131219),
                  datetime(2023, 3, 16, 13, 0, 23, 131219), datetime(2023, 3, 16, 14, 0, 23, 131219),
                  datetime(2023, 3, 16, 15, 0, 23, 131219), datetime(2023, 3, 16, 16, 0, 23, 131219),
                  datetime(2023, 3, 16, 17, 0, 23, 131219), datetime(2023, 3, 16, 18, 0, 23, 131219),
                  datetime(2023, 3, 16, 19, 0, 23, 131219), datetime(2023, 3, 16, 20, 0, 23, 131219),
                  datetime(2023, 3, 16, 21, 0, 23, 131219), datetime(2023, 3, 16, 22, 0, 23, 131219),
                  datetime(2023, 3, 16, 23, 0, 23, 131219), datetime(2023, 3, 17, 0, 0, 23, 131219),
                  datetime(2023, 3, 17, 1, 0, 23, 131219), datetime(2023, 3, 17, 2, 0, 23, 131219)],
        'canopy temperature': [87, 83, 79, 76, 90, 84, 79, 90, 94, 93, 81, 81, 82, 82, 89, 81, 76, 76, 88, 91, 85, 88,
                               84, 86, 85, 85, 88, 92, 89, 84],
        'ambient temperature': [80, 102, 102, 93, 96, 98, 102, 95, 94, 102, 104, 94, 95, 75, 81, 102, 101, 85, 74, 79,
                                101, 103, 77, 95, 89, 92, 94, 84, 103, 94],
        'rh': [33, 58, 55, 43, 27, 25, 41, 25, 58, 59, 15, 30, 54, 23, 21, 56, 29, 43, 39, 37, 31, 37, 37, 56, 43, 53,
               58, 25, 53, 25],
        'vpd': [4, 5, 3, 5, 2, 4, 5, 2, 1, 4, 3, 1, 3, 3, 1, 4, 5, 5, 1, 4, 4, 5, 3, 3, 4, 1, 2, 2, 1, 2],
        'vwc_1': [26, 26, 37, 29, 42, 37, 40, 25, 35, 35, 33, 42, 20, 35, 33, 23, 21, 23, 42, 39, 21, 28, 35, 43, 44,
                  22, 26, 44, 41, 42],
        'vwc_2': [28, 20, 31, 20, 23, 23, 38, 28, 42, 27, 28, 32, 30, 25, 27, 25, 42, 22, 38, 24, 21, 38, 44, 23, 25,
                  26, 40, 44, 28, 43],
        'vwc_3': [20, 27, 42, 40, 21, 39, 40, 22, 27, 24, 23, 22, 36, 20, 24, 35, 39, 37, 21, 26, 32, 37, 27, 21, 42,
                  32, 22, 41, 29, 42], 'vwc_1_ec': [], 'vwc_2_ec': [], 'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                         0.0, 0.0, 60, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0]}

    med_edges = {
        'dates': [datetime(2023, 3, 15, 16, 0, 21, 121390), datetime(2023, 3, 15, 17, 0, 21, 121390),
                  datetime(2023, 3, 15, 18, 0, 21, 121390), datetime(2023, 3, 15, 19, 0, 21, 121390),
                  datetime(2023, 3, 15, 20, 0, 21, 121390), datetime(2023, 3, 15, 21, 0, 21, 121390),
                  datetime(2023, 3, 15, 22, 0, 21, 121390), datetime(2023, 3, 15, 23, 0, 21, 121390),
                  datetime(2023, 3, 16, 0, 0, 21, 121390), datetime(2023, 3, 16, 1, 0, 21, 121390),
                  datetime(2023, 3, 16, 2, 0, 21, 121390), datetime(2023, 3, 16, 3, 0, 21, 121390),
                  datetime(2023, 3, 16, 4, 0, 21, 121390), datetime(2023, 3, 16, 5, 0, 21, 121390),
                  datetime(2023, 3, 16, 6, 0, 21, 121390), datetime(2023, 3, 16, 7, 0, 21, 121390),
                  datetime(2023, 3, 16, 8, 0, 21, 121390), datetime(2023, 3, 16, 9, 0, 21, 121390),
                  datetime(2023, 3, 16, 10, 0, 21, 121390), datetime(2023, 3, 16, 11, 0, 21, 121390),
                  datetime(2023, 3, 16, 12, 0, 21, 121390), datetime(2023, 3, 16, 13, 0, 21, 121390),
                  datetime(2023, 3, 16, 14, 0, 21, 121390), datetime(2023, 3, 16, 15, 0, 21, 121390),
                  datetime(2023, 3, 16, 16, 0, 21, 121390), datetime(2023, 3, 16, 17, 0, 21, 121390),
                  datetime(2023, 3, 16, 18, 0, 21, 121390), datetime(2023, 3, 16, 19, 0, 21, 121390),
                  datetime(2023, 3, 16, 20, 0, 21, 121390), datetime(2023, 3, 16, 21, 0, 21, 121390),
                  datetime(2023, 3, 16, 22, 0, 21, 121390), datetime(2023, 3, 16, 23, 0, 21, 121390),
                  datetime(2023, 3, 17, 0, 0, 21, 121390), datetime(2023, 3, 17, 1, 0, 21, 121390),
                  datetime(2023, 3, 17, 2, 0, 21, 121390), datetime(2023, 3, 17, 3, 0, 21, 121390),
                  datetime(2023, 3, 17, 4, 0, 21, 121390), datetime(2023, 3, 17, 5, 0, 21, 121390),
                  datetime(2023, 3, 17, 6, 0, 21, 121390), datetime(2023, 3, 17, 7, 0, 21, 121390)],
        'canopy temperature': [91, 87, 87, 85, 76, 93, 86, 88, 78, 84, 91, 79, 91, 81, 83, 82, 82, 78, 77, 94, 79, 92,
                               90, 94, 93, 88, 91, 84, 83, 89, 77, 94, 94, 79, 85, 77, 84, 77, 76, 86],
        'ambient temperature': [100, 74, 90, 80, 98, 90, 96, 85, 77, 92, 87, 76, 88, 102, 95, 77, 83, 96, 80, 104, 95,
                                100, 79, 77, 104, 78, 91, 91, 74, 78, 78, 97, 77, 79, 99, 102, 100, 75, 86, 102],
        'rh': [56, 34, 24, 47, 54, 32, 21, 24, 46, 18, 30, 41, 36, 27, 32, 49, 55, 32, 52, 21, 21, 58, 41, 43, 44, 58,
               27, 51, 50, 22, 46, 18, 48, 43, 32, 46, 49, 33, 51, 56],
        'vpd': [1, 4, 2, 5, 1, 2, 5, 2, 5, 5, 5, 2, 3, 5, 3, 1, 4, 5, 3, 3, 1, 2, 5, 1, 4, 4, 2, 5, 1, 3, 1, 2, 1, 2, 2,
                1, 2, 5, 4, 2],
        'vwc_1': [27, 37, 26, 27, 34, 24, 31, 32, 21, 44, 39, 37, 25, 21, 22, 42, 42, 27, 42, 34, 44, 26, 22, 28, 26,
                  42, 26, 23, 23, 39, 29, 29, 37, 24, 26, 39, 24, 26, 25, 41],
        'vwc_2': [35, 31, 31, 27, 32, 22, 42, 33, 28, 44, 35, 30, 26, 30, 40, 33, 26, 39, 25, 41, 30, 28, 33, 27, 25,
                  31, 20, 43, 31, 38, 44, 39, 24, 37, 43, 31, 20, 22, 42, 44],
        'vwc_3': [27, 20, 40, 24, 37, 34, 34, 35, 38, 44, 24, 37, 32, 28, 29, 26, 22, 25, 23, 34, 32, 43, 23, 40, 28,
                  42, 30, 35, 29, 28, 33, 29, 25, 44, 23, 34, 28, 24, 26, 34], 'vwc_1_ec': [], 'vwc_2_ec': [],
        'vwc_3_ec': [], 'daily gallons': [],
        'daily switch': [0.0, 0.0, 60, 60, 60, 60, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 60, 60, 60, 60, 60, 0.0, 0.0, 0.0,
                         0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 60, 60, 60, 0.0, 0.0]}

    overnight_irrigation_day_one = {
        'dates': [datetime(2023, 7, 20, 2, 0), datetime(2023, 7, 20, 3, 0), datetime(2023, 7, 20, 4, 0),
                  datetime(2023, 7, 20, 5, 0), datetime(2023, 7, 20, 6, 0), datetime(2023, 7, 20, 7, 0),
                  datetime(2023, 7, 20, 8, 0), datetime(2023, 7, 20, 9, 0), datetime(2023, 7, 20, 10, 0),
                  datetime(2023, 7, 20, 11, 0), datetime(2023, 7, 20, 12, 0), datetime(2023, 7, 20, 13, 0),
                  datetime(2023, 7, 20, 14, 0), datetime(2023, 7, 20, 15, 0), datetime(2023, 7, 20, 16, 0),
                  datetime(2023, 7, 20, 17, 0), datetime(2023, 7, 20, 18, 0), datetime(2023, 7, 20, 19, 0),
                  datetime(2023, 7, 20, 20, 0), datetime(2023, 7, 20, 21, 0), datetime(2023, 7, 20, 22, 0),
                  datetime(2023, 7, 20, 23, 0), datetime(2023, 7, 21, 0, 0), datetime(2023, 7, 21, 1, 0),
                  datetime(2023, 7, 21, 2, 0), datetime(2023, 7, 21, 3, 0)],
        'canopy temperature': [55.256, 52.646, 50.432, 54.014, 52.646, 54.032, 60.836, 65.53399999999999, 69.476,
                               73.112, 76.622, 79.43,
                               81.10400000000001, 81.69800000000001, 81.518, 82.4, 81.95, 81.05000000000001, 78.35,
                               75.992, 69.782,
                               60.980000000000004, 56.678, 54.59, 53.474000000000004, 51.602000000000004],
        'ambient temperature': [63.032, 61.736000000000004, 59.900000000000006, 61.358, 60.403999999999996, 59.252,
                                64.598, 68.684, 73.346, 78.332, 82.706, 86.36, 89.924, 92.588, 94.244,
                                95.23400000000001, 95.036, 94.028, 90.878, 86.25200000000001, 81.536, 72.84200000000001,
                                68.53999999999999, 65.012, 62.852000000000004, 60.8],
        'rh': [80.52663133187149, 84.9819295323405, 85.41674613884034, 80.08178528513635, 80.04695849791608,
               83.16846927153982, 75.26573241955552, 70.81848333402235, 62.873531439573505, 56.13080204457853,
               53.03899459857565, 50.8968949812234, 46.67802474050185, 40.80560744310242, 34.37989538811474,
               33.32984365718972, 32.0236663681046, 31.38527916263018, 36.93748457949296, 39.63722226367993,
               40.44633845441565, 59.775765008068696, 70.74914888721767, 78.17381475287601, 82.4200261823118,
               82.39672792694286],
        'vpd': [0.3828092904396303, 0.282046319709216, 0.25660811894772273, 0.3691055404373449, 0.35744845339418174,
                0.28940161040013845, 0.5136422510676344, 0.6980308939829762, 1.0404511422480271, 1.451343961529694,
                1.7920602690880454, 2.1070279709709396, 2.561116696698284, 3.0898708301793576, 3.605490249825002,
                3.7765930278545765, 3.8272110422802132, 3.7449728003172726, 3.120902235250944, 2.5812835113023276,
                2.1880037708846514, 1.108297234218054, 0.6962406481023409, 0.4598435833744532, 0.3434087461203972,
                0.3198197180442852],
        'vwc_1': [37.81065358526901, 37.79828504828351, 37.79004264992585, 37.761214986098366, 37.72419817839436,
                  37.69954984881112, 37.6380322318548, 37.389371860434025, 36.958990627096796, 36.38341214214202,
                  35.828577821592766, 35.3420855502482, 34.83238091480594, 35.17096666579316, 38.83677691331808,
                  39.07367340431285, 39.208122199644, 39.31703769827042, 39.40886566369435, 39.46586716084299,
                  39.492215771878136, 39.61551279683273, 39.68621804209782, 39.810391166228996, 39.868233487841124,
                  39.92173395220333],
        'vwc_2': [36.75641704982944, 36.75246138128032, 36.75246138128032, 36.748506339913774, 36.74455192560839,
                  36.73664497769727, 36.708990394633425, 36.67347955751499, 36.61834103141008, 36.55547508905435,
                  36.49276815504403, 36.422412301226316, 36.352256358073596, 36.282299619206526, 36.23964604119867,
                  36.23964604119867, 36.247395730636626, 36.26290244917483, 36.2900627749064, 36.352256358073596,
                  36.426315708681486, 36.524101777013044, 36.62621047918755, 36.75641704982944, 36.91117861667661,
                  37.0468830523206],
        'vwc_3': [35.24521542653574, 35.24893394212669, 35.252653037420515, 35.24893394212669, 35.23778013397809,
                  35.23778013397809, 35.21548815657716, 35.17467361276228, 35.115431652549134, 35.041586267445936,
                  34.96797005037418, 34.89091860430733, 34.81046666495629, 34.74484591327776, 34.66126258585405,
                  34.57436120037958, 34.520210985084, 34.513000410496275, 34.49858592464108, 34.51660542005071,
                  34.54185604711748, 34.58520628060442, 34.63588308364716, 34.690301353063546, 34.7995171730743,
                  34.98635267312977],
        'vwc_1_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'vwc_2_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'vwc_3_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'daily gallons': [],
        'daily switch': [0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 26, 60, 60, 60, 60, 60, 60,
                         60, 60, 60, 60, 60, 60]}

    overnight_irrigation_day_two = {
        'dates': [datetime(2023, 7, 21, 1, 0), datetime(2023, 7, 21, 2, 0), datetime(2023, 7, 21, 3, 0),
                  datetime(2023, 7, 21, 4, 0), datetime(2023, 7, 21, 5, 0), datetime(2023, 7, 21, 6, 0),
                  datetime(2023, 7, 21, 7, 0), datetime(2023, 7, 21, 8, 0), datetime(2023, 7, 21, 9, 0),
                  datetime(2023, 7, 21, 10, 0), datetime(2023, 7, 21, 11, 0), datetime(2023, 7, 21, 12, 0),
                  datetime(2023, 7, 21, 13, 0), datetime(2023, 7, 21, 14, 0), datetime(2023, 7, 21, 15, 0),
                  datetime(2023, 7, 21, 16, 0), datetime(2023, 7, 21, 17, 0), datetime(2023, 7, 21, 18, 0),
                  datetime(2023, 7, 21, 19, 0), datetime(2023, 7, 21, 20, 0), datetime(2023, 7, 21, 21, 0),
                  datetime(2023, 7, 21, 22, 0), datetime(2023, 7, 21, 23, 0), datetime(2023, 7, 22, 0, 0),
                  datetime(2023, 7, 22, 1, 0), datetime(2023, 7, 22, 2, 0)],
        'canopy temperature': [54.59, 53.474000000000004, 51.602000000000004, 50.522, 50.666, 49.226, 54.734, 66.524,
                               69.512, 74.156, 77.55799999999999, 81.212, 83.732, 85.55000000000001, 85.69399999999999,
                               86.21600000000001, 85.82, 84.758, 83.94800000000001, 81.68, 73.22, 68.576, 65.39,
                               62.150000000000006, 60.674, 58.532],
        'ambient temperature': [65.012, 62.852000000000004, 60.8, 59.792, 60.476, 58.298, 59.432, 69.404, 75.686,
                                80.348, 85.082, 89.042, 92.318, 95.108, 97.052, 98.65400000000001, 99.464, 99.536,
                                97.538, 94.55000000000001, 83.98400000000001, 77.52199999999999, 73.634, 70.898, 69.44,
                                66.344],
        'rh': [78.17381475287601, 82.4200261823118, 82.39672792694286, 83.34996194814208, 84.68639802744981,
               86.41868764880799, 86.44958779401792, 73.17213815093343, 58.20340342969467, 52.87868262989771,
               47.53165878959346, 45.52971631712544, 43.02344763973963, 40.99106914614765, 36.61769065878929,
               34.558226440246166, 34.31095218764193, 34.081545019652154, 37.921839234075506, 42.007816273763446,
               62.11206565314386, 72.42868645910339, 77.25167903094132, 80.93471469328003, 80.78140174444168,
               83.65649351429238],
        'vpd': [0.4598435833744532, 0.3434087461203972, 0.3198197180442852, 0.29185023034442614, 0.2750381305944678,
                0.22567762907448796, 0.2344882974855773, 0.6577528738868952, 1.2667506022921484, 1.6655056023450625,
                2.161359706479848, 2.5446741768934658, 2.949247191178093, 3.329692538107112, 3.7959085317662358,
                4.114938426925017, 4.233006531524808, 4.257040557524185, 3.773291682458248, 3.216586818070887,
                1.5066830711980659, 0.8880994207624084, 0.643711958913475, 0.4918571205358999, 0.4717729516669893,
                0.36064281091893724],
        'vwc_1': [39.810391166228996, 39.868233487841124, 39.92173395220333, 39.970866930672024, 39.993228886621466,
                  40.02456588706544, 40.042488610257806, 40.015608853715804, 39.993228886621466, 39.988755055024505,
                  39.97980955268336, 39.96192718802165, 39.99770343866413, 40.02456588706544, 40.042488610257806,
                  40.06939435081267, 40.11878911490907, 40.1367725539453, 40.1772776764044, 40.2133315549563,
                  40.258464284680564, 40.30819432891111, 40.34894814283696, 40.40337855847787, 40.42608897370039,
                  40.45336563172306],
        'vwc_2': [36.75641704982944, 36.91117861667661, 37.0468830523206, 37.11501074163381, 37.151152973989184,
                  37.16321188771556, 37.17125436162099, 37.13106756872091, 37.09896412581797, 37.05889220971872,
                  37.02288191017966, 36.97095787484219, 36.91515947184176, 36.87537934688412, 36.85948496992455,
                  36.835662315643525, 36.823759491438594, 36.83963118300228, 36.8674308970145, 36.903218802307336,
                  36.96696815857785, 37.042881272306836, 37.07491333127058, 37.11902399079601, 37.151152973989184,
                  37.17929939512668],
        'vwc_3': [34.690301353063546, 34.7995171730743, 34.98635267312977, 35.286151001679, 35.552069624724545,
                  35.74111697485347, 35.881969738714005, 35.96610907095909, 35.95844800581616, 35.90870978026892,
                  35.820959986301574, 35.710769736515324, 35.61240370425121, 35.53324619765732, 35.48437415173851,
                  35.45809956500814, 35.45809956500814, 35.503159331271746, 35.60107954599157, 35.74111697485347,
                  35.88960674851385, 36.04285247977002, 36.10442144014985, 36.14298092881212, 36.18160124622358,
                  36.1970664323692],
        'vwc_1_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'vwc_2_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'vwc_3_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None], 'daily gallons': [],
        'daily switch': [60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60,
                         60, 60]}

    overnight_irrigation_day_three = {
        'dates': [datetime(2023, 7, 22, 2, 0), datetime(2023, 7, 22, 3, 0), datetime(2023, 7, 22, 4, 0),
                  datetime(2023, 7, 22, 5, 0), datetime(2023, 7, 22, 6, 0), datetime(2023, 7, 22, 7, 0),
                  datetime(2023, 7, 22, 8, 0), datetime(2023, 7, 22, 9, 0), datetime(2023, 7, 22, 10, 0),
                  datetime(2023, 7, 22, 11, 0), datetime(2023, 7, 22, 12, 0), datetime(2023, 7, 22, 13, 0),
                  datetime(2023, 7, 22, 14, 0), datetime(2023, 7, 22, 15, 0), datetime(2023, 7, 22, 16, 0),
                  datetime(2023, 7, 22, 17, 0), datetime(2023, 7, 22, 18, 0), datetime(2023, 7, 22, 19, 0),
                  datetime(2023, 7, 22, 20, 0), datetime(2023, 7, 22, 21, 0), datetime(2023, 7, 22, 22, 0),
                  datetime(2023, 7, 22, 23, 0), datetime(2023, 7, 23, 0, 0), datetime(2023, 7, 23, 1, 0),
                  datetime(2023, 7, 23, 2, 0), datetime(2023, 7, 23, 3, 0)],
        'canopy temperature': [58.532, 56.948, 55.147999999999996, 54.41, 53.672, 56.804, 66.884, 69.98, 73.922, 77.846,
                               80.582, 82.886, 85.04599999999999, 85.80199999999999, 85.56800000000001, 85.298,
                               84.30799999999999, 83.048, 79.016, 73.22, 71.258, 68.594, 66.362, 65.53399999999999,
                               65.03, 64.958],
        'ambient temperature': [66.344, 65.156, 63.30200000000001, 62.635999999999996, 61.664, 61.772,
                                70.05199999999999, 76.766, 80.474, 85.51400000000001, 88.952, 91.094, 94.676, 97.412,
                                99.35600000000001, 100.274, 100.058, 98.76200000000001, 89.78, 81.19399999999999,
                                77.48599999999999, 75.326, 73.58000000000001, 71.654, 70.646, 69.80000000000001],
        'rh': [83.65649351429238, 84.3944299919226, 84.60400435099564, 83.20356948394618, 83.16993384184633,
               84.2357419641575, 75.12122437098719, 55.451363620329666, 52.35162079540826, 46.232604551676936,
               41.666313570327986, 43.27197278745828, 40.192790165691264, 36.54826043011582, 32.05664575550409,
               31.07477928066611, 30.332670272672672, 35.61829353341872, 60.44205115297182, 70.01781272516467,
               75.1569111796218, 75.2687377411146, 78.06462167640082, 80.92529333584851, 82.02210854992953,
               82.69107240727365],
        'vpd': [0.36064281091893724, 0.3304383192955198, 0.3055396360133882, 0.32561875158039033, 0.3152731024683151,
                0.29643692981774383, 0.6236151620489818, 1.3994917258434119, 1.6910782745029445, 2.2457060686395165,
                2.71743947707016, 2.826439812924091, 3.3301628241633403, 3.8420077458036417, 4.3640051256890215,
                4.5514258247385175, 4.57058296915861, 4.061556017406509, 1.891439320591132, 1.0893611419507776,
                0.7992689936938517, 0.7406031614779964, 0.6195829578734493, 0.5048856790057883, 0.4598469477200515,
                0.43015340311300454],
        'vwc_1': [40.45336563172306, 40.485221743660915, 40.49433009094425, 40.50799811634924, 40.5171138043519,
                  40.51255559316331, 40.49433009094425, 40.4761163309697, 40.462463712173324, 40.44427048247332,
                  40.42608897370039, 40.42608897370039, 40.41700261223875, 40.430633252480504, 40.44427048247332,
                  40.47156472479321, 40.49433009094425, 40.5216727500366, 40.55360594588748, 40.5992873219101,
                  40.63588545758981, 40.66794755055376, 40.67711479316234, 40.68628499071156, 40.700045829668326,
                  40.709223418807916],
        'vwc_2': [37.17929939512668, 37.19137174675629, 37.211505144535685, 37.22359287557082, 37.2316545700992,
                  37.227623402036954, 37.19942318350298, 37.17125436162099, 37.14311689484595, 37.11501074163381,
                  37.09495406646659, 37.066901496992614, 37.0468830523206, 37.02288191017966, 36.99091596624822,
                  36.97095787484219, 36.97095787484219, 36.98692307955209, 37.030879747926626, 37.09495406646659,
                  37.15919161031036, 37.19942318350298, 37.227623402036954, 37.25585505876743, 37.2639270265811,
                  37.267963974456194],
        'vwc_3': [36.1970664323692, 36.22028251350562, 36.23577211351336, 36.251271492631744, 36.27065948021286,
                  36.27065948021286, 36.243520580199196, 36.200934253560746, 36.13912224465059, 36.07361756452598,
                  36.00445057900821, 35.90870978026892, 35.8400090624988, 35.79051255997969, 35.77910463132148,
                  35.78290667726512, 35.824768604829906, 35.91253218883277, 36.07361756452598, 36.23189879702258,
                  36.29394527203087, 36.33280397887848, 36.348364653062994, 36.36004161270008, 36.37561950376518,
                  36.37561950376518],
        'vwc_1_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'vwc_2_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'vwc_3_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None],
        'daily gallons': [],
        'daily switch': [60, 60, 60, 60, 60, 60, 59, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60,
                         60, 60]

    }

    overnight_irrigation_day_four = {
        'dates': [datetime(2023, 7, 23, 2, 0), datetime(2023, 7, 23, 3, 0), datetime(2023, 7, 23, 4, 0),
                  datetime(2023, 7, 23, 5, 0), datetime(2023, 7, 23, 6, 0), datetime(2023, 7, 23, 7, 0),
                  datetime(2023, 7, 23, 8, 0), datetime(2023, 7, 23, 9, 0), datetime(2023, 7, 23, 10, 0),
                  datetime(2023, 7, 23, 11, 0), datetime(2023, 7, 23, 12, 0), datetime(2023, 7, 23, 13, 0),
                  datetime(2023, 7, 23, 14, 0), datetime(2023, 7, 23, 15, 0), datetime(2023, 7, 23, 16, 0),
                  datetime(2023, 7, 23, 17, 0), datetime(2023, 7, 23, 18, 0), datetime(2023, 7, 23, 19, 0),
                  datetime(2023, 7, 23, 20, 0)],
        'canopy temperature': [58.532, 56.948, 55.147999999999996, 54.41, 53.672, 56.804, 66.884, 69.98, 73.922, 77.846,
                               80.582, 82.886, 85.04599999999999, 85.80199999999999, 85.56800000000001, 85.298,
                               84.30799999999999, 83.048, 79.016],
        'ambient temperature': [66.344, 65.156, 63.30200000000001, 62.635999999999996, 61.664, 61.772,
                                70.05199999999999, 76.766, 80.474, 85.51400000000001, 88.952, 91.094, 94.676, 97.412,
                                99.35600000000001, 100.274, 100.058, 98.76200000000001, 89.78],
        'rh': [83.65649351429238, 84.3944299919226, 84.60400435099564, 83.20356948394618, 83.16993384184633,
               84.2357419641575, 75.12122437098719, 55.451363620329666, 52.35162079540826, 46.232604551676936,
               41.666313570327986, 43.27197278745828, 40.192790165691264, 36.54826043011582, 32.05664575550409,
               31.07477928066611, 30.332670272672672, 35.61829353341872, 60.44205115297182],
        'vpd': [0.36064281091893724, 0.3304383192955198, 0.3055396360133882, 0.32561875158039033, 0.3152731024683151,
                0.29643692981774383, 0.6236151620489818, 1.3994917258434119, 1.6910782745029445, 2.2457060686395165,
                2.71743947707016, 2.826439812924091, 3.3301628241633403, 3.8420077458036417, 4.3640051256890215,
                4.5514258247385175, 4.57058296915861, 4.061556017406509, 1.891439320591132],
        'vwc_1': [40.45336563172306, 40.485221743660915, 40.49433009094425, 40.50799811634924, 40.5171138043519,
                  40.51255559316331, 40.49433009094425, 40.4761163309697, 40.462463712173324, 40.44427048247332,
                  40.42608897370039, 40.42608897370039, 40.41700261223875, 40.430633252480504, 40.44427048247332,
                  40.47156472479321, 40.49433009094425, 40.5216727500366, 40.55360594588748],
        'vwc_2': [37.17929939512668, 37.19137174675629, 37.211505144535685, 37.22359287557082, 37.2316545700992,
                  37.227623402036954, 37.19942318350298, 37.17125436162099, 37.14311689484595, 37.11501074163381,
                  37.09495406646659, 37.066901496992614, 37.0468830523206, 37.02288191017966, 36.99091596624822,
                  36.97095787484219, 36.97095787484219, 36.98692307955209, 37.030879747926626],
        'vwc_3': [36.1970664323692, 36.22028251350562, 36.23577211351336, 36.251271492631744, 36.27065948021286,
                  36.27065948021286, 36.243520580199196, 36.200934253560746, 36.13912224465059, 36.07361756452598,
                  36.00445057900821, 35.90870978026892, 35.8400090624988, 35.79051255997969, 35.77910463132148,
                  35.78290667726512, 35.824768604829906, 35.91253218883277, 36.07361756452598],
        'vwc_1_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None],
        'vwc_2_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None],
        'vwc_3_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None],
        'daily gallons': [],
        'daily switch': [60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60]

    }

    overnight_irrigation_day_five = {
        'dates': [datetime(2023, 7, 23, 20, 0), datetime(2023, 7, 23, 21, 0), datetime(2023, 7, 23, 22, 0),
                  datetime(2023, 7, 23, 23, 0), datetime(2023, 7, 24, 0, 0), datetime(2023, 7, 24, 1, 0),
                  datetime(2023, 7, 24, 2, 0), datetime(2023, 7, 24, 3, 0), datetime(2023, 7, 24, 4, 0),
                  datetime(2023, 7, 24, 5, 0), datetime(2023, 7, 24, 6, 0), datetime(2023, 7, 24, 7, 0),
                  datetime(2023, 7, 24, 8, 0), datetime(2023, 7, 24, 9, 0), datetime(2023, 7, 24, 10, 0),
                  datetime(2023, 7, 24, 11, 0), datetime(2023, 7, 24, 12, 0), datetime(2023, 7, 24, 13, 0),
                  datetime(2023, 7, 24, 14, 0), datetime(2023, 7, 24, 15, 0), datetime(2023, 7, 24, 16, 0),
                  datetime(2023, 7, 24, 17, 0), datetime(2023, 7, 24, 18, 0), datetime(2023, 7, 24, 19, 0),
                  datetime(2023, 7, 24, 20, 0), datetime(2023, 7, 24, 21, 0),
                  datetime(2023, 7, 24, 22, 0), datetime(2023, 7, 24, 23, 0), datetime(2023, 7, 25, 0, 0)],
        'canopy temperature': [58.532, 56.948, 55.147999999999996, 54.41, 53.672, 56.804, 66.884, 69.98, 73.922, 77.846,
                               80.582, 82.886, 85.04599999999999, 85.80199999999999, 85.56800000000001, 85.298,
                               84.30799999999999, 83.048, 79.016, 73.22, 71.258, 68.594, 66.362, 65.53399999999999,
                               65.03, 64.958, 0, 0, 0],
        'ambient temperature': [66.344, 65.156, 63.30200000000001, 62.635999999999996, 61.664, 61.772,
                                70.05199999999999, 76.766, 80.474, 85.51400000000001, 88.952, 91.094, 94.676, 97.412,
                                99.35600000000001, 100.274, 100.058, 98.76200000000001, 89.78, 81.19399999999999,
                                77.48599999999999, 75.326, 73.58000000000001, 71.654, 70.646, 69.80000000000001,
                                0, 0, 0],
        'rh': [83.65649351429238, 84.3944299919226, 84.60400435099564, 83.20356948394618, 83.16993384184633,
               84.2357419641575, 75.12122437098719, 55.451363620329666, 52.35162079540826, 46.232604551676936,
               41.666313570327986, 43.27197278745828, 40.192790165691264, 36.54826043011582, 32.05664575550409,
               31.07477928066611, 30.332670272672672, 35.61829353341872, 60.44205115297182, 70.01781272516467,
               75.1569111796218, 75.2687377411146, 78.06462167640082, 80.92529333584851, 82.02210854992953,
               82.69107240727365, 0, 0, 0],
        'vpd': [0.36064281091893724, 0.3304383192955198, 0.3055396360133882, 0.32561875158039033, 0.3152731024683151,
                0.29643692981774383, 0.6236151620489818, 1.3994917258434119, 1.6910782745029445, 2.2457060686395165,
                2.71743947707016, 2.826439812924091, 3.3301628241633403, 3.8420077458036417, 4.3640051256890215,
                4.5514258247385175, 4.57058296915861, 4.061556017406509, 1.891439320591132, 1.0893611419507776,
                0.7992689936938517, 0.7406031614779964, 0.6195829578734493, 0.5048856790057883, 0.4598469477200515,
                0.43015340311300454, 0, 0, 0],
        'vwc_1': [40.45336563172306, 40.485221743660915, 40.49433009094425, 40.50799811634924, 40.5171138043519,
                  40.51255559316331, 40.49433009094425, 40.4761163309697, 40.462463712173324, 40.44427048247332,
                  40.42608897370039, 40.42608897370039, 40.41700261223875, 40.430633252480504, 40.44427048247332,
                  40.47156472479321, 40.49433009094425, 40.5216727500366, 40.55360594588748, 40.5992873219101,
                  40.63588545758981, 40.66794755055376, 40.67711479316234, 40.68628499071156, 40.700045829668326,
                  40.709223418807916, 0, 0, 0],
        'vwc_2': [37.17929939512668, 37.19137174675629, 37.211505144535685, 37.22359287557082, 37.2316545700992,
                  37.227623402036954, 37.19942318350298, 37.17125436162099, 37.14311689484595, 37.11501074163381,
                  37.09495406646659, 37.066901496992614, 37.0468830523206, 37.02288191017966, 36.99091596624822,
                  36.97095787484219, 36.97095787484219, 36.98692307955209, 37.030879747926626, 37.09495406646659,
                  37.15919161031036, 37.19942318350298, 37.227623402036954, 37.25585505876743, 37.2639270265811,
                  37.267963974456194, 0, 0, 0],
        'vwc_3': [36.1970664323692, 36.22028251350562, 36.23577211351336, 36.251271492631744, 36.27065948021286,
                  36.27065948021286, 36.243520580199196, 36.200934253560746, 36.13912224465059, 36.07361756452598,
                  36.00445057900821, 35.90870978026892, 35.8400090624988, 35.79051255997969, 35.77910463132148,
                  35.78290667726512, 35.824768604829906, 35.91253218883277, 36.07361756452598, 36.23189879702258,
                  36.29394527203087, 36.33280397887848, 36.348364653062994, 36.36004161270008, 36.37561950376518,
                  36.37561950376518, 0, 0, 0],
        'vwc_1_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None, None, None, None],
        'vwc_2_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None, None, None, None],
        'vwc_3_ec': [None, None, None, None, None, None, None, None, None, None, None, None, None, None, None, None,
                     None, None, None, None, None, None, None, None, None, None, None, None, None],
        'daily gallons': [],
        'daily switch': [60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60, 60,
                         60, 60, 0, 0, 0]

    }

    # test_case_1 = SwitchTestCase()
    # test_case_standard = SwitchTestCase(120, standard_data_2023, 0, [480])
    # test_case_half_and_half = SwitchTestCase(55, half_and_half, 0, [415, 240])
    # test_case_multiple_days = SwitchTestCase(60, multiple_days, 60, [660, 300])
    # test_case_small_edges = SwitchTestCase(60, small_edges, 0, [660])
    # test_case_lacking_full_days = SwitchTestCase(0, lacking_full_days, 60, [300])
    # test_case_small_med_edges = SwitchTestCase(0, small_med_edges, 0, [480])
    # test_case_med_edges = SwitchTestCase(240, med_edges, 180, [300])

    test_case_overnight_irrigation_day_one = SwitchTestCase(0, overnight_irrigation_day_one, 240, [506])
    test_case_overnight_irrigation_day_two = SwitchTestCase(0, overnight_irrigation_day_two, 240, [1440])
    test_case_overnight_irrigation_day_three = SwitchTestCase(0, overnight_irrigation_day_three, 240, [1439])
    test_case_overnight_irrigation_day_four = SwitchTestCase(0, overnight_irrigation_day_four, 240, [1260])
    test_case_overnight_irrigation_day_five = SwitchTestCase(0, overnight_irrigation_day_five, 240, [1320])

    # test_cases.append(test_case_standard)
    # test_cases.append(test_case_half_and_half)
    # test_cases.append(test_case_multiple_days)
    # test_cases.append(test_case_small_edges)
    # test_cases.append(test_case_lacking_full_days)
    # test_cases.append(test_case_small_med_edges)
    # test_cases.append(test_case_med_edges)
    test_cases.append(test_case_overnight_irrigation_day_one)
    test_cases.append(test_case_overnight_irrigation_day_two)
    test_cases.append(test_case_overnight_irrigation_day_three)
    test_cases.append(test_case_overnight_irrigation_day_four)
    test_cases.append(test_case_overnight_irrigation_day_five)

    # test_cases.append(test_case_small_edges)

    test_results = []

    logger = Logger('z6', '', '', 'tomato', 'Clay', 0, 0, 'N', None, planting_date='3/1/2023')

    # Loop through each test case and record results
    for ind, test_case in enumerate(test_cases):
        print(f'================TEST CASE {ind + 1}================')
        test_result = test_data_pipeline(test_case, logger)
        test_result_tup = (ind, test_result)
        test_results.append(test_result_tup)
    print('All Test Results: ', test_results)
    # print(test_results)


def test_data_pipeline(test_case, logger):
    """
    Method to simulate a standard STomato run including switch processing, high/low temp processing,
     final results cleanup, kc calculation, last day removal
    :param test_case: SwitchTestCase instance with all the switch test criteria
    :return:
    """

    # Grab test criteria
    in_prev_switch = test_case.in_prev_switch
    in_test_case_data = test_case.in_test_case_data
    out_prev_switch = test_case.out_prev_switch
    out_switch_values = test_case.out_switch_values

    test_pass = False
    cwsi_processor = CwsiProcessor()
    # logger = Logger('z6', '', '', 'tomato', 'Clay', 0, 0, 'N', None, planting_date='3/1/2023')

    for key, values in in_test_case_data.items():
        print(key, " : ", values)
    print('=========================================')
    print()
    print('>================Testing switch================')
    cwsi_processor.update_irrigation_ledger(in_test_case_data, logger.irrigation_ledger)
    print('After updating ledger: ')
    for date, list in logger.irrigation_ledger.items():
        print(f'{date} : {list}')
    print('<================Testing switch================')
    print()

    print('>================Testing Getting High and Low Temp Indexes================')
    highest_temp_values_ind, lowest_temp_values_ind, _ = cwsi_processor.get_highest_and_lowest_temperature_indexes(
        in_test_case_data
    )
    print('<================Testing Getting High and Low Temp Indexes================')
    print()

    print('>================Testing Final Results================')
    final_results_converted = cwsi_processor.final_results(
        in_test_case_data, highest_temp_values_ind, lowest_temp_values_ind, logger
    )
    print('After final results ledger: ')
    for date, list in logger.irrigation_ledger.items():
        print(f'{date} : {list}')
    print('<================Testing Final Results================')
    print()

    print('>================Testing Switch Overflow================')
    logger.check_and_update_delayed_ledger_filled_lists()

    print('After switch overflow ledger: ')
    for date, list in logger.irrigation_ledger.items():
        print(f'{date} : {list}')
    print('<================Testing Switch Overflow================')

    print('>================Testing get kc================')
    final_results_converted = logger.get_kc(final_results_converted)
    print('<================Testing get kc================')
    print()

    print()
    print('=======================================')
    print('================RESULTS================')
    for key, values in final_results_converted.items():
        print(key, " : ", values)
    print()

    switch_list_length_match = False
    switch_list_values_match = False
    output_prev_switch_match = False

    data_points = len(out_switch_values)
    switch_data_points = len(final_results_converted['daily switch'])
    if data_points == switch_data_points:
        switch_list_length_match = True

    if switch_list_length_match:
        if final_results_converted['daily switch'] == out_switch_values:
            switch_list_values_match = True

    if switch_list_length_match and switch_list_values_match:
        test_pass = True
        print('SUCCESS')
    else:
        print('FAIL')
        print('Switch list length match: ', switch_list_length_match)
        print('Switch list values match: ', switch_list_values_match)
    print('=======================================')
    print()
    print()
    return test_pass


def cleanup_cimis_stations_pickle():
    """
    Go through the cimis stations pickle and remove any cimis station objects for stations that are no longer part
    of our current operating growers pickle
    """
    growers = open_pickle()
    cimis_stations_pickle_list = open_pickle(filename="cimisStation.pickle")
    grower_pickle_cimis_stations = []

    for grower in growers:
        for field in grower.fields:
            if field.cimis_station not in grower_pickle_cimis_stations:
                grower_pickle_cimis_stations.append(field.cimis_station)

    # print('Grower Pickle Active Stations #:')
    # grower_pickle_cimis_stations.sort()
    # print(grower_pickle_cimis_stations)

    pickle_indexes_to_be_removed = []
    for ind, station in enumerate(cimis_stations_pickle_list):
        if station.station_number not in grower_pickle_cimis_stations:
            pickle_indexes_to_be_removed.append(ind)

    # Reverse the list of indexes we will be removing using pop() so we don't run into an index out of bounds issue
    # from removing from our list from the beginning
    pickle_indexes_to_be_removed.reverse()
    for ind in pickle_indexes_to_be_removed:
        cimis_stations_pickle_list.pop(ind)

    # print('Pickle Cimis Stations:')
    # pickle_stations = []
    # for station in cimis_stations_pickle_list:
    #     pickle_stations.append(station.station_number)
    # pickle_stations.sort()
    # print(pickle_stations)
    write_pickle(cimis_stations_pickle_list, filename="cimisStation.pickle")


def turn_ai_game_data_into_csv(pickle_name, pickle_path, csv_name):
    global ai_game_data, vwc, field_capacity, wilting_point, psi, psi_threshold, psi_critical, et_hours
    grower_pickle_file_name = pickle_name
    grower_pickle_file_path = pickle_path
    ai_game_data = open_pickle(filename=grower_pickle_file_name, specific_file_path=grower_pickle_file_path)
    # ai_game_data.show_content()
    stage_dict = {'Stage 1': 1, 'Stage 2': 2, 'Stage 3': 3, 'Stage 4': 4}
    all_data = []
    all_labels = []
    for data_point in ai_game_data.ai_game_data:
        print(data_point)
        data_point_values = []

        crop_stage = stage_dict[data_point.crop_stage]
        vwc = data_point.vwc_avg
        field_capacity = data_point.field_capacity
        wilting_point = data_point.wilting_point
        psi = data_point.psi
        psi_threshold = data_point.psi_threshold
        psi_critical = data_point.psi_critical
        et_hours = data_point.et_hours
        label_hours = data_point.human_p2

        data_point_values.append(crop_stage)
        data_point_values.append(vwc)
        data_point_values.append(field_capacity)
        data_point_values.append(wilting_point)
        data_point_values.append(psi)
        data_point_values.append(psi_threshold)
        data_point_values.append(psi_critical)
        data_point_values.append(et_hours)
        all_data.append(data_point_values)
        all_labels.append(label_hours)
    df = pd.DataFrame(
        all_data,
        columns=['crop_stage', 'vwc', 'field_capacity', 'wilting_point', 'psi', 'psi_threshold',
                 'psi_critical',
                 'et_hours']
    )
    df['irrigation_hours'] = all_labels
    df.to_csv(csv_name + '.csv', index=False)
    print(all_data)
    print(all_labels)


def get_tomato_yield_data(pickle_name: str, pickle_path: str, excel_filename: str, excel_data_start_row: int,
                          excel_data_end_row: int):
    growers = open_pickle(filename=pickle_name, specific_file_path=pickle_path)
    for grower in growers:
        for field in grower.fields:
            field.crop_type = field.loggers[0].cropType
            field.net_yield = None
            field.paid_yield = None

    whole_file_df = pd.read_excel(excel_filename)
    df = whole_file_df.loc[excel_data_start_row:excel_data_end_row,
         ['Grower', 'Field - variety area', 'Grower field #', 'Gradient Field #', 'Loads', 'Acres', 'Net T/A',
          'Paid T/A']]

    mask_grower = None
    for grower in growers:
        tomato_fields = 0
        for field in grower.fields:
            if field.crop_type in ['Tomato', 'Tomatoes', 'tomato', 'tomatoes']:
                if hasattr(field.loggers[0], 'rnd') and not field.loggers[0].rnd:
                    # print(f'Grower: {grower.name} - Field: {field.nickname}')
                    mask_grower_field = (df['Grower'] == grower.name) & (df['Gradient Field #'] == field.nickname)
                    tomato_fields += 1
                    results_count = mask_grower_field.sum()
                    search_grower_field = df[mask_grower_field]

                    net_yield_search = search_grower_field.loc[mask_grower_field, 'Net T/A']
                    if not net_yield_search.empty:
                        net_yield = round(net_yield_search.mean(), 1)
                    else:
                        net_yield = None

                    paid_yield_search = search_grower_field.loc[mask_grower_field, 'Paid T/A']
                    if not paid_yield_search.empty:
                        paid_yield = round(paid_yield_search.mean(), 1)
                    else:
                        paid_yield = None

                    print(
                        f'{grower.name:25} - {field.nickname:20}\t Net: {str(net_yield):10}  Paid: {str(paid_yield):10} Results: {results_count}'
                    )
                    field.net_yield = net_yield
                    field.paid_yield = paid_yield
                    results_count = 0

    write_pickle(growers, filename=pickle_name, specific_file_path=pickle_path)


def graph_yields(pickle_name: str, pickle_path: str, yield_type: str):
    yield_data = []
    growers = open_pickle(filename=pickle_name, specific_file_path=pickle_path)
    for grower in growers:
        for field in grower.fields:
            if field.net_yield is not None:
                # field_names.append(grower.name + '-' + field.nickname)
                # net_yields.append(field.net_yield)
                # paid_yields.append(field.paid_yield)
                yield_data.append((grower.name + '-' + field.nickname, field.net_yield, field.paid_yield))

    # Extract the field names and yield values
    field_names, net_yields, paid_yields = zip(*yield_data)

    # Create a figure with two subplots
    fig, ax = plt.subplots(ncols=1, figsize=(50, 10))

    if yield_type in ['Net', 'net', 'n', 'N']:
        # Sort the data by net_yield
        sorted_data = sorted(zip(net_yields, paid_yields, field_names), reverse=True)

        # Unpack the sorted data into separate lists
        net_yields, paid_yields, field_names = zip(*sorted_data)

        # Plot the net yields
        ax.bar(field_names, net_yields)
        ax.set_title('Net Yields')
        ax.tick_params(axis='x', rotation=90)  # Rotate x-axis labels by 90 degrees

        # Add data labels to the net yields bars
        for i, v in enumerate(net_yields):
            ax.text(i, v + 1, str(v), ha='center', fontsize=8)

    if yield_type in ['Paid', 'paid', 'p', 'P']:
        # Sort the data by paid_yield
        sorted_data = sorted(zip(paid_yields, net_yields, field_names), reverse=True)

        # Unpack the sorted data into separate lists
        paid_yields, net_yields, field_names = zip(*sorted_data)

        # Plot the net yields
        ax.bar(field_names, paid_yields)
        ax.set_title('Paid Yields')
        ax.tick_params(axis='x', rotation=90)  # Rotate x-axis labels by 90 degrees

        # Add data labels to the paid yields bars
        for i, v in enumerate(paid_yields):
            ax.text(i, v + 1, str(v), ha='center', fontsize=8)

    fig.tight_layout()
    plt.show()


def compare_new_psi_algo_vs_old():
    # all_data = {
    #     'logger': [],
    #     'field': [],
    #     'grower': [],
    #     'soil type': [],
    #     'field capacity': [],
    #     'wilting point': [],
    #     'net yield': [],
    #     'paid yield': [],
    #     'ambient temp hours in opt with sun': [],
    #     'ambient temp hours in opt without sun': [],
    #     'vwc 1 in optimum hours': [],
    #     'vwc 2 in optimum hours': [],
    #     'vwc 3 in optimum hours': [],
    #     'vwc 1 in optimum %': [],
    #     'vwc 2 in optimum %': [],
    #     'vwc 3 in optimum %': [],
    #     'vwc 1_2 in optimum hours': [],
    #     'vwc 2_3 in optimum hours': [],
    #     'vwc 1_2 in optimum %': [],
    #     'vwc 2_3 in optimum %': [],
    #     'vwc total datapoints': [],
    #     'psi average': [],
    #     'psi first 3 values': [],
    #     'psi first 3 dates': []
    # }

    all_data_pickle_file_name = 'all_2022_analysis.pickle'
    all_data_pickle_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Pickles\\'
    new_algo_data = open_pickle(filename=all_data_pickle_file_name, specific_file_path=all_data_pickle_file_path)

    db_2022_psi_file_name = 'psi_pickle_2022.pickle'
    # db_2022_psi_file_path = 'H:\\Shared drives\\Stomato\\2023\\Pickle\\'
    db_2022_psi_file_path = 'H:\\Shared drives\\Stomato\\Data Analysis\\All Data\\Pickles\\'
    old_algo_data = open_pickle(filename=db_2022_psi_file_name, specific_file_path=db_2022_psi_file_path)

    for ind, dp in enumerate(new_algo_data['logger']):
        logger_index = -1
        new_algo_first_date = None
        old_algo_last_date = None
        days_diff = None
        try:
            logger_index = old_algo_data['logger'].index(dp)
        except ValueError:
            print(f'{dp} not in database tables')

        if logger_index >= 0:
            if new_algo_data['psi first 3 dates'][ind]:
                new_algo_first_date_dt = new_algo_data['psi first 3 dates'][ind][0]
                new_algo_first_date = new_algo_first_date_dt.date()

            if old_algo_data['dates'][logger_index]:
                old_algo_last_date = old_algo_data['dates'][logger_index][-1]

        if new_algo_first_date is not None and old_algo_last_date is not None:
            days_diff_dt = new_algo_first_date - old_algo_last_date
            days_diff = days_diff_dt.days

        print(f'Field: {new_algo_data["field"][ind]}\tLogger: {dp} found')
        if days_diff is not None:
            print(f'Days between: {days_diff}')
            print(f'New dates: {new_algo_data["psi first 3 dates"][ind]}')
            print(f'Old dates: {old_algo_data["dates"][logger_index]}')
        else:
            print('One of the two dates is None')
            print(f'New: {new_algo_first_date}')
            print(f'Old {old_algo_last_date}')
        print()


def setup_weather_stations():
    updated_stations = []

    # Atmos 41 Weather Stations
    eight_mile = WeatherStation(
        'z6-07111',
        '99294-35668',
        '8 Mile',
        'Tomatoes',
        datetime(2023, 1, 1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='248'
    )
    eight_mile.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(eight_mile)

    ave_7 = WeatherStation(
        'z6-02178',
        '74962-10103',
        'Ave 7',
        'Tomatoes',
        datetime(2023, 1, 1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='80'
    )
    ave_7.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(ave_7)

    ben_fast = WeatherStation(
        'z6-15905',
        '28402-51648',
        'Ben Fast',
        'Tomatoes',
        datetime(2023, month=4, day=12).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='5'
    )
    ben_fast.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(ben_fast)

    bone_farms = WeatherStation(
        'z6-16164',
        '64334-12769',
        'Bone Farms',
        'Tomatoes',
        datetime(2023, 1, 1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='146'
    )
    bone_farms.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(bone_farms)

    bullseye = WeatherStation(
        'z6-02054',
        '16128-76869',
        'Bullseye',
        'Tomatoes',
        datetime(2023, 1, 1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='250'
    )
    bullseye.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(bullseye)

    bullero = WeatherStation(
        'z6-11967',
        '36891-99736',
        'Bullero',
        'Tomatoes',
        datetime(2023, 1, 1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='226'
    )
    bullero.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(bullero)

    david_santos = WeatherStation(
        'z6-12376',
        '78094-52955',
        'David Santos',
        'Tomatoes',
        datetime(2023, 5, 17).date(),
        planting_date=None,
        start_date=datetime(2023, 4, 14).date(),
        end_date=datetime(2023, 9, 2).date(),
        station_type='Weather Station',
        cimis_station='124'
    )
    david_santos.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(david_santos)

    dresick_n = WeatherStation(
        'z6-16147',
        '13969-76188',
        'Dresick N',
        'Tomatoes',
        datetime(2023, month=4, day=15).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='2'
    )
    dresick_n.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(dresick_n)

    dresick_s = WeatherStation(
        'z6-16138',
        '65607-66874',
        'Dresick S',
        'Tomatoes',
        datetime(2023, month=4, day=15).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='2'
    )
    dresick_s.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(dresick_s)

    fantozzi = WeatherStation(
        'z6-23435',
        '92415-62348',
        'Fantozzi',
        'Tomatoes',
        datetime(2023, month=7, day=5).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='71'
    )
    fantozzi.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(fantozzi)

    lafayet = WeatherStation(
        'z6-07111',
        '99294-35668',
        'Lafayet',
        'Tomatoes',
        datetime(2023, month=6, day=16).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='248'
    )
    lafayet.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(lafayet)

    matteoli_mb = WeatherStation(
        'z6-11406',
        '71062-40854',
        'Matteoli MB',
        'Tomatoes',
        datetime(2023, month=1, day=1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='235'
    )
    matteoli_mb.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(matteoli_mb)

    matteoli_kbasin = WeatherStation(
        'z6-11920',
        '84949-16401',
        'Matteoli KBasin',
        'Tomatoes',
        datetime(2023, month=1, day=1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='235'
    )
    matteoli_kbasin.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(matteoli_kbasin)

    nees = WeatherStation(
        'z6-16154',
        '51205-71538',
        'Nees',
        'Tomatoes',
        datetime(2023, month=1, day=1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='124'
    )
    nees.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(nees)

    rg_farms = WeatherStation(
        'z6-23255',
        '56884-23705',
        'RG Farms',
        'Tomatoes',
        datetime(2023, month=7, day=27).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='126'
    )
    rg_farms.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(rg_farms)

    tp = WeatherStation(
        'z6-16139',
        '29296-99927',
        'TP',
        'Tomatoes',
        datetime(2023, month=1, day=1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='250'
    )
    tp.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(tp)

    wild_oak = WeatherStation(
        'z6-01874',
        '67392-80462',
        'Wild Oak',
        'Tomatoes',
        datetime(2023, month=1, day=1).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='39'
    )
    wild_oak.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(wild_oak)

    sanguinetti = WeatherStation(
        'z6-07162',
        '68090-60446',
        'Sanguinetti',
        'Tomatoes',
        datetime(2023, month=5, day=26).date(),
        planting_date=None,
        start_date=None,
        end_date=None,
        station_type='Weather Station',
        cimis_station='70'
    )
    sanguinetti.uninstall_date = datetime(2023, month=12, day=31).date()
    updated_stations.append(sanguinetti)

    # Gradient Atmos 14 Stations
    bullseye_oe10_E = WeatherStation(
        'z6-11974',
        '60644-59745',
        'Bullseye OE10 East Gradient',
        'Tomatoes',
        datetime(2023, month=4, day=26).date(),
        planting_date=datetime(2023, month=4, day=16).date(),
        start_date=None,
        end_date=None,
        station_type='Gradient Station',
        cimis_station='226'
    )
    bullseye_oe10_E.uninstall_date = datetime(2023, month=8, day=11).date()
    updated_stations.append(bullseye_oe10_E)

    bullseye_oe10_C = WeatherStation(
        'z6-11407',
        '60892-87745',
        'Bullseye OE10 Center Gradient',
        'Tomatoes',
        datetime(2023, month=4, day=26).date(),
        planting_date=datetime(2023, month=4, day=16).date(),
        start_date=None,
        end_date=None,
        station_type='Gradient Station',
        cimis_station='226'
    )
    bullseye_oe10_C.uninstall_date = datetime(2023, month=8, day=11).date()
    updated_stations.append(bullseye_oe10_C)

    bullseye_oe10_W = WeatherStation(
        'z6-11580',
        '93196-73070',
        'Bullseye OE10 West Gradient',
        'Tomatoes',
        datetime(2023, month=4, day=26).date(),
        planting_date=datetime(2023, month=4, day=16).date(),
        start_date=None,
        end_date=None,
        station_type='Gradient Station',
        cimis_station='226'
    )
    bullseye_oe10_W.uninstall_date = datetime(2023, month=8, day=11).date()
    updated_stations.append(bullseye_oe10_W)

    # lucero_towerline_blue_s = WeatherStation(
    #     'z6-12295',
    #     '52213-43606',
    #     'Lucero Towerline Blue S Gradient',
    #     'Tomatoes',
    #     datetime(2023, month=4, day=6).date(),
    #     planting_date=datetime(2023, month=3, day=28).date(),
    #     start_date=None,
    #     end_date=None,
    #     station_type='Gradient Station'
    # )
    # lucero_towerline_blue_s.uninstall_date = datetime(2023, month=7, day=14).date()
    # updated_stations.append(lucero_towerline_blue_s)

    print()
    pickle_file_name = 'weather_stations_2023.pickle'
    pickle_file_path = 'H:\\Shared drives\\Stomato\\HeatUnits\\Pickles\\'
    write_pickle(updated_stations, filename=pickle_file_name, specific_file_path=pickle_file_path)


def generate_gradient_grower_fields_report():
    growers = open_pickle()
    data = []
    data2 = {
        'Grower': [],
        'Field': [],
        'Crop Type': [],
        'Field Type': [],
        'Acres': [],
        'Region': [],
        'Latitude': [],
        'Longitude': []
    }
    growers_per_sheet = 20
    sheet_count = 1
    count = 0
    data2 = {key: [] for key in data2}
    for grower in growers:
        count += 1

        print(grower.name)
        for field in grower.fields:
            print('\t', field.nickname, field.crop_type, field.acres)
            rnd = False
            for logger in field.loggers:
                if logger.rnd:
                    print('\t\t', logger.name, 'R&D')
                    rnd = True
                    break
            if rnd:
                field_type = 'R&D'
            else:
                field_type = field.field_type
            field_dict = {
                'Grower': grower.name,
                'Field': field.nickname,
                'Crop Type': field.crop_type,
                'Field Type': field_type,
                'Acres': float(field.acres),
                'Region': grower.region,

            }
            data.append(field_dict)
            data2['Grower'].append(grower.name)
            data2['Field'].append(field.nickname)
            data2['Crop Type'].append(field.crop_type)
            data2['Field Type'].append(field_type)
            data2['Acres'].append(float(field.acres))
            data2['Region'].append(grower.region)
            data2['Latitude'].append(field.lat)
            data2['Longitude'].append(field.long)


    print()
    # Write remaining growers if any
    if count % growers_per_sheet != 0:
        df2 = pd.DataFrame(data2)
        df2.to_excel(f'gradient_grower_fields_2024_pt3.xlsx', engine='xlsxwriter', index=False)



def update_grower_field_gpm_and_irrigation_set_acres(grower_name: str, field_name: str, gpm = None, irrigation_set_acres = None):
    growers = open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if gpm is not None:
                            logger.gpm = float(gpm)
                        if irrigation_set_acres is not None:
                            logger.irrigation_set_acres = float(irrigation_set_acres)
    write_pickle(growers)


def subtract_from_dxd_mrid(logger_id: int, subtract_from_mrid: int = 0):
    """

    :param logger_id:
    :param subtract_from_mrid:
    """
    try:

        file_path = path.join(DXD_DIRECTORY, logger_id + '.dxd')

        print(f'Changing dxd file MRID {file_path}...')

        if not path.isfile(file_path):
            raise FileNotFoundError(f"The file '{file_path}' does not exist.")

        with open(file_path, 'r', encoding="utf8") as fd:
            parsed_json = json.load(fd)

        if subtract_from_mrid > -1:
            print(f'Modifying file MRID with the set back MRID: {subtract_from_mrid}')
            if "created" in parsed_json:
                # Modify the MRID to the new value
                og_mrid = parsed_json['device']['timeseries'][-1]['configuration']['values'][-1][1]
                new_mrid = og_mrid - subtract_from_mrid
                if new_mrid < 0:
                    new_mrid = 0
                parsed_json['device']['timeseries'][-1]['configuration']['values'][-1][1] = new_mrid

        # Write the modified JSON back to the file
        with open(file_path, 'w', encoding="utf8") as fd:
            json.dump(parsed_json, fd, indent=4, sort_keys=True, default=str)

        print('Successfully changed MRID')

    except Exception as error:
        print(f'ERROR in changing dxd MRID file for json data for {logger_id}')
        print(error)


def update_historical_et_stations():
    """
    Update the Historical ET table for each station with the new years etos from our BQ
    and recalculate average
    In this iteration going to try
    1. Pull Hist Et Table from BQ -> dict
    2. Pull CIMIS data and add to Hist Et dict
    3. Add new key/column to dict and recalculate average
    4. Write back to the DB
    """
    db = DBWriter()
    cimis = CIMIS()
    project = 'stomato-info'
    hist_dataset_id = 'Historical_ET'
    # TODO: some Hist ET tables have different years than others, need to take that in account with the prev_year, maybe a retry on the grab_all_table_data changing the order by each time
    current_year = datetime.now().year
    prev_year = current_year - 1
    start_of_year = date(2023, 1, 1)
    end_of_year = date(2023, 12, 31)

    hist_et_tables = db.get_tables(hist_dataset_id, project=project)

    for table in hist_et_tables:

        station_number = table.table_id
        print(f"Updating Historical ET for station {station_number}")
        # Get historical ET table from BigQuery and convert to a dictionary
        # Retry mechanism
        while True:
            try:
                bq_table, table_info = db.grab_all_table_data(hist_dataset_id, station_number, project,
                                                              order_by=f'Year_{prev_year}')
                if bq_table is not None:
                    break  # Exit loop if data is successfully fetched
            except Exception as e:
                print(f"Error fetching data for Year_{prev_year - 1}: {e}")
                prev_year -= 1  # Decrement year and retry
            continue

        # Pull CIMIS data
        et_data = cimis.getDictForStation(station_number, start_of_year, end_of_year)

        # Add new columns to the dictionary
        for i, row in enumerate(bq_table):
            row[f'Year_{current_year}'] = et_data["dates"][i]
            row[f'Year_{current_year}_ET'] = et_data["eto"][i]

            # Calculate average if needed
            # row['Average'] = (row.get(f'Year_{prev_year}_ET', 0) + row[f'Year_{current_year}_ET']) / 2

        column_based_data = {key: [row[key] for row in bq_table] for key in bq_table[0]}

        averages = cimis.get_average_et(column_based_data)

        for i, row in enumerate(bq_table):
            row['Average'] = averages[i]
        # Write the modified data back to a CSV file
        filename = f'new_hist_et_{station_number}.csv'
        with open(filename, 'w', newline='') as outfile:
            writer = csv.DictWriter(outfile, fieldnames=bq_table[0].keys())
            writer.writeheader()
            writer.writerows(bq_table)

        # Update schema to include new columns
        new_schema = table_info.schema + [
            bigquery.SchemaField(f'Year_{current_year}', 'DATE'),
            bigquery.SchemaField(f'Year_{current_year}_ET', 'FLOAT'),
        ]

        # Write the new table back to BigQuery
        print("Writing to table from csv")
        db.write_to_table_from_csv(hist_dataset_id + '_test', station_number + '_test', filename, new_schema, project)

    pass




# show_pickle()

# growers = open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             old_opt_low = logger.soil.optimum_lower
#             old_opt_high = logger.soil.optimum_upper
#             new_soil = Soil(field_capacity=logger.soil.field_capacity, wilting_point=logger.soil.wilting_point)
#             logger.soil = new_soil
#             new_soil_low = new_soil.optimum_lower
#             if old_opt_low != new_soil_low:
#                 print(f'Old optimum: {old_opt_low} - {old_opt_high}')
#                 print(f'New optimum: {new_soil_low} - {new_soil.optimum_upper}')
#                 print()
#
# write_pickle(growers)


# show_pickle()


# new_year_pickle_cleanup()
# remove_inactive_fields_from_growers_from_pickle()


# growers = open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             if logger.name == 'LM-17J-S':
#                 logger.show_irrigation_ledger()
#                 for date, switch_list in logger.irrigation_ledger.items():
#                     for ind, switch_val in enumerate(switch_list):
#                         if switch_val == 15:
#                             logger.irrigation_ledger[date][ind] = 60
#                 print()
#                 logger.show_irrigation_ledger()
# write_pickle(growers)
# print()


# growers = open_pickle()
# for grower in growers:
#     print(grower)
#
# cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
#
# pickle_file_name = 'weather_stations_2023.pickle'
# pickle_file_path = 'H:\\Shared drives\\Stomato\\HeatUnits\\'
# weather_stations = open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)


# weather_stations = open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)

# only_certain_growers_field_logger_update('Barrios Farms', 'Barrios Farms25E', 'BF-25E-C', write_to_db=True, subtract_from_mrid=62)

# dbwriter = DBWriter()
# db_dates = dbwriter.grab_specific_column_table_data('Barrios_Farms25E', 'BF-25E-C', 'stomato-2023', 'date')
# db_dates_list = [row[0] for row in db_dates]
# print()

# reset_updated_all()
# update_information(get_et=True, write_to_db=True)


# for grower in growers:
# print()
# for grower in growers:
#     for field in grower.fields:
#         if field.name == 'Lucero Rio VistaB 17-22':
#             for logger in field.loggers:
#                 print(type(logger.daily_switch))
#                 logger.gpm = 4488
#                 logger.irrigation_set_acres = 215
# #     print(logger.name, logger.soil.soil_type)
#             #     logger.soil.set_soil_type('Sandy Clay Loam')
#             #     print(logger.name, logger.soil.soil_type)
# write_pickle(growers)

# cimis = CIMIS()
# # # cimisStation = CimisStation()
# # # stations = get_all_current_cimis_stations()
# # # stations = cimisStation.return_list_of_stations()
# cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
# # Convert the integers to strings
# cimis_station_string_list = [str(station.station_number) for station in cimis_stations_pickle]
#
# # Join the strings with commas
# cimis_stations_list_string = ', '.join(cimis_station_string_list)
# print(cimis_stations_list_string)
#
# start_date = str(date.today() - timedelta(3))
# end_date = str(date.today() - timedelta(1))
# all_cimis_station_et = cimis.get_eto(cimis_stations_list_string, start_date, end_date)
# print()
# pprint.pprint(all_cimis_station_et)

# show_pickle()

# growers = open_pickle()
# for grower in growers:
#     print(grower.name)

# Testing new single CIMIS API call
# dict_of_stations = {}
# all_current_cimis_stations = open_pickle(filename="cimisStation.pickle")
# for station in all_current_cimis_stations:
#     dict_of_stations[station.station_number] = {'station': station, 'dates': [], 'eto': []}
# print()

# ########################################
# ### TESTING CIMIS CALL AT 12
# cimis = CIMIS()
# cimis_stations_pickle = open_pickle(filename="cimisStation.pickle")
# print()
# start_date = str(date.today() - timedelta(1))
# end_date = str(date.today() - timedelta(1))
# for stations in cimis_stations_pickle:
#     stations.updated = False
# all_cimis_station_et = cimis.get_all_stations_et_data(cimis_stations_pickle, start_date, end_date)
# print()
# #########################################
# write_all_et_values_to_db(all_cimis_station_et)

# print()

# project = 'stomato-info'
# dataset_id = f"{project}.ET.105"
# dataset_id = "`" + dataset_id + "`"
# #
# dbwriter = DBWriter()
# dml_statement = f"SELECT * FROM {dataset_id} WHERE date BETWEEN DATE(\'2023-03-01\') and DATE(\'2023-03-25\') ORDER BY date ASC"
# result = dbwriter.run_dml(dml_statement, project=project)
#
# date_data = []
# eto_data = []
#
# for row in result:
#     date_data.append(row['date'])
#     eto_data.append(row['eto'])
# print()

# dml = f"UPDATE `test.test.test`" \
#       + f" SET daily_switch = 55"\
#       + f", daily_hours = 0.8"\
#       + f", daily_inches = 1.5 WHERE date = 'date'"
#
# print(dml)


# growers = open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             logger.irrigation_ledger = {}
# write_pickle(growers)
# print()

# for each_dict in dict_of_stations:
#     print(f"Station: {dict_of_stations[each_dict]['station'].station_number}")
# print()

# my_list = ['pear']
# if 'apple' not in my_list:
#     print("apple not in list")

# all_cimis_station_et = cimis.get_all_stations_et_data(cimis_stations_pickle, start_date, end_date)


# show_pickle()
# temp_ai_application()
# Testing writing a html doc for notifications instead of a txt
# lat = 37.0544503152434
# long = -120.80964223605376
# saulisms = Saulisms()
# saying, saying_date = saulisms.get_random_saulism()
# file_path = "C:\\Users\\javie\\Desktop\\notification_html_test.html"
# with open(file_path, 'a') as the_file:
#     the_file.write("<!DOCTYPE html>\n")
#     the_file.write("<html>\n")
#     the_file.write("<body>\n")
#     the_file.write("<h2>SENSOR ERRORS</h2>\n")
#     the_file.write(f"<h2 style='font-style: italic; font-size:150%;'>\"{saying}\", {saying_date}</h2>")
#     the_file.write("<h3>=== New Grower ===</h3>\n")
#     the_file.write("<p>-------------------</p>\n")
#     the_file.write("<p>Field: Lucero Rio Vista74, 75</p>\n")
#     the_file.write("<p>Logger: RV-74_75-SE</p>\n")
#     the_file.write("<p>Logger ID: z6-12306</p>\n")
#     the_file.write("<p>Date: 06/26/23</p>\n")
#     the_file.write("<p>Sensor: Canopy Temp</p>\n")
#     the_file.write("<p>-> Canopy Temp is showing None at some point in the day (Not necessarily at the hottest time). Connection issue?</p>\n")
#     the_file.write(f"<a href='https://www.google.com/maps/search/?api=1&query={lat},{long}' target='_blank'>Location</a>\n")
#     the_file.write("<p>-------------------</p>\n")
#     the_file.write("</body>\n")
#     the_file.write("</html>\n")

# show_pickle()

# Change Soil Type for a Logger
# growers = open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             if logger.name == 'RC-24-SW':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RN-2LLC-E':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RN-2LLC-N':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Sandy Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RN-2LLC-W':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Sandy Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RC-16-E':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RC-13-E':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RC-4E-C':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Clay Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#             if logger.name == 'RC-3-NE':
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 logger.soil.set_soil_type('Sandy Loam')
#                 print(logger.name, logger.soil.soil_type, logger.soil.field_capacity, logger.soil.wilting_point)
#                 print('---------------------')
#
#
# write_pickle(growers)

# # Testing some notification generation and writing
# growers = open_pickle()
# techs = get_all_technicians(growers)
# notifications_setup(growers, techs, file_type='html')
# reset_updated_all()
# update_information(get_data=True, check_for_notifications=True)

# only_certain_growers_field_logger_update('Saul', 'Meza', 'Development-C', check_for_notifications=True, subtract_from_mrid=24)

# growers = open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         for logger in field.loggers:
#             print()
#             print(f"https://www.google.com/maps/search/?api=1&query={logger.lat},{logger.long}")
#             print()
#         if field.crop_type not in ['Tomatoes', 'tomatoes', 'tomato', 'Tomato']:
#             print(field.name, field.crop_type)
#         if field.loggers[0].rnd:
#             field.field_type = 'R&D'
#         else:
#             field.field_type = 'Commercial'
# print(f'{field.name} - {field.field_type}')
# write_pickle(growers)

# print(field.name)
# for logger in field.loggers:
#     print(f'\t{logger.name} - {logger.rnd}')

# df = pd.DataFrame(columns=['Gradient Grower', 'Gradient Grower-Field', 'Gradient Field', 'Gradient Planting Date', 'Gradient Uninstall Date'])
#
# pickle_file_name = "2022_pickle.pickle"
# pickle_file_path = "H:\\Shared drives\\Stomato\\2022\\Pickle\\"
# growers = open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         if field.crop_type in ['Tomatoes', 'tomatoes', 'tomato', 'Tomato']:
#             df = df.append({'Gradient Grower': grower.name, 'Gradient Grower-Field': field.name, 'Gradient Field': field.nickname, 'Gradient Planting Date': field.loggers[0].planting_date, 'Gradient Uninstall Date': field.loggers[0].uninstall_date}, ignore_index=True)
#
# print()
# print(df)
# df.to_excel('growers_fields.xlsx', index=False)

# pickle_file_name = "ai_game_2023_data_errors.pickle"
# pickle_file_path = "C:\\Users\\javie\\Desktop\\AI Game v3 Distribution\\"
# ai_data = open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)
# print()
# ai_data = []
#
# write_pickle(ai_data, filename=pickle_file_name, specific_file_path=pickle_file_path)


# pickle_file_name = "2022_pickle.pickle"
# pickle_file_path = "H:\\Shared drives\\Stomato\\2022\\Pickle\\"
# growers = open_pickle(filename=pickle_file_name, specific_file_path=pickle_file_path)
# for grower in growers:
#     for field in grower.fields:
#         if field.loggers[0].rnd:
#             field.field_type = 'R&D'
#         else:
#             field.field_type = 'Commercial'
#         print(f'{field.name} - {field.field_type}')
# write_pickle(growers, filename=pickle_file_name, specific_file_path=pickle_file_path)

# pickle_file_name = "2022_pickle.pickle"
# pickle_file_path = "H
# show_pickle()
#
# growers = open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         if field.name == 'Lucero Rio VistaR':
#             for logger in field.loggers:
#                 if logger.name == 'RV-R-C' and logger.id == 'z6-07288':
#                     logger.install_date = date(2024, 8, 3)
                    # logger.active = True
                    # new_logger = copy.deepcopy(logger)
                    # new_logger.id = 'z6-07288'
                    # new_logger.password = '27779-50689'
                    # new_logger.install_date = '2024-08-03'
                    # print(new_logger)
                    # logger.active = False
                    # field.add_logger(new_logger)
                    # break
# write_pickle(growers)
# cimisst = CimisStation()
# cimisst.deactivate_cimis_station('5')

# only_certain_growers_update('Kubo & YoungKF1', get_weather=True, get_data=True, write_to_db=True, write_to_portal=True)
# only_certain_growers_field_update('Lucero Rio Vista', 'Lucero Rio VistaR', get_weather=True, get_data=True,
#                                   write_to_db=True, write_to_portal=True, subtract_from_mrid=80)

# subtract_from_dxd_mrid()
# generate_gradient_grower_fields_report()
# open_pickle("2024_pickle_test.pickle")
# show_pickle(filename="2024_pickle_test.pickle")
# show_pickle()