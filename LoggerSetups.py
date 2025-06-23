import re
import time
from collections import OrderedDict
from datetime import datetime, timedelta

import requests

import DBWriter
import Decagon
import GSheetCredentialSevice
import SQLScripts
import gSheetReader
from CIMIS import CIMIS
from CimisStation import CimisStation
from DBWriter import DBWriter
from Field import Field
from Grower import Grower
from Logger import Logger
from Notifications import Notification_LoggerSetups
from Zentra import Zentra

YEAR = '2025'
LOGGER_SETUPS_SHEET_ID = '1rsPrX44pCHOhbOHZfWubDkuuexP8pSG8kspW03FAqOU'
NEW_LOGGER_SETUPS_SHEET_ID_2024 = '1ycEimLNLXpg7rZ5QxAwTfnAO1BkNBIwOzfpM1dqAxdg'
NEW_LOGGER_SETUPS_SHEET_ID = '1RUo7w9mOfzGCncDNIgUC_xvI3DrodITEyamcjottIL8'


def setup_grower(grower: str, technician_name: str, email: str = '', region: str = ''):
    """
    Function setups a new grower in the pickle
    :param grower: name of grower
    :param technician_name: name of technician
    :param email: grower email
    :param region: grower region
    """
    print("Adding new grower to Pickle: " + grower)
    newGrower = Decagon.setup_grower(grower, technician_name, email, region=region)
    # Decagon.addGrowerToGrowers(newGrower)


def setup_growers_fields_loggers_lists() -> tuple[list, list, list]:
    """
    Function sets up a list of growers, fields, and active loggers in the current pickle
    :return: Returns a grower list, field list, and active logger list
    """
    grower_list = []
    field_list = []
    logger_list = []
    growers = Decagon.open_pickle()
    for grower in growers:
        grower_list.append(grower.name)
        for field in grower.fields:
            field_list.append(field.name)
            for logger in field.loggers:
                if logger.active and field.active:
                    logger_list.append(logger.id)
    return grower_list, field_list, logger_list


def check_if_grower_in_list(grower_name: str, grower_list: list, technician: str, region: str) -> list:
    """
    Checks to see if grower exists in the grower list, and if not it creates a new grower in the pickle
    :param grower_name: Grower Name
    :param grower_list: List of growers
    :param technician: Assigned Technician
    :param region: Grower Region
    :return: Grower List
    """
    if grower_name not in grower_list:
        print("Did not find grower, setting up new grower")
        setup_grower(grower_name, technician, region=region)
        grower_list.append(grower_name)
    return grower_list


def add_logger_id_to_pickle(logger_id: str):
    """
    Function adds a logger id to the logger id pickle
    :param logger_id: Logger ID
    """
    loggers = Decagon.open_pickle(filename="loggerList.pickle")
    loggers.append(logger_id)
    Decagon.write_pickle(loggers, filename="loggerList.pickle")


def remove_field(grower_target: str, field_target: str):
    """
    Function removes a field from a grower
    :param grower_target: Grower Name
    :param field_target: Field Name
    """
    growers = Decagon.open_pickle()
    for grower in growers:
        if grower.name == grower_target:
            for field in grower.fields:
                if field.name == field_target:
                    print(f"Removing Field: {field_target}")
                    for logger in field.loggers:
                        remove_logger_id_from_pickle(logger.id)
                Decagon.remove_field(grower_target, field_target, True)
    # Decagon.deactivate_field(grower, field)

    # Decagon.write_pickle(growers)


def deactivate_field(grower: str, field: str, uninstall_date):
    """
    Function deactivates a field from a grower
    :param grower: Grower Name
    :param field: Field Name
    :param uninstall_date: Uninstall Date
    """
    growers = Decagon.open_pickle()
    for g in growers:
        if g.name == grower:
            for f in g.fields:
                if f.name == field:
                    print("Removing Field: " + field)
                    for l in f.loggers:
                        remove_logger_id_from_pickle(l.id)
                        l.uninstall_date = uninstall_date
    Decagon.write_pickle(growers)
    Decagon.deactivate_field(grower, field)


def remove_logger_id_from_pickle(logger_id: str):
    """

    :param logger_id: Logger ID
    """
    loggers = Decagon.open_pickle(filename="loggerList.pickle")
    try:
        loggers.remove(logger_id)
        Decagon.write_pickle(loggers, filename="loggerList.pickle")
        print("Removed Logger from Logger ID List")
    except ValueError:
        print("Could not find logger: " + logger_id)


def remove_all_logger_id_from_pickle():
    """
    Removes all logger ID's from a pickle file
    """
    id_list = Decagon.open_pickle(filename="loggerList.pickle")
    for logger_id in id_list:
        id_list.remove(logger_id)
    Decagon.write_pickle(id_list, filename="loggerList.pickle")


def show_logger_id_pickle():
    """
    Show all the logger ID's from a pickle file
    """
    logger = Decagon.open_pickle(filename="loggerList.pickle")
    for logger in logger:
        print(logger)


def set_up_password_dict(service, sheet_id: str) -> dict:
    """
    Sets up dictionary of passwords that belong to each logger
    :param service: G Sheet Service
    :param sheet_id: G Sheet ID
    :return: Returns Logger Dictionary with passwords
    """
    range_name = "Logger Passwords"
    logger_passwords_dict = {}

    result = gSheetReader.getServiceRead(range_name, sheet_id, service)

    row_result = result['valueRanges'][0]['values']

    for index, row in enumerate(row_result):
        if index == 0:
            continue
        logger_id = row[0]
        logger_id = logger_id.replace(' ', '')
        logger_password = row[1]
        logger_password = logger_password.replace(' ', '')
        logger_id = logger_id.lower()

        logger_passwords_dict[logger_id] = logger_password

    # print(loggerDict)
    return logger_passwords_dict


def find_password(logger_id: str, logger_dict: dict) -> str:
    """
    Find password for a logger in the given logger dictionary
    :param logger_id: Logger ID
    :param logger_dict: Logger Dictionary
    :return: Logger Password
    """
    return logger_dict.get(logger_id)


def check_if_logger_has_been_added_to_field_prev(logger_id: str, field_name: str, grower_name: str) -> bool:
    """
    Checks if logger has been added to a previous field
    :param logger_id: Logger ID
    :param field_name: Field Name
    :param grower_name: Grower Name
    :return: True if logger has been added to a field previously, else False
    """
    growers = Decagon.open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.id == logger_id:
                            return True
    return False


def add_logger_to_field(row, result, logger_num: str, logger_dict: dict, logger_list: list, grower_name: str,
                        field_list: list, field_name_pickle: str, tab_id, field_name: str,
                        additional_stations: bool):
    """

    :param row:
    :param result:
    :param logger_num: The index of the logger
    :param logger_dict: Dictionary of all known loggers and their passwords
    :param logger_list: List of loggers
    :param grower_name: Grower Name
    :param field_list: List of fields
    :param field_name_pickle: Field Name in Pickle File
    :param tab_id:
    :param field_name: Field Name
    :param additional_stations: Boolean to know whether we are adding additional stations to a previous field
    :return: None, only used if an error occurred and need to break out.
    """
    # Assign Headers
    crop_type_header = gSheetReader.getColumnHeader("Crop", result)
    planting_header = gSheetReader.getColumnHeader("Planting Date", result)
    field_type_header = gSheetReader.getColumnHeader("Field type", result)
    region_header = gSheetReader.getColumnHeader("North or South", result)
    logger_id_name_header = gSheetReader.getColumnHeader("Logger ID Logger " + logger_num, result)
    logger_name_header = gSheetReader.getColumnHeader("Logger Name Logger " + logger_num, result)
    soil_type_header = gSheetReader.getColumnHeader("Soil Type Logger " + logger_num, result)
    gpm_header = gSheetReader.getColumnHeader("Gallons Per Minute Logger " + logger_num, result)
    acres_header = gSheetReader.getColumnHeader("Acres Logger " + logger_num, result)
    lat_name_header = gSheetReader.getColumnHeader("Lat Logger " + logger_num, result)
    long_name_header = gSheetReader.getColumnHeader("Long Logger " + logger_num, result)
    total_acres_header = gSheetReader.getColumnHeader("Total Field Acres", result)
    install_header = gSheetReader.getColumnHeader("Install Date", result)

    # Assign Values
    logger_id = row[logger_id_name_header]
    logger_name = row[logger_name_header]
    if logger_id == "" and logger_name == "":
        return
    elif logger_id == "" and logger_name:
        print("Logger ID is blank for Logger: " + logger_name)

    # Removing blanks form Logger ID and Logger Name
    logger_id = logger_id.replace(' ', '')
    logger_name = logger_name.replace(' ', '')

    # Should always add logger as long as it hasn't been added to the field previously
    # This prevents a bug where logger gets added multiple times to the same field due to additional stations flag
    should_add_logger = True
    if additional_stations:
        should_add_logger = not check_if_logger_has_been_added_to_field_prev(logger_id, field_name, grower_name)
    soil_type = row[soil_type_header]
    gpm = float(row[gpm_header])
    acres = float(row[acres_header])
    total_acres = float(row[total_acres_header])
    planting_date = row[planting_header]
    crop_type = row[crop_type_header]
    field_type = row[field_type_header]
    region = row[region_header]
    install_date = row[install_header]
    install_date_converted = datetime.strptime(install_date, '%m/%d/%Y').date()
    logger_direction = logger_name.split('-')[-1]
    if field_type == "R&D":
        rnd = True
    else:
        rnd = False
    lat = row[lat_name_header]
    long = row[long_name_header]
    field_id = ""
    # Remove trailing blanks from field name
    if field_name_pickle[-1] == ' ':
        field_name_pickle = field_name_pickle.rstrip()

    if (lat or long) == "":
        print("Lat or Long is empty")
        return

    planting_date_converted = datetime.strptime(planting_date, '%m/%d/%Y')

    # Check that planting date is not from a previous year if dealing with tomatoes
    if planting_date_converted.year < datetime.today().year and (crop_type == 'Tomatoes' or crop_type == 'tomatoes'
                                                                 or crop_type == 'tomato' or crop_type == 'Tomato'):
        print(f"Error with planting date: Planting Date is from a previous year")
        planting_date_converted = planting_date_converted.replace(year=datetime.today().year)
        planting_date = planting_date_converted.strftime('%m/%d/%Y')
        print(f"Planting Date Fixed: {planting_date}")

    # Check to see if field has been set up previously
    if field_name_pickle not in field_list and logger_id not in logger_list and should_add_logger:
        logger_id = logger_id.replace(' ', '')
        logger_name = logger_name.replace(' ', '')
        logger_password = find_password(logger_id, logger_dict)

        # If logger password has been found, set up a new field and loggers
        if logger_password:
            print("Adding New " + grower_name + " Field: " + field_name)
            try:
                # Get closest cimis station
                print("\tAttempting to get closest cimis station")
                cimis = CIMIS()
                cimis_station = CimisStation()
                inactive_cimis_station_list = cimis_station.return_inactive_cimis_stations_list()
                cimis_stations_data = cimis.get_list_of_active_eto_stations()
                closest_cimis_station = cimis.get_closest_station(cimis_stations_data, float(lat), float(long),
                                                                  inactive_cimis_station_list)

                if closest_cimis_station is None:
                    print(
                        "ERROR with return from cimis.get_closest_cimis_station() - did not get closest cimis station")
                    return
            except Exception as error:
                print(f'\tERROR in loggerSetups add_logger_to_field() when trying to get closest cimis station')
                print(error)
                print("\t\tLat: " + lat)
                print("\t\tLong: " + long)
                return

            # Setup Field and Logger to Pickle
            field = Decagon.setup_field(field_name_pickle, lat, long, closest_cimis_station, total_acres, crop_type,
                                        field_type=field_type)
            logger = Decagon.setup_logger(logger_id, logger_password, logger_name, crop_type, soil_type, gpm, acres,
                                          logger_direction, lat, long,
                                          install_date_converted, planting_date=planting_date, rnd=rnd)
            field.add_logger(logger)
            Decagon.add_field_to_grower(grower_name, field)
            field_list.append(field_name_pickle)
            add_logger_id_to_pickle(logger_id)
            print("Added logger: " + logger_id)
        else:
            print("\tCouldn't find logger ID in password sheet")

    # Checks to see if logger has to be added to field, only if field has been set up previously
    elif logger_id not in logger_list and should_add_logger:
        logger_password = find_password(logger_id, logger_dict)

        # If logger password has been found, set up new logger
        if logger_password:
            logger = Decagon.setup_logger(logger_id, logger_password, logger_name, crop_type, soil_type, gpm, acres,
                                          logger_direction, lat, long,
                                          install_date_converted, planting_date=planting_date, rnd=rnd)
            growers = Decagon.open_pickle()
            print("\tLogger is not in List")
            for grower in growers:
                for field in grower.fields:
                    if field.name == field_name_pickle and grower.name == grower_name:
                        field.add_logger(logger)
                        for logger in field.loggers:
                            if logger.grower == None:
                                logger.grower = grower
                            if logger.field == None:
                                logger.field = field
                        print("\t\tAdded logger: " + logger_id)
                        add_logger_id_to_pickle(logger_id)
            Decagon.write_pickle(growers)
        else:
            print("\t\tPassword does not match logger ID")


def loop_through_loggers(field_name_pickle: str, grower_name: str, field_name: str, field_list: list, logger_list: list,
                         row, result, logger_dict: dict, num_of_loggers: int, tab_id="",
                         add_stations: bool = False):
    """

    :param field_name_pickle: Field Name in Pickle File
    :param grower_name: Grower Name
    :param field_name: Field Name
    :param field_list: Field List
    :param logger_list: Logger List
    :param row: G Sheet logger row for current logger
    :param result: G Sheet result that contains all logger rows
    :param logger_dict: Dictionary of all known loggers and their passwords
    :param num_of_loggers: Number of loggers in field
    :param tab_id:
    :param add_stations: Boolean to know whether we are adding additional stations to a previous field
    """
    for logger_num in range(1, num_of_loggers + 1):
        add_logger_to_field(row, result, str(logger_num), logger_dict, logger_list, grower_name, field_list,
                            field_name_pickle, tab_id, field_name,
                            add_stations)


def logger_setups_process() -> None:
    """
    Function that handles all the logger setups process. This function makes sure that any new field submissions
    to logger setups are created and added to the pickle, and that their irrigation scheduling is set up.

    """
    now = datetime.today()
    print(">>>>>>>>>>>>>>>>>>>LOGGER SETUPS PROCESS 2.0<<<<<<<<<<<<<<<<<<<")
    print("                                 - " + now.strftime("%m/%d/%y  %I:%M %p"))
    print()
    print()

    logger_setups_process_start_time = time.time()

    # Grab our current growers, fields and loggers from the pickle and create lists for all of them separately
    grower_name_list, field_name_list, logger_id_list = setup_growers_fields_loggers_lists()

    # Setup Google Sheet information
    sheet_tab_name = 'Form Submissions'

    # Setups service to read google sheets
    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()

    # Set up logger dictionary with passwords
    logger_passwords_dict = set_up_password_dict(service, NEW_LOGGER_SETUPS_SHEET_ID)

    # Read Google Sheet
    result = gSheetReader.getServiceRead(sheet_tab_name, NEW_LOGGER_SETUPS_SHEET_ID, service)
    row_result = result['valueRanges'][0]['values']

    # TODO Store all cimis station locations as well as the last updated list so we don't query cimis every time
    # Other classes and variables we will need
    cimis = CIMIS()
    cimis_station = CimisStation()
    inactive_cimis_station_list = []
    cimis_stations_data = None
    number_of_fields_successfully_setup = number_of_fields_failed_setup = 0
    field_names_successfully_setup = []
    field_names_failed_setup = []

    # Loop through each row in the Google sheet and check if the field is in our current pickle. If it isn't,
    #  go through the setup process using that row's data.
    for row_index, row in enumerate(row_result):
        try:
            # Ignore header row
            if row_index == 0:
                continue

            # Set up variables
            field_name_col = gSheetReader.getColumnHeader("Field Name (Grower)", row_result)
            field_name = row[field_name_col].strip()

            grower_name_col = gSheetReader.getColumnHeader("Grower Name", row_result)
            grower_name = row[grower_name_col].strip()

            field_name_pickle = grower_name + field_name

            if field_name_pickle not in field_name_list:
                # New Field
                print(f'--------------- New Field Found: {field_name_pickle} ---------------')
                # Boolean that starts as True and changes to False if any of the setup process fails
                successful_setup = True

                # CIMIS Info setup
                # Grab inactive cimis stations and active eto cimis stations from CIMIS API
                print(f'\tChecking for CIMIS station data...')
                if len(inactive_cimis_station_list) == 0:
                    inactive_cimis_station_list = cimis_station.return_inactive_cimis_stations_list()

                cimis_api_attempts = 0
                cimis_api_attempt_limit = 5
                while cimis_stations_data is None and cimis_api_attempts <= cimis_api_attempt_limit:
                    print(f'\t\t\t\t CIMIS API attempt {cimis_api_attempts}')
                    cimis_stations_data = cimis.get_list_of_active_eto_stations()
                    cimis_api_attempts += 1

                if cimis_stations_data is None:
                    print()
                    print(
                        f'\t\tFailed the CIMIS call for stations {cimis_api_attempt_limit} times. Ending logger setups')
                    successful_setup = False
                    return None
                else:
                    print(f'\t<-CIMIS station data acquired successfully')
                    print()

                growers = Decagon.open_pickle()
                if grower_name not in grower_name_list:
                    # New Grower
                    print(f'\tField is from a new Grower: {grower_name}')
                    new_grower = True

                    technician_name_col = gSheetReader.getColumnHeader("Technician", row_result)
                    technician_name = row[technician_name_col].strip()
                    technician = Decagon.get_technician(technician_name, growers=growers)

                    region_name_col = gSheetReader.getColumnHeader("Region", row_result)
                    region = row[region_name_col].strip()

                    print(f'\t>Creating new Grower: {grower_name}')
                    grower = Grower(grower_name, [], technician, region=region)

                    grower_name_list.append(grower_name)
                    print(f'\t<Grower: {grower_name} Created')
                    print()
                else:
                    # Already existing grower
                    new_grower = False
                    print(f'\tField is from an already existing Grower: {grower_name}')
                    print()
                    for g in growers:
                        if g.name == grower_name:
                            grower = g

                # Grab pertinent data and setup new field
                new_field = setup_new_field(
                    field_name_pickle,
                    grower,
                    row,
                    row_result,
                    cimis_stations_data,
                    inactive_cimis_station_list
                )

                if new_field is None:
                    print(f'\t\tERROR')
                    print(f'\t\tIssue with field setup for: {field_name_pickle}')
                    print(f'\t\tSkipping this field')
                    number_of_fields_failed_setup += 1
                    field_names_failed_setup.append(field_name_pickle)
                    successful_setup = False
                    continue

                num_of_loggers_col = gSheetReader.getColumnHeader("Number of Loggers in Field", row_result)
                num_of_loggers = int(row[num_of_loggers_col].strip())

                print(f'\t\t\tProcessing loggers in field {field_name_pickle}. {num_of_loggers} logger/s found')
                print()
                for logger_num in range(1, num_of_loggers + 1):
                    # Get Values
                    logger_name_col = gSheetReader.getColumnHeader(f"Logger Name (L{logger_num})", row_result)
                    logger_name = row[logger_name_col].strip()

                    logger_id_col = gSheetReader.getColumnHeader(f"Logger ID (L{logger_num})", row_result)
                    logger_id = row[logger_id_col].strip()

                    print(f'\t\t\tLogger #{logger_num} -> {logger_id} - {logger_name}...')

                    if logger_id not in logger_id_list:
                        print(f'\t\t\t{logger_id} is a new Logger')

                        # Grab pertinent data and setup new logger
                        new_logger = setup_new_logger(
                            logger_id,
                            logger_name,
                            logger_num,
                            logger_passwords_dict,
                            grower,
                            new_field,
                            row,
                            row_result
                        )
                        # If the field lat and long werent set before (blank on the sheet) use the location we got from Zentra
                        if new_field.lat == '' or new_field.long == '':
                            new_field.lat, new_field.long = new_logger.lat, new_logger.long

                        if new_logger is None:
                            print(f'\t\t\tERROR')
                            print(f'\t\t\tIssue with logger {logger_id} - {logger_name}')
                            print(f'Skipping field setup for {field_name}')
                            successful_setup = False
                            continue
                        new_field.loggers.append(new_logger)
                        logger_id_list.append(logger_id)
                        print(f'\t\t\t<-Done with logger')
                        print()
                    else:
                        print(f'\t\tERROR')
                        print(f'\t\t\t{logger_id} - {logger_name} already exists, skipping')
                        print(f'\t\tIssue with field setup for: {field_name_pickle}')
                        print(f'\t\tSkipping this field')
                        number_of_fields_failed_setup += 1
                        field_names_failed_setup.append(field_name_pickle)
                        successful_setup = False
                        continue

                if successful_setup is False:
                    print(f'Skipping field setup for {field_name}')
                    number_of_fields_failed_setup += 1
                    field_names_failed_setup.append(field_name_pickle)
                    continue

                grower.fields.append(new_field)
                field_name_list.append(new_field.name)
                print(f'--------------- Done with: {field_name_pickle} ---------------')
                print()

                if new_grower:
                    growers.append(grower)
                Decagon.write_pickle(growers) # TODO this shouldnt write until a full successful run so after irr scheduling

                # Setup Irrigation Scheduling
                print(f'--------------- Setting up Irrigation Scheduling: {field_name_pickle} ---------------\n')
                # Create irr sched table for field
                setup_irr_scheduling(new_field, cimis_stations_data)
                print(f'--------------- Done with: {field_name_pickle} Irrigation Scheduling ---------------')
                print()

                number_of_fields_successfully_setup += 1
                field_names_successfully_setup.append(field_name_pickle)

        except IndexError as err:
            print(err)
            continue
        except Exception as err:
            print(err)
            continue

    logger_setups_process_start_end_time = time.time()
    print("----------FINISHED----------")
    print()
    print('Done with Logger Setups Process')
    print(f'{number_of_fields_successfully_setup} Fields Successfully Set Up')
    print(f'   -> {field_names_successfully_setup}')
    print(f'{number_of_fields_failed_setup} Fields Failed')
    print(f'   -> {field_names_failed_setup}')
    print()

    logger_setups_process_elapsed_time_seconds = logger_setups_process_start_end_time - logger_setups_process_start_time

    logger_setups_process_elapsed_time_hours = int(logger_setups_process_elapsed_time_seconds // 3600)
    logger_setups_process_elapsed_time_minutes = int((logger_setups_process_elapsed_time_seconds % 3600) // 60)
    logger_setups_process_elapsed_time_seconds = int(logger_setups_process_elapsed_time_seconds % 60)

    print(f"Logger Setups Process execution time: {logger_setups_process_elapsed_time_hours}:"
          + f"{logger_setups_process_elapsed_time_minutes}:"
          + f"{logger_setups_process_elapsed_time_seconds} (hours:minutes:seconds)")
    print()
    print()


def setup_new_logger(logger_id, logger_name, logger_num, logger_passwords_dict, grower, new_field,
                     row, row_result):
    # New Logger
    print(f'\t\t\tLooking for password for logger id: {logger_id}...')
    logger_password = logger_passwords_dict.get(logger_id)
    if logger_password is None:
        print(f'\t\t\tERROR')
        print(f'\t\t\t<-Password for logger {logger_id} not found')
        print(f'\t\t\t<-Skipping this logger so setup might be incomplete')
        return None
    else:
        print(f'\t\t\t<-Password found: {logger_password}')

    # Grab pertinent variables
    lat_col = gSheetReader.getColumnHeader(f"Lat (L{logger_num})", row_result)
    long_col = gSheetReader.getColumnHeader(f"Long (L{logger_num})", row_result)

    if row[lat_col] == '' or row[long_col] == '':
        lat, long = get_logger_geolocation(logger_id)  # float
    else:
        lat = float(row[lat_col].strip())
        long = float(row[long_col].strip())

    gpm_col = gSheetReader.getColumnHeader(f"Irr. Gallons Per Minute (L{logger_num})", row_result)
    gpm = float(row[gpm_col].strip())

    irrigation_set_acres_col = gSheetReader.getColumnHeader(f"Irrigation Acres (L{logger_num})", row_result)
    irrigation_set_acres = float(row[irrigation_set_acres_col].strip())

    soil_type_col = gSheetReader.getColumnHeader(f"Soil Type (L{logger_num})", row_result)
    soil_type = row[soil_type_col].strip()

    if soil_type == '':
        print(f'\t\t\t\tGrabbing soil type from Coords: {lat}, {long}')
        soil_type, status = get_soil_type_from_coords(lat, long)
        if soil_type is None or status is True:
            print('\t\t\t\tError grabbing soil type')
            if soil_type is None:
                print(f'\t\t\t\tNo soil type found at {lat}, {long} ')
            elif status is True:
                print(f'\t\t\t\tUsing backup soil description {soil_type}, notifying techs... ')
            grower.technician.all_notifications.add_notification(
                Notification_LoggerSetups(
                    datetime.now(),
                    grower.name,
                    new_field.name,
                    issue=f'Soil type pulled from Soil Survey is uncertain. Using {soil_type} as best guess. Please '
                          f'check Soil Survey to verify'
                )
            )
    planting_date_col = gSheetReader.getColumnHeader("Planting Date", row_result)
    planting_date = row[planting_date_col].strip()
    planting_date_converted = datetime.strptime(planting_date, '%Y-%m-%d').date()

    install_date_col = gSheetReader.getColumnHeader("Install Date", row_result)
    install_date = row[install_date_col].strip()
    install_date_converted = datetime.strptime(install_date, '%Y-%m-%d').date()

    logger_direction = logger_name.split('-')[-1]
    crop_type = new_field.crop_type

    print(f'\t\t\tCreating Logger: {logger_name} - {logger_id}...')
    new_logger = Logger(
        logger_id,
        logger_password,
        logger_name,
        crop_type,
        soil_type,
        gpm,
        irrigation_set_acres,
        logger_direction,
        install_date_converted,
        lat,
        long,
        grower=grower,
        field=new_field,
        planting_date=planting_date_converted
    )

    print(f'\t\t\t<-Logger: {logger_name} Created')
    return new_logger


def setup_new_field(field_name, grower, row, row_result, cimis_stations_data, inactive_cimis_station_list):
    """

    :param field_name:
    :param grower:
    :param row:
    :param row_result:
    :return:
    """
    print(f'\t\tSetting up new field: {field_name}...')

    field_name_ms_col = gSheetReader.getColumnHeader("Field Name (MS)", row_result)
    field_name_ms = row[field_name_ms_col].strip()
    field_name_ms_formatted = format_field_name_ms_standard(field_name_ms)

    field_type_col = gSheetReader.getColumnHeader("Field Type", row_result)
    field_type = row[field_type_col].strip()

    region_col = gSheetReader.getColumnHeader("Region", row_result)
    region = row[region_col].strip()

    field_acres_col = gSheetReader.getColumnHeader("Total Field Acres", row_result)
    field_acres = float(row[field_acres_col].strip())

    crop_type_col = gSheetReader.getColumnHeader("Crop", row_result)
    crop_type = row[crop_type_col].strip()

    # Lat long are from the first logger in the field
    lat_col = gSheetReader.getColumnHeader("Lat (L1)", row_result)
    long_col = gSheetReader.getColumnHeader("Long (L1)", row_result)

    # If no lat long provided look up first logger IDs Zentra geolocation
    logger_id_col = gSheetReader.getColumnHeader("Logger ID (L1)", row_result)
    logger_id = row[logger_id_col].strip()

    if row[lat_col] == '' or row[long_col] == '':
        lat, long = get_logger_geolocation(logger_id)
    else:
        lat = float(row[lat_col].strip())
        long = float(row[long_col].strip())

    print(f'\t\tGrabbing closest cimis station...')
    cimis = CIMIS()
    closest_cimis_station = cimis.get_closest_station(cimis_stations_data, float(lat), float(long),
                                                      inactive_cimis_station_list)
    print(f'\t\t<-Closest cimis station found {closest_cimis_station}')

    print(f'\t\tCreating new Field...')
    new_field = Field(
        field_name,
        [],
        lat,
        long,
        closest_cimis_station,
        field_acres,
        crop_type,
        grower=grower,
        field_type=field_type,
        name_ms=field_name_ms_formatted
    )

    print(f'\t\t<-Field: {field_name} Created')
    print()
    return new_field


def setup_field():
    """
    Starts Logger Setup Process to setup loggers using the logger setup form Google sheet
    """

    # Logger Setups Google Sheet ID
    sheet_id = '1rsPrX44pCHOhbOHZfWubDkuuexP8pSG8kspW03FAqOU'
    logger_dict = {}

    # Logger Setups Tab Name
    range_name = "S-Logger Info"

    # Setups service to read google sheets
    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()

    # Set up logger dictionary with passwords
    logger_dict = set_up_password_dict(service, sheet_id)

    # Read Google Sheet
    result = gSheetReader.getServiceRead(range_name, sheet_id, service)
    row_result = result['valueRanges'][0]['values']

    # Assign indexes to columns
    grower_header = gSheetReader.getColumnHeader("Grower Name", row_result)
    field_name_header = gSheetReader.getColumnHeader("Grower Field", row_result)
    region_header = gSheetReader.getColumnHeader("North or South", row_result)
    technician_header = gSheetReader.getColumnHeader("Technician", row_result)
    email_header = gSheetReader.getColumnHeader("Grower Email", row_result)
    num_of_loggers_header = gSheetReader.getColumnHeader("Total Number of Loggers in Field", row_result)
    adding_additional_stations_header = gSheetReader.getColumnHeader("Are you adding stations to a previous field?",
                                                                     row_result)
    planting_header = gSheetReader.getColumnHeader("Planting Date", row_result)
    install_header = gSheetReader.getColumnHeader("Install Date", row_result)

    # Setup grower, field, and logger lists
    grower_list, field_list, logger_list = setup_growers_fields_loggers_lists()

    # Loop through each row in the Google sheet and set it up if it hasn't been set up before
    for row_index, row in enumerate(row_result):
        try:
            # Ignore header row and empty rows
            if row_index == 0:
                continue
            elif row[grower_header] == "":
                continue

            # Set up variables
            grower_name = row[grower_header]
            field_name = row[field_name_header].rstrip()
            field_name_pickle = grower_name + field_name
            region = row[region_header]
            email = row[email_header]
            technician = row[technician_header]
            num_of_loggers = int(row[num_of_loggers_header])
            additional_stations = row[adding_additional_stations_header]
            planting_date = row[planting_header]
            install_date = row[install_header]
            planting_date_converted = datetime.strptime(planting_date, '%m/%d/%Y')
            install_date_converted = datetime.strptime(install_date, '%m/%d/%Y')

            # Check to make sure technician entered planting date correctly
            if planting_date_converted > install_date_converted:
                # Send Notification that field was setup incorrectly. Planting date is after install date
                print(f"Error Planting Date is after Install Date For Field: {field_name}")
                print(f"\tPlanting Date: {planting_date}; Install Date: {install_date}")
                growers = Decagon.open_pickle()
                for grower in growers:
                    if grower.name == grower_name:
                        grower.technician.all_notifications.add_notification(
                            Notification_LoggerSetups(
                                datetime.now(),
                                grower_name,
                                field_name,
                                issue="Planting Date is after Install Date"
                            )
                        )
                Decagon.write_pickle(growers)
                continue

            # Check to see if field has already been added before
            if field_name_pickle not in field_list or additional_stations == 'Yes':
                # If field has been added and this is an additional station, assign additional station flag
                if additional_stations == 'Yes':
                    print('Attempting to add additional stations')
                    additional_stations_boolean = True

                else:
                    print("Field is not in pickle, setting up new field: " + field_name_pickle)
                    print("Checking to see if grower is in list")
                    grower_list = check_if_grower_in_list(grower_name, grower_list, technician, region=region)
                    additional_stations_boolean = False

                # Loop through each logger that is in the form for that specific field
                loop_through_loggers(field_name_pickle, grower_name, field_name, field_list, logger_list, row,
                                     row_result, logger_dict,
                                     num_of_loggers, add_stations=additional_stations_boolean)

                # Sets up nickname for field
                setup_nickname(field_name_pickle)

                # Setup Irrigation Scheduling
                if additional_stations == 'No':
                    growers = Decagon.open_pickle()
                    for grower in growers:
                        for field in grower.fields:
                            if field.name == field_name_pickle:
                                # Create dataset for field
                                setup_dataset(field)

                                # Create irr sched table for field
                                # Changed function def to pass in cimis station data so this may break if ever used again
                                setup_irr_scheduling(field, None)

        except IndexError:
            continue
        except Exception as err:
            print(err)
            continue


def sort_best_station(stations_info: dict) -> str:
    """
    Function sorts through stations info dict to find the best station with the most valid data
    :param stations_info: A dictionary of stations and their historical data for previous years
    :return best_station: The station number of most valid station
    """
    print(f'Looking for station with most valid data out of tried stations')

    # check if 30 null points not 30 valid points
    valid_points = 365 - 30
    station_years = {}  # 'station_number' : valid_years
    search_string = 'ET'

    # Check if the dict is empty meaning we never found even invalid stations - extreme edge case
    if not stations_info:
        print('\tNo stations to sort through, we cannot make an Irrigation Scheduling Page')
        return None

    for station in stations_info:
        station_years[station] = 0
        filtered_keys = {key: value for key, value in stations_info[station].items() if search_string in key}
        if len(filtered_keys) > 0:
            # Find the amount of valid years per station
            for year in filtered_keys.values():
                # If for this year there are less than valid_points, return Invalid
                valid = sum(1 for value in year if value is not None)
                if valid >= valid_points:
                    station_years[station] += 1

    best_station = max(station_years, key=station_years.get)
    if station_years[best_station] < 2:
        print('\tThe best station has less than 2 years of valid data, we cannot make an Irrigation Scheduling Page')
        return None
    print(f'\tFound the best station of the already tried: {best_station}')
    return best_station


def setup_irr_scheduling(field, cimis_stations_data):
    """
    Create the irrigation scheduling table for the field
    :param field: Field object
    :return:
    """
    # print(f'\tSetting up irrigation scheduling table for {field.name}')

    dbwriter = DBWriter()
    dataset = "Historical_ET"
    station_number = field.cimis_station

    # Does historical et table exist for this station
    print(f'\tChecking for existing Historical ET Table for station {station_number}....')
    if not dbwriter.check_if_table_exists(dataset, station_number, project='stomato-info'):
        print()
        # Get stations historical data to make hist et table

        cimis = CIMIS()
        cached_stations = OrderedDict()

        # Check if the current station has valid data we can use for historical setup
        station_is_valid, station_results = cimis.new_et_station_data(station_number)
        # cache results for later sorting if necessary
        cached_stations[station_number] = station_results
        if station_is_valid:
            # new ET station we havent used
            Decagon.write_new_historical_et_to_db_2(dataset, station_number, station_results, overwrite=True)

        # Try all in county stations
        else:
            cimis_station = CimisStation()
            stations_to_skip = cimis_station.return_inactive_cimis_stations_list()
            # active_stations = cimis.get_list_of_active_eto_stations()

            handle_coastal_list(cimis_stations_data, station_number, stations_to_skip)

            stations_to_skip.append(station_number)
            station_number = cimis.get_closest_station_in_county(station_number, stations_to_skip, cached_stations,
                                                                 cimis_stations_data)

            # Found a good county station that's valid
            if station_number is not None:
                Decagon.write_new_historical_et_to_db_2(dataset, station_number, cached_stations[station_number],
                                                        overwrite=True)
            # Tried all in the county now look for closest
            else:
                print('\tChecking Closest in Range Stations....')
                station_number = cimis.get_closest_valid_station(float(field.lat), float(field.long), stations_to_skip,
                                                                 cached_stations, cimis_stations_data)
                if station_number is not None:
                    Decagon.write_new_historical_et_to_db_2(dataset, station_number, cached_stations[station_number],
                                                            overwrite=True)
                else:
                    print('\tNo valid stations found in county or range')
                    print('\t\tSorting for most Valid CIMIS Station...')
                    station_number = sort_best_station(cached_stations)
                    if station_number is None:
                        return
                    else:
                        Decagon.write_new_historical_et_to_db_2(dataset, station_number,
                                                                cached_stations[station_number],
                                                                overwrite=True)

    setup_dataset(field)
    SQLScripts.setup_irrigation_scheduling_db(station_number, field.name)


def handle_coastal_list(active_stations, station_number, stations_to_skip):
    # Grabbing all active stations to filter out all coastals not just ones in county,
    # because this skippable list will be used later in the closest range func
    stations_list = [str(station['StationNbr']) for station in active_stations]
    # NOTE: coastals list was aggregated by visually looking at stations
    # on the coast and manually adding, may need annual revision
    coastals = ['187', '157', '254', '213', '253', '171', '178', '104', '209', '129', '116', '193', '193', '210',
                '229', '160', '52', '202', '231', '64', '107', '152', '99', '174', '75', '245', '241', '173', '150',
                '184', '147']
    noncoastals = list(set(stations_list) - set(coastals))
    station_is_coastal = station_number in coastals
    if station_is_coastal:
        stations_to_skip.append(noncoastals)
    else:
        stations_to_skip.append(coastals)


def check_for_missing_report_or_preview_in_pickle():
    """
    Sets up missing report preview links for the fields in the pickle
    """

    # Set up a list of missing report and preview fields
    missing_report_preview_list = setup_missing_report_preview_list()

    # G Sheet ID and tab
    # OLD SHEET ID
    # sheet_id = '1rsPrX44pCHOhbOHZfWubDkuuexP8pSG8kspW03FAqOU'

    # NEW SHEET ID
    sheet_id = NEW_LOGGER_SETUPS_SHEET_ID
    range_name = "Setup"

    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()

    # Grab Sheet Values form S-Logger Info
    result = gSheetReader.getServiceRead(range_name, sheet_id, service)
    row_result = result['valueRanges'][0]['values']

    # Setup variables
    field_name_header = gSheetReader.getColumnHeader("Field Name (Grower)", row_result)
    grower_header = gSheetReader.getColumnHeader("Grower Name", row_result)
    field_setup_done_header = gSheetReader.getColumnHeader("Backend Setup Done", row_result)
    report_header = gSheetReader.getColumnHeader("Report", row_result)
    preview_header = gSheetReader.getColumnHeader("Preview", row_result)

    # Loop through Sheet values by row
    for ind, row in enumerate(row_result):
        try:
            if ind == 0:
                continue
            elif row[grower_header] == "":
                continue
            grower_name = row[grower_header].strip()
            field_name = row[field_name_header].strip()
            field_name_pickle = grower_name + field_name
            field_setup_done = row[field_setup_done_header]
            if field_setup_done == "TRUE":
                field_setup_done = True
            else:
                field_setup_done = False

            field_name_pickle_cleaned_up = field_name_pickle.strip()

            # Check to see if field has missing report or preview and has been flagged as done
            if field_name_pickle_cleaned_up in missing_report_preview_list and field_setup_done:
                report = row[report_header]
                preview = row[preview_header]
                # Update the field in the pickle with the correct report and preview values
                update_report_and_image_in_pickle(field_name_pickle_cleaned_up, report, preview)
                SQLScripts.update_field_portal_report_and_images(field_name_pickle_cleaned_up, report_url=report, preview_url=preview)
                # Send notification that field is finished and ready to be shown to the grower
                growers = Decagon.open_pickle()
                for grower in growers:
                    if grower.name == grower_name:
                        grower.technician.all_notifications.add_notification(
                            Notification_LoggerSetups(
                                datetime.now(),
                                grower_name,
                                field_name,
                                page_link=report
                            )
                        )

                Decagon.write_pickle(growers)
        except IndexError:
            continue
        except Exception as err:
            print(err)
            continue


def notification_setup():
    """

    """
    growers = Decagon.open_pickle()
    print('Notification Setup')
    all_technicians = Decagon.get_all_technicians(growers)
    Decagon.reset_notifications(all_technicians)
    Decagon.notifications_setup(growers, all_technicians, file_type='html')
    Decagon.write_pickle(growers)


def setup_missing_report_preview_list() -> list:
    """
    Sets up list of fields that has the default report value
    :return: List of fields that has the default report value
    """
    missing_report_preview_list = []
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            # If report is the default link then add to missing report and preview list
            if field.report_url == 'https://i.imgur.com/04UdmBH.png' and field.active:
                print('Found field without report url:', field.name)
                missing_report_preview_list.append(field.name)
    return missing_report_preview_list


def setup_nickname(field_name: str):
    """
    Setups up field nickname as field name without grower name
    :param field_name: Pickle Field Name
    """
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            if field.name == field_name:
                field.nickname = field.name.split(grower.name)[-1]
    Decagon.write_pickle(growers)


def update_field(grower_name: str, field_name: str, only_one_logger: bool = False, logger_name: str = "",
                 subtract_mrid: int = 0, rerun: bool = False):
    """
    Updates either the whole field or only a specific logger for the field
    and resets previous day switch and removes duplicate data and updates ET at once
    :param grower_name: Grower Name
    :param field_name: Field Name
    :param only_one_logger: Are you updating only one logger or the whole field?
    :param logger_name: Logger Name
    :param subtract_mrid: Subtract MRID value in case you want to go back days
    :param rerun: Is this field being rerun again after the Default run?
    """
    growers = Decagon.open_pickle()
    # Set previous day switch to 0 for loggers that will be updated
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    if only_one_logger:
                        for logger in field.loggers:
                            if logger_name == logger.name:
                                if subtract_mrid > 0:
                                    print("Setting previous day switch to zero")
                                    logger.prev_day_switch = 0
                                    logger.crashed = False
                                logger.updated = False
                                Decagon.write_pickle(growers)
                    else:
                        for logger in field.loggers:
                            logger.updated = False
                            # If field is a rerun, need to clear previous day switch value to not mess up any summed totals
                            if rerun:
                                logger.prev_day_switch = 0
                            # print(l.updated)
                            Decagon.write_pickle(growers)

    # Update field for one logger, and then delete duplicate date and update the logger ET value in the DB
    if only_one_logger:
        Decagon.only_certain_growers_field_logger_update(grower_name, field_name, logger_name,
                                                         write_to_db=True, subtract_from_mrid=subtract_mrid)
        for grower in growers:
            if grower.name == grower_name:
                for field in grower.fields:
                    if field.name == field_name:
                        for logger in field.loggers:
                            if logger.name == logger_name:
                                SQLScripts.remove_duplicate_data(logger)
        SQLScripts.update_logger_et(field_name, logger_name)
    else:
        # Update all loggers in the field
        Decagon.only_certain_growers_field_update(grower_name, field_name, get_et=False, get_weather=True,
                                                  get_data=True,
                                                  write_to_db=True)
        for grower in growers:
            if grower.name == grower_name:
                for field in grower.fields:
                    if field.name == field_name:
                        for logger in field.loggers:
                            SQLScripts.remove_duplicate_data(logger)
        SQLScripts.update_field_et(field_name)


def col_to_letter(col):
    """Gets the letter of a column number, only works for letters A-Z"""
    r = ''
    v = col % 26
    r = chr(v + 65)
    return r


def setup_dataset(field):
    """
    Function to create a dataset for fields, if one already exists do nothing
    :param field:
    """
    db = DBWriter()
    field_name = db.remove_unwanted_chars_for_db_dataset(field.name)
    project = db.get_db_project(field.loggers[-1].crop_type)
    dataset = db.check_if_dataset_exists(field_name, project)
    if not dataset:
        db.create_dataset(field_name, project)


def update_report_and_image_in_pickle(field_name_pickle, report, portal_image):
    """

    :param field_name_pickle:
    :param report:
    :param portal_image:
    """
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            if field.name == field_name_pickle:
                field.report_url = report
                field.preview_url = portal_image
                print('Updated ', field_name_pickle)
                print('\tReport URL', report)
                print('\tPortal Image', portal_image)
                print()
    Decagon.write_pickle(growers)


def update_missing_data_yesterday():
    """
    Updates any fields with missing data for yesterday
    """
    dbwriter = DBWriter.DBWriter()
    growers = Decagon.open_pickle()
    today = datetime.today()
    yesterday = today - timedelta(days=1)
    # Loop through pickle checking active fields to see if they updated yesterday
    for grower in growers:
        for field in grower.fields:
            if field.active:
                field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
                print("Working on Field: ", field.name)
                for logger in field.loggers:
                    print('\t Working on Logger: ', logger.name)
                    updated = False
                    # Check to see if data updated yesterday for the logger
                    print("\t\tChecking Date to see if yesterday updated")
                    project = dbwriter.get_db_project(logger.crop_type)
                    dataset_id = project + "." + field_name + "." + logger.name
                    dml_statement = "select date from`" + dataset_id + "` order by date"
                    result = dbwriter.run_dml(dml_statement, project=project)
                    for r in result:
                        if str(r[0]) == str(yesterday.date()):
                            # print("Field Updated")
                            updated = True
                    # If the data wasn't updated for the logger, then update the logger
                    if not updated:
                        print("\t\t\tField did not update: ", field.name, "\n Logger: ", logger.name)
                        print("\t\t\tUpdating Field")
                        update_field(grower.name, field.name, True, logger.name, subtract_mrid=24)


def return_active_fields(region: str = "Both") -> list:
    """
    Return active fields for a specific region or both regions
    :param region: Default value is "Both", but can be "North" or "South"
    :return: Returns a list of fields that are active and belong to the given region
    """
    field_list = []
    growers = Decagon.open_pickle()

    for grower in growers:
        if region == "Both":
            for field in grower.fields:
                if field.active:
                    field_list.append(field.name)
        else:
            if grower.region == region:
                for field in grower.fields:
                    if field.active:
                        field_list.append(field.name)
    return field_list


def check_for_new_cimis_stations():
    """
    Check to see if any new cimis stations has been added to the pickle
    """
    cimisStation = CimisStation()
    stomato_pickle = Decagon.open_pickle()
    cimisStation.check_for_new_cimis_stations(stomato_pickle=stomato_pickle)


def swap_logger(
        old_logger_id: str,
        new_logger_id: str,
        old_logger_name: str,
        new_logger_password: str,
        swap_date
) -> bool:
    """
    Function sets up a new logger using the information of the old logger
    :param old_logger_id: Old Logger ID
    :param new_logger_id: New Logger ID
    :param old_logger_name: Old Logger Name
    :param new_logger_password: New Logger Password
    :param swap_date: Date logger swap occurred
    :return: Was logger swapped successfully? True or False
    """
    logger_added_successfully = False
    logger_information_saved = False
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            for logger in field.loggers:
                # Set up old logger as broken and store old information to set up new logger with
                if old_logger_id == logger.id and old_logger_name == logger.name:
                    print(f'\t\tDeactivating old logger')
                    logger.broken = True
                    logger.active = False
                    logger.uninstall_date = swap_date

                    crop_type = logger.crop_type
                    soil_type = logger.soil.soil_type
                    gpm = logger.gpm
                    irrigation_acres = logger.irrigation_set_acres
                    logger_direction = logger.logger_direction
                    lat = logger.lat
                    long = logger.long
                    planting_date = logger.planting_date
                    rnd = logger.rnd
                    field_name = field.name
                    logger_information_saved = True
                    break

    # Add new logger using old logger information
    if logger_information_saved:
        print(f'\t\tCreating new logger')
        new_logger = Decagon.setup_logger(
            new_logger_id,
            new_logger_password,
            old_logger_name,
            crop_type,
            soil_type,
            gpm,
            irrigation_acres,
            logger_direction,
            lat,
            long,
            swap_date,
            planting_date=planting_date,
            rnd=rnd
        )
        if new_logger is not None:
            logger_added_successfully = True

        print(f"\t\tAdding new logger to field {field_name}")
        for grower in growers:
            for field in grower.fields:
                if field_name == field.name:
                    field.add_logger(new_logger)
                    for logger in field.loggers:
                        if logger.grower == None:
                            logger.grower = grower
                        if logger.field == None:
                            logger.field = field
                    print("\tAdded logger: " + new_logger_id)
                    # Add new logger id to pickle of logger ID's in the pickle
                    add_logger_id_to_pickle(new_logger_id)

        Decagon.write_pickle(growers)

    return logger_added_successfully


def logger_swap_process():
    """
    Checks to see if there are any new submissions for loggers that need to be swapped
    """
    # G Sheet ID and tab name
    # sheet_id = '1rsPrX44pCHOhbOHZfWubDkuuexP8pSG8kspW03FAqOU'
    sheet_id = NEW_LOGGER_SETUPS_SHEET_ID

    # range_name = "Logger Swap Form"
    range_name = "Logger Swap"

    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()

    result = gSheetReader.getServiceRead(range_name, sheet_id, service)
    row_result = result['valueRanges'][0]['values']

    logger_dict = set_up_password_dict(service, sheet_id)

    backed_swap_done_header = gSheetReader.getColumnHeader("Swap Done", row_result)
    timestamp_header = gSheetReader.getColumnHeader("Timestamp", row_result)

    # Loop through each row
    for row_index, row in enumerate(row_result):
        try:
            if row_index == 0:
                continue
            elif row[timestamp_header] == "":
                continue

            backed_swap_done = row[backed_swap_done_header]
            # Check swap flag to see if we have already completed the swap before
            if backed_swap_done == 'FALSE':
                backed_swap_done = False
            if not backed_swap_done:
                # Assign indexes to columns
                print(f'\tFound logger to swap...')
                old_logger_id_header = gSheetReader.getColumnHeader("Old Logger ID", row_result)
                new_logger_id_header = gSheetReader.getColumnHeader("New Logger ID", row_result)
                old_logger_name_header = gSheetReader.getColumnHeader("Old / New Logger Name", row_result)
                date_swapped_header = gSheetReader.getColumnHeader("Date of the Swap", row_result)

                old_logger_id = row[old_logger_id_header].strip()
                new_logger_id = row[new_logger_id_header].strip()
                old_logger_name = row[old_logger_name_header].strip()
                date_swapped = row[date_swapped_header].strip()
                date_swapped_converted = datetime.strptime(date_swapped, '%Y-%m-%d').date()

                print(f'\tSwapping old logger: {old_logger_id} for new logger: {new_logger_id}')
                # Replace logger
                replaced_logger_successfully = swap_logger(old_logger_id, new_logger_id, old_logger_name,
                                                           logger_dict[new_logger_id],
                                                           date_swapped_converted)

                # If logger was replaced update swap flag on G Sheet
                if replaced_logger_successfully:
                    target_cell = f'{range_name}!G{row_index + 1}'
                    # print(target_cell)
                    gSheetReader.write_target_cell(target_cell, True, sheet_id, service)
                    print("\tLogger Replaced Successfully")


        except IndexError:
            continue

        except Exception as err:
            print(err)
            continue


def field_uninstall_process():
    """
    Function that handles field uninstallation. Function loops through a Gsheet and checks for any rows that have the
    Uninstall Done check box unchecked. For those that do, it grabs those field names and uninstalls them.

    """
    now = datetime.today()
    print(">>>>>>>>>>>>>>>>>>>FIELD UNINSTALL PROCESS 2.0<<<<<<<<<<<<<<<<<<<")
    print("                                 - " + now.strftime("%m/%d/%y  %I:%M %p"))
    print()
    print()

    dbw = DBWriter()

    sheet_id = NEW_LOGGER_SETUPS_SHEET_ID
    g_sheets_tab_name = "Uninstall"

    g_sheet_credential = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet_credential.getService()

    result = gSheetReader.getServiceRead(g_sheets_tab_name, sheet_id, service)
    g_sheet_row_results = result['valueRanges'][0]['values']

    # Assign indexes to columns
    field_name_header = gSheetReader.getColumnHeader("Fields Uninstalled", g_sheet_row_results)
    uninstall_done_header = gSheetReader.getColumnHeader("Uninstall Done", g_sheet_row_results)

    num_of_fields_to_uninstall = 0
    fields_uninstalled_list = []

    grower_pickle = Decagon.open_pickle()

    for field_row_iterator, field_row_data in enumerate(g_sheet_row_results):
        try:
            # Skip G Sheet Headers
            if field_row_iterator == 0:
                continue
            # if field name is blank, probably a blank row so skip
            elif field_row_data[field_name_header] == "":
                continue

            uninstall_done = field_row_data[uninstall_done_header]
            if uninstall_done == "FALSE":
                uninstall_done = False

            if not uninstall_done:
                cleaned_split_field_names = None
                print(f'\tFound field/s to uninstall...')
                field_names = field_row_data[field_name_header]
                print(f'\t>>>{field_names}')
                split_field_names = field_names.split(',')
                cleaned_split_field_names = [s.strip() for s in split_field_names]
                num_of_fields_to_uninstall += len(cleaned_split_field_names)

                try:
                    successful_uninstall = True
                    for grower in grower_pickle:
                        for field in grower.fields:
                            if field.name in cleaned_split_field_names:
                                print("\tUninstalling ", field.name)
                                crop_type = field.loggers[-1].crop_type
                                project = dbw.get_db_project(crop_type)
                                grower_name_db = dbw.remove_unwanted_chars_for_db_dataset(field.grower.name)
                                field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field.name)

                                # Get uninstallation Date using last date of database
                                print(f"\t\tGrabbing last data date for uninstall date")
                                table_id = f"`{project}.{field_name_db}.{field.loggers[-1].name}`"
                                select_uninstall_date_query = f"select t.date from {table_id} as t order by t.date DESC limit 1"
                                result_uninstall_date_query = dbw.run_dml(select_uninstall_date_query)
                                field_uninstallation_date = None
                                for row in result_uninstall_date_query:
                                    field_uninstallation_date = row.date
                                print(
                                    f"\t\t\t{field.name}:{field.loggers[-1].name} Last Data Date:{field_uninstallation_date}")
                                if not field_uninstallation_date:
                                    print(f"\t\t\tNo data for field to get uninstall date for")
                                    continue

                                print(f"\t\tSetting field to uninstalled in portal")
                                project = f'growers-{YEAR}'
                                uninstall_dml_field_averages = (
                                    f'UPDATE `{project}.{grower_name_db}.field_averages` as t '
                                    f'SET t.order = -999, t.soil_moisture_num = null, '
                                    f't.soil_moisture_desc = "Uninstalled", t.si_num = null, '
                                    f't.si_desc = "Uninstalled" WHERE t.field = "{field.nickname}"'
                                )
                                dbw.run_dml(uninstall_dml_field_averages)
                                print(f'\t\t\tField Averages Table Done')

                                uninstall_dml_loggers = (
                                    f'UPDATE `{project}.{grower_name_db}.loggers` as t '
                                    f'SET t.order = -999, t.soil_moisture_num = null, '
                                    f't.soil_moisture_desc = "Uninstalled", t.si_num = null, '
                                    f't.si_desc = "Uninstalled" WHERE t.field = "{field.nickname}"'
                                )
                                dbw.run_dml(uninstall_dml_loggers)
                                print(f'\t\t\tLoggers Table Done')

                                for logger in field.loggers:
                                    logger.uninstall_date = field_uninstallation_date

                                field.deactivate()
                                fields_uninstalled_list.append(field.name)
                                print("\t<- Uninstall Done ", field.name)
                except Exception as err:
                    print(f'ERROR {err}')
                    successful_uninstall = False

                if successful_uninstall:
                    target_cell = f'{g_sheets_tab_name}!D{field_row_iterator + 1}'
                    # print(target_cell)
                    gSheetReader.write_target_cell(target_cell, True, sheet_id, service)
                    print(f"\tFields Uninstalled Successfully - {field_names}")

        except IndexError:
            continue

    Decagon.write_pickle(grower_pickle)
    print("Removed the following Fields: ", fields_uninstalled_list)
    print(f"{len(fields_uninstalled_list)} / {num_of_fields_to_uninstall} removed")


def change_psi_for_specific_field_logger(field_name: str, logger_name: str, should_be_on: bool = False):
    """
    Turns on PSI for a specific logger in a specific field
    :param should_be_on: Should PSI be on or off
    :param field_name: Name of field
    :param logger_name: Name of logger
    :return:
    """
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            if field.name == field_name:
                for logger in field.loggers:
                    if logger.name == logger_name:
                        if should_be_on:
                            logger.ir_active = True
                            print(f"Turned on PSI for {field.name}:{logger_name}")
                        else:
                            logger.ir_active = False
                            print(f"Turned off PSI for {field.name}:{logger_name}")
    Decagon.write_pickle(growers)



def get_soil_type_from_coords(latitude, longitude):
    """
    Grabs soil type from ADA API given latitude and longitude
    :param latitude: Latitude coordinate
    :param longitude: Longitude coordinate
    :return: Dominant soil type (e.g., 'Loam', 'Clay Loam', 'Sandy Loam')
    """
    point_wkt = f"POINT({longitude} {latitude})"
    # SQL query to get soil texture information
    query = f"""
    SELECT mu.muname, c.localphase, ctg.texdesc, ch.hzname
    FROM mapunit AS mu
    JOIN component AS c ON c.mukey = mu.mukey
    JOIN chorizon AS ch ON ch.cokey = c.cokey
    JOIN chtexturegrp AS ctg ON ctg.chkey = ch.chkey
    WHERE mu.mukey IN (
            SELECT DISTINCT mukey
            FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('{point_wkt}')
        )
    ORDER BY ch.hzdept_r ASC
    """

    # SDA request payload
    request_payload = {
        "format": "JSON+COLUMNNAME+METADATA",
        "query": query
    }

    sda_url = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(sda_url, json=request_payload, headers=headers)

    # Soil types ordered from most specific to least specific
    soil_types = ['Silty Clay Loam', 'Sandy Clay Loam',
                  'Sandy Clay', 'Silt Loam', 'Clay Loam',
                  'Silty Clay', 'Loamy Sand', 'Sandy Loam',
                  'Sand', 'Clay', 'Loam', 'Silt']

    if response.status_code == 200:
        data = response.json()
        if "Table" in data and len(data["Table"]) > 2:

            # Step 1: Check muname and localphase first
            for row in data["Table"][2:]:
                muname = str(row[0]).lower()
                localphase = str(row[1]).lower()

                # 1st attempt at main name - muname
                for soil_type in soil_types:
                    if soil_type.lower() in muname:
                        print(f'\t\t\t\tSoil Match: {soil_type}')
                        return soil_type, False

                # 2nd attempt at localphase
                for soil_type in soil_types:
                    if soil_type.lower() in localphase:
                        print(f'\t\t\t\tSoil Match in localphase: {soil_type}')
                        return soil_type, False

            # Step 2: Fallback to texdesc but only if hzname is "Ap"
            for row in data["Table"][2:]:
                texdesc = str(row[2]).lower()
                hzname = str(row[3]).lower()

                # Only consider texture description if horizon name is "Ap"
                if "A" in hzname:
                    for soil_type in soil_types:
                        if soil_type.lower() in texdesc:
                            print(f'\t\t\t\tFallback Match (texdesc with Ap horizon): {soil_type}')
                            return soil_type, True

            # print("\t\t\t\tNo soil type found based on the given attributes.")
        else:
            print("\t\t\t\tNo soil information found for the given coordinates.")
    else:
        print(f"Error: {response.status_code}, {response.text}")

    return None, False



def grab_ms_field_names_from_setups_to_pickle():
    """
    Function that goes through the pickle and adds a field name (ms) depending on what we have in the logger
    setups form submission. Field Name (MS) will be useful going forward so we want to start storing that in the
    Field class

    """
    growers = Decagon.open_pickle()

    # Setup Google Sheet information
    sheet_tab_name = 'Setup'
    sheet_tab_name_old = 'Old Setups'

    # Setups service to read google sheets
    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()

    # Read Google Sheet for Current Logger Setups
    result = gSheetReader.getServiceRead(sheet_tab_name, NEW_LOGGER_SETUPS_SHEET_ID, service)
    row_result = result['valueRanges'][0]['values']

    # Read Google Sheet for Current Logger Setups
    result_old = gSheetReader.getServiceRead(sheet_tab_name_old, NEW_LOGGER_SETUPS_SHEET_ID, service)
    row_result_old = result_old['valueRanges'][0]['values']

    # NEW LOGGER SETUPS SHEET
    # Loop through each row in the Google sheet and check if the field is in our current pickle. If it isn't,
    #  go through the setup process using that row's data.
    print('\nProcessing new logger setups sheet')
    for row_index, row in enumerate(row_result):
        # Ignore header row
        if row_index == 0:
            continue

        # Set up variables
        field_name_col = gSheetReader.getColumnHeader("Field Name (Grower)", row_result)
        field_name = row[field_name_col].strip()

        field_name_ms_col = gSheetReader.getColumnHeader("Field Name (MS)", row_result)
        field_name_ms = row[field_name_ms_col].strip()

        grower_name_col = gSheetReader.getColumnHeader("Grower Name", row_result)
        grower_name = row[grower_name_col].strip()

        field_name_pickle = grower_name + field_name

        for grower in growers:
            for field in grower.fields:
                if field.name == field_name_pickle:
                    print(f'Found field in pickle {field.name}')
                    if field.name_ms == 'NA' or field.name_ms == '':
                        # Format the field ms string to be consistent. If it's empty, NA, or 1234, 5678 or 1234 5678, it turns it
                        # into a string that is comma separated
                        field_name_ms_formatted = format_field_name_ms_standard(field_name_ms)
                        print(f'\tFormatted field name ms: {field_name_ms_formatted}')
                        print(f'\t\tChanged field ms name from {field.name_ms} -> {field_name_ms_formatted}')
                        field.name_ms = field_name_ms_formatted
    Decagon.write_pickle(growers)

    growers = Decagon.open_pickle()
    # OLD LOGGER SETUPS SHEET
    # Loop through each row in the Google sheet and check if the field is in our current pickle. If it isn't,
    #  go through the setup process using that row's data.
    print('\nProcessing old logger setups sheet')
    for row_index, row in enumerate(row_result_old):
        # Ignore header row
        if row_index == 0:
            continue

        # Set up variables
        field_name_col = gSheetReader.getColumnHeader("Grower Field", row_result_old)
        field_name = row[field_name_col].strip()

        field_name_ms_col = gSheetReader.getColumnHeader("MS Field", row_result_old)
        field_name_ms = row[field_name_ms_col].strip()

        grower_name_col = gSheetReader.getColumnHeader("Grower Name", row_result_old)
        grower_name = row[grower_name_col].strip()

        field_name_pickle = grower_name + field_name

        for grower in growers:
            for field in grower.fields:
                if field.name == field_name_pickle:
                    print(f'Found field in pickle {field.name}')
                    if field.name_ms == 'NA' or field.name_ms == '':
                        # Format the field ms string to be consistent. If it's empty, NA, or 1234, 5678 or 1234 5678, it turns it
                        # into a string that is comma separated
                        field_name_ms_formatted = format_field_name_ms_standard(field_name_ms)
                        print(f'\tFormatted field name ms: {field_name_ms_formatted}')
                        print(f'\t\tChanged field ms name from {field.name_ms} -> {field_name_ms_formatted}')
                        field.name_ms = field_name_ms_formatted
    Decagon.write_pickle(growers)


def format_field_name_ms_standard(input_str):
    """
    Function that takes a string that could be in several formats
    1. ''
    2. 'NA'
    3. '1234, 5678, 9111'
    4. '1234 5678 9111'
    and returns a string that is in either of 2 formats
    1. 'NA' if '' or 'NA'
    2. '1234, 5678, 9111' if comma separated or space separated
    This function is used in conjunction with the Field Name (MA) column from logger setups since the techs can input the
    field names (ms) in several different ways and we want to handle all of them

    :param input_str:
    :return:
    """
    # Check if the input string is empty or 'NA'
    if not input_str or input_str.upper() == 'NA':
        return 'NA'

    # Find all 4-digit numbers in the string
    numbers = re.findall(r'\b\d{4}\b', input_str)

    # Join the numbers with ', ' if any are found, otherwise return 'NA'
    if numbers:
        return ', '.join(numbers)
    else:
        return 'NA'


def update_uninstalled_table():
    growers = Decagon.open_pickle()
    dbw = DBWriter()
    table_fields = {}
    already_uninstalled = []
    regions = ['North', 'South']

    # Fetch all fields from BigQuery (not just uninstalled ones)
    for region in regions:
        dataset = f'stomato-info.uninstallation_progress.{region}_2025'
        dml_statement = f"SELECT Grower, Field FROM {dataset}"
        result = dbw.run_dml(dml_statement)

        for row in result:
            table_fields[row[0] + row[1]] = region  # Store fullname to check later

        # check which fields are still not uninstalled
        dml_statement = f"SELECT Grower, Field FROM {dataset} Where Uninstalled_Date is not null"
        result = dbw.run_dml(dml_statement)
        for row in result:
            already_uninstalled.append(row[0] + row[1])

    # Process each field in the pickle file
    for grower in growers:
        for field in grower.fields:
            region = grower.region
            dataset = f'stomato-info.uninstallation_progress.{region}_2025'
            fullname = grower.name + field.nickname  # Unique identifier

            if fullname not in table_fields:  # Field not in BigQuery, insert it
                print(f"Inserting new field: {grower.name} - {field.nickname}")

                # Handle ir_date being NULL
                # TODO use the Thresholds object for the IR date
                ir_date = field.loggers[0].get_ir_date() if field.loggers else None
                ir_date = 'NULL' if ir_date is None else f"'{ir_date}'"

                # Insert new field with Uninstalled_Date = NULL
                insert_statement = (
                    f"INSERT INTO {dataset} "
                    f"(Grower, Field, Uninstalled_Date, Acres, Latt_Long, Maturity_Date, Number_Of_Loggers, crop_type, ir_date) "
                    f"VALUES ('{grower.name}', '{field.nickname}', NULL, {field.acres}, '{field.lat},{field.long}', "
                    f"NULL, {len(field.loggers)}, '{field.loggers[0].crop_type}', {ir_date})"
                )
                dbw.run_dml(insert_statement)
                print(f"Inserted {grower.name} - {field.nickname} into {dataset}")

            # Now add UPDATE logic if field already exists
            # for logger in field.loggers:
            all_loggers_uninstalled = all(logger.uninstall_date is not None for logger in field.loggers)
            if all_loggers_uninstalled and fullname not in already_uninstalled:  # If any logger has been uninstalled, update the DB
                update_statement = (
                    f"UPDATE {dataset} "
                    f"SET Uninstalled_Date = '{field.loggers[-1].uninstall_date}' "  # using last logger in case the first ones were swapped and old, the latest in the field should be up to date 
                    f"WHERE Grower = '{grower.name}' AND Field = '{field.nickname}'"
                )
                rows_affected = dbw.run_dml(update_statement)

                if rows_affected:
                    print(f"\tUpdated uninstall date for {grower.name} - {field.nickname}")
                break  # Only need to update once per field





def update_acres_for_region(region, stats_dataset):
    print(f"Updating Stats for {region}")
    dbw = DBWriter()
    installed_acres, uninstalled_acres = Decagon.calculate_stats_for_acres(region)

    if installed_acres or uninstalled_acres:
        type_installed = f'Acres Installed {region}'
        type_uninstalled = f'Acres Uninstalled {region}'
        dml_statement = f"update `{stats_dataset}` set acres = {installed_acres} where type = '{type_installed}'"
        dbw.run_dml(dml_statement)
        print(f'\tInstalled acres: {installed_acres}')

        dml_statement = f"update `{stats_dataset}` set acres = {uninstalled_acres} where type = '{type_uninstalled}'"
        dbw.run_dml(dml_statement)
        print(f'\tUninstalled acres: {uninstalled_acres}')
    else:
        print(f"\tError, not updating stats for {region}")


def get_logger_geolocation(logger_id):
    """

    :param logger_id:
    :return:
    """
    response = Zentra.get_settings(device_sn=logger_id)
    if response.ok:
        print(f'\t\t\t\tGrabbing Coords from Zentra: {logger_id}...')
        content = response.json()
        lat = content['device']['locations'][0]['latitude']
        long = content['device']['locations'][0]['longitude']
    else:
        print(f'\t\t\t\tError {response.text}')
        lat, long = '', ''
    return lat, long


def update_technician_portal():
    """
    Updates the BQ uninstallation progress table for both regions then calculates the acres un/installed
    for the technician portal
    """
    update_uninstalled_table()

    stats_dataset = 'stomato-info.uninstallation_progress.stats_acres_2025'
    update_acres_for_region('North', stats_dataset)
    update_acres_for_region('South', stats_dataset)

# def main():
#     update_technician_portal()
#     print('Cloud Run Job success')
#     sys.exit(0)
#
# if __name__ == "__main__":
#     main()
# if g.name == 'CM Ochoa':
#     for l in f.loggers:
#         if l.name == 'CM-L37WM-N' or l.name == 'CM-L37WM2-S' or l.name == 'CM-L35WM-N' or l.name == 'CM-L35WM2-S':
#             print(l.name)
#             l.active = False
#             print('\t' + str(l.active))
# print(l.active)
# f.cwsi_processor = CwsiProcessor.CwsiProcessor()
# print('done')
# if f.name == 'Dougherty BrosPC':
# f.report_url = 'https://datastudio.google.com/reporting/29513a03-37e1-4873-85e0-17c1bcd2b636'
# f.preview_url = 'https://i.imgur.com/0kQGrxZ.png'
#             print(f.active)
#             f.active = False
#             print(f.active)
#         print(f.name)
# Decagon.write_pickle(growers)
# if g.name == 'Hughes':
# report = input("Please input portal report for field: " + f.name + "\n")
# preview = input("Please input portal preview for field: " + f.name + "\n")
# f.report_url = report
# f.preview_url = preview
# print(f.report_url)
# print(f.preview_url)
#     if newNickname == "":
#         print("Keeping nickname the same")
#     else:
#         print("Changing nickname to : " + newNickname)
#         f.nickname = newNickname
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
# if g.name == 'Carvalho':
#     f.nickname = input("Enter Nickname for Carvalho Field: " + f.name + "\n")
# f.nickname = f.name.split(g.name)[-1]
# print(f.nickname)
# Decagon.write_pickle(growers)

# Decagon.removeGrower('Jesus')
# Decagon.remove_field("Fantozzi", "Fantozzi2_7 East")
# removeLoggerIDFromPickle("z6-07262")
# removeLoggerIDFromPickle("z6-07264")
# removeLoggerIDFromPickle("z6-12337")

# setupField()
# showLoggerIDPickle()
# testBug("Bone Farms LLCF7")
# Decagon.show_pickle()
# fieldName = 'Carvalho315'
# loggerName = ''
# fc = 36
# wp = 22
# dbwriter = DBWriter.DBWriter()
# Decagon.show_pickle()
# Decagon.removeGrower('Lucero Watermark')
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.name == 'RKB115_116_107':
#             for l in f.loggers:
#                 if l.active:
#                     print(l.id)
# Decagon.removeLogger('Bullseye Farms', 'Bullseye FarmsME4', 'z6-11968')
#             field_name = dbwriter.remove_unwanted_chars_for_db(f.name)
#             print("Working on Field: ", f.name)
#             for l in f.loggers:
#                 print('\t Working on Logger: ', l.name)
#                 # print("\t\tDeleting Repeat Data")
#                 # SQLScripts.delete_repeat_data(f.name,l.name)
#                 updated = False
#                 print("\t\tChecking Date to see if yesterday updated")
#                 dataset_id = "stomato." + field_name + "." + l.name
#                 dmlStatement = "select date from`" + dataset_id + "` order by date"
#                 # print(dmlStatement)
#                 result = dbwriter.run_dml(dmlStatement)
#                 for r in result:
#                     if str(r[0]) == '2022-07-15':
#                         # print("Field Updated")
#                         updated = True
#                 if not updated:
#                     print("\t\t\tField did not update: ", f.name, "\n Logger: ", l.name)
#                     print("\t\t\tUpdating Field")
#                     updateField(g.name, f.name, True, l.name, subtractMRID=24)
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.name == 'DCBVerway T1, T2, T3':
#             for l in f.loggers:
#                 if l.id == 'z6-12309':
#                     l.password = '82466-02422'
# Decagon.write_pickle(growers)
#                     print("Changing fc and wp for logger: " + l.name)
#                     l.fieldCapacity = fc
#                     l.wiltingPoint = wp
#                     SQLScripts.update_FC_WP(f.name, l.name, fc, wp)
#                 elif loggerName == '':
#                     print("Changing fc and wp for logger: " + l.name)
#                     l.fieldCapacity = fc
#                     l.wiltingPoint = wp
#                     SQLScripts.update_FC_WP(f.name, l.name, fc, wp)
#
#
# Decagon.write_pickle(growers)

# count = 0
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.active == True:
#             if f.name == "DCBVerway T1, T2, T3":
#                 for l in f.loggers:
#                     if l.id == 'z6-01892':
#                         Decagon.removeLogger(g.name, f.name, l.id)
# print("Do I have a second logger?")
#                         l.name = 'Bull-RG60-SE'
#                     if l.id == 'z6-01953':
#                         l.name = 'Bull-RG60-NW'
# #             print(f.name)
#             f.name = 'RKB115_116_107'
#             # for l in f.loggers:
#                 # print("\t" + l.name)
# Decagon.write_pickle(growers)
# Decagon.show_pickle()

# l.name = 'SMonica1-BlkY-C'
# if l.name == 'MA-BW-PI':
#     l.name = 'MA-BWPI-SW'
# if l.name == 'MA-YW-PI':
#     l.name = 'MA-YWPI-SW'
# if l.name == 'MA-BE-PI':
#     l.name = 'MA-BEPI-NE'
# if l.name == 'MA-YE-PI':
#     l.name = 'MA-YEPI-NE'

# if g.name == "Meza":
#     for f in g.fields:
#         print(f.name)
#         for l in f.loggers:
#             print(l.name)
#             print(l.id)
#             print(l.password)
# print(count)
# Decagon.deactivate_growers_with_all_inactive_fields()
# fieldName = "OPC3-2"
# for g in growers:
#     for f in g.fields:
#         if fieldName == f.name:
#             f.active = True
#             for l in f.loggers:
#                 l.active = True
# Decagon.write_pickle(growers)
#             Decagon.deactivate_field(g.name, f.name)
#             # for l in f.loggers:
#             #     Decagon.deactivate_logger(g.name, f.name, l.id)
# # Decagon.deactivate_field("OPC", 'OPC3-2')

# SQLScripts.deleteLastDay('DCBNees 7-8', 'z6-12427', '2021-08-18')
# updateField("DCB", "DCBRogerro 30-40", True, 'DCB-Rog30_40-N', subtractMRID=24)

# updateField('RnD', 'RnDDouble-Single Trial', True, 'SLine-DTape-S', subtractMRID=6*24+10)
# updateField('Lucero Bakersfield', 'Lucero BakersfieldTowerline', True, 'TO-Green-N', subtractMRID=0)
# updateField('Carvalho', 'Carvalho308', True, 'z6-03436', subtractMRID=0)
# updateField('JJB Farms', 'JJB FarmsJones Tract', True, 'z6-12336', subtractMRID=31)
# updateField('JJB Farms', 'JJB FarmsJones Tract', True, 'z6-12429', subtractMRID=31)

# Decagon.removeField("JHP", "JHPBase 5")
# removeLoggerIDFromPickle("z6-07214")
# removeLoggerIDFromPickle("5G118559")
# Decagon.removeField("DCB", "DCBNees 7-8")
# removeLoggerIDFromPickle("z6-11976")
# removeLoggerIDFromPickle("z6-01887")
# removeLoggerIDFromPickle("z6-01871")
# #
# removeAllLoggerIDFromPickle()
# showLoggerIDPickle()
# Decagon.removeGrower("KTN JV")
# Decagon.removeLastGrower()
# addLoggerIDToPickle("5G105816")

# updateField('DCB', 'DCBEdgemar 228', hasLogger=True, logger='z6-12309', subtractMRID=24*12)
#
# Decagon.onlyCertainGrowersFieldUpdate("UC Davis", "UC DavisUCD Veg Crops", get_et=False, get_weather=True, get_data=False,
#                                       write_to_sheet=True, write_to_portal_sheet=False, write_to_db=False)
# Decagon.onlyCertainGrowersFieldUpdate("David Santos", "David SantosSP3", get_et=False, get_weather=True, get_data=True,
#                                       write_to_sheet=True, write_to_portal_sheet=True, write_to_db=True)

# Decagon.onlyCertainGrowersFieldUpdate("DCB", "DCBMadd", get_et=False, get_weather=True, get_data=True,
#                                       write_to_sheet=True, write_to_portal_sheet=True, write_to_db=True)

# Decagon.onlyCertainGrowersFieldUpdate("Maricopa Orchards", "Maricopa Orchards3425", get_et=False, get_weather=True, get_data=True,
#                                       write_to_sheet=True, write_to_portal_sheet=True, write_to_db=True)

# Decagon.onlyCertainGrowersFieldUpdate("Hughes", "Hughes301", get_et=False, get_weather=True, get_data=True,
#                                       write_to_sheet=True, write_to_portal_sheet=True, write_to_db=True)

# Decagon.onlyCertainGrowersFieldLoggerUpdate("Hughes", "Hughes309-4", "z6-07220", write_to_sheet=True,
#                                             write_to_portal_sheet=True, write_to_db=True, subtract_from_mrid=28)

# ET Update

# Decagon.onlyCertainGrowersETUpdate("Hughes", writeToSheet=False, writeToDB=True)

# Decagon.onlyCertainGrowersUpdate("Carvalho", get_et=True, write_to_sheet=True)

# Decagon.onlyCertainGrowersUpdate("DCB", get_et=False, get_weather=True, get_data=True,
#                                  write_to_sheet=True, write_to_portal_sheet=True, write_to_db=False)

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:\\Users\\jsalcedo\\PycharmProjects\\Stomato\\credentials.json"

# Decagon.onlyCertainGrowersFieldLoggerUpdate("Maricopa Orchards", "Maricopa Orchards3425", "z6-06012", write_to_sheet=True,
#                                             write_to_portal_sheet=True, write_to_db=True, subtract_from_mrid=33)

# Decagon.show_pickle()

# Decagon.onlyCertainGrowersFieldLoggerUpdate("OPC", "OPC24-3", "5G129309", write_to_sheet=False,
#                                             write_to_portal_sheet=False, write_to_db=False, subtract_from_mrid=80)

# creds = GSheetCredentialSevice.GSheetCredentialSevice()
# creds.getCreds()

# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         for logger in f.loggers:
#             setup_new_logger(logger.id, logger.name, 1, )
# check_for_missing_report_or_preview_in_pickle()
# logger_setups_process()

# setup_field()
# remove_field('Matteoli Bros', 'Matteoli BrosM14')
# remove_field('Dougherty Bros', 'Dougherty BrosT3')
# logger_setups_process()
# notification_setup()

# check_for_missing_report_or_preview_in_pickle()

# check_for_broken_loggers()

# remove_field('Kubo & Young', 'Kubo & YoungKF1')
# remove_field('Lucero Rio Vista', 'Lucero Rio VistaD')
# logger_setups_process()
# check_for_missing_report_or_preview_in_pickl
#
# le()

# Decagon.show_pickle()
# Decagon.remove_grower('Dwelley Farms')

# logger_setups_process()
# remove_field()
# soil = get_soil_type_from_coords(39.503691, -122.281732)
# print()
# update_technician_portal()
# logger_swap_process()
# check_for_missing_report_or_preview_in_pickle()
# logger_swap_process()
growers = Decagon.open_pickle()
# print(growers)
cimis = CIMIS()
cimis_stations_data = cimis.get_list_of_active_eto_stations()
for grower in growers:
    for field in grower.fields:
        if 'GR1' in field.name:
            print(field.name)
            setup_irr_scheduling(field, cimis_stations_data)
# update_uninstalled_table()