import calendar
import itertools
import pickle
import pprint
from datetime import datetime, timedelta, date
from os import path
from random import choices
from statistics import mean
from string import ascii_uppercase, digits

import google.api_core.exceptions
import numpy
from dateutil.relativedelta import relativedelta
from google.api_core import exceptions
from google.cloud import bigquery

import Decagon
from CIMIS import CIMIS
from CwsiProcessor import CwsiProcessor
from DBWriter import DBWriter

dbwriter = DBWriter()
DIRECTORY_YEAR = "2025"
PICKLE_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Pickle\\"


def update_value_for_date(project, field_name, logger_name, date, value_name, value):
    dml = 'UPDATE `' + str(project) + '.' + str(field_name) + '.' + str(logger_name) + '`' \
          + ' SET ' + str(value_name) + ' = ' + str(value) \
          + " WHERE date = '" + str(date) + "'"
    print(dml)

    dbwriter.run_dml(dml, project=project)


def delete_null_rows(project, field, logger, row_value='', start_date='', end_date=''):
    print('Deleting nulls in: ' + str(field) + ' ' + str(logger))
    if start_date == '' and end_date == '':
        if row_value == '':
            dml = "DELETE   FROM " \
                  + "`" + project + "." + str(field) + "." + str(logger) + "`" \
                  + " WHERE logger_id is NULL"
        else:
            dml = "DELETE   FROM " \
                  + "`" + project + "." + str(field) + "." + str(logger) + "`" \
                  + " WHERE " + row_value + " is NULL"
    else:
        if row_value == '':
            dml = "DELETE   FROM " \
                  + "`" + project + "." + str(field) + "." + str(logger) + "`" \
                  + " WHERE logger_id is NULL AND date BETWEEN DATE('" + start_date + "') AND DATE('" + end_date + "') "
        else:
            dml = "DELETE   FROM " \
                  + "`" + project + "." + str(field) + "." + str(logger) + "`" \
                  + " WHERE " + row_value + " is NULL AND date BETWEEN DATE('" + start_date + "') AND DATE('" + end_date + "') "
    dbwriter.run_dml(dml, project=project)


def delete_all_null_rows(project, row_value='', start_date='', end_date=''):
    datasets = dbwriter.get_datasets(project=project)
    for d in datasets[0]:
        if d.dataset_id == 'ET' or d.dataset_id == 'Historical_ET':
            continue
        tables = dbwriter.get_tables(d.dataset_id, project=project)
        for t in tables:
            if t.table_id == 'Lat Long Trial':
                continue
            delete_null_rows(project, d.dataset_id, t.table_id, row_value, start_date, end_date)


def update_irrigation_hours_for_date(project, field_name, logger_name, daily_hours, date):
    # Get gpm and acres
    gpm = Decagon.get_gpm(field_name, logger_name)
    acres = Decagon.get_acres(field_name, logger_name)
    field_db = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    # Calculate daily hours, flow, and inches using Switch Data
    switch_data = daily_hours * 60
    daily_inches = round((switch_data * float(gpm)) / (float(acres) * 27154), 1)
    # Set up and run Query
    dml = "UPDATE `" + str(project) + "." + str(field_db) + "." + str(logger_name) + "`" \
          + " SET daily_switch = " + str(switch_data) + ", daily_hours = " + str(daily_hours) + ", daily_inches = " \
          + str(daily_inches) + " WHERE date = '" + str(date) + "'"
    # print(dml)
    dbwriter.run_dml(dml, project=project)
    print("Done Updating Irr. Hours")


# update_irrigation_hours_for_date('stomato-2023', 'Lucero Mandeville17 I J', 'LM-17J-S', 4.2, '2023-08-08')
# update_irrigation_hours_for_date('stomato-2023', 'Lucero Mandeville17 I J', 'LM-17J-S', 5.1, '2023-08-10')
# update_irrigation_hours_for_date('stomato-2023', 'Lucero Mandeville17 I J', 'LM-17J-S', 3.5, '2023-08-12')
# update_irrigation_hours_for_date('stomato-2023', 'Lucero Mandeville17 I J', 'LM-17J-S', 13.8, '2023-08-15')
# update_irrigation_hours_for_date('stomato-2023', 'Lucero Mandeville17 I J', 'LM-17J-S', 6.8, '2023-08-16')

def update_irrigation_inches_for_whole_table(project, field_name, logger_name):
    # Useful when gpm or acres change and you want to recalculate all the inches
    gpm = Decagon.get_gpm(field_name, logger_name)
    acres = Decagon.get_acres(field_name, logger_name)
    field_db = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    # Calculate inches using Switch Data
    # daily_inches = round((switch_data * float(gpm)) / (float(acres) * 27154), 1)
    # Set up and run Query
    dml = f"UPDATE `{str(project)}.{str(field_db)}.{str(logger_name)}`" \
          + f" SET  daily_inches = ((daily_switch * {float(gpm)}) / ({float(acres)} * 27154))" \
          + f" WHERE logger_id is not null"
    # print(dml)
    dbwriter.run_dml(dml, project=project)
    print("Done Updating Irr. Inches")


# update_irr_inches_for_date('stomato-2023', 'Lucero Dillard RoadD4', 'DI-D4-W')


def update_irrigation_hours_for_date_range(project, field_name, logger_name, daily_hours, start_date, end_date):
    # Get gpm and acres
    gpm = Decagon.get_gpm(field_name, logger_name)
    acres = Decagon.get_acres(field_name, logger_name)

    field_db = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)

    # Turn date string into datetimes
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date_dt - start_date_dt + timedelta(days=1)

    # Calculate daily hours, flow, and inches using Switch Data
    switch_data = daily_hours * 60
    flow = round((switch_data * float(gpm)) / float(acres))
    daily_inches = flow / 27154
    # Set up and run Query
    dml = "UPDATE `" + str(project) + "." + str(field_db) + "." + str(logger_name) + "`" \
          + " SET daily_switch = " + str(switch_data) + ", daily_hours = " + str(daily_hours) + ", daily_inches = " \
          + str(daily_inches) + " WHERE date BETWEEN DATE('" + start_date + "') AND DATE('" + end_date + "') "
    dbwriter.run_dml(dml, project=project)
    print("Done Updating Irr. Hours")


def update_eto_etc(project, field_name, logger_name, list_etos, start_date, end_date):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    list_etos = list_etos

    # Setup dataset_id from passed in field_name and logger_name parameters
    dataset_id = project + '.' + field_name + '.' + logger_name
    dataset_id = "`" + dataset_id + "`"

    # Turn date string into datetimes
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date_dt - start_date_dt + timedelta(days=1)

    # check that length of listETos = endDate - startDate
    if len(list_etos) == delta.days:
        for eto in list_etos:
            # startDateS = datetime.strftime(start_date_dt, "%Y-%m-%d")
            startDateS = '{0}-{1}-{2}'.format(start_date_dt.year, start_date_dt.month, start_date_dt.day)
            startDateS = "'" + startDateS + "'"
            kc_dml_statement = "SELECT kc FROM " + dataset_id + ' WHERE date = ' + startDateS
            print('Getting kc from DB')
            kc_response = dbwriter.run_dml(kc_dml_statement, project=project)
            kc = 0
            for e in kc_response:
                kc = e["kc"]
                print(kc)
            print(' Done. Got kc: ' + str(kc))
            etc = kc * eto
            print()
            print('etc: ' + str(etc))
            print('Updating etc and eto in DB')
            etc_dml_statement = "UPDATE " + dataset_id + ' SET etc = ' + str(etc) + ', eto = ' + str(
                eto) + ' WHERE date = ' + startDateS
            dbwriter.run_dml(etc_dml_statement, project=project)
            start_date_dt = start_date_dt + timedelta(days=1)
            print()

            if start_date_dt == end_date_dt + timedelta(1):
                print('Start date = End date')
                break


def update_kcs(project, field_name, logger_name, list_kcs, start_date, end_date):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)

    # Setup dataset_id from passed in field and logger_id parameters
    dataset_id = project + '.' + field_name + '.' + logger_name
    dataset_id = "`" + dataset_id + "`"

    # Turn date string into datetimes
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date_dt - start_date_dt + timedelta(days=1)

    # check that length of listETos = endDate - startDate
    if len(list_kcs) == delta.days:
        for kc in list_kcs:
            # startDateS = datetime.strftime(start_date_dt, "%Y-%m-%d")
            startDateS = '{0}-{1}-{2}'.format(start_date_dt.year, start_date_dt.month, start_date_dt.day)
            startDateS = "'" + startDateS + "'"

            print('Updating kc DB for date: ' + startDateS)
            kc_dml_statement = "UPDATE " + dataset_id + ' SET kc = ' + str(kc) + ' WHERE date = ' + startDateS
            dbwriter.run_dml(kc_dml_statement, project=project)
            start_date_dt = start_date_dt + timedelta(days=1)
            print()

            if start_date_dt == end_date_dt + timedelta(1):
                print('Start date = End date')
                break


def update_values_for_date_range(project, field_name, logger_name, value_name, values_list, start_date, end_date):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)

    # Setup dataset_id from passed in field and logger_id parameters
    dataset_id = project + '.' + field_name + '.' + logger_name
    dataset_id = "`" + dataset_id + "`"

    # Turn date string into datetimes
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    delta = end_date_dt - start_date_dt + timedelta(days=1)

    # check that length of listETos = endDate - startDate
    if len(values_list) == delta.days:
        for val in values_list:
            # start_date_s = datetime.strftime(start_date_dt, "%Y-%m-%d")
            start_date_s = '{0}-{1}-{2}'.format(start_date_dt.year, start_date_dt.month, start_date_dt.day)
            start_date_s = "'" + start_date_s + "'"

            print('Updating val DB for date: ' + start_date_s)
            dml = "UPDATE " + dataset_id + ' SET ' + str(value_name) + ' = ' + str(
                val) + ' WHERE date = ' + start_date_s
            dbwriter.run_dml(dml, project=project)
            start_date_dt = start_date_dt + timedelta(days=1)
            print()

            if start_date_dt == end_date_dt + timedelta(1):
                print('Start date = End date')
                break
    else:
        print('Values and dates dont match')
        print('Values: ' + str(len(values_list)))
        print("Days: " + str(delta.days))


def copy_values_from_table_to_table():
    table = '`stomato-permanents.Riley_Chaney_Farms16.RC-16-E`'
    dml = f"SELECT date, daily_inches, daily_hours, daily_switch, canopy_temperature, ambient_temperature, vpd, psi, sdd, rh, lowest_ambient_temperature FROM {table} WHERE date > '2024-01-01' ORDER BY date ASC"
    result = dbwriter.run_dml(dml, project='stomato')
    for e in result:
        date = e["date"]
        if date is not None:
            inches = e["daily_inches"]
            hours = e["daily_hours"]
            switch = e["daily_switch"]

            dml = "UPDATE `stomato-permanents.Riley_Chaney_Farms16.RC-16-W` " \
                  + " SET daily_switch = " + str(switch) + \
                  ", daily_hours = " + str(hours) + \
                  ", daily_inches = " + str(inches) + \
                  " WHERE date = '" + str(date) + "'"
            dbwriter.run_dml(dml, project='stomato')
    print('done')


def copy_gdd_values_from_temp_table_to_table(project, field, original_table, temp_table):
    dml_statement = "MERGE `" + project + "." + field + "." + original_table + "` T " \
                    + "USING `" + project + "." + field + "." + temp_table + "` S " \
                    + "ON T.date = S.date " \
                    + "WHEN MATCHED THEN " \
                    + "UPDATE SET " \
                      "lowest_ambient_temperature = s.lowest_ambient_temperature, " \
                      "gdd = s.gdd," \
                      "crop_stage = s.crop_stage, " \
                      "id = s.id, " \
                      "planting_date = s.planting_date"

    result = dbwriter.run_dml(dml_statement, project=project)
    print('done')


def copy_vp4_vals_from_table_to_table(project, fieldName, source, target, date):
    fieldName = dbwriter.remove_unwanted_chars_for_db_dataset(fieldName)
    # print(date)
    dml = f"SELECT date, ambient_temperature, rh, vpd FROM `{project}.{fieldName}.{source}` ORDER BY date ASC"
    print(dml)
    result = dbwriter.run_dml(dml, project=project)
    for e in result:
        # print(e)
        dbDate = e["date"]
        # print(dbDate)
        if str(dbDate) == date:
            ambient_temperature = e["ambient_temperature"]
            rh = e["rh"]
            vpd = e["vpd"]

            dml = "UPDATE `" + project + "." + fieldName + "." + target + "`" \
                  + " SET ambient_temperature = " + str(ambient_temperature) + \
                  ", rh = " + str(rh) + \
                  ", vpd = " + str(vpd) + \
                  " WHERE date = '" + str(date) + "'"
            print(dml)
            dbwriter.run_dml(dml, project=project)
    print('done updating VP4 Data')


def merge_table_into_table_updating_some_values(main_dataset_id: str, merge_dataset_id: str):
    #
    # dml = "MERGE " + dataset_id + " T " \
    #                         + "USING " + et_id + " S " \
    #                         + "ON (T.date = S.date AND T.eto IS NULL)  " \
    #                         + "WHEN MATCHED THEN " \
    #                         + "UPDATE SET eto = s.eto, etc = s.eto * t.kc, et_hours = ROUND(" + str(
    #             et_hours_pending_etc_mult) + " * s.eto * t.kc)"

    # date, daily_inches, daily_hours, daily_switch, canopy_temperature, ambient_temperature, vpd, psi, sdd, rh, lowest_ambient_temperature

    dml = "MERGE `" + main_dataset_id + "` T " \
          + "USING `" + merge_dataset_id + "` S " \
          + "ON (t.date = s.date AND T.date > '2024-01-01') " \
          + "WHEN MATCHED THEN " \
          + "UPDATE SET " \
            "daily_inches = s.daily_inches, " \
            "daily_hours = s.daily_hours," \
            "daily_switch = s.daily_switch, " \
            "canopy_temperature = s.canopy_temperature, " \
            "ambient_temperature = s.ambient_temperature, " \
            "vpd = s.vpd, " \
            "psi = s.psi, " \
            "sdd = s.sdd, " \
            "rh = s.rh, " \
            "lowest_ambient_temperature = s.lowest_ambient_temperature"
    result = dbwriter.run_dml(dml, project='stomato-permanents')


# merge_table_into_table_updating_some_values('stomato-permanents.Barrios_Farms22.BF-22-NW2', 'stomato-permanents.Barrios_Farms22.BF-22-NW')


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)


def keep_db_days(field_name, startDate, endDate):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)

    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field_name:
                for log in f.loggers:
                    logger_id = log.id
                    # Setup dataset_id from passed in field and logger_id parameters
                    project = dbwriter.get_db_project(log.crop_type)
                    dataset_id = project + '.' + field_name + '.' + logger_id
                    dataset_id = "`" + dataset_id + "`"

                    # Keep dates specified by user Start and End Date
                    val_dml_statement = "CREATE OR REPLACE TABLE " + dataset_id + " AS " + "SELECT * FROM " + dataset_id \
                                        + "WHERE date BETWEEN DATE('" + startDate + "') AND DATE('" + endDate + "') "
                    print(val_dml_statement)
                    dbwriter.run_dml(val_dml_statement, project=project)


def delete_and_update_db(grower, field, db_end_date, start_date):
    keep_db_days(field, '2021-01-01', db_end_date)
    Decagon.get_previous_data_field(grower, field, start_date, write_to_sheet=True, write_to_db=True)


def delete_and_update_db_grower(grower, db_end_date, start_date):
    growers = Decagon.open_pickle()
    for g in growers:
        if g.name == grower:
            print("Found grower, these are his fields: ")
            for f in g.fields:
                field = f.name
                print(f.name)
                keep_db_days(field, '2021-01-01', db_end_date)
                # Todo pass in field and grower objects to parameters
                Decagon.get_previous_data_field(grower, field, start_date, write_to_sheet=True, write_to_db=True)


def remove_psi_specific(project, field_name, logger_name, start_date, end_date):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    dataset_id = project + '.' + field_name + '.' + logger_name
    dataset_id = "`" + dataset_id + "`"
    val_dml_statement = "Update " + dataset_id + " Set " + " psi = null, sdd = null, canopy_temperature = null " \
                        + "WHERE date BETWEEN DATE('" + start_date + "') AND DATE('" + end_date + "') "
    # print(val_dml_statement)
    print("Removing PSI from field pages")
    dbwriter.run_dml(val_dml_statement, project=project)


def remove_psi(field_name_pickle, start_date, end_date, portal_year):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name_pickle)
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field_name_pickle:
                grower_name = dbwriter.remove_unwanted_chars_for_db_dataset(g.name)
                print("Removing PSI from field pages")
                for log in f.loggers:
                    logger_name = log.name
                    project = dbwriter.get_db_project(log.crop_type)
                    dataset_id = project + '.' + field_name + '.' + logger_name
                    dataset_id = "`" + dataset_id + "`"
                    val_dml_statement = "Update " + dataset_id + " Set " + " psi = null, sdd = null, canopy_temperature = null " \
                                        + "WHERE date BETWEEN DATE('" + start_date + "') AND DATE('" + end_date + "') "
                    # print(val_dml_statement)
                    dbwriter.run_dml(val_dml_statement, project=project)
                print("Removing PSI from portal")
                dataset_id_portal_field_averages = 'growers-' + portal_year + '.' + grower_name + '.field_averages'
                dataset_id_portal_field_averages = "`" + dataset_id_portal_field_averages + "`"
                dataset_id_portal_loggers = 'growers-' + portal_year + '.' + grower_name + '.loggers'
                dataset_id_portal_loggers = "`" + dataset_id_portal_loggers + "`"

                val_dml_statement = "Update " + dataset_id_portal_field_averages + " Set" + " si_num = null, si_desc = null " \
                                    + "Where field = '" + f.nickname + "'"
                print("Removing PSI from field_averages")
                dbwriter.run_dml(val_dml_statement, project='growers-' + portal_year)
                val_dml_statement = "Update " + dataset_id_portal_loggers + " Set" + " si_num = null, si_desc = null " \
                                    + "Where field = '" + f.nickname + "'"
                print("Removing PSI from loggers")
                dbwriter.run_dml(val_dml_statement, project='growers-' + portal_year)


def update_missing_et_data(logger):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(logger.field.name)
    project = dbwriter.get_db_project(logger.crop_type)
    dataset_id = project + "." + field_name + "." + logger.name
    et_id = "stomato-info.ET." + str(logger.field.cimis_station)

    if isinstance(logger.irrigation_set_acres, str):
        acres = float(logger.irrigation_set_acres.replace(',', ''))
    elif isinstance(logger.irrigation_set_acres, int):
        acres = float(logger.irrigation_set_acres)
    elif isinstance(logger.irrigation_set_acres, float):
        acres = logger.irrigation_set_acres
    else:
        acres = 0
    if isinstance(logger.gpm, str):
        gpm = float(logger.gpm.replace(',', ''))
    elif isinstance(logger.gpm, int):
        gpm = float(logger.gpm)
    elif isinstance(logger.gpm, float):
        gpm = logger.gpm
    else:
        gpm = 0

    if gpm != 0:
        et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))

        print("Updating eto data for table: " + dataset_id)
        print(" from")
        print("ET table: " + et_id)
        # Use when eto is NULL
        # dml_statement = "MERGE `" + dataset_id + "` T " \
        #                 + "USING `" + et_id + "` S " \
        #                 + "ON T.date = S.date " \
        #                 + "WHEN MATCHED AND t.eto is NULL THEN " \
        #                 + "UPDATE SET eto = s.eto, etc = s.eto * t.kc, et_hours = ROUND(" + str(
        #     et_hours_pending_etc_mult) + " * s.eto * t.kc)"

        # Use when eto is not NULL

        dml_statement = "MERGE `" + dataset_id + "` T " \
                        + "USING `" + et_id + "` S " \
                        + "ON (T.date = S.date AND T.eto IS NULL)  " \
                        + "WHEN MATCHED THEN " \
                        + "UPDATE SET eto = s.eto, etc = s.eto * t.kc, et_hours = ROUND(" + str(
            et_hours_pending_etc_mult) + " * s.eto * t.kc)"
        # print(dml_statement)
        dbwriter.run_dml(dml_statement, project=project)


def update_missing_et_hours(logger):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(logger.field.name)
    project = dbwriter.get_db_project(logger.crop_type)
    dataset_id = project + "." + field_name + "." + logger.name

    if isinstance(logger.irrigation_set_acres, str):
        acres = float(logger.irrigation_set_acres.replace(',', ''))
    elif isinstance(logger.irrigation_set_acres, int):
        acres = float(logger.irrigation_set_acres)
    else:
        acres = 0
    if isinstance(logger.gpm, str):
        gpm = float(logger.gpm.replace(',', ''))
    elif isinstance(logger.gpm, int):
        gpm = float(logger.gpm)
    else:
        gpm = 0

    if gpm != 0:
        et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))

        print("Updating et_hours data for table: " + dataset_id)

        dml_statement = "UPDATE `" + dataset_id + "` SET et_hours = ROUND(" + str(
            et_hours_pending_etc_mult) + " * eto * kc) " + " WHERE et_hours is NULL"

        dbwriter.run_dml(dml_statement, project=project)
    else:
        print('Cannot calculate ET Hours. GPM is: ' + str(gpm))


def add_column_to_db_specific_field(field_name: str, column_name_and_type_dict: dict, project='stomato'):
    """

    :param field_name:
    :param column_name_and_type_dict: Dict[column_name] = column_type
    :return:
    """
    fieldName = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field_name:
                for log in f.loggers:
                    logger_id = log.id
                    for val in column_name_and_type_dict:
                        try:
                            dbwriter.add_new_column_to_table(fieldName, logger_id, val, column_name_and_type_dict[val],
                                                             project=project)
                        except:
                            print("Exception")


def add_column_to_db_logger_table(column_name_and_type_dict: dict, project='stomato'):
    """

    :param column_name_and_type_dict: Dict[column_name] = column_type
    :return:
    """
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            fieldName = dbwriter.remove_unwanted_chars_for_db_dataset(f.name)
            print(fieldName)
            for log in f.loggers:
                logger_id = log.id
                for val in column_name_and_type_dict:
                    try:
                        dbwriter.add_new_column_to_table(fieldName, logger_id, val, column_name_and_type_dict[val],
                                                         project=project)
                    except:
                        print("Field:" + fieldName + " already has ET Hours")


def add_column_to_db_grower_portal_logger_table(column_name_and_type_dict: dict, project=f'growers-{DIRECTORY_YEAR}'):
    """

    :param column_name_and_type_dict: Dict[column_name] = column_type
    :return:
    """
    growers = Decagon.open_pickle()
    for g in growers:
        # if g.name == 'Surjit Chahal':
        dataset_name = dbwriter.remove_unwanted_chars_for_db_dataset(g.name)
        table_name = 'loggers'

        print(dataset_name, table_name)

        for val in column_name_and_type_dict:
            try:
                dbwriter.add_new_column_to_table(dataset_name, table_name, val, column_name_and_type_dict[val],
                                                 project=project)
            except:
                print(f'Error when trying to add column to table {dataset_name}.{table_name}')


# db_column = {
#     'location': 'STRING'
# }
# add_column_to_db_grower_portal_logger_table(db_column)

def remove_duplicate_rows(project, dataset, table, rowName):
    dmlStatement = "create or replace table `" + str(project) + "." + str(dataset) + "." + str(table) + "` as ( \
    select * except(row_num) from (SELECT *, ROW_NUMBER() OVER (PARTITION BY " + str(rowName) + " ORDER BY " + str(
        rowName) + " desc) row_num \
    FROM `" + project + "." + str(dataset) + "." + str(table) + "`) t \
    WHERE row_num=1)"

    dbwriter.run_dml(dmlStatement, project=project)


def removeDuplicateET():
    cimisStations = Decagon.get_all_current_cimis_stations()
    project = 'stomato-info'
    dataset = "ET"
    rowName = "date"
    for station in cimisStations:
        print("Removing dupes in station " + str(station))
        remove_duplicate_rows(project, dataset, station, rowName)


def remove_double_data_late_hours(field_name, logger_name):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    growers = Decagon.open_pickle()
    dataset_id = "stomato." + field_name + "." + logger_name
    for g in growers:
        for f in g.fields:
            if f.name == field_name:
                for log in f.loggers:
                    if logger_name == log.name:
                        project = dbwriter.get_db_project(log.crop_type)
                        dml_statement = "DELETE `" + project + "." + dataset_id + "` where time = '11:00 PM' or time = '10:00 PM'"
                        dbwriter.run_dml(dml_statement, project=project)


def update_kc_values_with_a_max_including_etc_etc_hours(logger):
    if isinstance(logger.irrigation_set_acres, str):
        acres = float(logger.irrigation_set_acres.replace(',', ''))
    elif isinstance(logger.irrigation_set_acres, int):
        acres = float(logger.irrigation_set_acres)
    else:
        acres = 0
    if isinstance(logger.gpm, str):
        gpm = float(logger.gpm.replace(',', ''))
    elif isinstance(logger.gpm, int):
        gpm = float(logger.gpm)
    else:
        gpm = 0

    if gpm != 0:
        et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))

        field_name = dbwriter.remove_unwanted_chars_for_db_dataset(logger.field.name)
        project = dbwriter.get_db_project(logger.crop_type)
        dataset_id = project + "." + field_name + "." + logger.id
        print("Updating field: {0} and logger {1}".format(field_name, logger.id))
        dmlStatement = "UPDATE `" + dataset_id + "` as t \
        SET t.kc = 1.1, t.etc = t.eto * 1.1, et_hours = ROUND(" + str(et_hours_pending_etc_mult) + " * eto * kc) \
        WHERE t.kc > 1.1"
        dbwriter.run_dml(dmlStatement, project=project)


def update_kc_values_with_a_max(logger):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(logger.field.name)
    project = dbwriter.get_db_project(logger.crop_type)
    dataset_id = project + "." + field_name + "." + logger.name
    print("Updating field: {0} and logger {1}".format(field_name, logger.name))
    dml_statement = "UPDATE `" + dataset_id + "` as t \
    SET t.kc = 1.1, t.etc = t.eto * 1.1 \
    WHERE t.kc > 1.1"
    dbwriter.run_dml(dml_statement, project=project)


def delete_last_day(project: str, field_name: str, logger_name: str, day=''):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    if day == '':
        today = date.today() - timedelta(days=1)
    else:
        today = day
    dataset_id = project + "." + field_name + "." + logger_name
    dmlStatement = "Delete `" + dataset_id + "` where date = '" + str(today) + "'"
    print(dmlStatement)
    try:
        dbwriter.run_dml(dmlStatement, project=project)
    except:
        print("Field Not Found. Please Try With a Different Name")


def delete_et_day(field, date, date2):
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field:
                for l in f.loggers:
                    logger_name = l.name
                    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field)
                    project = dbwriter.get_db_project(l.crop_type)
                    dataset_id = project + "." + field_name + "." + logger_name
                    dmlStatement = "Update `" + dataset_id + "` Set eto = null, etc = null, et_hours = null where date between " + \
                                   "date('" + date + "') and date('" + date2 + "') "
                    print(dmlStatement)
                    dbwriter.run_dml(dmlStatement, project=project)
                    # update_missing_et_data(l)


def delete_negative_et(field, date, kc):
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == field:
                for l in f.loggers:
                    logger_name = l.name
                    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field)
                    project = dbwriter.get_db_project(l.crop_type)
                    dataset_id = project + "." + field_name + "." + logger_name
                    dmlStatement = "Update `" + dataset_id + "` Set eto = null, etc = null, et_hours = null, kc = " + \
                                   kc + " where date = '" + date + "'"
                    print(dmlStatement)
                    dbwriter.run_dml(dmlStatement, project=project)
                    # update_missing_et_data(l)


def get_average_psi_during_growth():
    start_cut = 8
    end_cut = 21
    psi_averages = {}
    datasets = dbwriter.get_datasets()
    for d in datasets[0]:
        if d.dataset_id == 'ET':
            continue
        tables = dbwriter.get_tables(d.dataset_id)
        dataset_psis = []
        for t in tables:
            psi_avg = average_table_psi(d.dataset_id, t.table_id, start_cut, end_cut)
            dataset_psis.append(psi_avg)
        dataset_psi_avg = numpy.mean(dataset_psis)
        psi_averages[d.dataset_id] = dataset_psi_avg
        print()
        print(d.dataset_id, 'psi average:')
        print(dataset_psi_avg)
        print()

    pprint.pprint(psi_averages)


def all_tables_analysis(crop=None, project='stomato'):
    all_dataset_ats = []
    all_dataset_vpds = []
    all_dataset_rhs = []
    all_dataset_psi = []

    start_cut = 0
    end_cut = 0

    field_count = 0
    logger_count = 0

    if crop is not None:
        fields_with_crop = get_fields_for_crop(crop)
        for field in fields_with_crop:
            field_count = field_count + 1
            for logger in field.loggers:
                logger_count = logger_count + 1
                field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
                psi_avg = average_table_psi(project, field_name, logger.name, start_cut, end_cut)
                at_avg, vpd_avg, rh_avg = table_analysis(project, field_name, logger.name)
                all_dataset_ats.append(at_avg)
                all_dataset_vpds.append(vpd_avg)
                all_dataset_rhs.append(rh_avg)
                all_dataset_psi.append(psi_avg)
            # time.sleep(5)
    else:
        datasets = dbwriter.get_datasets()

        for d in datasets[0]:
            if d.dataset_id == 'ET':
                continue
            field_count = field_count + 1
            tables = dbwriter.get_tables(d.dataset_id)
            for t in tables:
                if t.table_id == 'Lat Long Trial':
                    continue
                logger_count = logger_count + 1
                at_avg, vpd_avg, rh_avg = table_analysis(project, d.dataset_id, t.table_id)
                psi_avg = average_table_psi(d.dataset_id, t.table_id, start_cut, end_cut)
                all_dataset_ats.append(at_avg)
                all_dataset_vpds.append(vpd_avg)
                all_dataset_rhs.append(rh_avg)
                all_dataset_psi.append(psi_avg)

    all_dataset_at_avg = numpy.mean(all_dataset_ats)
    all_dataset_vpd_avg = numpy.mean(all_dataset_vpds)
    all_dataset_rh_avg = numpy.mean(all_dataset_rhs)
    all_dataset_psi_avg = numpy.mean(all_dataset_psi)
    print()
    print('Total fields processed: ', field_count)
    print('Total loggers processed: ', logger_count)
    if crop is not None:
        print('Only showing fields with crop: ', crop)
    print('All Ambient Temp Avg: {}'.format(all_dataset_at_avg))
    print('All VPD Avg: {}'.format(all_dataset_vpd_avg))
    print('All RH Avg: {}'.format(all_dataset_rh_avg))
    print('All PSI Avg: {}'.format(all_dataset_psi_avg))


def get_fields_for_crop(crop):
    count = 0
    fields_with_crop = []
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.loggers[0].crop_type == crop:
                fields_with_crop.append(f)
                count = count + 1
    print('{} total fields for {} found'.format(count, crop))
    return fields_with_crop


def average_table_psi(project, field_name, logger_name, start_cut=0, end_cut=0):
    print('Grabbing psi average: ' + str(field_name) + ' ' + str(logger_name))
    dml = "SELECT date, psi FROM " \
          + "`" + project + "." + str(field_name) + "." + str(logger_name) + "`" \
          + " WHERE psi is not NULL ORDER BY date ASC"
    result = dbwriter.run_dml(dml, project=project)
    date_list = []
    psi_list = []
    for e in result:
        date = e["date"]
        psi = e['psi']

        date_list.append(date)
        psi_list.append(psi)

    if start_cut != 0 and end_cut != 0:
        new_date_list = date_list[start_cut:-end_cut]
        new_psi_list = psi_list[start_cut:-end_cut]
    else:
        new_date_list = date_list
        new_psi_list = psi_list
    psi_avg = numpy.mean(new_psi_list)
    # print('\t Logger:',logger)
    # print('\t Psi avg:',psi_avg)
    print('Analysing: {} - {}'.format(field_name, logger_name))
    print('PSI Avg: ', psi_avg)

    return psi_avg


def table_analysis(project: str, field: str, logger: str):
    print('Grabbing at, vpd and rh averages: ' + str(field) + ' ' + str(logger))
    dml = "SELECT date, ambient_temperature, vpd, rh, psi FROM " \
          + "`stomato." + str(field) + "." + str(logger) + "`" \
          + " WHERE ambient_temperature is not NULL and vpd is not NULL and rh is not NULL ORDER BY date ASC"
    result = dbwriter.run_dml(dml, project=project)
    date_list = []
    at_list = []
    vpd_list = []
    rh_list = []
    # psi_list = []
    for e in result:
        date = e["date"]
        at = e['ambient_temperature']
        vpd = e['vpd']
        rh = e['rh']
        # psi = e['psi']

        date_list.append(date)
        at_list.append(at)
        vpd_list.append(vpd)
        rh_list.append(rh)
        # psi_list.append(psi)

    at_avg = numpy.mean(at_list)
    vpd_avg = numpy.mean(vpd_list)
    rh_avg = numpy.mean(rh_list)
    # psi_avg = numpy.mean(psi_list)

    print('Analysing: {} - {}'.format(field, logger))
    print('Ambient Temp Avg: ', at_avg)
    print('VPD Avg: ', vpd_avg)
    print('RH Avg: ', rh_avg)
    # print('PSI Avg: ', psi_avg)
    print()
    # print('\t Logger:',logger)
    # print('\t Psi avg:',psi_avg)
    return at_avg, vpd_avg, rh_avg  ##, psi_avg


def get_date_range(year, start_day=1, num_days=365, just_date: bool = False):
    """ Helper function to generate date range for a given year. """
    start_date = datetime(year, 1, 1)
    if just_date:
        start_date = date(year, 1, 1)
    return [start_date + timedelta(days=d) for d in range(start_day - 1, num_days)]


def get_a_logger_for_field(fieldName):
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.name == fieldName:
                for l in f.loggers:
                    return l


def is_leap_year_date(date_obj):
    return date_obj.month == 2 and date_obj.day == 29 and calendar.isleap(date_obj.year)


def setup_irrigation_scheduling_db(etStation: str, fieldName: str):
    """
    Sets up the Irrigation Scheduling Table for a Field in the DB
    :param etStation: ET Statin Number
    :param fieldName: Field Name
    """
    print("Setting up irrigation scheduling DB table")
    today = datetime.now()
    previous_year = today.year - 1
    start_date, end_date = datetime(previous_year, 1, 1), datetime(previous_year, 12, 31)

    # Save Historical ET onto Dict
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(fieldName)
    historical_et = returnHistoricalETDict(etStation, start_date, end_date)

    # Load Logger and KC values
    dates_list = get_date_range(previous_year)
    logger = get_a_logger_for_field(fieldName)
    kc_values = logger.get_kc({"dates": dates_list})

    csv_data = {
        "current_date": get_date_range(today.year, just_date=True),
        "historical_eto": [],
        "kc": [],
        "historical_etc": [],
        "historical_hours": []
    }

    for index, date in enumerate(dates_list):
        date_only = date.date()  # Convert from (2024, 1, 1, 0, 0) to (2024, 1 ,1)
        if not is_leap_year_date(date_only):
            eto = historical_et[date_only]
            # if it is a leap year date 2/29 then itll use prev days eto
        kc = kc_values['kc'][index]
        if eto is None:
            etc = None
            irrigation_hours = None
        else:
            etc = eto * kc
            irrigation_hours = round((etc * 449 * float(logger.irrigation_set_acres)) / (float(logger.gpm) * 0.85), 0)

        csv_data["historical_eto"].append(eto)
        csv_data["kc"].append(kc)
        csv_data["historical_etc"].append(etc)
        csv_data["historical_hours"].append(irrigation_hours)

        # Write CSV and update DB
    Decagon.update_irr_scheduling(field_name + '_Irr_Scheduling', field_name, csv_data, overwrite=True,
                                  logger=logger)


def returnHistoricalETDict(etStation: str, start_date: date, end_date: date) -> dict:
    """
    Returns a dictionary with the historical average ET date and value
    :param etStation: ET station
    :param start_date: Start Date
    :param end_date: End Date
    :return: Dictionary of historical average ET date and value
    """
    # Returns last year dates and averages of Historical ET into a Dictionary
    project = 'stomato-info'
    et_id = project + ".Historical_ET." + etStation
    last_year = "Year_" + str(start_date.year)
    dml_statement = "select " + last_year + ", Average from " + et_id + \
                    " where " + last_year + " between date('" + str(start_date) + \
                    "') and date('" + str(end_date) + "') order by " + last_year
    etValue = dbwriter.return_query_dict(dml_statement, last_year, 'Average', project)
    return etValue


def move_logger_db_info(project: str, field_name: str, new_logger_name: str, old_logger_name: str):
    """
    Use if copying data from an old logger to a new logger table
    :param project: Big Query Project
    :param field_name: Field Name
    :param new_logger_name: New Logger Name
    :param old_logger_name: Old Logger Name
    """
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    new_logger_dataset = project + "." + field_name + "." + new_logger_name
    old_logger_dataset = project + "." + field_name + "." + old_logger_name

    print("Updating data for table: " + new_logger_dataset)
    print(" from")
    print("old logger table: " + old_logger_dataset)

    # Use when eto is not NULL
    dml_statement = "MERGE `" + new_logger_dataset + "` T " \
                    + "USING `" + old_logger_dataset + "` S " \
                    + "ON T.date = S.date " \
                    + "WHEN NOT MATCHED THEN " \
                    + "INSERT (logger_id, date, time, canopy_temperature, ambient_temperature, vpd, vwc_1, vwc_2, vwc_3, " \
                      "field_capacity, wilting_point,  daily_gallons, daily_switch, daily_hours, daily_pressure, " \
                      "daily_inches, psi, psi_threshold, psi_critical, sdd, rh, eto, kc, etc, et_hours, " \
                      "phase1_adjustment, phase1_adjusted, phase2_adjustment, phase2_adjusted, phase3_adjustment, " \
                      "phase3_adjusted, vwc_1_ec, vwc_2_ec, vwc_3_ec) " \
                    + "Values (S.logger_id, S.date,    S.time, S.canopy_temperature, S.ambient_temperature, S.vpd, S.vwc_1, S.vwc_2, S.vwc_3, " \
                      "S.field_capacity, S.wilting_point,  S.daily_gallons, S.daily_switch, S.daily_hours, S.daily_pressure, " \
                      "S.daily_inches, S.psi, S.psi_threshold, S.psi_critical, S.sdd, S.rh, S.eto, S.kc, S.etc, S.et_hours, " \
                      "S.phase1_adjustment, S.phase1_adjusted, S.phase2_adjustment, S.phase2_adjusted, S.phase3_adjustment, " \
                      "S.phase3_adjusted, S.vwc_1_ec, S.vwc_2_ec, S.vwc_3_ec)"

    dbwriter.run_dml(dml_statement, project=project)

    print("Changing logger_id to match new one in old data")
    dml_statement = (
            "Update `" + new_logger_dataset + "`  set logger_id = '" + new_logger_name + "' where logger_id = '" + old_logger_name + "'")
    # print(dml_statement)
    dbwriter.run_dml(dml_statement, project=project)
    print("Clean up nulls")
    dml_statement = ("Delete `" + new_logger_dataset + "` where date is NULL")
    dbwriter.run_dml(dml_statement, project=project)


def update_field_et(fieldName):
    growers = Decagon.open_pickle()
    for g in growers:
        # if g.name == fieldName:
        for f in g.fields:
            if f.name == fieldName:
                for l in f.loggers:
                    # print("doing et")
                    update_missing_et_data(l)


def update_all_field_et():
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            if f.active:
                for l in f.loggers:
                    try:
                        update_missing_et_data(l)
                    except:
                        print("Couldn't update et for field: ", f.name,
                              "\n logger: ", l.name)


def update_logger_et(fieldName, loggerName):
    growers = Decagon.open_pickle()
    for g in growers:
        # if g.name == fieldName:
        for f in g.fields:
            if f.name == fieldName:
                for l in f.loggers:
                    if l.name == loggerName:
                        update_missing_et_data(l)


def delete_where_eto_is_null(logger, date1, date2):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(logger.field.name)
    project = dbwriter.get_db_project(logger.crop_type)
    dataset = project + "." + field_name + "." + logger.name
    dml_statement = ("Delete `" + dataset + "` where date between " + "date('" + date1 + "') and date('" + date2 + "') "
                     + 'and eto is null')
    dbwriter.run_dml(dml_statement, project=project)
    # print(dml_statement)


def update_fc_wp(project, field_name_pickle, logger_name, fc, wp):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name_pickle)
    dataset = project + "." + field_name + "." + logger_name
    dml_statement = (
            "Update `" + dataset + "`  set field_capacity = " + str(fc) + ", wilting_point = " + str(wp) +
            " where date is not null")
    print(dml_statement)
    dbwriter.run_dml(dml_statement, project=project)
    print('Done updating data studio')
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            if field.name == field_name_pickle:
                for logger in field.loggers:
                    if logger.name == logger_name:
                        logger.soil.set_field_capacity_wilting_point(fc, wp)
                        print("Updated FC and WP in pickle")
    Decagon.write_pickle(growers)


def update_portal_image(grower_name, field_name, image_url, portal_year, update_db=True):
    client = DBWriter.grab_bq_client(dbwriter, 'growers-' + portal_year)
    # field.preview_url = imageUrl
    # print("Updated image preview in pickle for field: " + field.name)
    if update_db:
        grower_name_db = DBWriter.remove_unwanted_chars_for_db_dataset(dbwriter, grower_name)
        field_averages_portal_dataset_id = client.project + "." + grower_name_db + '.field_averages'
        dml_statement = (
                "Update `" + field_averages_portal_dataset_id + "`  set preview = '" + image_url +
                "' where field = '" + field_name + "'")
        print(dml_statement)
        dbwriter.run_dml(dml_statement, project=client.project)
        print('Done updating portal')
    print('Done updating pickle')


def remove_duplicate_data(logger):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(logger.field.name)
    project = dbwriter.get_db_project(logger.crop_type)
    dataset = field_name + "." + logger.name
    dml_statement = ("CREATE OR REPLACE TABLE `" + project + "." + dataset +
                     "`AS SELECT * from ( SELECT *, ROW_NUMBER() OVER (PARTITION BY date order by ambient_temperature DESC) row_number "
                     "FROM `" + dataset + "`) WHERE row_number = 1 ")
    # print(dml_statement)
    print("Removing Duplicate data for field: ", logger.field.name, "\n \t For logger: ", logger.name)
    # After removing duplicates a new column gets added, only way to bypass this is by
    # specifying which columns to select in from clause
    try:
        dbwriter.run_dml(dml_statement, project=project)
        drop_column(project, dataset, "row_number")
    except exceptions.BadRequest as err:
        print("Bad Request Error: ", err)


def drop_column(project, dataset, column):
    print("\t Dropping extra column")
    dml_statement = ("alter table " + "`" + project + "." + dataset + "` drop column " + column)
    # print(dml_statement)
    try:
        dbwriter.run_dml(dml_statement, project=project)
    except exceptions.BadRequest as err:
        print("Bad Request Error: ", err)


def fix_weather_db(field):
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
    project = dbwriter.get_db_project(field.loggers[0].crop_type)
    dataset = field_name + ".weather_forecast`"
    today = datetime.today() - timedelta(days=1)
    day = str(today.day)
    month = str(today.month)
    year = str(today.year)
    date = year + "-" + month + "-" + day
    # print(date)

    dml_statement = (
            "Update `" + project + "." + dataset + " as t" + " Set t.order = 99 where t.date < " + "'" + date + "'")
    dbwriter.run_dml(dml_statement, project=project)
    print(f"Finished fixing weather for {field.name}")
    # print(dml_statement)


def returnHistoricalData(etStation):
    print("Grabbing previous years historical data")

    et_id_old = "stomato.ET." + str(etStation)
    etValue = []
    startDate = '2022-01-01'
    endDate = '2022-12-31'
    dml_statement = "select date, eto from " + et_id_old + \
                    " as t where t.date between date('" + str(startDate) + \
                    "') and date('" + str(endDate) + "') order by date asc"
    etValue2022 = dbwriter.return_query_dict(dml_statement, 'date', 'eto', 'stomato')
    etValue.append(etValue2022)

    et_id = "stomato.Historical_ET." + str(etStation)
    startDate = '2021-01-01'
    endDate = '2021-12-31'
    dml_statement = "select Year1, Year1ET from " + et_id + \
                    " where Year1 between date('" + str(startDate) + \
                    "') and date('" + str(endDate) + "') order by Year1"
    etValue2021 = dbwriter.return_query_dict(dml_statement, 'Year1', 'Year1ET', 'stomato')
    etValue.append(etValue2021)

    startDate = '2020-01-01'
    endDate = '2020-12-31'
    dml_statement = "select Year2, Year2ET from " + et_id + \
                    " where Year2 between date('" + str(startDate) + \
                    "') and date('" + str(endDate) + "') order by Year2"
    etValue2020 = dbwriter.return_query_dict(dml_statement, 'Year2', 'Year2ET', 'stomato')
    etValue.append(etValue2020)

    startDate = '2019-01-01'
    endDate = '2019-12-31'
    dml_statement = "select Year3, Year3ET from " + et_id + \
                    " where Year3 between date('" + str(startDate) + \
                    "') and date('" + str(endDate) + "') order by Year3"
    etValue2019 = dbwriter.return_query_dict(dml_statement, 'Year3', 'Year3ET', 'stomato')
    etValue.append(etValue2019)

    startDate = '2018-01-01'
    endDate = '2018-12-31'
    dml_statement = "select Year4, Year4ET from " + et_id + \
                    " where Year4 between date('" + str(startDate) + \
                    "') and date('" + str(endDate) + "') order by Year4"
    etValue2018 = dbwriter.return_query_dict(dml_statement, 'Year4', 'Year4ET', 'stomato')
    etValue.append(etValue2018)

    # pprint.pprint(etValue[0])
    startDate = '2022-01-01'
    endDate = '2022-12-31'
    start = datetime.strptime(startDate, '%Y-%m-%d').date()
    end = datetime.strptime(endDate, '%Y-%m-%d').date()
    # print(dt)
    # pprint.pprint(etValue[0][start])
    dict2022 = {'Year_2022': [], 'Year_2022_ET': []}
    dict2021 = {'Year_2021': [], 'Year_2021_ET': []}
    dict2020 = {'Year_2020': [], 'Year_2020_ET': []}
    dict2019 = {'Year_2019': [], 'Year_2019_ET': []}
    dict2018 = {'Year_2018': [], 'Year_2018_ET': []}
    dictAverage = {'Average': []}

    for single_date in daterange(start, end + relativedelta(days=1)):
        etValue2022 = etValue[0][single_date]
        dict2022['Year_2022_ET'].append(etValue2022)
        dict2022['Year_2022'].append(single_date)
        single_date = single_date - relativedelta(years=1)
        etValue2021 = etValue[1][single_date]
        dict2021['Year_2021_ET'].append(etValue2021)
        dict2021['Year_2021'].append(single_date)
        single_date = single_date - relativedelta(years=1)
        etValue2020 = etValue[2][single_date]
        dict2020['Year_2020_ET'].append(etValue2020)
        dict2020['Year_2020'].append(single_date)
        single_date = single_date - relativedelta(years=1)
        etValue2019 = etValue[3][single_date]
        dict2019['Year_2019_ET'].append(etValue2019)
        dict2019['Year_2019'].append(single_date)
        single_date = single_date - relativedelta(years=1)
        etValue2018 = etValue[4][single_date]
        dict2018['Year_2018_ET'].append(etValue2018)
        dict2018['Year_2018'].append(single_date)
        average_list = [etValue2021, etValue2020, etValue2019, etValue2018]
        dictAverage['Average'].append(mean(average_list))

    return dict2022, dict2021, dict2020, dict2019, dict2018, dictAverage


def return_cimis_data_in_dict(etStation, startDate, endDate, dateKey, valueKey):
    """

    :param etStation: CIMIS ET station number
    :param startDate: start date of cimis data
    :param endDate: end date of cimis data
    :param dateKey: the name of key that will be containing the ET dates
    :param valueKey: the name of the key that will be containing the ET values
    :return:
    """
    cimis = CIMIS()
    etos = cimis.get_eto(targets=[etStation], start_date=startDate, end_date=endDate)

    try:
        if etos is None:
            print("ETo is none, Issue with API Call")
            return None
        elif len(etos['Data']['Providers'][0]['Records']) == 0:
            print('ETo call is returning blank list for values. Usually indicates station is inactive')
            print(f'Station: {etStation}')
            return None
    except Exception as error:
        print('ERROR in grabbing actual eto values from API return')
        print(error)

    etDict = {dateKey: [],
              valueKey: []}

    if etos['Data']['Providers'][0]['Records']:
        cimis_et_data = etos['Data']['Providers'][0]['Records']
        for single_et_data_point in cimis_et_data:
            # Need to ignore extra day since it only happens once every 4 years
            if single_et_data_point['Date'][-5:] == '02-29':
                continue
            else:
                etDict[dateKey].append(single_et_data_point['Date'])
                etDict[valueKey].append(single_et_data_point['DayEto']['Value'])
    return etDict


def return_if_et_table_found(etStation):
    """

    :param etStation: CIMIS ET station number
    :return: returns True if ET station was found in Historical_ET dataset
    """
    tables = dbwriter.get_tables("Historical_ET", project="stomato-info")
    tableFound = False

    for table in tables:
        # print(table.table_id)
        if table.table_id == str(etStation):
            # print(table.table_id)
            tableFound = True

    return tableFound


def fix_irr_inches_for_all_fields(start_date, end_date):
    """

    :param start_date: date you want fixes to start
    :param end_date: date you want fixes to end
    """
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            if field.active:
                print(f"Fixing Irrigation for Field: {field.name}")
                for logger in field.loggers:
                    print(f"\tWorking on logger {logger.name}")
                    select_irr_hours_for_date(logger, start_date, end_date)


def select_irr_hours_for_date(loggers, start_date, end_date):
    """

    :param loggers: logger object from pickle
    :param start_date: date you want fixes to start
    :param end_date: date  you want fixes to end
    """
    project = dbwriter.get_db_project(loggers.crop_type)
    field_db = dbwriter.remove_unwanted_chars_for_db_dataset(loggers.field.name)
    dataset = f"`{project}.{field_db}.{loggers.name}`"
    dml_statement = "select date, daily_switch from " + dataset + \
                    " where date between date('" + str(start_date) + \
                    "') and date('" + str(end_date) + "')"
    # print(dml_statement)
    daily_switch_dates_dictionary = dbwriter.return_query_dict(dml_statement, 'date', 'daily_switch', project)
    #  Loop through all dates and update inches for each date
    for date_data in daily_switch_dates_dictionary:
        daily_hours = daily_switch_dates_dictionary[date_data] / 60
        update_irrigation_hours_for_date(project, loggers.field.name, loggers.name, daily_hours, str(date_data))


def fill_missing_dates(dates: list[date], start_date: str, end_date: str) -> tuple[list[str], list[int]]:
    filled_dates = []
    missing_indexes = []
    date_format = "%Y-%m-%d"
    start_date = datetime.strptime(start_date, date_format)
    end_date = datetime.strptime(end_date, date_format)
    delta = timedelta(days=1)
    current_date = start_date
    index = 0

    while current_date <= end_date:
        if current_date.month == 2 and current_date.day == 29:
            current_date += delta
            continue
        elif index < len(dates) and dates[index] == current_date.strftime(date_format):
            filled_dates.append(dates[index])
            index += 1
        else:
            filled_dates.append(current_date.strftime(date_format))
            missing_indexes.append(len(filled_dates) - 1)
        current_date += delta

    return filled_dates, missing_indexes


def fill_missing_et(et_list: list, index: list[int]):
    for each_index in index:
        et_list.insert(each_index, '0.00')
    return et_list


def copy_missing_data_to_logger_from_other_logger(field_name: str, logger_destination_name: str,
                                                  logger_source_name: str):
    """
    :param field_name: Name of field
    :param logger_destination_name: Name of logger that is missing data
    :param logger_source_name: Name of logger that has data
    """
    field = ''
    logger_destination = ''
    logger_source = ''

    growers = Decagon.open_pickle()
    for grower_pickle in growers:
        for field_pickle in grower_pickle.fields:
            if field_pickle.name == field_name:
                field = field_pickle
                for logger_pickle in field.loggers:
                    if logger_pickle.name == logger_destination_name and logger_pickle.active:
                        logger_destination = logger_pickle
                    elif logger_pickle.name == logger_source_name and logger_pickle.active:
                        logger_source = logger_pickle

    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
    project = dbwriter.get_db_project(logger_destination.crop_type)

    logger_source_dataset = project + "." + field_name + "." + logger_source.name
    logger_destination_dataset = project + "." + field_name + "." + logger_destination.name

    print("Inserting data for table: " + logger_destination_dataset)
    print(" from")
    print("Logger table: " + logger_source_dataset)

    # Use when eto is not NULL
    dml_statement = "MERGE `" + logger_destination_dataset + "` T " \
                    + "USING `" + logger_source_dataset + "` S " \
                    + "ON T.date = S.date " \
                    + "WHEN NOT MATCHED THEN " \
                    + "INSERT (logger_id, date, time, canopy_temperature, ambient_temperature, vpd, vwc_1, vwc_2, vwc_3, " \
                      "field_capacity, wilting_point,  daily_gallons, daily_switch, daily_hours, daily_pressure, " \
                      "daily_inches, psi, psi_threshold, psi_critical, sdd, rh, eto, kc, etc, et_hours, " \
                      "phase1_adjustment, phase1_adjusted, phase2_adjustment, phase2_adjusted, phase3_adjustment, " \
                      "phase3_adjusted, vwc_1_ec, vwc_2_ec, vwc_3_ec) " \
                    + "Values (S.logger_id, S.date,    S.time, S.canopy_temperature, S.ambient_temperature, S.vpd, S.vwc_1, S.vwc_2, S.vwc_3, " \
                      "S.field_capacity, S.wilting_point,  S.daily_gallons, S.daily_switch, S.daily_hours, S.daily_pressure, " \
                      "S.daily_inches, S.psi, S.psi_threshold, S.psi_critical, S.sdd, S.rh, S.eto, S.kc, S.etc, S.et_hours, " \
                      "S.phase1_adjustment, S.phase1_adjusted, S.phase2_adjustment, S.phase2_adjusted, S.phase3_adjustment, " \
                      "S.phase3_adjusted, S.vwc_1_ec, S.vwc_2_ec, S.vwc_3_ec)"
    # print(dml_statement)
    dbwriter.run_dml(dml_statement, project=project)

    print("Changing logger_id to match new one in old data")
    dml_statement = (
            "Update `" + logger_destination_dataset + "`  set logger_id = '" + logger_destination.id + "' where logger_id = '" + logger_source.id + "'")
    # print(dml_statement)
    dbwriter.run_dml(dml_statement, project=project)
    print("Clean up nulls")
    dml_statement = ("Delete `" + logger_destination_dataset + "` where date is NULL")
    dbwriter.run_dml(dml_statement, project=project)


#Grab dates and psi where first psi was turned on. If possible pull the first 3 days of values. Loop through 2022 year dataset.
def select_first_psi_for_all_datasets():
    """
    Function goes into database project for 2022 and selects the first 3 days of PSI values. It stores the data into a pickle called
    psi_pickle_2022.pickle.
    """
    datasets = dbwriter.get_datasets('stomato')
    psi_dict = {'field': [], 'logger': [], 'dates': [], 'psi': []}
    specific_file_path: str = PICKLE_DIRECTORY
    filename = "psi_pickle_2022.pickle"
    for dataset in datasets[0]:
        # print(dataset.dataset_id)
        tables_list = dbwriter.get_tables(dataset.dataset_id, 'stomato')
        if dataset.dataset_id == '1_growers_list' or dataset.dataset_id == "1_technician_portal" or \
                dataset.dataset_id == "1_uninstallation_progress" or dataset.dataset_id == "1_users_schema" or \
                dataset.dataset_id == "2022_uninstallation_progress" or dataset.dataset_id == "ET" or dataset.dataset_id == "Historical_ET" \
                or dataset.dataset_id == "Historical_ET_New" or dataset.dataset_id == "Meza" or "RnD" in dataset.dataset_id \
                or dataset.dataset_id == "SaulTest" or dataset.dataset_id == "YaraAyra" or dataset.dataset_id == "TestField":
            continue
        else:
            for table in tables_list:
                # print(table.table_id)
                if table.table_id == 'weather_forecast' or "Irr_Scheduling" in table.table_id or "temp" in table.table_id or "copy" in table.table_id \
                        or "5G" in table.table_id or "z6" in table.table_id:
                    continue
                else:
                    # print(f"Dataset: {dataset.dataset_id:<20} Table:{table.table_id:<20} Date List:{date_list:<20} PSI List:{psi_list:<20}")
                    try:
                        date_list, psi_list = select_psi(dataset.dataset_id, table.table_id)
                        psi_dict['field'].append(dataset.dataset_id)
                        psi_dict['logger'].append(table.table_id)
                        psi_dict['dates'].append(date_list)
                        psi_dict['psi'].append(psi_list)
                        print(f"Working on Field: {dataset.dataset_id} \n\t Logger: {table.table_id}")

                    except google.api_core.exceptions.BadRequest as e:
                        print("Caught BadRequest exception:", e)
    if path.exists(specific_file_path):
        with open(specific_file_path + filename, "wb") as file:
            pickle.dump(psi_dict, file)


def select_psi(field_name: str, logger_name: str) -> tuple[list[date], list[int]]:
    """
    Function grabs first 3 values where psi occurs and returns them as a list of dates and a list of psi values
    :param field_name: Dataset Field Name
    :param logger_name: Dataset Logger Name
    :return: date_list: List of Dates of the first 3 psi occurrences
            psi_list: List of PSI of the first 3 psi occurrences
    """
    field_db = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    dataset = f"`{'stomato'}.{field_db}.{logger_name}`"
    dml_statement = f"select date, psi from {dataset} where psi is not null order by date asc"
    # print(dml_statement)
    psi_dict = dbwriter.return_query_dict(dml_statement, 'date', 'psi', 'stomato')
    psi_only_three_dict = dict(itertools.islice(psi_dict.items(), 3))
    date_list = list(psi_only_three_dict.keys())
    psi_list = list(psi_only_three_dict.values())
    return date_list, psi_list


def show_psi_pickle_2022(specific_file_path: str = PICKLE_DIRECTORY, filename: str = "psi_pickle_2022.pickle"):
    """
    Function opens and returns psi pickle for 2022

    :param specific_file_path:
    :param filename:
    :return:
    """
    with open(specific_file_path + filename, "rb") as file:
        data = pickle.load(file)
    return data


def copy_last_day_from_old_date_to_new_date(project: str, field_name: str, logger_name: str, old_date: str,
                                            new_date: str):
    """
    This function copies the old date's data from a specific logger for a specific field and inserts it into the same logger but using a different
    date. This function is useful for when a logger gets disconnected for a day and we lost data for that day.
    :param project: Project name
    :param field_name: Field Name
    :param logger_name: Logger Name
    :param old_date: Old date you want to copy the information from
    :param new_date: New date you want to copy the information to
    """
    # Set up BigQuery client
    client = dbwriter.grab_bq_client(project)
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    # Define the source and destination table information
    source_table = f"{project}.{field_name}.{logger_name}"
    destination_table = f"{project}.{field_name}.{logger_name}"
    source_date = old_date  # Date of the source row
    destination_date = new_date  # Date for the new row

    # Query to select values from the source table for a specific date
    select_query = f"""
        SELECT *
        FROM `{source_table}`
        WHERE date = '{source_date}'
    """

    query_job = client.query(select_query)
    query_job.result()  # Wait for the query to complete

    if query_job.errors:
        print("Encountered errors while selecting rows.")
    else:
        print("Rows successfully selected into the destination table.")
        for row in query_job:
            # Convert the Row object to a dictionary
            row_dict = dict(row)

            # Set the date to the destination date
            row_dict["date"] = destination_date
            # Convert planting date to a string and save that as the planting date. Big query will recongize it as a date time, even if it's
            # inserted as string
            planting_date_string = row_dict["planting_date"].strftime('%Y-%m-%d')
            row_dict["planting_date"] = planting_date_string

        # print(row_dict)
        values = list(row_dict.values())

        # Query to insert the selected values into the destination table with a different date
        insert_query = f"INSERT INTO `{destination_table}` "
        insert_query += "VALUES ("
        insert_query += ", ".join(
            [f"{'Null' if value is None else value if not isinstance(value, str) else convert_string(value)}" for
             value in
             values])  # Loop through all the values in row_dict. If your value is None, convert into a string of Null.
        # If your value is a string you want to convert that value into a string by adding a ' to the beginning and end. If not when you loop
        # through the values they get inserted without the ''
        insert_query += ")"

        # print(insert_query)

        # Run the insert query
        query_job = client.query(insert_query)
        query_job.result()  # Wait for the query to complete

        if query_job.errors:
            print("Encountered errors while inserting rows.")
        else:
            print("Rows successfully inserted into the destination table.")


def convert_string(text):
    return f"'{text}'"


def find_lowest_psi_fields():
    """
        Function finds the lowest psi fields in the database and returns a list of the lowest psi fields.
        """
    psi_list = []
    logger_name_list = []
    project = f'stomato-{DIRECTORY_YEAR}'
    # Get list of datasets in database
    client = dbwriter.grab_bq_client(project)
    datasets = dbwriter.get_datasets(project)
    # Loop through all datasets
    for dataset in datasets[0]:
        # print(f"Working on field: {dataset.dataset_id}")
        # Get list of tables in dataset
        tables = client.list_tables(dataset.dataset_id)
        # Loop through all tables in dataset
        for table in tables:
            # print(table.table_id)
            # table_id = table.table_id
            if not ("Irr_Scheduling" in table.table_id) and not ("weather_forecast" in table.table_id):
                # Check if table is in algorithm list
                table_id = f"`{project}.{dataset.dataset_id}.{table.table_id}`"
                select_psi = f"select avg(psi) as average_psi, count(psi) as number_of_data_points from {table_id} where psi is not null"
                result = dbwriter.run_dml(select_psi)
                for row in result:
                    if row.average_psi:
                        # psi_list.append({'field': table_id,'psi':row.average_psi})
                        if row.number_of_data_points > 20:
                            psi_list.append(row.average_psi)
                            logger_name_list.append(table.table_id)
                            print(f"{table.table_id};{row.average_psi};{row.number_of_data_points}")

    # for ind, psi in enumerate(psi_list):
    #     print(f"{logger_name_list[ind]}: {psi}")


def generate_invite_code():
    """
    Generates a random invite code
    :return: Returns invite code as a 6 letter/number string
    """
    invite_code = ''.join(choices(ascii_uppercase + digits, k=6))
    return invite_code


def insert_grower_field(name: str, region: str, tech_assigned: str, stations: str, crop_type: str):
    """
    Function inserts the growers field information in the database
    :param crop_type: crop type
    :param name: name of field
    :param region: North or South region
    :param tech_assigned: Technician assigned to field
    :param stations: stations assigned to field
    """
    project = 'stomato-info'
    dataset = 'gradient_fields'
    dataset_id = f"`{project}.{dataset}.all`"

    dml_statement = f"insert into {dataset_id} (name, region, tech_assigned, stations, crop_type) " \
                    f"values ('{name}', '{region}', '{tech_assigned}', '{stations}', '{crop_type}')"

    dbwriter.run_dml(dml_statement, project=project)


def insert_grower_loggers(grower: str, field: str, logger_name: str, logger_direction: str, lat: str, long: str,
                          logger_id: str,
                          logger_password: str, crop_type: str):
    """
    Function inserts the growers logger information in the database
    :param crop_type: crop type
    :param grower: name of grower
    :param field: name of field
    :param logger_name: name of logger
    :param logger_direction: direction of logger
    :param lat: latitude of logger
    :param long: longitude of logger
    :param logger_id: logger id
    :param logger_password: logger password

    """
    project = 'stomato-info'
    dataset = 'gradient_loggers'
    dataset_id = f"`{project}.{dataset}.all`"

    dml_statement = f"insert into {dataset_id} (grower, field, logger_name, logger_direction, lat, long, logger_id, logger_password, crop_type) " \
                    f"values ('{grower}', '{field}', '{logger_name}', '{logger_direction}', '{lat}', '{long}', '{logger_id}', '{logger_password}'," \
                    f" '{crop_type}')"

    dbwriter.run_dml(dml_statement, project=project)


def add_new_celsius_columns_to_permanent_datasets():
    """
    Function adds the canopy_temperature_celsius, ambient_temperature_celsius, lower_ambient_temperature_celsius, sdd_celsius
    columns to the permanent datasets
    """
    client = bigquery.Client()
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            if (
                    field.crop_type.lower() == 'almonds' or field.crop_type.lower() == 'almonds' or field.crop_type.lower() == 'pistachio'
                    or field.crop_type.lower() == 'pistachios'):
                print(f"Field: {field.name}")
                for logger in field.loggers:
                    table_id = f"stomato-permanents.{dbwriter.remove_unwanted_chars_for_db_dataset(field.name)}.{logger.name}"
                    # print(f"Table: {table_id}")
                    table = client.get_table(table_id)
                    original_schema = table.schema
                    new_schema = original_schema[:]
                    new_schema.append(bigquery.SchemaField("canopy_temperature_celsius", "FLOAT"))
                    new_schema.append(bigquery.SchemaField("ambient_temperature_celsius", "FLOAT"))
                    new_schema.append(bigquery.SchemaField("lowest_ambient_temperature_celsius", "FLOAT"))
                    new_schema.append(bigquery.SchemaField("sdd_celsius", "FLOAT"))

                    table.schema = new_schema
                    try:
                        table = client.update_table(table, ["schema"])
                        if len(table.schema) == len(original_schema) + 4 == len(new_schema):
                            print("A new column has been added.")
                        else:
                            print("The column has not been added.")
                    except Exception as e:
                        print(f"Error updating table schema: {e}")


def update_grower_portal_report_and_images(grower_names: str):
    """
    Function updates the grower portal DB reports and images from pickle
    :param grower_names: Grower names
    """
    print(f"Updating reports and previews for {grower_names}")
    project = f'growers-{DIRECTORY_YEAR}'
    growers = Decagon.open_pickle()
    for g in growers:
        if g.name in grower_names:
            grower_db_name = dbwriter.remove_unwanted_chars_for_db_dataset(g.name)
            base_table_id = f"{project}.{grower_db_name}"
            for f in g.fields:
                dml_statement_field = f"UPDATE `{base_table_id}.field_averages` SET report = '{f.report_url}', preview = '{f.preview_url}' WHERE field = '{f.nickname}'"
                dml_statement_logger = f"UPDATE `{base_table_id}.loggers` SET report = '{f.report_url}', crop_image = '{CwsiProcessor().get_crop_image(f.crop_type)}' WHERE field = '{f.nickname}'"
                for statement in (dml_statement_field, dml_statement_logger):
                    dbwriter.run_dml(statement, project=project)
                    print(statement)
    print('Done updating reports and previews')


def update_field_portal_report_and_images(field_name: str, report_url: str = None, preview_url: str = None):
    """
    Function updates the fields reports and images in pickle and DB

    """

    print(f"Updating reports and previews for {field_name}")
    project = f'growers-{DIRECTORY_YEAR}'
    growers = Decagon.open_pickle()
    for g in growers:
        for f in g.fields:
            grower_db_name = dbwriter.remove_unwanted_chars_for_db_dataset(g.name)
            base_table_id = f"{project}.{grower_db_name}"
            if f.name == field_name:
                # update pickle
                if report_url:
                    f.report_url = report_url
                if preview_url:
                    f.preview_url = preview_url

                # update DB
                dml_statement_field = f"UPDATE `{base_table_id}.field_averages` SET report = '{f.report_url}', preview = '{f.preview_url}' WHERE field = '{f.nickname}'"
                dml_statement_logger = f"UPDATE `{base_table_id}.loggers` SET report = '{f.report_url}', crop_image = '{CwsiProcessor().get_crop_image(f.crop_type)}' WHERE field = '{f.nickname}'"
                for statement in (dml_statement_field, dml_statement_logger):
                    dbwriter.run_dml(statement, project=project)
                    print(statement)
    print('Done updating reports and previews')
    Decagon.write_pickle(growers)


def sum_db_total_for_column_for_list_of_fields(pickle_name: str, pickle_directory: str, db_column: str,
                                               list_of_field_names: list[str]) -> None:
    """
    Function that takes in a pickle name, the directory where that pickle is located, a column name from the db,
    and a list of grower field names, and totals up the column from each grower field. This actually totals up the
    column from each logger inside that grower field and then shows the average of all the loggers as well as
    the maximum from the loggers. Useful to total up the irrigation inches from several grower fields

    :param pickle_name: String of the name of the pickle file including the .pickle extension
    :param pickle_directory: String of the directory where the pickle file is located
    :param db_column: String of the db column name
    :param list_of_field_names: List of strings of the field names we are looking total up
    """
    growers = Decagon.open_pickle(pickle_name, pickle_directory)

    dbw = DBWriter()
    for grower in growers:
        for field in grower.fields:
            if field.name in list_of_field_names:
                print(f'Found {field.name}. Processing...')
                logger_totals = []
                for logger in field.loggers:
                    print(f'\t{logger.name}')
                    field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field.name)
                    db_project = dbw.get_db_project(field.crop_type)
                    dml = f'SELECT SUM({db_column}) AS total FROM `{db_project}.{field_name_db}.{logger.name}`'
                    result = dbw.run_dml(dml)
                    for row in result:
                        logger_totals.append(row['total'])
                        # print()
                average_total = (sum(logger_totals) / len(logger_totals))
                max_total = (max(logger_totals))
                print(
                    f'\t\tAverage Total {db_column} for {len(logger_totals)} loggers = {average_total:.1f}')
                print(
                    f'\t\tMax Total {db_column} for {len(logger_totals)} loggers = {max_total:.1f}')
                print()


def change_logger_soil_type(logger_name: str, field_name: str, grower_name: str, new_soil_type: str):
    """
    Single function to change the soil type for a logger in both the pickle and the db

    :param logger_name:
    :param field_name:
    :param grower_name:
    :param new_soil_type:
    """
    print(f'Changing soil type for logger: {logger_name} to {new_soil_type}')

    growers = Decagon.open_pickle()
    dbw = DBWriter()

    logger_found = False

    # Change soil type in the pickle
    print('-Changing soil type in the pickle')
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            print('\tFound logger...changing')
                            old_soil_type = logger.soil.soil_type
                            logger.soil.set_soil_type(new_soil_type)
                            field_capacity = logger.soil.field_capacity
                            wilting_point = logger.soil.wilting_point
                            crop_type = logger.crop_type
                            logger_found = True
    Decagon.write_pickle(growers)
    print('\tDone with pickle')

    if logger_found:
        # Change soil type parameters in the DB
        print('-Changing soil type in the db')
        field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field_name)
        db_project = dbw.get_db_project(crop_type)
        dml = (f'UPDATE `{db_project}.{field_name_db}.{logger_name}` '
               f'SET field_capacity = {field_capacity}, wilting_point = {wilting_point} '
               f'WHERE TRUE')
        result = dbw.run_dml(dml)
        print(f'\tDone with DB')
        print()
        print(f'Soil type for {logger_name} changed from {old_soil_type} to {new_soil_type}')
        print()
    else:
        print(f'Logger {logger_name} not found')


def get_db_table(field, logger, year):
    result_dict = {}
    field_name_db = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
    db_project = dbwriter.get_db_project(field.crop_type)
    # prev_year = int(year) - 1
    # next_year = int(year) + 1
    # Query all columns for the logger
    # dml = f"SELECT * FROM `{db_project}.{field_name_db}.{logger.name}` Where date > '{prev_year}-12-31' and date < '{next_year}-12-31'"
    dml = f"SELECT * FROM `{db_project}.{field_name_db}.{logger.name}`"
    result = dbwriter.run_dml(dml)

    # Initialize the logger's data dictionary if not already present
    if logger.name not in result_dict:
        result_dict[logger.name] = {}

    # Process each row in the result
    for row in result:
        for column_name, value in row.items():
            if column_name not in result_dict[logger.name]:
                result_dict[logger.name][column_name] = []
            result_dict[logger.name][column_name].append(value)
    return result_dict


def get_logger_db(field, logger, year):
    """

    :param field:
    :param logger:
    :param year:
    :return:
    """
    dbw = DBWriter()
    result_dict = {}
    field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field.name)
    db_project = dbw.get_db_project(field.crop_type)

    # Query all columns for the logger
    dml = f"SELECT * FROM `{db_project}.{field_name_db}.{logger.name}` Where date > '{year}-12-31'"
    result = dbw.run_dml(dml)

    # Process each row in the result
    for row in result:
        for column_name, value in row.items():
            if column_name not in result_dict:
                result_dict[column_name] = []
            result_dict[column_name].append(value)
    return result_dict


def get_entire_table_points(list_of_field_names: list[str], year='2025', pickle = '2025_pickle.pickle') -> dict:
    """
    Function that takes in a list of grower field names and retrieves all columns from each logger inside that grower field.
    The data is returned in a dictionary where each logger's name is the key, and the value is another dictionary.
    This inner dictionary contains column names as keys and lists of column values as values.

    :param year:
    :param list_of_field_names: List of strings of the field names to process
    :return: Dictionary containing all columns for each logger, with each column as its own key
    """
    growers = Decagon.open_pickle(pickle)
    dbw = DBWriter()
    result_dict = {}  # This will now accumulate multiple loggers

    for grower in growers:
        for field in grower.fields:
            if field.name in list_of_field_names:
                print(f'Found {field.name}. Processing...')
                for logger in field.loggers:
                    print(f'\t{logger.name}')

                    # Get the logger data
                    logger_data = get_db_table(field, logger, year)

                    # Merge the results correctly
                    for logger_name, data in logger_data.items():
                        if logger_name not in result_dict:
                            result_dict[logger_name] = data  # If logger is new, add it
                        else:
                            # Merge columns, appending data if the logger already exists
                            for column, values in data.items():
                                if column not in result_dict[logger_name]:
                                    result_dict[logger_name][column] = values
                                else:
                                    result_dict[logger_name][column].extend(values)  # Append values

    return result_dict


def get_logger_table(logger_name: str, year='2025', growers=None) -> dict:
    """
    Function that takes in a list of grower field names and retrieves all columns from each logger inside that grower field.
    The data is returned in a dictionary where each logger's name is the key, and the value is another dictionary.
    This inner dictionary contains column names as keys and lists of column values as values.

    :param growers:
    :param year:
    :param logger_name:
    :return: Dictionary containing all columns for each logger, with each column as its own key
    """
    if growers is None:
        growers = Decagon.open_pickle()
    dbw = DBWriter()
    result_dict = {}
    for grower in growers:
        for field in grower.fields:
            for logger in field.loggers:
                if logger.name == logger_name:
                    print(f'Found {logger.name}. Processing...')

                    result_dict = get_logger_db(field, logger, year)

    return result_dict


def get_values_for_date(project, field_name, logger_name, date):
    """
    Get vwc_1, vwc_2, vwc_3 values for a specific date from the DB.
    """
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    dataset_id = project + '.' + field_name + '.' + logger_name
    dataset_id = "`" + dataset_id + "`"

    date_s = date.strftime("%Y-%m-%d")
    date_s = "'" + date_s + "'"

    # Query to get values for the specific date
    query = f"""
    SELECT vwc_1, vwc_2, vwc_3, field_capacity, wilting_point
    FROM {dataset_id}
    WHERE date = {date_s}
    """

    rows = dbwriter.run_dml(query, project=project)

    # Convert the RowIterator to a list
    result = list(rows)

    if result:
        return result[0]
    else:
        return None

def update_vwc_for_date_range(project, field_name, logger_name, start_date, end_date, vwcs):
    # Ensure field_name is safe for database use
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)

    dataset_id = project + '.' + field_name + '.' + logger_name
    dataset_id = "`" + dataset_id + "`"

    # Parse dates
    start_date_dt = datetime.strptime(start_date, "%Y-%m-%d")
    end_date_dt = datetime.strptime(end_date, "%Y-%m-%d")
    today = datetime.today()

    # Ensure the end_date is not today or in the future
    if end_date_dt >= today:
        raise ValueError("end_date cannot be today or in the future. Please provide a valid date range.")

    # Fetch the previous day's data for the start_date
    start_date_prev_day = start_date_dt - timedelta(days=1)

    # Fetch values from the day before start_date
    previous_day_values = get_values_for_date(project, field_name, logger_name, start_date_prev_day)

    if previous_day_values:
        vwc_1_value, vwc_2_value, vwc_3_value, fc, wp = previous_day_values
        current_date = start_date_dt

        # Loop through each date in the range and update/insert VWC data
        while current_date <= end_date_dt:
            current_date_s = current_date.strftime("%Y-%m-%d")
            current_date_s = "'" + current_date_s + "'"

            # Check if data exists for the current date
            check_query = f"""
            SELECT COUNT(*) as count
            FROM {dataset_id}
            WHERE date = {current_date_s}
            """

            check_rows = dbwriter.run_dml(check_query, project=project)
            check_result = list(check_rows)

            # Build the VWC-related parts of the DML based on the 'vwcs' parameter
            vwc_set_clauses = []
            vwc_insert_columns = []
            vwc_insert_values = []

            if 'VWC 1' in vwcs:
                vwc_set_clauses.append(f"vwc_1 = {vwc_1_value}")
                vwc_insert_columns.append("vwc_1")
                vwc_insert_values.append(vwc_1_value)

            if 'VWC 2' in vwcs:
                vwc_set_clauses.append(f"vwc_2 = {vwc_2_value}")
                vwc_insert_columns.append("vwc_2")
                vwc_insert_values.append(vwc_2_value)

            if 'VWC 3' in vwcs:
                vwc_set_clauses.append(f"vwc_3 = {vwc_3_value}")
                vwc_insert_columns.append("vwc_3")
                vwc_insert_values.append(vwc_3_value)

            if check_result and check_result[0][0] == 0:
                # No data exists for this date, insert a new row
                insert_dml = (
                    f"INSERT INTO {dataset_id} "
                    f"(date, {', '.join(vwc_insert_columns)}, field_capacity, wilting_point) "
                    f"VALUES ({current_date_s}, {', '.join(map(str, vwc_insert_values))}, {fc}, {wp})"
                )
                dbwriter.run_dml(insert_dml, project=project)
                print(f'Inserted new row for date: {current_date_s}')
            else:
                # Update existing row
                update_dml = (
                    f"UPDATE {dataset_id} "
                    f"SET {', '.join(vwc_set_clauses)}, "
                    f"    field_capacity = {fc}, "
                    f"    wilting_point = {wp} "
                    f"WHERE date = {current_date_s}"
                )
                dbwriter.run_dml(update_dml, project=project)
                print(f'Updated row for date: {current_date_s}')

            current_date += timedelta(days=1)

        print('Date range update completed.')
    else:
        print('No values found for the day before the start_date')


def rerun_logger_from_scratch_to_fix_different_minute_interval_issues(grower_name, field_name, logger_name):
    """
    This function requires that inside Loggers update function, the specific code to run the larger 96 bucket irrigation
    ledger is uncommented, update_irrigation_ledger_2 or _3, and the standard update_irrigation_ledger is commented out

    :param grower_name:
    :param field_name:
    :param logger_name:
    :return:
    """
    dbw = DBWriter()
    growers = Decagon.open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            print(f'Fixing logger {logger.name} for non standard minute interval issues')

                            #Delete table values
                            crop_type = logger.crop_type
                            field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field_name)
                            db_project = dbw.get_db_project(crop_type)
                            print(f'Deleting table contents for {db_project}.{field_name_db}.{logger_name}')
                            dml = (f'DELETE FROM `{db_project}.{field_name_db}.{logger_name}` '
                                   f'WHERE TRUE')
                            print(f'dml: {dml}')
                            result = dbw.run_dml(dml)
                            print(f'\tDone with table clear')

                            #Update logger from 0
                            print(f'Updating logger {logger.name} from 0')
                            logger.ir_active = False
                            Decagon.write_pickle(growers)
                            Decagon.only_certain_growers_field_logger_update(
                                grower.name,
                                field.name,
                                logger.name,
                                logger.id,
                                write_to_db=True,
                                specific_mrid=0
                            )
                            print('\tDone with logger update')


def set_to_null_column(grower_name, field_name, logger_name, column_names, start_date, end_date=None):
    """
    Function to set a certain column or columns of a loggers DB table to null for a certain date range,
    if end_date not passed it will do all dates after the start_date.
    This will be useful for deleting psi values that started early.

    :param grower_name: Name of the grower
    :param field_name: Name of the field
    :param logger_name: Name of the logger
    :param column_names: List of column names to set to NULL
    :param start_date: Start date (inclusive) in YYYY-MM-DD format
    :param end_date: End date (inclusive) in YYYY-MM-DD format
    """
    dbw = DBWriter()
    growers = Decagon.open_pickle()

    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            if end_date:
                                print(
                                    f'Updating logger {logger.name} {column_names} to NULL from date {start_date} to {end_date}'
                                )
                            else:
                                print(
                                    f'Updating logger {logger.name} {column_names} to NULL from date {start_date} onward'
                                )

                            crop_type = logger.crop_type
                            field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field_name)
                            db_project = dbw.get_db_project(crop_type)

                            # Generating the SQL query
                            column_updates = ", ".join(f"{col} = NULL" for col in column_names)
                            if end_date:
                                date_condition = f"BETWEEN '{start_date}' AND '{end_date}'"
                            else:
                                date_condition = f">= '{start_date}'"

                            dml = (
                                f'UPDATE `{db_project}.{field_name_db}.{logger_name}` '
                                f'SET {column_updates} '
                                f'WHERE date {date_condition}'
                            )

                            # Execute the query
                            dbw.run_dml(dml)
                            print(f'Ran: {dml}')
                            return

    print("Logger not found. No updates were made.")

# def update_grower_portal_data

# rerun_logger_from_scratch_to_fix_different_minute_interval_issues('Lucero Rio Vista', 'Lucero Rio VistaC', 'RV-C-NE')
# rerun_logger_from_scratch_to_fix_different_minute_interval_issues('Lucero Rio Vista', 'Lucero Rio VistaC', 'RV-C-W')

# rerun_logger_from_scratch_to_fix_different_minute_interval_issues('Lucero Rio Vista', 'Lucero Rio VistaD', 'LR-D-W')


# list_of_grower_fields_to_process = ['Bone Farms LLCF7', 'Fransicioni & Griva8', 'Mike Silva01-MS3', 'Nuss Farms Inc7',
#                                     'Lucero 8 Mile11, 12, 13, 14', 'Lucero Dillard RoadD7', 'Lucero Watermark2,3',
#                                     'Lucero Mandeville15', 'Greenfield Tyler Island1, 3', 'Mumma Bros11,12',
#                                     'Matteoli BrothersN3', 'CM OchoaA8']
# sum_db_total_for_column_for_list_of_fields('2023_pickle.pickle', "H:\\Shared drives\\Stomato\\2023\\Pickle\\", 'daily_inches', list_of_grower_fields_to_process)


# update_portal_reports('Lucero Ryer Island')
# IMPORTANT: This function updates historical et for all tables or a specific one if specified
# add_new_year_to_historical_et()
# add_new_year_to_historical_et("148")


# add_new_celsius_columns_to_permanent_datasets()
# add_new_year_to_historical_et()
# cimisStation = CimisStation()
# cimisStation.showCimisStations()
# cimisStation = cimisStation.open_cimis_station_pickle()
# for station in cimisStation:
# print(f"{station.station_number} : {station.latest_eto_value}")
# print(f"{station.station_number} : {station.active}")
# Dict with keys being Field, Logger, Days as a List, PSI as a List
# copy_missing_data_to_logger_from_other_logger('Lucero Dillard RoadD1', 'DI-D1-NE', 'DI-D3-NW')
# select_first_psi_for_all_datasets()
# find_lowest_psi_fields()
# psi_pickle = show_psi_pickle()
# # print(psi_pickle)
# for ind, dataset in enumerate(psi_pickle):
#     print(dataset[ind]['field'])
#     print(dataset[ind]['logger'])
#     print(dataset[ind]['dates'])
#     print(dataset[ind]['psi'])
# print(f"Field: {dataset['field']:20} Logger: {dataset['logger']:20} \n Dates: \n{' '.join(str(date) for date in dataset['dates']):20}"
#       f" \nPSI: \n{' '.join(str(date) for date in dataset['psi']):20}")
# fix_irr_inches_for_all_fields('2023-04-01', '2023-04-06')
# copy_last_day_from_old_date_to_new_date('stomato-2023', 'Lucero Dillard RoadD1', "DI-D1-NE", '2023-06-26', '2023-06-27')
# update_fc_wp('stomato-2023', 'Lucero Dillard RoadD 8, 11', 'DI-D8-C', 36, 22)
# grab_historical_data('250')
# southCount = 0
# northCount = 0
# update_fc_wp('stomato-2023', 'Lucero Dillard RoadD2', 'DI-D2-W', 36, 22)
# update_fc_wp('stomato-2023', 'Turlock Fruit Co1250', 'TU-1250-SE', 28, 14)
# Decagon.show_pickle()

# growers = Decagon.open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         if field.name == 'Lucero Rio Vista1':
#             Decagon.remove_field(grower.name, field.name)
#             print(f"{field.name}")
#             forecast = field.get_weather_forecast()
#             pprint.pprint(forecast)
# removeDuplicateET()
# update_field_et('S&S Ranch33-3')
# growers = Decagon.open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         if field.crop_type.lower() == 'almonds' or field.crop_type.lower() == 'almond':
#             # field.name = 'JJB FarmsGI 19, 21'
#             # field.nickname  = 'GI 19, 21'
#             # print(field.name)
#             # print(field.nickname)
#             for logger in field.loggers:
#                 remove_psi(field.name, '2023-11-1', '2023-12-15', '2023')
# print(logger.name)
# print(f"Old Logger Active: {logger.ir_active}")
# logger.ir_active = False
# print(f"New Logger Active: {logger.ir_active}")
#                 if logger.active and logger.name == 'SS-333-SW':
#
# #                     print(logger.id)
# #                     logger.ir_active = True
# #                     print(f"{field.name}:\n\t{logger.name}:{logger.consecutive_ir_values}:{logger.ir_active}")
# Decagon.write_pickle(growers)
# # # #                 if logger.name == 'BF-57-SW' and logger.active:
# # # #                     project = dbwriter.get_db_project(logger.crop_type)
# # # #                     update_irr_hours_for_date(project, field.name, logger.name, 1.3, '2023-06-16')
#                     remove_duplicate_data(logger)
#                     update_missing_et_data(logger)
#         if field.name == 'S&S Ranch33-3' or field.name == "S&S Ranch33-1":
#             for logger in field.loggers:
#                 print(f"{logger.name}: {logger.id}: {logger.field_capacity} : {logger.wilting_point}")
# if logger.name == 'SS-331-SW':
#     update_fc_wp(project,field.name,logger.name, 36, 22)

# Decagon.show_pickle()

# growers = Decagon.open_pickle()
# for grower in growers:
#     for field in grower.fields:
#         if field.name == 'Lucero Dillard RoadD4':
#             for logger in field.loggers:
#                 if logger.name == 'DI-D4-W':
#                     update_irr_hours_for_date_range('stomato-2023', logger.field.name, logger.name, 0, '2023-06-30', '2023-07-5')

# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.name == 'Kruppa Enterprises LLCRokpileb':
#             for l in f.loggers:
# # #                 # update_fc_wp('stomato-2023', f.name, l.name, 36, 22)
#                 if l.logger_direction == 'W':
#                     project = dbwriter.get_db_project(l.crop_type)
#                     update_irr_hours_for_date_range(project, f.name, l.name, 11.2, '2023-06-18', '2023-06-18')
#                     remove_psi_specific(project, f.name, l.name, '2023-05-15', '2023-05-15')
#                     delete_repeat_data(project, f.name, l.name)
#             print(l)
#             if not l.rnd:
#                 if g.region == 'South' or g.region == 'south':
#                     southCount = southCount + 1
#
#                 if g.region == 'North' or g.region == 'north':
#                     northCount = northCount + 1
# print("South\n\t" + str(southCount))
# print("North\n\t" + str(northCount))
# print("Total\n\t" + str(southCount+northCount))
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.name == 'Mike Silva01-MS3':
#             for logger in f.loggers:
#                 if logger.name == 'MS-01MS3-NE':
#                     project = dbwriter.get_db_project(logger.crop_type)
#                     update_irr_hours_for_date(project, f.name, logger.name, 4.5, '2023-5-6')
#         update_fc_wp(project, f.name, logger.name, 18, 8)
#     # print(project)
#     print(logger.name)
# remove_duplicate_data(logger)
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.active:
#             print("Erasing weather issue for field: ", f.name)
# fixWeatherDB(f.name)
# start_date = date(2022, 8, 15)
# end_date = date(2022, 8, 16)
# for single_date in daterange(start_date, end_date):
#     copy_vp4_vals_from_table_to_table('Lucero Rio Vista2', 'RV-02-NW', 'RV-02-S', single_date.strftime("%Y-%m-%d"))
# copy_gdd_values_from_temp_table_to_table()
# delete_repeat_data('Meza', 'Development-C')
# update_portal_image('DCB', 'F3_F4', 'https://i.imgur.com/C6ePFHh.png', True)

# copy_gdd_values_from_temp_table_to_table('Barrios_Farms22', 'BF-22-SE', 'BF-22-SE_temp')

# update_FC_WP('Bullseye FarmsRG42', 'Bull-RG42-C', 31, 11)
# move_logger_DB_info('DCBD_T', 'BR-DT1-NE', 'BR-DT1-NE_copy')
# move_logger_DB_info('Lucero_Sun_Pacific31', 'LU-31WM-SE', 'LU-31-SE')
# copy_vp4_vals_from_table_to_table('LemonicaTango K', 'TAG-LE-SE', 'TAG-WM-SE', '2022-04-18')
# update_FC_WP('Carvalho308A', 'KC-308A-S', 36, 22)
# update_logger_et('La QuintaDates Block 36 DB', 'DAT-Blk36DB-NW')
# deleteWhereEToIsNull('OPC3-2', 'OP-AL-NW', '2022-04-02', '2022-04-03')
# removeDuplicateET()
# update_all_field_et()
# update_field_et("OPC15-4")
# delete_et_day('CM OchoaA8', '2023-05-06', '2023-05-09')
# update_field_et("CM OchoaA8")
# update_field_et("Andrew3104")
# update_field_et("Andrew3101-3103")
# update_field_et('DCBWarren')
# growerSucks = 0
# fieldCount = 0
# stationCount = 0
# count = 0
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.name == 'CM OchoaA8':
#             delete_et_day(f, '2023-05-06', '2023-05-09')

# for l in f.loggers:
#             print(g.name,";", f.nickname,";", l.rnd)
#             if l.rnd:
#                 count += 1
# print(count)

# print("Grower fields: ", growerSucks)
# print("Field Count: ", fieldCount)
# print("Station Count: ", stationCount)

# if g.name != 'RnD':
#             for l in f.loggers:
#                 if l.cropType == 'Tomatoes':
#                     print(g.name, ';', f.nickname, ';', l.name, ';')
#         if f.name == 'DCBVerway':
#             previewImage = 'https://i.imgur.com/ROE4By8.png'
#             update_portal_image(g, f, previewImage)
# for l in f.loggers:
#             update_missing_et_data(l)
# update_field_et('LemonicaTango K')
# update_field_et("F&SAirport 3 ")
# update_all_field_et()
# removePSISpecific('Hughes303', 'HU-303-NE', '2022-05-27', '2022-05-28')
# remove_psi('Bone Farms LLCN42 N43', '2023-05-16', '2023-05-17', '2023')
# startDate = date(datetime.now().year, 1, 1)
# print(startDate)
# endDate = date(2021, 12, 31)
# setupIrrigationSchedulingDB(105, "Andrew3125", startDate, endDate)
# Decagon.show_pickle()
# all_tables_analysis()
# get_average_psi_during_growth()

# delete_all_null_rows()
# update_missing_et_data('z6-01995')
# delete_repeat_data('OPC3-3', 'OP-33-NE')
# remove_duplicate_data("RnD77", 'LH-77Y-N')
# update_field_portal_report_and_images('S&S Farms33-1', preview_url='https://imgur.com/bRayrlG.png')
# update_field_portal_report_and_images('Lucero Rio VistaS South', preview_u'rl='https://i.imgur.com/KZsthm9.png', report_url='https://lookerstudio.google.com/reporting/64fe5e52-69a9-4a05-a5d5-fa7c9eb6984a')
# copy_missing_data_to_logger_from_other_logger('Lucero LatropLTP7 9', )
# update_vwc_for_date_range('stomato-2024', 'Lucero Rio VistaB West', 'RV-B-C', '2024-07-23', '2024-07-23')
# update_vwc_for_date_range('stomato-permanents', 'Carvalho316', 'CA-316-NW', '2025-03-28', '2025-03-28', ['VWC 1','VWC 2', 'VWC 3'])
