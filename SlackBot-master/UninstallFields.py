import datetime

import DBWriter
import Decagon
import GSheetCredentialSevice
import GSheetWriter
import LoggerSetups
import gSheetReader


def remove_from_uninstalled_form(service, sheetID, numRows):
    write = GSheetWriter.GSheetWriter()
    # range = "Uninstallation Form!A2:D"
    tabID = 931991301
    # There are fields to be removed
    write.deleteRangeRows(service, sheetID, tabID, 1, numRows)


def add_to_uninstalled_list(service, sheetID, grower, field):
    print("Adding Uninstalled fields to Uninstalled List")
    range = 'Uninstalled Fields'
    # Add today to payload of data
    today = datetime.datetime.today()
    date = str(today.date())
    data = {
        "values": [
            [
                date, grower, field
            ]
        ]
    }
    write = GSheetWriter.GSheetWriter()
    write.append_to_sheet(service, sheetID, range, data)


def remove_fields():
    dbw = DBWriter.DBWriter()
    print("Checking Uninstallation form for any new fields to uninstall")
    #2024
    # sheetID = '1rsPrX44pCHOhbOHZfWubDkuuexP8pSG8kspW03FAqOU'
    #2023
    sheetID = '1rsPrX44pCHOhbOHZfWubDkuuexP8pSG8kspW03FAqOU'
    g_sheets_tab_name = "Uninstallation Form"

    g_sheet_credential = GSheetCredentialSevice.GSheetCredentialSevice()

    service = g_sheet_credential.getService()
    result = gSheetReader.getServiceRead(g_sheets_tab_name, sheetID, service)
    # avoidUserInput = True

    g_sheet_row_results = result['valueRanges'][0]['values']

    # Assign indexes to columns
    fieldNameHeader = gSheetReader.getColumnHeader("Grower Field Name", g_sheet_row_results)
    dateHeader = gSheetReader.getColumnHeader('Timestamp', g_sheet_row_results)
    fieldList = []
    current_year = datetime.datetime.today().year

    grower_pickle = Decagon.open_pickle()

    for field_row_iterator, field_row_data in enumerate(g_sheet_row_results):
        try:
            # Skip G Sheet Headers
            if field_row_iterator == 0:
                continue
            # if field name is blank, probably a blank row so skip
            elif field_row_data[fieldNameHeader] == "":
                continue
            field_name = field_row_data[fieldNameHeader]

            # print(date)

            for grower in grower_pickle:
                for field in grower.fields:
                    if field.name == field_name:
                        grower_name_db = dbw.remove_unwanted_chars_for_db_dataset(grower.name)
                        field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field.name)
                        crop_type = field.loggers[-1].crop_type
                        crop_image = get_crop_image(crop_type)
                        project = dbw.get_db_project(crop_type)
                        # Get uninstallation Date using last date of database
                        table_id = f"`{project}.{field_name_db}.{field.loggers[-1].name}`"
                        select_uninstall_date_query = f"select t.date from {table_id} as t order by t.date DESC limit 1"
                        result_uninstall_date_query = dbw.run_dml(select_uninstall_date_query)
                        for row in result_uninstall_date_query:
                            field_uninstallation_date = row.date
                        print(f"{field.name}:{field.loggers[-1].name}:{field_uninstallation_date}")
                        if not field_uninstallation_date:
                            print(f"No data for field")
                            continue


                        print("\tUninstalling ", field.name)
                        LoggerSetups.deactivate_field(grower.name, field_name, field_uninstallation_date)
                        fieldList.append(field_name)

                        # dataset = 'stomato-info.uninstallation_progress' + "." + grower.region
                        # dml_statement = (
                        #         "Update `" + dataset + "`  set Uninstalled_Date = '" + str(field_uninstallation_date) +
                        #         "' where Field = '" + field.nickname + "' and Grower = '" + grower.name + "'")
                        # # print(dml_statement)
                        # DBWriter.dbwriter.run_dml(dml_statement)
                        # print("\t\t Field Uninstalled in technician portal")
                        # field_averages_portal_dataset_id = 'growers-' + str(current_year) + '.' + \
                        #                                    grower_name + '.field_averages'
                        # logger_portal_dataset_id = 'growers-' + str(current_year) + '.' + \
                        #                            grower_name + '.loggers'
                        # final_results_portal_db = {
                        #     "order": -999, "field": field.nickname, "crop_type": crop_type, "crop_image": crop_image,
                        #     "soil_moisture_num": 'null', "soil_moisture_desc": "Uninstalled",
                        #     "si_num": 'null', "si_desc": "Uninstalled", "report": field.report_url,
                        #     "preview": field.preview_url
                        # }
                        # print('\t\tUninstalling main portal')
                        # update_portal_table_value(dbw, field_averages_portal_dataset_id, 'field', field.nickname, final_results_portal_db, project)
                        # print('\t\tUninstalling logger portal')
                        # for l in field.loggers:
                        #     final_results_logger_db = {
                        #         "order": -999, "field": field.nickname, "crop_type": crop_type, "crop_image": crop_image,
                        #         "soil_moisture_num": 'null', "soil_moisture_desc": "Uninstalled",
                        #         "si_num": 'null', "si_desc": "Uninstalled", "report": field.report_url,
                        #         "preview": field.preview_url, "logger_name": l.name,
                        #         "logger_direction": l.logger_direction
                        #     }
                        #     update_portal_table_value(dbw, logger_portal_dataset_id, 'logger_name', l.name, final_results_logger_db, project)

        except IndexError:
            continue
    print("Removed the following Fields: ", fieldList)


def calculate_stats_for_acres(region):
    dataset = "stomato-info." + 'uninstallation_progress' + "." + region
    installed_acres = 0
    uninstalled_acres = 0
    try:
        dml_statement_InstalledAcres = (
                "Select sum(acres) as installed_acres from `" + dataset + "` where Uninstalled_Date is null")
        # print(dml_statement)
        result = DBWriter.dbwriter.run_dml(dml_statement_InstalledAcres)
        for row in result:
            installed_acres = row.installed_acres

        dml_statement_UninstalledAcres = (
                "Select sum(acres) as Uninstalled_acres from `" + dataset + "` where Uninstalled_Date is not null")
        result = DBWriter.dbwriter.run_dml(dml_statement_UninstalledAcres)
        for row in result:
            uninstalled_acres = row.Uninstalled_acres

        # print(installedAcres)
        if installed_acres is None:
            installed_acres = 0

        if uninstalled_acres is None:
            uninstalled_acres = 0

        return installed_acres, uninstalled_acres

    except Exception as err:
        print(err)
        return False, False


def update_stats_for_acres():
    print("Calculating Stats for North and South")
    stats_dataset = "stomato-info." + 'uninstallation_progress' + "." + 'stats_acres'
    installed_acres_north, uninstalled_acres_north = calculate_stats_for_acres('North')
    # If data is False, we resulted in a crash and we shouldn't update acres
    if installed_acres_north or uninstalled_acres_north:
        type_installed_north = 'Acres Installed North'
        type_uninstalled_north = 'Acres Uninstalled North'
        print("\t Updating Stats for North")
        dml_statement = (
                "update `" + stats_dataset + "`" + " set acres = " + str(installed_acres_north)
                + " where type = '" + type_installed_north + "'")
        # print(dml_statement)
        DBWriter.dbwriter.run_dml(dml_statement)
        dml_statement = (
                "update `" + stats_dataset + "`" + " set acres = " + str(uninstalled_acres_north)
                + " where type = '" + type_uninstalled_north + "'")
        DBWriter.dbwriter.run_dml(dml_statement)
    else:
        print("\t Incurred an error, not updating stats for North")

    installed_acres_south, uninstalled_acres_south = calculate_stats_for_acres('South')
    if installed_acres_south or uninstalled_acres_south:
        type_installed_south = 'Acres Installed South'
        type_uninstalled_south = 'Acres Uninstalled South'
        print("\t Updating Stats for South")
        dml_statement = (
                "update `" + stats_dataset + "`" + " set acres = " + str(installed_acres_south)
                + " where type = '" + type_installed_south + "'")
        DBWriter.dbwriter.run_dml(dml_statement)
        dml_statement = (
                "update `" + stats_dataset + "`" + " set acres = " + str(uninstalled_acres_south)
                + " where type = '" + type_uninstalled_south + "'")
        DBWriter.dbwriter.run_dml(dml_statement)
    else:
        print("\t Incurred an error, not updating stats for South")


def get_crop_image(crop_type):
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
    hemp = defUrl + 'mvdZtjV.png'
    tangerines = defUrl + 'Fs8edNK.png'
    squash = defUrl + 'siBofu6.png'
    cherry = defUrl + 'LqbA0Ie.png'
    onion = defUrl + '8wCFjgF.png'
    watermelon = defUrl + 'V9V5jAN.png'
    corn = defUrl + 'V9V5jAN.png'
    default = defUrl + 'B2coKxO.png'

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
        'watermelons': watermelon
    }.get(crop_type, default)


def update_portal_table_value(dbw, dataset_id, column_name, value_name, processed_portal_data, project):
    print('\t\t\t Data already found in table')
    print('\t\t\t Updating table values')

    # DML statement doesn't like None for update, so we changed to 'null'
    if processed_portal_data["si_num"] is None:
        processed_portal_data["si_num"] = 'null'
    if processed_portal_data["soil_moisture_num"] is None:
        processed_portal_data["soil_moisture_num"] = 'null'
    dml = "UPDATE " + dataset_id + " as t SET t.order = " + str(processed_portal_data["order"]) \
          + ", t.soil_moisture_num = " + str(processed_portal_data["soil_moisture_num"]) \
          + ", t.soil_moisture_desc = '" + str(processed_portal_data["soil_moisture_desc"]) + "'" \
          + ", t.si_num = " + str(processed_portal_data["si_num"]) \
          + ", t.si_desc = '" + str(processed_portal_data["si_desc"]) + "'" \
          + ", t.report = '" + str(processed_portal_data["report"]) + "'" \
          + ", t.preview = '" + str(processed_portal_data["preview"]) + "'" \
          + ", t.field = '" + str(processed_portal_data["field"]) + "'" \
          + " WHERE t." + column_name + " = '" + str(value_name) + "'"
    print('\t\t\t  - ' + str(dml))
    dbw.run_dml(dml, project=project)


def add_fields_to_technician_portal():
    north_field_list = get_current_fields_from_technician_portal(region='North')
    south_field_list = get_current_fields_from_technician_portal(region='South')
    technician_portal_field_list = north_field_list + south_field_list
    growers = Decagon.open_pickle()
    for grower in growers:
        for field in grower.fields:
            # If nickname is not the same as field name, we need to make sure it doesn't get added to the database by creating a grower + nickname variable
            grower_field_nickname = grower.name + field.nickname
            if (field.name not in technician_portal_field_list and grower_field_nickname not in technician_portal_field_list) and field.active:
                # print(grower.name, ";", field.nickname, ";", field.loggers[-1].acres)
                # print(f"{field.grower.name}: {field.grower.region}")
                print(f"Adding new field to technician portal: {field.name}")
                write_new_field_to_technician_database(field)
                print(f"Done writing to field.")


def get_current_fields_from_technician_portal(region):
    dataset = "stomato-info." + 'uninstallation_progress' + "." + region
    field_list = []
    # noinspection SqlResolve
    dml_statement_InstalledAcres = ("Select Grower, Field from `" + dataset + "`")
    # print(dml_statement_InstalledAcres)
    query_result = DBWriter.dbwriter.run_dml(dml_statement_InstalledAcres)
    for installed_field in query_result:
        grower = installed_field.Grower
        field = installed_field.Field
        # print(grower + field)
        field_list.append(grower + field)
    return field_list


def write_new_field_to_technician_database(field):
    # print(field.grower.name, ";", field.nickname, ";", field.acres, ";", field.loggers[-1].lat, ";", field.loggers[-1].long, ";", len(field.loggers))
    uninstallation_progress_dataset = f"stomato-info.uninstallation_progress.{field.grower.region}"
    lat_long = str(field.lat) + "," + str(field.long)
    dml_statement_add_new_field = (
        f"insert into {uninstallation_progress_dataset}(Grower, Field, Acres, Latt_Long, Number_Of_Loggers) "
        f"values ('{field.grower.name}','{field.nickname}',{field.acres},'{lat_long}',{len(field.loggers)})")
    # print(dml_statement_add_new_field)
    field.dbwriter.run_dml(dml_statement_add_new_field)


# add_fields_to_technician_portal()
# dbw = DBWriter.DBWriter()
#
# print("#!/bin/sh")
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
#         if f.name == "Shiraz RanchS6 Pistachios":
#             g.region = 'South'
# Decagon.write_pickle(growers)
            # print(g.region)
#         if f.active:
#             name = dbw.remove_unwanted_chars_for_db(f.name)
#             # print(name)
#             print("""
# export SOURCE_DATASET="stomato:""" + name + """"
# export DEST_PREFIX="stomato-permanents:""" + name + """."
# for f in `bq ls -n 20 $SOURCE_DATASET |grep TABLE | awk '{print $1}'`
# do
#   export CLONE_CMD="bq --nosync cp $SOURCE_DATASET.$f $DEST_PREFIX$f"
#   echo $CLONE_CMD
#   echo `$CLONE_CMD`
# done """)

#             dbw.create_dataset(name, project='stomato-permanents')
#         if f.nickname == '3101-3103':
#             f.acres = float(input("Enter acres for: \n" + f.nickname + "\n"))
#             print(f.acres)
# Decagon.write_pickle(growers)
# growers = Decagon.open_pickle()
# for g in growers:
#     for f in g.fields:
# if f.nickname == 'KC 307/307A' or f.nickname == 'NC 310/310A' or f.nickname == 'KC 306' or f.nickname == '3101-3103' \
#         or f.nickname == 'Dates Block 36 DB' or f.nickname == 'Dates' or f.nickname == 'R12-13' or f.nickname == 'F7':
#     acres = input(f'Enter total acres for {f.nickname}: ')
#     f.acres = str(acres)
# f.acres = f.loggers[-1].acres
# delattr(f, "totalAcres")
# if g.name == 'Bone Farms LLC':
#         #     if f.nickname == 'F7' or f.nickname == 'N42 N43':
#         #         acres = input(f'Enter total acres for {f.nickname}: ')
#         #         f.totalAcres = str(acres)
#         print(f"{g.name};{f.nickname};{f.acres}")
# Decagon.write_pickle(growers)
# removeFields()
# update_stats_for_acres()
# Decagon.show_pickle()
# removeFields()
