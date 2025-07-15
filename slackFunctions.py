import DBWriter
# import GSheetCredentialSevice
import SharedPickle
import gSheetReader


def change_logger_soil_type(logger_name: str, field_names: str, grower_name: str, new_soil_type: str):
    """
    Single function to change the soil type for a logger in both the pickle and the db

    :param logger_name:
    :param field_name:
    :param grower_name:
    :param new_soil_type:
    """
    print(f'Changing soil type for logger: {logger_name} to {new_soil_type}')

    growers = SharedPickle.open_pickle()
    dbw = DBWriter.DBWriter()
    crop_type = None
    old_soil_type = ''
    # Change soil type in the pickle
    print('-Changing soil type in the pickle')
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name in field_names:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            print('\tFound logger...changing')
                            old_soil_type = logger.soil.soil_type
                            logger.soil.set_soil_type(new_soil_type)
                            field_capacity = logger.soil.field_capacity
                            wilting_point = logger.soil.wilting_point
                            crop_type = logger.crop_type
    SharedPickle.write_pickle(growers)
    print('\tDone with pickle')

    # Change soil type parameters in the DB
    if crop_type:
        print('-Changing soil type in the db')
        field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field_names[0])
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
        print('Error: problem finding logger')
    return old_soil_type


def delete_psi_range(grower_name, field_name, logger_name, start_date, end_date):
    dbw = DBWriter.DBWriter()
    project = SharedPickle.get_project(field_name, grower_name)
    ds = dbw.remove_unwanted_chars_for_db_dataset(field_name)
    table = logger_name
    # only null psi/sdd/temperature between your dates
    dml = (
      f"UPDATE `{project}.{ds}.{table}` "
      f"SET psi=NULL, sdd=NULL, canopy_temperature=NULL "
      f"WHERE date BETWEEN '{start_date}' AND '{end_date}'"
    )
    try:
      dbw.run_dml(dml, project=project)
      # also update in-memory pickle
      for grower in SharedPickle.open_pickle():
        if grower.name==grower_name:
          for f in grower.fields:
            if f.name==field_name:
              for lg in f.loggers:
                if lg.name==logger_name:
                  # you could remove only those dates from lg.consecutive_ir_values,
                  # but simplest is to clear entirely
                  lg.ir_active=False
                  lg.consecutive_ir_values.clear()
      SharedPickle.write_pickle( SharedPickle.open_pickle() )
      return f"[{start_date}→{end_date}] Cleared PSI for {logger_name} in {field_name}"
    except Exception as e:
      return f"Error clearing {logger_name} in {field_name}: {e}"


def delete_psi_values_for_logger_and_range(
    grower_name: str,
    field_name:  str,
    logger_name: str,
    start_date:  str,
    end_date:    str,
    growers = None
) -> str:
    """
    Null out psi, sdd and canopy_temperature for a single logger
    between start_date and end_date (inclusive).

    :param growers: Pickle
    :param grower_name: name of the grower
    :param field_name:   name of the field
    :param logger_name:  name of the logger/table
    :param start_date:   'YYYY-MM-DD'
    :param end_date:     'YYYY-MM-DD'
    :returns:            a status message
    """
    if growers is None:
        growers = SharedPickle.open_pickle()
    dbw     = DBWriter.DBWriter()
    project = SharedPickle.get_project(field_name, grower_name)
    f_name = dbw.remove_unwanted_chars_for_db_dataset(field_name)
    table   = logger_name

    dml = f"""
      UPDATE `{project}.{f_name}.{table}`
      SET psi = NULL, sdd = NULL, canopy_temperature = NULL
      WHERE date BETWEEN '{start_date}' AND '{end_date}'
    """

    try:
        dbw.run_dml(dml, project=project)

        for g in growers:
            if g.name != grower_name:
                continue
            for f in g.fields:
                if f.name != field_name:
                    continue
                for lg in f.loggers:
                    if lg.name == logger_name:
                        lg.ir_active = False
                        lg.consecutive_ir_values.clear()

        SharedPickle.write_pickle(growers)

        return f"Cleared PSI for {logger_name} in {field_name} from {start_date} to {end_date}"

    except Exception as e:
        return f"Error clearing PSI for {logger_name}: {e}"


def bulk_toggle_psi(
    grower_name: str,
    field_names: list[str] | str,
    to_toggle: list[str],
    on_or_off: str
) -> list[str]:
    """
    Toggles IR (psi) for the given grower across all specified fields/loggers.
    Opens the pickle once, applies all updates in-memory so to save us time, writes once,
    and returns a list of status messages.
    """
    growers = SharedPickle.open_pickle()
    messages: list[str] = []
    # normalize fields to a list
    fields = field_names if isinstance(field_names, (list, tuple)) else [field_names]

    for g in growers:
        if g.name != grower_name:
            continue
        for f in g.fields:
            if f.name not in fields:
                continue
            for lg in f.loggers:
                if lg.name not in to_toggle:
                    continue
                # flip the switch
                lg.ir_active = (on_or_off == "on")
                verb = "On" if on_or_off == "on" else "Off"
                messages.append(f"Turned {verb} IR for {lg.name}")

    SharedPickle.write_pickle(growers)
    return messages


def delete_psi_values_for_specific_logger(grower_name, field_name, logger_name, growers=None):
    if growers is None:
        growers = SharedPickle.open_pickle()
    dbw = DBWriter.DBWriter()
    project = SharedPickle.get_project(field_name, grower_name, growers=growers)
    field_dataset = dbw.remove_unwanted_chars_for_db_dataset(field_name)
    dml = (f"UPDATE `{project}.{field_dataset}.{logger_name}` SET psi = NULL, sdd = NULL, canopy_temperature = NULL WHERE TRUE")
    try:
        dbw.run_dml(dml, project=project)
        # update pickle
        for grower in growers:
            if grower.name == grower_name:
                for field in grower.fields:
                    if field.name == field_name:
                        for logger in field.loggers:
                            if logger.name == logger_name:
                                logger.ir_active = False
                                # logger.consecutive_ir_values = deque()
        SharedPickle.write_pickle(growers)
        return f"Cleared PSI for {logger_name} in {field_name}"
    except Exception as e:
        return f"Error clearing {logger_name}: {e}"

def update_gpm_irrigation_acres_for_logger(
    grower_name, field_name, logger_name,
    new_gpm=None, new_irrigation_acres=None,
    growers=None
):
    """
    Update gpm and/or irrigation_acres for a single logger
    in both BigQuery and the pickle.
    """
    if new_gpm is None and new_irrigation_acres is None:
        return f"No updates specified for {logger_name} in {field_name}"

    # load or reuse pickle
    if growers is None:
        growers = SharedPickle.open_pickle()

    dbw = DBWriter.DBWriter()


    try:
        # dbw.run_dml(dml, project=project)
        for grower in growers:
            if grower.name != grower_name:
                continue
            for field in grower.fields:
                if field.name != field_name:
                    continue
                for logger in field.loggers:
                    if logger.name == logger_name:
                        if new_gpm is not None:
                            logger.gpm = float(new_gpm)
                        if new_irrigation_acres is not None:
                            logger.irrigation_set_acres = float(new_irrigation_acres)

        SharedPickle.write_pickle(growers)

        parts = []
        if new_gpm is not None:
            parts.append(f"GPM={new_gpm}")
        if new_irrigation_acres is not None:
            parts.append(f"Irr Acres={new_irrigation_acres}")
        return f"Updated {', '.join(parts)} for {logger_name} in {field_name}"
    except Exception as e:
        return f"Error updating {logger_name}: {e}"

def uninstall_field(fields, pickle = SharedPickle.open_pickle()):
    grower_pickle = pickle
    cleaned_split_field_names = fields
    dbw = DBWriter.DBWriter()
    YEAR = '2025'
    fields_uninstalled_list = []
    sheet_id = '1RUo7w9mOfzGCncDNIgUC_xvI3DrodITEyamcjottIL8'
    g_sheets_tab_name = "Uninstall"

    # g_sheet_credential = GSheetCredentialSevice.GSheetCredentialSevice()
    # service = g_sheet_credential.getService()
    #
    # result = gSheetReader.getServiceRead(g_sheets_tab_name, sheet_id, service)
    # g_sheet_row_results = result['valueRanges'][0]['values']

    # Assign indexes to columns
    # field_name_header = gSheetReader.getColumnHeader("Fields Uninstalled", g_sheet_row_results)
    # uninstall_done_header = gSheetReader.getColumnHeader("Uninstall Done", g_sheet_row_results)

    num_of_fields_to_uninstall = 0
    field_row_iterator = 0
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
        # target_cell = f'{g_sheets_tab_name}!D{field_row_iterator + 1}'
        # field_row_iterator += 1
        # # print(target_cell)
        # gSheetReader.write_target_cell(target_cell, True, sheet_id, service)
        print(f"\tFields Uninstalled Successfully - {fields_uninstalled_list}")

    SharedPickle.write_pickle(grower_pickle)
    print("Removed the following Fields: ", fields_uninstalled_list)
    print(f"{len(fields_uninstalled_list)} / {num_of_fields_to_uninstall} removed")
    return fields_uninstalled_list