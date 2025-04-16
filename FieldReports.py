import os
from calendar import month_name
from collections import defaultdict, OrderedDict
from datetime import timedelta, datetime

import pandas as pd
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

import Decagon
import GSheetCredentialSevice
import ReportCharts
import SQLScripts
import Soils
from GoogleDocsAPI import upload_image_to_drive, insert_into_table_row, insert_image_into_table_cell, header_with_image, \
    append_to_header_center_aligned, make_copy_doc
from gSheetReader import getServiceRead, getColumnHeader

# Scopes for Google Drive and Docs APIs
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents']
SHEET_ID = "1X07t-2B8PU4o-PDSNV0jVdiG4eHjqnAnQ1ROOjVm1y4"


def get_credentials():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
                print("Credentials refreshed successfully.")
            except Exception as e:
                print(f"Error refreshing credentials: {e}")
        else:
            print("Starting reauthentication flow...")
            flow = InstalledAppFlow.from_client_secrets_file(
                'google-docs-creds.json', SCOPES
            )
            creds = flow.run_local_server(port=8080, access_type='offline', prompt='consent')

        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
            print("Saved new credentials to token.json")

    return creds


def get_vwc_data(table_results):
    """
    Grabs all VWC data points from VWC_2 and VWC_3, categorizes them in their moisture descriptions,
    and averages the two depths.
    :return: Dictionary of averaged VWC data
    """
    columns = ['vwc_2', 'vwc_3']
    range_descrips = {'Very Low Moisture': 0, 'Low Moisture Levels': 0, 'Below Optimum': 0,
                      'Optimum Moisture': 0, 'High Soil Moisture': 0, 'Very High Soil Moisture': 0}

    total_fc = 0
    total_wp = 0
    logger_count = 0
    soil = Soils.Soil(soil_type='Clay')  # initialize a default
    # Process each column (vwc_2 and vwc_3)
    for column in columns:
        for logger in table_results:
            fc = table_results[logger]['field_capacity'][-1]  # get last fc
            wp = table_results[logger]['wilting_point'][-1]

            total_fc += fc
            total_wp += wp
            logger_count += 1

            soil = Soils.Soil(field_capacity=fc, wilting_point=wp)

            for vwc in table_results[logger][column]:
                vwc_range = soil.find_vwc_range_description(vwc)
                if vwc_range in range_descrips:
                    range_descrips[vwc_range] += 1

    avg_fc = total_fc / logger_count if logger_count > 0 else 0
    avg_wp = total_wp / logger_count if logger_count > 0 else 0

    # Create a Soil object with the averaged fc and wp
    averaged_soil_type = soil.soil_type_lookup(avg_fc, avg_wp)
    new_soil = Soils.Soil(soil_type=averaged_soil_type)
    # Average the counts for the two columns
    averaged_range_descrips = {key: value / len(columns) for key, value in range_descrips.items()}

    return averaged_range_descrips, new_soil


def get_psi_categorized_data(table_results):
    """

    :return: Dictionary of PSI data
    """
    column = 'psi'
    psi_ranges = {
        'Under 0.5 PSI': 0,
        'Under 0.75 PSI': 0,
        'Under 1.0 PSI': 0,
        'Under 1.25 PSI': 0,
        'Under 1.6 PSI': 0,
        'Under 2.2 PSI (Threshold)': 0,
        'Above 2.2 PSI (Critical)': 0
    }

    for logger in table_results:
        for psi in table_results[logger][column]:
            psi_range = find_psi_range_description(psi)
            if psi_range in psi_ranges and psi_range is not None:
                psi_ranges[psi_range] += 1

    # Average the counts for the two columns
    # averaged_range_descrips = {key: value / len(column) for key, value in psi_ranges.items()}

    return psi_ranges


def find_psi_range_description(psi):
    if psi is None:
        return None
    elif 0 <= psi < 0.5:
        return 'Under 0.5 PSI'
    elif 0.5 <= psi < 1.0:
        return 'Under 1.0 PSI'
    elif 1.0 <= psi < 1.6:
        return 'Under 1.6 PSI'
    elif 1.6 <= psi < 2.2:
        return 'Under 2.2 PSI (Threshold)'
    elif 2.2 <= psi < 6.0:
        return 'Above 2.2 PSI (Critical)'


def get_loggers_total_per_day(table_results, column):
    """
    Adds the loggers' values for a day and places that value into a new list.
    For example, if on May 22, two loggers had irr hours of 5.0 and 6.0, the new list would hold 11.0 for May 22.

    :param table_results: A dictionary where the key is the logger identifier, and the value is a dictionary containing lists for 'date' and the specified column.
    :param column: The specific column to sum the values for (e.g., 'daily_hours').
    :return: A dictionary where the key is the date and the value is the total summed value for that day across all loggers.
    """
    summed_values_per_day = {}

    # Iterate through each logger in the results
    for logger in table_results:
        # Get the list of values and the corresponding dates
        values = table_results[logger][column]
        dates = table_results[logger]['date']

        # Ensure that both lists have the same length
        if len(values) != len(dates):
            raise ValueError(f"Mismatch in length between values and dates for logger {logger}")

        # Loop through each date and value, summing the values for each date
        for i in range(len(dates)):
            date = dates[i]
            value = values[i]

            # Skip None values
            if value is None:
                continue

            # Sum values for the same date
            if date in summed_values_per_day:
                summed_values_per_day[date] += value
            else:
                summed_values_per_day[date] = value

    return summed_values_per_day


def get_total_sum_column(table_results, column):
    """
    Gets the total summed amount of a columns data points
    :param table_results:
    :param column:
    :return:
    """
    total_value = 0
    for logger in table_results:
        values = table_results[logger][column]
        filtered_vals = [x for x in values if x is not None]
        if filtered_vals:
            total_value += sum(filtered_vals)
    return total_value


def get_column_averaged(table_results, column):
    """
    Gets the total summed amount of a columns data points
    :param table_results:
    :param column:
    :return:
    """
    total_value = 0
    for logger in table_results:
        values = table_results[logger][column]
        filtered_vals = [x for x in values if x is not None]
        if filtered_vals:
            total_value += sum(filtered_vals)
    return total_value / len(table_results)


def get_et_hours(table_results, column='et_hours'):
    """
    Gets the total summed amount of a columns data points
    :param table_results:
    :param column:
    :return:
    """
    total_value = 0
    for logger in table_results:
        values = table_results[logger][column]
        filtered_vals = [x for x in values if x is not None]
        if filtered_vals:
            total_value += sum(filtered_vals)
            break
    return total_value


def get_each_logger_column(table_results, column):
    """
    :param table_results:
    :param column:
    :return:
    """
    loggers = {}
    for logger in table_results:
        values = table_results[logger][column]
        filtered_vals = [x for x in values if x is not None]
        loggers[logger] = filtered_vals
    return loggers


def get_psi_values(table_results):
    """
    This function takes in a dictionary of loggers, calculates the average psi values at each index,
    and handles cases where psi values are None by excluding them from the average calculation.
    The function returns a list of averaged psi values, excluding indices with no valid data.
    :return: list
    """

    column = 'psi'
    num_loggers = len(table_results)
    if num_loggers < 1:
        raise ValueError("There should be at least one logger in the table_results.")

    # Find the maximum length of the psi column across all loggers
    max_length = max(len(table_results[logger][column]) for logger in table_results)

    # Initialize the combined_psi and count_non_none lists with zeros
    combined_psi = [0] * max_length
    count_non_none = [0] * max_length

    # Iterate over all loggers and sum their psi values, disregarding None values
    for logger in table_results:
        psi_values = table_results[logger][column]
        for i, psi in enumerate(psi_values):
            if psi is not None:
                combined_psi[i] += psi
                count_non_none[i] += 1

    # Calculate the average by dividing the summed psi values by the count of non-None values
    averaged_psi = []
    for i in range(max_length):
        if count_non_none[i] > 0:  # No nones
            averaged_psi.append(combined_psi[i] / count_non_none[i])

    return averaged_psi


def get_daily_average_value(table_results, column):
    """
    Averages the values by the amount of days then averages the two loggers average
    :param table_results:
    :param column:
    :return:
    """
    average = 0
    for logger in table_results:
        value = table_results[logger][column]
        filtered_vals = [x for x in value if x is not None]
        if filtered_vals:
            average += sum(filtered_vals) / len(filtered_vals)  # Average each day
    return average / len(table_results)  # Average the sum of each average


def get_loggers_averaged(table_results, column):
    """
    Adds the sum of each loggers values for the season and averages them instead of averaging per day
    :param table_results:
    :param column:
    :return: num
    """
    average = 0
    for logger in table_results:
        value = table_results[logger][column]
        filtered_vals = [x for x in value if x is not None]
        if filtered_vals:
            average += sum(filtered_vals)
    return average / len(table_results)  # Average the sum of each total value


def get_average_time(table_results):
    column = 'time'
    total_time = timedelta()
    count = 0

    for logger in table_results:
        times = table_results[logger][column]
        filtered_times = [x for x in times if x is not None]

        for time_str in filtered_times:
            # Convert the time string to a datetime object
            time_obj = datetime.strptime(time_str, '%I:%M %p')
            total_time += timedelta(hours=time_obj.hour, minutes=time_obj.minute)
            count += 1

    if count == 0:
        return None

    # Calculate the average time
    average_time = total_time / count

    # Round to the nearest hour
    rounded_hours = round(average_time.total_seconds() / 3600)

    # Convert rounded hours to a datetime object
    rounded_time = datetime(1, 1, 1) + timedelta(hours=rounded_hours)

    # Format the rounded time in '05:00 PM' format
    return rounded_time.strftime('%I:%M %p')


def get_monthly_column(table_results, column, average=False):
    """
    Gets and either adds up or averages a column's daily data, grouping it by month.

    :param table_results: The table results containing 'daily_hours', 'vpd', and 'date'.
    :param column: The column to process (e.g., 'daily_hours').
    :param average: Boolean flag to determine if the values should be averaged instead of summed.
    :return: An ordered dictionary of months and their corresponding total or average values.
    """
    column_date = 'date'
    current_year = datetime.now().year  # Get the current year

    # Initialize a defaultdict for storing monthly sums and counts
    monthly_values = defaultdict(float)
    monthly_counts = defaultdict(int)

    for logger in table_results:
        logger_hours = table_results[logger][column]
        logger_dates = table_results[logger][column_date]

        for date, inches in zip(logger_dates, logger_hours):
            if inches is not None and date.year == current_year:  # Filter for the current year
                # Parse the date and extract the month name
                month = date.strftime('%B')
                # Sum hours for each month or accumulate values for averaging
                monthly_values[month] += inches
                monthly_counts[month] += 1

    # Prepare the final ordered dictionary
    ordered_months = OrderedDict()
    for month in month_name[1:]:  # Skip the first item which is an empty string
        if month in monthly_values:
            if average and monthly_counts[month] > 0:
                # Calculate the average
                ordered_months[month] = monthly_values[month] / monthly_counts[month]
            else:
                # Use the sum
                ordered_months[month] = monthly_values[month]

    return ordered_months


def get_monthly_column_loggers(table_results, column, average=False):
    """
    Gets and either adds up or averages a column's daily data, grouping it by month, and returns
    a nested dictionary where each logger is a key.

    :param table_results: The table results containing 'daily_hours', 'vpd', and 'date'.
    :param column: The column to process (e.g., 'daily_hours').
    :param average: Boolean flag to determine if the values should be averaged instead of summed.
    :return: A nested dictionary with each logger as a key and each month as a nested key,
             containing either the total or average value.
    """
    column_date = 'date'
    current_year = datetime.now().year

    # Initialize the main dictionary to hold logger data
    results = {}

    for logger in table_results:
        logger_hours = table_results[logger][column]
        logger_dates = table_results[logger][column_date]

        # Initialize dictionaries for monthly sums and counts for this logger
        monthly_values = defaultdict(float)
        monthly_counts = defaultdict(int)

        for date, inches in zip(logger_dates, logger_hours):
            if inches is not None and date.year == current_year:  # Filter for the current year
                # Parse the date and extract the month name
                month = date.strftime('%B')
                # Sum or accumulate values for averaging
                monthly_values[month] += inches
                monthly_counts[month] += 1

        # Prepare the final ordered dictionary for this logger
        ordered_months = OrderedDict()
        for month in month_name[1:]:  # Skip the first item which is an empty string
            if month in monthly_values:
                if average and monthly_counts[month] > 0:
                    # Calculate the average
                    ordered_months[month] = monthly_values[month] / monthly_counts[month]
                else:
                    # Use the sum
                    ordered_months[month] = monthly_values[month]

        # Add this logger's monthly data to the main results dictionary
        results[logger] = ordered_months

    return results


def get_max_column(table_results, column):
    """
    Finds the max value of all days of the column
    :param table_results:
    :return:
    """
    # Need to check both loggers
    max_temp = 0
    for logger in table_results:
        temp = table_results[logger][column]
        new_max_temp = max(temp)
        # If one logger has higher temp than the other
        if new_max_temp > max_temp:
            max_temp = new_max_temp
    return max_temp


def get_max_column_with_date_and_time(table_results, column):
    """
    Finds the max value of all days of the specified column, the date it occurred, and the corresponding time.

    :param table_results: Dictionary with logger data including temperature, dates, and times.
    :param column: The column to find the max value from (e.g., 'air_temp').
    :return: Tuple containing max temperature, the date it occurred, and the corresponding time.
    """
    max_temp = float('-inf')  # Initialize to a very low number
    max_temp_date = None
    max_temp_time = None

    for logger in table_results:
        temp = table_results[logger][column]
        dates = table_results[logger]['date']
        times = table_results[logger]['time']

        for t, d, time in zip(temp, dates, times):
            if t is not None and t > max_temp:
                max_temp = t
                max_temp_date = d
                max_temp_time = time

    return max_temp, max_temp_date, max_temp_time


def get_column_value_on_date(table_results, target_date, column):
    """
    Finds the value of the specified column on a given date.

    :param table_results: Dictionary with logger data including temperature and dates.
    :param target_date: The date to find the value for (as a string in 'YYYY-MM-DD' format).
    :param column: The column to retrieve the value from (e.g., 'psi').
    :return: List of values from the specified column on the given date.
    """
    values_on_date = 0

    for logger in table_results:
        temp_values = table_results[logger][column]
        dates = table_results[logger]['date']

        for value, date in zip(temp_values, dates):
            if date == target_date:
                values_on_date = value

    return values_on_date


def get_start_end_dates(table_results):
    start_date = datetime(2222, 1, 1).date()
    end_date = datetime(1999, 1, 1).date()

    for logger in table_results:
        # Filter out None values before calling min and max
        valid_dates = [date for date in table_results[logger]['date'] if date is not None]

        if valid_dates:
            new_start_date = min(valid_dates)
            if new_start_date < start_date:
                start_date = new_start_date

            new_end_date = max(valid_dates)
            if new_end_date > end_date:
                end_date = new_end_date

    return start_date, end_date


def days_above_100(table_results):
    """
    Finds the days above 100 degrees ambient temperature of both loggers
    :param table_results:
    :return:
    """
    column = 'ambient_temperature'
    # Need to check both loggers
    days_over = 0
    for logger in table_results:
        temp = table_results[logger][column]
        days_over = len([x for x in temp if x > 100.0])
    return days_over


def create_or_use_folder_and_move_document(docs_service, document_id, folder_name):
    """
    Creates a folder in the same parent folder as the specified document, or uses an existing folder,
    and moves the specified document into this folder.

    Parameters:
    document_id (str): The ID of the document to find its parent folder and move it to the new/existing folder.
    folder_name (str): The name of the folder to create or use if it already exists.

    Returns:
    str: The ID of the created or existing folder.
    """

    # Get the Drive API service
    service = docs_service

    # Retrieve the parent folder ID of the document
    file = service.files().get(fileId=document_id, fields='parents').execute()
    parent_folder_id = file.get('parents', [None])[0]

    if not parent_folder_id:
        raise ValueError("The specified document has no parent folder.")

    # Check if a folder with the specified name already exists in the parent folder
    query = f"'{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and name='{folder_name}'"
    result = service.files().list(q=query, fields='files(id, name)').execute()
    existing_folders = result.get('files', [])

    if existing_folders:
        # Folder already exists, use its ID
        folder_id = existing_folders[0]['id']
        print(f"Folder '{folder_name}' already exists with ID: {folder_id}")
    else:
        # Folder does not exist, create a new one
        folder_metadata = {
            'name': folder_name,
            'mimeType': 'application/vnd.google-apps.folder',
            'parents': [parent_folder_id]
        }
        folder = service.files().create(body=folder_metadata, fields='id').execute()
        folder_id = folder.get('id')
        print(f"Folder '{folder_name}' created with ID: {folder_id}")

    # Move the document into the folder
    try:
        # Remove the document from its current parent and add it to the new folder
        service.files().update(
            fileId=document_id,
            addParents=folder_id,
            removeParents=parent_folder_id,
            fields='id, parents'
        ).execute()
        print(f"Document with ID {document_id} moved to folder '{folder_name}' with ID: {folder_id}")
    except HttpError as error:
        print(f"An error occurred: {error}")
        return None

    return folder_id


def get_grower_field_row(sheet_id, range_name, grower_name, field, result=None):
    """
    Gets the row called 'Grower' in a sheet.

    :param result:
    :param sheet_id: The ID of the Google Sheet.
    :param range_name: The range name (or sheet name) to search in.
    :param grower_name: The name of the grower to match.
    :param field: Field object
    :return: List of rows matching the grower and field criteria.
    """

    # g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    # service = g_sheet.getService()
    #
    # # Read Google Sheet
    # result = getServiceRead(range_name, sheet_id, service)
    row_result = result['valueRanges'][0]['values']

    grower_header = getColumnHeader("Grower", row_result)
    field_header = getColumnHeader("Grower field #", row_result)
    our_nickname = getColumnHeader("Our Nickname", row_result)
    ms_field = getColumnHeader("MS Field - Variety", row_result)

    grower_field = []

    for row_index, row in enumerate(row_result):
        try:
            # Ignore header row and empty rows
            if row_index == 0 or not row[grower_header]:
                continue

            if grower_name == row[grower_header]:
                # Check if field.name_ms contains any substring that matches row[ms_field]
                ms_field_values = row[ms_field].split(",") if row[ms_field] else []
                name_ms_substrings = field.name_ms.split(",") if field.name_ms else []

                # Strip whitespace from substrings
                ms_field_values = [value.strip() for value in ms_field_values]
                name_ms_substrings = [substring.strip() for substring in name_ms_substrings]

                if (
                        field.nickname == row[our_nickname]
                        or field.nickname == row[field_header]
                        or any(substring in row[ms_field] for substring in name_ms_substrings)
                ):
                    grower_field.append(row)
        except IndexError:
            continue
        except Exception as err:
            print(f"Error on row {row_index}: {err}")
            continue

    return grower_field


def get_field_average(sheet_rows):
    """
    Calculate the average of specific fields from sheet rows.

    :param sheet_rows: A list of rows, where each row is a list of values.
    :return: A dictionary with the averages of 'net', 'paid', 'green', 'mold', and 'mot'.
    """
    net_acre = 0.0
    net_tons = 0.0
    paid_acres = 0.0
    paid_tons = 0.0
    green = 0.0
    mold = 0.0
    mot = 0.0
    acres = 0.0
    num_rows = len(sheet_rows)

    for row in sheet_rows:
        net_acre += float(row[18].replace(',', ''))
        net_tons += float(row[14].replace(',', ''))
        paid_acres += float(row[19].replace(',', ''))
        paid_tons += float(row[15].replace(',', ''))
        green += float(row[8].replace('%', ''))
        mold += float(row[7].replace('%', ''))
        mot += float(row[9].replace('%', ''))
        acres += float(row[17].replace('%', ''))

    #  averages
    if num_rows > 0:
        net_acres_weighted = net_tons / acres
        paid_acres_weighted = paid_tons / acres
        green_avg = green / num_rows
        mold_avg = mold / num_rows
        mot_avg = mot / num_rows
    else:
        net_acres_weighted = paid_acres_weighted = green_avg = mold_avg = mot_avg = 0

    return {
        'net_avg': net_acres_weighted,
        'paid_avg': paid_acres_weighted,
        'green_avg': green_avg,
        'mold_avg': mold_avg,
        'mot_avg': mot_avg
    }


def make_field_name(field_name):
    new_name = field_name
    if 'Field' in field_name:
        return new_name
    elif 'Block' in field_name:
        return new_name
    else:
        new_name = f'Field {field_name}'
    return new_name


def calculate_percent_difference(val1, val2):
    """Calculate the percentage difference between two values."""
    if val1 == 0 and val2 == 0:
        return 0
    return abs(val1 - val2) / ((val1 + val2) / 2) * 100


def check_significant_differences(loggers_hours, threshold=20):
    """
    Checks if any logger's total hours are significantly different from others.

    :param loggers_hours: Dictionary with logger names as keys and lists of hours as values.
    :param threshold: Percentage difference threshold for a significant difference.
    :return: List of logger pairs with significant differences.
    """
    # Calculate total hours for each logger
    total_hours = {logger: sum(hours) for logger, hours in loggers_hours.items()}

    # Get all logger names
    loggers = list(total_hours.keys())
    significant_diffs = []

    # Pairwise comparison of each logger
    for i in range(len(loggers)):
        for j in range(i + 1, len(loggers)):
            logger1, logger2 = loggers[i], loggers[j]
            diff = calculate_percent_difference(total_hours[logger1], total_hours[logger2])

            if diff >= threshold:
                significant_diffs.append((logger1, logger2, diff))
                print(f"Significant difference between {logger1} and {logger2}: {diff:.2f}%")

    return significant_diffs


def get_document_id(crop_type, version, logger_len):
    # Document IDs
    doc_ids = {
        "Tomatoes": {
            "v1": "1DnqzTxylzQ9ExuQYfQ04qjzWHTjyvgO1Ktj_iu6FgXc",
            "v2": {
                1: "1ri2g-0fwH0EFeCcMA0zRm3CSj_MjEL4tYnaGTIBgVy8",
                2: "1h0JSSaJn_oEOs0KnGoHoVtt1uZdoAcT2joEQy_KnKp8",
                3: "1LVkxftJ1BgDGZUJyd3pGbA0BPXf-QEntnmmLgY3oj24",
                4: "10WpDZIu_buZVzCanCRjuGhbrXLxrkvHgZTHQefYbKNM",
            },
        },
        "Default": {
            "v1": "1t0RtF6gMzwtbuG94aBeOhWo1s6zvbdYWZS8PPn2ILhg",
            "v2": {
                1: "1PGq3KUcUIJODi3tW9yQYoXizlQN79JD6_7ux6TFwXHc",
                2: "1S8G1lHQNK5rtUoCPSPrlb50dXyPzONx3UuPh8RyASYs",
                3: "1PKEgmFXiSjcK4AjL0gHYxhW6n3ZRZ2YQEg3AttZpHZ0",
                4: "1SYHXx_MRhZyGCrEdgn0YNMKnknO6eCKn6U365e_-hPg",
            },
        },
    }
    crop_docs = doc_ids.get(crop_type, doc_ids["Default"])
    if version == "v1":
        return crop_docs["v1"]
    elif version == "v2" and logger_len in crop_docs["v2"]:
        return crop_docs["v2"][logger_len]
    return None


def update_field_yields(sheet_id, growers, range_name='Sheet1'):
    """
    Updates the net_yield and paid_yield attributes of Field objects from Google Sheets.

    :param growers:
    :param sheet_id: Google Sheet ID containing yield data.
    :param range_name: The sheet range to search within.
    :param growers_pickle_path: Path to the pickle file storing grower objects.
    """
    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()

    # Read Google Sheet
    result = getServiceRead(range_name, sheet_id, service)
    for grower in growers:
        for field in grower.fields:
            # Fetch matching rows from Google Sheets

            sheet_rows = get_grower_field_row(sheet_id, range_name, grower.name, field, result=result)

            if sheet_rows:
                # Compute yield averages
                field_averages = get_field_average(sheet_rows)

                # Update field attributes
                field.net_yield = field_averages["net_avg"]
                field.paid_yield = field_averages["paid_avg"]

                print(f"Updated {field.name}: Net Yield = {field.net_yield}, Paid Yield = {field.paid_yield}")

    Decagon.write_pickle(data=growers, filename='pickle_pre_purge.pickle')

    print("✅ Field yield updates completed.")


def modify_doc(grower_name, field_name, version='v2'):
    """
    Main driver for filling in the doc template copy with field info
    :param grower_name:
    :param field_name:
    """
    creds = get_credentials()

    # Field info
    field = Decagon.get_field(field_name, grower_name)
    logger_len = len(field.loggers)
    logger_cardinals = [logger.logger_direction for logger in field.loggers]
    logger_names = [logger.name for logger in field.loggers]

    # Check logger hour percent diff
    table_results = SQLScripts.get_entire_table_points(field_name, this_year=True)
    loggers_hours = get_each_logger_column(table_results, 'daily_hours')

    # Create the Drive and Docs service objects
    drive_service = build('drive', 'v3', credentials=creds)
    docs_service = build('docs', 'v1', credentials=creds)

    # Main logic
    name = f"Field Report {field_name}"
    document_id = get_document_id(field.crop_type, version, logger_len)

    if document_id:
        new_doc_id = make_copy_doc(drive_service, document_id, name)
        document_id = new_doc_id

        if field.crop_type == "Tomatoes":
            # T STAR SHEET INFO

            range = "Sheet1"
            grower_field = get_grower_field_row(SHEET_ID, range, grower_name, field)
            field_averages = get_field_average(grower_field)
            net, paid, green, mold, mot = (
                field_averages["net_avg"],
                field_averages["paid_avg"],
                field_averages["green_avg"],
                field_averages["mold_avg"],
                field_averages["mot_avg"],
            )

        # Handle the file/move it to the corresponding folder
        create_or_use_folder_and_move_document(drive_service, document_id, grower_name)

    # Get VWC, Inches, PSI, Hours/VPD, Max Temp
    print('Getting table results')
    vwc_data_dict, soil = get_vwc_data(table_results)
    inches_averaged = get_column_averaged(table_results, 'daily_inches')
    inches = get_total_sum_column(table_results, 'daily_inches')  # List
    total_et_hours = get_et_hours(table_results, 'et_hours')
    total_irrigation_hours = get_total_sum_column(table_results, 'daily_hours')
    month_irrigation = get_monthly_column(table_results, 'daily_inches')  # dict of months inches summed
    month_psi = get_monthly_column(table_results, 'psi', average=True)  # dict of months psi average
    psi_values = get_psi_values(table_results)
    month_irr_loggers = get_monthly_column_loggers(table_results, 'daily_inches')

    # MAX VALUES ON MAX TEMP DAY
    max_temp, max_temp_date, peak_time_on_date = get_max_column_with_date_and_time(table_results, 'ambient_temperature')
    max_rh_on_date = get_column_value_on_date(table_results, max_temp_date, 'rh')
    max_vpd_on_date = get_column_value_on_date(table_results, max_temp_date, 'vpd')
    max_temp_date = max_temp_date.strftime('%m/%d')

    # AVERAGES
    # psi_ranges = get_psi_categorized_data(table_results)
    average_psi = get_daily_average_value(table_results, 'psi')
    average_time = get_average_time(table_results)
    average_temp = get_daily_average_value(table_results, 'ambient_temperature')
    average_rh = get_daily_average_value(table_results, 'rh')
    average_vpd = get_daily_average_value(table_results, 'vpd')
    average_hours = get_loggers_averaged(table_results, 'daily_hours')

    # CHARTS
    pie_image_path = ReportCharts.create_pie_chart(vwc_data_dict)
    psi_bucket_image_path = ReportCharts.create_alternative_psi_graph(psi_values, field.crop_type, average_psi)
    if version == 'v1':
        bar_chart_path = ReportCharts.create_bar_graph(month_irrigation, inches)
    else:
        bar_chart_path = ReportCharts.create_combined_bar_graph(month_irr_loggers)

    # Upload the image to Google Drive
    print('Uploading images to Google drive')
    pie_file_id = upload_image_to_drive(pie_image_path, drive_service)
    bar_file_id = upload_image_to_drive(bar_chart_path, drive_service)
    psi_bucket_file_id = upload_image_to_drive(psi_bucket_image_path, drive_service)
    crop_image_url = ''
    if field.crop_type == 'Tomatoes':
        tomato_path = 'Logos/tomato_image.png'
        tomato_id = upload_image_to_drive(tomato_path, drive_service)
        crop_image_url = f'https://drive.google.com/uc?id={tomato_id}'
    if field.crop_type == 'Almonds':
        almonds_path = 'Logos/almonds.png'
        almonds_id = upload_image_to_drive(almonds_path, drive_service)
        crop_image_url = f'https://drive.google.com/uc?id={almonds_id}'
    if field.crop_type == 'Pistachios':
        pistachio_path = 'Logos/pistachio.png'
        pistachio_id = upload_image_to_drive(pistachio_path, drive_service)
        crop_image_url = f'https://drive.google.com/uc?id={pistachio_id}'
    if field.crop_type == 'Corn':
        corn_path = 'Logos/corn.png'
        corn_id = upload_image_to_drive(corn_path, drive_service)
        crop_image_url = f'https://drive.google.com/uc?id={corn_id}'

    # SETUP HEADER
    nickname = make_field_name(field.nickname)
    start_date, end_date = get_start_end_dates(table_results)
    start_date = start_date.strftime('%B %d')
    end_date = end_date.strftime('%B %d')
    header_text_before = f'{field.grower.name}\n{nickname}\n'
    if field.grower.name == 'Kubo & Young':
        header_text_before = f'Kubo & Yeung\n{nickname}\n'
    header_text_after = f' - {field.acres} acres\n{start_date} - {end_date}'
    header_text2 = '2024 End of Year Report'

    # HEADER
    header_with_image(docs_service, document_id, header_text_before, field.crop_type, header_text_after, crop_image_url)
    append_to_header_center_aligned(docs_service, document_id, header_text2)

    # IRRIGATION
    et = f'{round(total_et_hours, 1)} Hours'
    acre_ft = round(inches / 12, 1)
    if version == 'v1':
        hours = f'{round(average_hours, 1)} Hours'
        bodies = [et, hours, acre_ft]
        insert_into_table_row(docs_service, document_id, bodies)
    elif version == 'v2':
        # Generate headers dynamically
        headers = ['ET Hours'] + logger_cardinals[:logger_len] + ['Acre/Ft']
        insert_into_table_row(docs_service, document_id, headers, row_index=0)

        # Generate data dynamically
        loggers_hours_data = [f"{round(sum(loggers_hours[logger_names[i]]), 1)} Hours" for i in range(logger_len)]
        data = [et] + loggers_hours_data + [acre_ft]
        insert_into_table_row(docs_service, document_id, data, row_index=1)

    # ENVIRONMENTAL DATA
    air_temp = f'{round(average_temp, 1)}°F'
    bodies = [average_time, air_temp, f'{round(average_rh, 1)}%', round(average_vpd, 1)]
    insert_into_table_row(docs_service, document_id, bodies, table_index=1)

    # HOTTEST DAY
    max_temp = f'{round(max_temp, 1)}°F'
    max_temp_day = f'{max_temp_date} - {peak_time_on_date}'
    bodies = [max_temp_day, max_temp, f'{round(max_rh_on_date, 1)}%', round(max_vpd_on_date, 1)]
    insert_into_table_row(docs_service, document_id, bodies, table_index=2)

    # YIELD DATA
    if field.crop_type == 'Tomatoes' and net != 0.0:
        net = f'{round(net, 1)} t/ac'
        paid = f'{round(paid, 1)} t/ac'
        green = f'{round(green, 1)}%'
        mold = f'{round(mold, 1)}%'
        mot = f'{round(mot, 1)}%'
        bodies = [net, paid, green, mold, mot]
        insert_into_table_row(docs_service, document_id, bodies, table_index=3)

    # IMAGES INTO TABLES
    insert_image_into_table_cell(docs_service, document_id, pie_file_id, table_index=5, row_index=1, height=255,
                                 width=285)
    insert_image_into_table_cell(docs_service, document_id, psi_bucket_file_id, table_index=5, row_index=1,
                                 cell_index=2, height=265)
    insert_image_into_table_cell(docs_service, document_id, bar_file_id, table_index=5, row_index=3, cell_index=1,
                                 height=215, width=435)

    # download_google_doc_as_docx(document_id, drive_service, f'{field_name}.docx')
    print('Data inserted into Google Doc')


def create_excel_with_loggers_hours(output_file="loggers_hours.xlsx"):
    """
    Creates an Excel file listing logger hours for each field and logger.
    :param output_file: Name of the output Excel file.
    """
    growers = Decagon.open_pickle()

    # Initialize a list to store data
    data = []

    # Iterate through growers
    try:
        for grower in growers:
            if 'Lucero' in grower.name:
                for field in grower.fields:
                    # Get the table results for the field
                    table_results = SQLScripts.get_entire_table_points(
                        field.name, this_year=True)

                    # Calculate the total loggers hours
                    logger_hours = get_each_logger_column(table_results, 'daily_hours')

                    # Append each logger's data to the data list
                    for logger, hours in logger_hours.items():
                        total_hours = sum(hours)
                        data.append({
                            "Grower": grower.name,
                            "Field Name": field.name,
                            "Logger": logger,
                            "Hours": total_hours
                        })
    except Exception as e:
        print(f"An error occurred: {e}")

    # Create a DataFrame from the data
    df = pd.DataFrame(data)

    # Expand the 'Hours' column so each hour appears in a separate row
    df = df.explode('Hours').reset_index(drop=True)

    # Write the DataFrame to an Excel file
    df.to_excel(output_file, index=False, engine='openpyxl')

    print(f"Excel file created: {output_file}")


def main():
    guille = ['Riley Chaney Farms', 'Carvalho', 'Ryan Jones', 'Andrew']
    redo = ['Turlock FruitCo']
    done = []
    # done = []
    num = 0
    not_done = []  # made it to knight farms index 24 of growers need to run the rest
    growers = Decagon.open_pickle()
    try:
        for grower in growers:
            if grower.name in redo:
                try:
                    done.append(grower.name)
                    for field in grower.fields:
                        # if field.name in redo:
                        try:
                            modify_doc(grower.name, field.name)
                            # else:
                            #     not_done.append(field.name)
                        except Exception as e:
                            print(f"Error processing field {field.name}: {e}")
                            not_done.append(field.name)  # append the field that had an error
                except Exception as e:
                    print(f"Error processing grower {grower.name}: {e}")
        print(done)
    except Exception as e:
        print(f"General error: {e}")
    print(f'Not done: {not_done}')


# create_excel_with_loggers_hours()
main()

# growers = Decagon.open_pickle('pickle_pre_purge.pickle')
# # update_field_yields(SHEET_ID, growers)
# total_net_yield = 0
# count = 0
# for grower in growers:
#     for field in grower.fields:
#         if field.net_yield is not None:
#             total_net_yield += field.net_yield
#             count += 1
#
# avg_net_yield = total_net_yield / count if count > 0 else 0
# print(avg_net_yield)
# 45.1 49, 52,