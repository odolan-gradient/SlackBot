import os
from calendar import month_name
from collections import defaultdict, OrderedDict
from datetime import timedelta, datetime

import googleapiclient
import matplotlib.pyplot as plt
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaFileUpload

import Decagon
import GSheetCredentialSevice
import SQLScripts
import Soils
import gSheetReader

# Scopes for Google Drive and Docs APIs
SCOPES = ['https://www.googleapis.com/auth/drive', 'https://www.googleapis.com/auth/documents']


def get_credentials():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('google-docs-creds.json', SCOPES)
            creds = flow.run_local_server(port=8080)

        # Save the credentials for the next run
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


def get_t_star_sheet():
    g_sheet = GSheetCredentialSevice.GSheetCredentialSevice()
    service = g_sheet.getService()
    range_name = 'Sheet1'
    sheet_id = '1X07t-2B8PU4o-PDSNV0jVdiG4eHjqnAnQ1ROOjVm1y4'
    result = gSheetReader.getServiceRead(range_name, sheet_id, service)
    row_result = result['valueRanges'][0]['values']

    for index, row in enumerate(row_result):
        if index == 0:
            continue


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


def get_total_column(table_results, column):
    """
    Gets the total summed amount of a columns data points
    :param table_results:
    :param column:
    :return:
    """
    total_value = 0
    for logger in table_results:
        values = table_results[logger][column]
        for value in values:
            if value is not None:
                total_value += value
    return total_value


def get_psi_values(table_results):
    """
    This function takes in a dictionary of loggers, calculates the average psi values at each index,
    and handles cases where psi values are None by excluding them from the average calculation.
    The function returns a list of averaged psi values, excluding indices with no valid data.
    """

    column = 'psi'
    num_loggers = len(table_results)
    if num_loggers < 1:
        raise ValueError("There should be at least one logger in the table_results.")

    # Initialize the combined_psi list with zeros
    first_logger = list(table_results.keys())[0]
    combined_psi = [0] * len(table_results[first_logger][column])

    # A list to count how many valid psi values (non-None) exist at each index/each logger
    count_non_none = [0] * len(table_results[first_logger][column])

    # Iterate over all loggers and sum their psi values, disregarding None values
    for logger in table_results:
        psi_values = table_results[logger][column]
        for i, psi in enumerate(psi_values):
            if psi is not None:
                combined_psi[i] += psi
                count_non_none[i] += 1  # Increment the count for non-None values

    # Calculate the average by dividing the summed psi values by the count of non-None values
    averaged_psi = []
    for i in range(len(combined_psi)):
        if count_non_none[i] > 0:  # No nones
            averaged_psi.append(combined_psi[i] / count_non_none[i])

    return averaged_psi


def get_average_value(table_results, column):
    average = 0
    for logger in table_results:
        value = table_results[logger][column]
        filtered_vals = [x for x in value if x is not None]
        if filtered_vals:
            average += sum(filtered_vals) / len(filtered_vals)  # Add both loggers averge psi
    return average / 2  # Average the average


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

    # Initialize a defaultdict for storing monthly sums and counts
    monthly_values = defaultdict(float)
    monthly_counts = defaultdict(int)

    for logger in table_results:
        logger_hours = table_results[logger][column]
        logger_dates = table_results[logger][column_date]

        for date, hours in zip(logger_dates, logger_hours):
            if hours is not None:  # Skip None values
                # Parse the date and extract the month name
                month = date.strftime('%B')
                # Sum hours for each month or accumulate values for averaging
                monthly_values[month] += hours
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
            if t > max_temp:
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
    start_date = datetime(2222, 1, 1, 0, 0, 0)
    start_date = start_date.date()
    end_date = datetime(1999, 1, 1, 0, 0, 0)
    end_date = end_date.date()
    for logger in table_results:
        new_start_date = min(table_results[logger]['date'])
        if new_start_date < start_date:
            start_date = new_start_date
        new_end_date = max(table_results[logger]['date'])
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


def create_pie_chart(data_dict):
    # TODO colors should match our VWC chart
    # Filter out zero values
    labels = [label for label, size in data_dict.items() if size > 0]
    sizes = [size for size in data_dict.values() if size > 0]

    # Manually define a color gradient from green to red
    colors = [
        '#008000',  # Green
        '#66b266',  # Light Green
        '#cccc66',  # Yellowish Green
        '#ffcc66',  # Light Orange
        '#ff6666',  # Light Red
        '#ff0000',  # Red
        '#990000'  # Dark Red
    ]

    # Adjust the number of colors to match the number of labels
    colors = colors[:len(labels)]

    # Create the pie chart without labels
    wedges, texts, autotexts = plt.pie(sizes, colors=colors, autopct='%1.1f%%', startangle=90)

    # Add a legend
    plt.legend(wedges, labels, title="Categories", loc="center left", bbox_to_anchor=(1, 0, 0.5, 1))

    plt.axis('equal')  # Makes the pie chart a circle
    plt.title('Soil Moisture Distribution')

    # Save the chart as an image file
    image_path = 'pie_chart.png'
    plt.savefig(image_path, dpi=300, bbox_inches='tight')
    plt.close()

    return image_path


def create_bar_graph(month_data, title):
    """
    Creates a bar graph using Matplotlib with the given month data and title.
    Values are displayed inside the bars, and x-axis labels are removed.

    :param month_data: Dictionary where keys are month names and values are numbers.
    :param title: Title for the bar graph.
    """
    # Extract months and numbers from the dictionary
    months = list(month_data.keys())
    values = list(month_data.values())

    # Create the bar graph
    plt.figure(figsize=(10, 6))
    bars = plt.bar(months, values, color='skyblue')

    # Add title and labels
    plt.title(title, fontsize=16)
    plt.ylabel('Values', fontsize=14)

    # Remove x-axis labels
    plt.xticks([])

    # Add gridlines for better readability
    plt.grid(axis='y', linestyle='--', alpha=0.7)

    # Add value labels inside the bars
    for bar in bars:
        height = bar.get_height()
        plt.text(bar.get_x() + bar.get_width() / 2., height / 2,
                 f'{height:.0f}',
                 ha='center', va='center', fontweight='bold')

    # Adjust layout to prevent clipping of labels
    plt.tight_layout()

    # Save the chart as an image file
    image_path = 'bar_chart.png'
    plt.savefig(image_path, dpi=300, bbox_inches='tight')
    plt.close()

    return image_path


def create_combined_bar_line_graph(bar_values, line_values, bar_label, line_label, title):
    """
    Creates a combined bar and line graph with dual y-axes.

    :param bar_values: List of values for the bar chart.
    :param line_values: List of values for the line chart.
    :param bar_label: Label for the bar chart data (y-axis label).
    :param line_label: Label for the line chart data (y-axis label).
    :param title: Title of the graph.
    """

    # Convert OrderedDicts to lists if necessary
    if isinstance(bar_values, OrderedDict):
        bar_values = list(bar_values.values())
    if isinstance(line_values, OrderedDict):
        line_values = list(line_values.values())

    # Set up the figure and axis with larger size
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Create the bar chart on ax1
    ax1.bar(range(len(bar_values)), bar_values, color='b', alpha=0.6)
    ax1.set_xlabel('Data Points')
    ax1.set_ylabel(bar_label, color='b')
    ax1.tick_params(axis='y', labelcolor='b')

    # Add gridlines for better readability
    ax1.grid(True, which='both', linestyle='--', linewidth=0.5)

    # Create a secondary y-axis for the line chart
    ax2 = ax1.twinx()
    ax2.plot(range(len(line_values)), line_values, color='y', marker='o', markersize=5)
    ax2.set_ylabel(line_label, color='y')
    ax2.tick_params(axis='y', labelcolor='y')

    # Remove the x-axis labels to reduce clutter
    ax1.set_xticks([])

    # Add a title
    plt.title(title)

    # Save the chart as an image file
    image_path = 'bar_chart.png'
    plt.savefig(image_path, dpi=300, bbox_inches='tight')
    plt.close()

    return image_path


def create_psi_bucket_graph_with_percentages(psi_values):
    # TODO: this is for tomatoes not permanents
    # Define the PSI ranges and corresponding labels
    psi_ranges = [0, 0.5, 1.0, 1.6, 2.2, 3]
    labels = [
        '0.0 PSI',
        '0.5 PSI',
        '1.0 PSI',
        '1.6 PSI (Threshold)',
        '2.2 PSI (Critical)',
        'Above Critical PSI'
    ]

    # Define colors for the ranges from green to red
    colors = ['#008000', '#66b266', '#cccc66', '#ff6666', '#ff0000']

    # Map the actual PSI ranges to evenly spaced values
    num_ranges = len(psi_ranges) - 1
    evenly_spaced_y = list(range(num_ranges + 1))

    # Classify the PSI values into the correct ranges
    counts = [0] * num_ranges
    for psi in psi_values:
        for i in range(num_ranges):
            if psi_ranges[i] <= psi < psi_ranges[i + 1]:
                counts[i] += 1
                break

    # Calculate percentages for each range
    percentages = [count / len(psi_values) * 100 for count in counts]

    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(6, 8))

    # Draw horizontal lines for each evenly spaced range
    for i in range(1, len(evenly_spaced_y)):
        ax.hlines(y=evenly_spaced_y[i], xmin=0, xmax=1, color=colors[i - 1], linewidth=5, label=labels[i - 1])

    # Draw the "bucket" (vertical bar)
    for i in range(num_ranges):
        ax.barh(y=evenly_spaced_y[i], width=1, height=1,
                color=colors[i], edgecolor='black', align='edge', alpha=0.3)

    # Display the percentage text inside each section
    for i in range(num_ranges):
        ax.text(0.5, evenly_spaced_y[i] + 0.5, f'{percentages[i]:.1f}%',
                ha='center', va='center', fontsize=12, color='black', weight='bold')

    # Set the y-axis limits to fit the evenly spaced values
    ax.set_ylim(0, num_ranges)

    # Set the y-axis labels and ticks
    ax.set_yticks(evenly_spaced_y)
    ax.set_yticklabels(labels)

    # Set x-axis labels and remove ticks
    ax.set_xticks([])
    ax.set_xlabel('PSI Level')

    ax.set_title('PSI Bucket Visualization')

    # Add a legend
    ax.legend(loc='upper left', bbox_to_anchor=(1, 1), title='PSI Ranges')

    plt.tight_layout()

    # Save the chart as an image file
    image_path = 'bar_percent.png'
    plt.savefig(image_path, dpi=300, bbox_inches='tight')
    plt.close()
    return image_path


def create_vwc_bucket_chart(data_dict, value_ranges):
    # TODO the legend  is  upside down

    if len(value_ranges) != 2 * len(data_dict):
        raise ValueError("Length of value_ranges must be double the length of data_dict labels")

    labels = list(data_dict.keys())
    colors = ['#D32F2F', '#FF8F00', '#FFCC00', '#388E3C', '#FF8F00', '#D32F2F']

    value_ranges, labels, data_dict, colors = remove_duplicate_range_pairs(value_ranges, labels, data_dict, colors)

    total = sum(data_dict.values())
    percentages = [(value / total) * 100 if total > 0 else 0 for value in data_dict.values()]

    fig, ax = plt.subplots(figsize=(6, 8))

    # Draw each bar
    for i in range(len(labels)):
        lower_bound = value_ranges[2 * i]  # Start of the range
        upper_bound = value_ranges[2 * i + 1]  # End of the range
        height = upper_bound - lower_bound

        y_position = lower_bound  # Position based on lower bound

        # Draw the bars with the correct height
        ax.barh(y=y_position, width=1, height=height,
                color=colors[i], edgecolor='black', align='edge', alpha=0.3, label=labels[i])

        # Add the percentage text inside each bar
        ax.text(0.5, y_position + height / 2, f'{percentages[i]:.1f}%',
                ha='center', va='center', fontsize=12, color='black', weight='bold')

    # Adjust y-axis limits to match the exact range of the data
    ax.set_ylim(min(value_ranges), max(value_ranges))

    # Set the y-axis labels and ticks to align with the ranges
    ax.set_yticks([(value_ranges[2 * i] + value_ranges[2 * i + 1]) / 2 for i in range(len(labels))])
    ax.set_yticklabels(labels)

    # Set x-axis labels and remove ticks
    ax.set_xticks([])
    ax.set_xlabel('PSI Level')

    ax.set_title('Soil Moisture Distribution with Percentages')

    # Create custom legend with the colors
    handles = [plt.Rectangle((0, 0), 1, 1, color=colors[i], ec="black") for i in range(len(labels))]
    ax.legend(handles, labels, loc='upper left', bbox_to_anchor=(1, 1), title='Moisture Levels')

    plt.tight_layout()
    image_path = 'bucket_percent.png'
    plt.savefig(image_path, dpi=300, bbox_inches='tight')
    plt.close()
    return image_path


def remove_duplicate_range_pairs(value_ranges, labels, data_dict, colors):
    """
    This function removes duplicate range pairs and their associated labels, data, and colors
    from the provided lists and dictionary.

    :param value_ranges: A list of numeric ranges in pairs (e.g., [0, 25, 25, 29, ...]).
    :param labels: A list of labels corresponding to the ranges.
    :param data_dict: A dictionary with labels as keys and corresponding data as values.
    :param colors: A list of colors corresponding to each label.
    :return: Updated value_ranges, labels, data_dict, and colors with duplicates removed.
    """
    # Iterate over the range pairs
    i = 0
    while i < len(value_ranges) - 1:
        lower_bound = value_ranges[i]
        upper_bound = value_ranges[i + 1]

        # Check if the pair is a duplicate (both values are the same)
        if lower_bound == upper_bound:
            # Remove the range pair from value_ranges
            value_ranges = value_ranges[:i] + value_ranges[i + 2:]

            # Remove the corresponding label, data, and color
            label_to_remove = labels.pop(i // 2)
            del data_dict[label_to_remove]
            colors.pop(i // 2)

            # No need to increment i because the list has shrunk
        else:
            # Move to the next range pair
            i += 2

    return value_ranges, labels, data_dict, colors


def upload_image_to_drive(image_path, drive_service):
    file_metadata = {'name': os.path.basename(image_path)}
    media = MediaFileUpload(image_path, mimetype='image/png')
    file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    file_id = file.get('id')

    # Set the file to be publicly accessible
    permission = {
        'type': 'anyone',
        'role': 'reader',
    }
    drive_service.permissions().create(fileId=file_id, body=permission).execute()

    return file_id


def get_end_index(docs_service, document_id):
    """
    Retrieves the end index of the document to ensure content is added at the correct position.
    """
    document = docs_service.documents().get(documentId=document_id).execute()
    return document['body']['content'][-1]['endIndex']


def insert_image_into_doc(docs_service, document_id, file_id):
    image_url = f'https://drive.google.com/uc?id={file_id}'
    end_index = get_end_index(docs_service, document_id)
    requests = [
        {
            'insertInlineImage': {
                'location': {'index': end_index - 1},  # Insert at the end of the document
                'uri': image_url,
                'objectSize': {
                    'height': {'magnitude': 350, 'unit': 'PT'},
                    'width': {'magnitude': 350, 'unit': 'PT'}
                }
            }
        }
    ]
    docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()


def insert_text_into_doc(docs_service, document_id, header, body):
    end_index = get_end_index(docs_service, document_id)
    requests = [
        {
            'insertText': {
                'location': {'index': end_index - 1},
                'text': f"{header}\n\n"
            }
        },
        {
            'updateParagraphStyle': {
                'range': {
                    'startIndex': end_index - 1,
                    'endIndex': end_index - 1 + len(header) + 1
                },
                'paragraphStyle': {
                    'namedStyleType': 'HEADING_3'
                },
                'fields': 'namedStyleType'
            }
        },
        {
            'insertText': {
                'location': {'index': end_index - 1 + len(header) + 2},
                'text': f"{body}\n\n"
            }
        }
    ]
    docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()


def insert_three_columns(docs_service, document_id, headers, bodies, title):
    """

    :param docs_service: The Google Docs API service object.
    :param document_id: The ID of the Google Doc.
    :param headers: A list of three header strings.
    :param bodies: A list of three body strings or values corresponding to the headers.
    """
    # Ensure headers and bodies each have three elements
    if len(headers) != 3 or len(bodies) != 3:
        raise ValueError("Headers and bodies must each contain exactly three elements.")

    bodies = [str(body) for body in bodies]
    print(get_end_index(docs_service, document_id))
    index = get_end_index(docs_service, document_id) - 1
    requests = [

        {
            "insertTable":
                {
                    "rows": 2,
                    "columns": 3,
                    "location":
                        {
                            "index": index
                        }
                }
        },
        {
            "insertText":
                {
                    "text": bodies[2],
                    "location":
                        {
                            "index": index + 15  # 16
                        }
                }
        },
        {
            "insertText":
                {
                    "text": bodies[1],
                    "location":
                        {
                            "index": index + 13  # 14  # Each table cell is 2 away from the other?
                        }
                }
        },
        {
            "insertText":
                {
                    "text": bodies[0],
                    "location":
                        {
                            "index": index + 11  # 12
                        }
                }
        },  # START HEADERS
        {
            "insertText":
                {
                    "text": headers[2],
                    "location":
                        {
                            "index": index + 8  # 9
                        }
                }
        },
        {
            "insertText":
                {
                    "text": headers[1],
                    "location":
                        {
                            "index": index + 6  # 7
                        }
                }
        },
        {
            "insertText":
                {
                    "text": headers[0],
                    "location":
                        {
                            "index": index + 4  # 5
                        }
                }
        },
        # Insert the title
        {
            "insertText": {
                "text": title,
                "location": {
                    "index": index
                }
            }
        },
    ]

    # Execute the batchUpdate request to insert the text into the table
    docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
    print(get_end_index(docs_service, document_id))


def insert_into_second_row(docs_service, document_id, bodies, table_index=0):
    """
    Inserts values into the second row of a specified table in a Google Doc.

    :param docs_service: The Google Docs API service object.
    :param document_id: The ID of the Google Doc.
    :param bodies: A list of strings or values to insert into the table's second row.
    :param table_index: The index of the target table (0-based). Default is 0 (first table).
    """
    document = docs_service.documents().get(documentId=document_id).execute()

    # Find all tables in the document
    tables = [element for element in document['body']['content'] if 'table' in element]

    if not tables:
        raise ValueError("No tables found in the document.")

    if table_index >= len(tables):
        raise ValueError(
            f"Table index {table_index} is out of range. There are only {len(tables)} tables in the document.")

    table_element = tables[table_index]
    table_rows = table_element['table']['tableRows']

    if len(table_rows) < 2:
        raise ValueError(f"The table at index {table_index} does not have a second row.")

    second_row_cells = table_rows[1]['tableCells']

    if len(bodies) > len(second_row_cells):
        raise ValueError(
            f"Too many values to insert. The second row has {len(second_row_cells)} cells, but trying to insert {len(bodies)} values.")

    requests = []
    offset = 0  # Keep track of the cumulative offset due to insertions

    for i, body in enumerate(bodies):
        cell = second_row_cells[i]

        if not cell.get('content'):
            # If the cell is empty, create a new paragraph
            requests.append({
                'insertText': {
                    'location': {'index': cell['startIndex'] + offset},
                    'text': '\n'
                }
            })
            insert_index = cell['startIndex'] + offset + 1
            offset += 1  # Account for the newline character
        else:
            # Find the last paragraph in the cell
            last_paragraph = next(reversed([e for e in cell['content'] if 'paragraph' in e]), None)
            if last_paragraph:
                insert_index = last_paragraph['endIndex'] - 1 + offset
            else:
                insert_index = cell['startIndex'] + offset

        body_str = str(body)
        requests.append({
            'insertText': {
                'location': {'index': insert_index},
                'text': body_str
            }
        })
        offset += len(body_str)  # Increase the offset by the length of the inserted text

    try:
        result = docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
        print(f"Inserted {len(bodies)} values into the second row of table {table_index + 1}.")
        return result
    except googleapiclient.errors.HttpError as error:
        print(f"An error occurred: {error}")
        print(f"Error details: {error.content}")
        raise


def insert_image_into_table_cell(docs_service, document_id, file_id, table_index=0, row_index=0, cell_index=0):
    """
    Insert an image from Google Drive into a specific cell of a table in a Google Document.

    :param docs_service: The Google Docs API service object.
    :param document_id: The ID of the Google Doc.
    :param file_id: The ID of the image file in Google Drive.
    :param table_index: The index of the target table (0-based).
    :param row_index: The index of the target row (0-based).
    :param cell_index: The index of the target cell (0-based).
    """
    # Get the current document structure
    document = docs_service.documents().get(documentId=document_id).execute()

    # Find the specified table
    tables = [element for element in document['body']['content'] if 'table' in element]
    if not tables or table_index >= len(tables):
        raise ValueError("No tables found or table index out of range.")

    table_element = tables[table_index]
    table_rows = table_element['table']['tableRows']
    if row_index >= len(table_rows):
        raise ValueError("Row index out of range.")

    target_row_cells = table_rows[row_index]['tableCells']
    if cell_index >= len(target_row_cells):
        raise ValueError("Cell index out of range.")

    target_cell = target_row_cells[cell_index]

    # Ensure there's content in the cell
    if not target_cell['content']:
        # If the cell is empty, insert a paragraph first
        requests = [
            {
                'insertText': {
                    'location': {
                        'index': target_cell['startIndex']
                    },
                    'text': '\n'
                }
            }
        ]
        docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
        # Refresh the document to get the updated structure
        document = docs_service.documents().get(documentId=document_id).execute()
        target_cell = document['body']['content'][table_index]['table']['tableRows'][row_index]['tableCells'][
            cell_index]

    # Insert the image into the specified cell
    insert_index = target_cell['content'][0]['startIndex']
    requests = [
        {
            'insertInlineImage': {
                'location': {
                    'index': insert_index
                },
                'uri': f'https://drive.google.com/uc?id={file_id}',
                'objectSize': {
                    'height': {'magnitude': 300, 'unit': 'PT'},
                    'width': {'magnitude': 285, 'unit': 'PT'}
                }
            }
        }
    ]

    try:
        result = docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
        print("Image inserted successfully.")
    except googleapiclient.errors.HttpError as error:
        print(f"An error occurred: {error}")
        print(f"Error details: {error.content}")


def create_right_aligned_header(docs_service, document_id, header_text):
    """
    Creates a header in the Google Doc and writes right-aligned text into it with bold Arial 15 font.

    :param docs_service: The Google Docs API service object.
    :param document_id: The ID of the Google Doc.
    :param header_text: The text to insert into the header, aligned to the right.
    """
    # Retrieve the document to check if it already has headers
    doc = docs_service.documents().get(documentId=document_id).execute()

    # If there are no headers, create one
    if 'headers' not in doc:
        header_request = {
            'createHeader': {
                'type': 'DEFAULT'
            }
        }
        header_response = docs_service.documents().batchUpdate(
            documentId=document_id,
            body={'requests': [header_request]}
        ).execute()
        header_id = header_response['replies'][0]['createHeader']['headerId']
    else:
        header_id = list(doc['headers'].keys())[0]  # Use the existing header

    # Request to insert text into the header, align it to the right, and format it
    requests = [
        {
            'insertText': {
                'location': {
                    'segmentId': header_id,
                    'index': 0  # Start at the beginning of the header
                },
                'text': header_text
            }
        },
        {
            'updateParagraphStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': 0,
                    'endIndex': len(header_text)
                },
                'paragraphStyle': {
                    'alignment': 'END'
                },
                'fields': 'alignment'
            }
        },
        {
            'updateTextStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': 0,
                    'endIndex': len(header_text)
                },
                'textStyle': {
                    'bold': True,
                    'weightedFontFamily': {
                        'fontFamily': 'Arial'
                    },
                    'fontSize': {
                        'magnitude': 15,
                        'unit': 'PT'
                    }
                },
                'fields': 'bold,weightedFontFamily,fontSize'
            }
        }
    ]

    # Execute the batchUpdate request to apply the changes
    docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()


def append_to_header_center_aligned(docs_service, document_id, text_to_append):
    """
    Appends text to the last index of the existing header, center-aligned, in 19-point Arial Bold font.

    :param docs_service: The Google Docs API service object.
    :param document_id: The ID of the Google Doc.
    :param text_to_append: The text to append to the header.
    """
    # Get the current content of the header
    header_content = docs_service.documents().get(documentId=document_id, fields='headers').execute()

    # Check if headers exist
    if 'headers' not in header_content or not header_content['headers']:
        print("No headers found in the document.")
        return

    header_id = list(header_content['headers'].keys())[0]

    # Get the length of the current header content
    insert_index = header_content['headers'][header_id]['content'][-1]['endIndex'] - 1

    # Prepare the requests
    requests = [
        # Insert a newline character if the header is not empty
        {
            'insertText': {
                'location': {
                    'segmentId': header_id,
                    'index': insert_index
                },
                'text': '\n' if insert_index > 0 else ''
            }
        },
        # Insert the new text
        {
            'insertText': {
                'location': {
                    'segmentId': header_id,
                    'index': insert_index + (1 if insert_index > 0 else 0)
                },
                'text': text_to_append
            }
        },
        # Update the style of the entire inserted text
        {
            'updateTextStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': insert_index - 1,  # Start from the newline character
                    'endIndex': insert_index + len(text_to_append) + (1 if insert_index > 0 else 0)
                },
                'textStyle': {
                    'fontSize': {
                        'magnitude': 19,
                        'unit': 'PT'
                    },
                    'weightedFontFamily': {
                        'fontFamily': 'Arial'
                    },
                    'bold': True
                },
                'fields': 'fontSize,weightedFontFamily,bold'
            }
        },
        # Center-align the paragraph
        {
            'updateParagraphStyle': {
                'range': {
                    'segmentId': header_id,
                    'startIndex': insert_index + (1 if insert_index > 0 else 0),
                    'endIndex': insert_index + len(text_to_append) + (1 if insert_index > 0 else 0)
                },
                'paragraphStyle': {
                    'alignment': 'CENTER'
                },
                'fields': 'alignment'
            }
        }
    ]

    # Execute the batchUpdate request to apply the changes
    docs_service.documents().batchUpdate(documentId=document_id, body={'requests': requests}).execute()
    print(f"Text appended to header: '{text_to_append}'")


# Example usage:
# append_to_header_center_aligned(docs_service, 'your_document_id', 'New Centered Text')

def make_copy_doc(drive_service, source_doc_id, name):
    # Metadata for the new copy
    file_metadata = {
        'name': name,
        'mimeType': 'application/vnd.google-apps.document'
    }

    copied_file = drive_service.files().copy(
        fileId=source_doc_id,
        body=file_metadata,
        supportsAllDrives=True
    ).execute()

    # Get the ID of the new copy
    new_file_id = copied_file.get('id')
    print(f"New document created with ID: {new_file_id}")
    return new_file_id


def modify_doc():
    creds = get_credentials()
    field_name = 'Bays Ranch30'

    # Create the Drive and Docs service objects
    drive_service = build('drive', 'v3', credentials=creds)
    docs_service = build('docs', 'v1', credentials=creds)

    # document_id = '1mcTWM38oxH-S2JhfNqo9zP_QokKwaz2MDBMTiIewgXs' #  OG
    document_id = '1ri2g-0fwH0EFeCcMA0zRm3CSj_MjEL4tYnaGTIBgVy8'  # styled Javi version
    name = 'Field Report ' + field_name
    new_doc_id = make_copy_doc(drive_service, document_id, name)
    document_id = new_doc_id
    # Field info

    growers = Decagon.open_pickle()
    field = Decagon.get_field(field_name, 'Bays Ranch')
    planting_date = field.loggers[0].planting_date

    # Get VWC, Inches, PSI, Hours/VPD, Max Temp
    table_results = SQLScripts.get_entire_table_points([field_name])
    vwc_data_dict, soil = get_vwc_data(table_results)
    inches = get_total_column(table_results, 'daily_inches')  # List
    total_et_hours = get_total_column(table_results, 'et_hours')
    total_irrigation_hours = get_total_column(table_results, 'daily_hours')
    average_psi = get_average_value(table_results, 'psi')  # Num
    month_irrigation = get_monthly_column(table_results, 'daily_inches')  # dict of months inches summed
    month_psi = get_monthly_column(table_results, 'psi', average=True)  # dict of months psi average

    # MAX VALUES ON MAX TEMP DAY
    max_temp, max_temp_date, peak_time_on_date = get_max_column_with_date_and_time(table_results, 'ambient_temperature')
    max_rh_on_date = get_column_value_on_date(table_results, max_temp_date, 'rh')
    max_vpd_on_date = get_column_value_on_date(table_results, max_temp_date, 'vpd')
    max_temp_date = max_temp_date.strftime('%m/%d')

    # AVERAGES
    # over_100 = days_above_100(table_results)
    # psi_ranges = get_psi_categorized_data(table_results)
    # psi_values = get_psi_values(table_results)
    average_time = get_average_time(table_results)
    average_temp = get_average_value(table_results, 'ambient_temperature')
    average_rh = get_average_value(table_results, 'rh')
    average_vpd = get_average_value(table_results, 'vpd')

    # SETUP HEADER
    start_date, end_date = get_start_end_dates(table_results)
    start_date = start_date.strftime('%m/%d')
    end_date = end_date.strftime('%m/%d')
    header_text = f'{field.grower.name}\n{field.grower.name} {field.nickname} - {field.acres} acres\n{field.crop_type}\n{start_date} - {end_date}'
    header_text2 = '2024 End of Year Report'

    # Create charts using Matplotlib
    pie_image_path = create_pie_chart(vwc_data_dict)
    # bucket_image_path = create_psi_bucket_graph_with_percentages(psi_values)
    vwc_bucket_path = create_vwc_bucket_chart(vwc_data_dict, soil.bounds)
    bar_chart_path = create_bar_graph(month_irrigation, 'Monthly Irrigation Inches')

    # Upload the image to Google Drive
    pie_file_id = upload_image_to_drive(pie_image_path, drive_service)
    bar_file_id = upload_image_to_drive(bar_chart_path, drive_service)
    # bucket_file_id = upload_image_to_drive(bucket_image_path, drive_service)
    vwc_bucket_file_id = upload_image_to_drive(vwc_bucket_path, drive_service)
    # bar_line_id = upload_image_to_drive(bar_line_path, drive_service)

    # create_right_aligned_header(docs_service, document_id, field_name)
    # insert_image_into_doc(docs_service, document_id, pie_file_id)

    create_right_aligned_header(docs_service, document_id, header_text)
    append_to_header_center_aligned(docs_service, document_id, header_text2)
    # IRRIGATION
    bodies = [total_et_hours, round(total_irrigation_hours, 1), round(inches / 12, 1)]
    insert_into_second_row(docs_service, document_id, bodies)

    # ENVIRONMENTAL DATA
    air_temp = f'{round(average_temp, 1)}°F'
    bodies = [average_time, air_temp, f'{round(average_rh, 1)}%', round(average_vpd, 1)]
    insert_into_second_row(docs_service, document_id, bodies, table_index=1)

    # HOTTEST DAY
    max_temp = f'{round(max_temp, 1)}°F'
    bodies = [f'{max_temp_date} - {peak_time_on_date}', max_temp, f'{round(max_rh_on_date, 1)}%',
              round(max_vpd_on_date, 1)]
    insert_into_second_row(docs_service, document_id, bodies, table_index=2)
    # CHARTS
    insert_image_into_table_cell(docs_service, document_id, pie_file_id, table_index=5, row_index=1)
    insert_image_into_table_cell(docs_service, document_id, vwc_bucket_file_id, table_index=5, row_index=1,
                                 cell_index=2)
    insert_image_into_table_cell(docs_service, document_id, bar_file_id, table_index=5, row_index=3, cell_index=1)

    print('Data inserted into Google Doc')

# def irrigation_hours_report(table_results):

def main():
    modify_doc()
