import csv
import datetime
import json
import os
import pickle
import time
import xml.etree.ElementTree as ET
from itertools import zip_longest

import requests
from google.cloud import bigquery
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry

import DBWriter
import Decagon

# Set up permanent variables
pickle_path_2022 = 'H:\\Shared drives\\Stomato\\2022\\Pickle\\'
pickle_path_2023 = 'H:\\Shared drives\\Stomato\\2023\\Pickle\\'
API_KEY = "apk.b7373a2d1fbb1fd7f8493a7e7f4ef6bc34d68d856f1ece8e4508d35f58ddff9a"
base_url_stats_post_creation = "https://gate.eos.com/api/gdw/api?api_key="
base_url_stats_get_request = "https://gate.eos.com/api/gdw/api/"
header = {'Content-Type': 'application/json'}
url_combined = f"{base_url_stats_post_creation}{API_KEY}"
project_2022 = 'gradient-eos-data'
project_2023 = 'gradient-eos-data-2023'
directory_2022 = "H:\Shared drives\Stomato\Satellite Imagery\KML 2022 Files\EOS API"
current_year_directory = "H:\Shared drives\Stomato\Satellite Imagery\KML 2023 Files\EOS API"
RATE_LIMIT = 10  # ten requests per minute
MINUTE_IN_SECONDS = 60
retry_count = 3
backoff_factor = 5
dbwriter = DBWriter.DBWriter()
algorithm_list = ['NDVI', 'NDRE', 'MSAVI', 'NDSI', 'NDWI', 'RECI', 'NDMI', 'SAVI', 'ARVI', 'CCCI', 'EVI', 'GCI', 'MSI', 'NBR', 'SIPI']
algorithm_list_part_1 = ['NDVI', 'NDRE', 'MSAVI', 'NDWI', 'RECI', 'NDMI', 'SAVI', 'NDSI']
algorithm_list_part_2 = ['ARVI', 'CCCI', 'EVI', 'GCI']
algorithm_list_part_3 = ['MSI', 'NBR', 'SIPI']
tasks_dict = {}
failed_fields = []
today = datetime.datetime.today().strftime('%Y-%m-%d')


def check_if_dataset_has_valid_data(algorithm: str, field_name: str, project: str) -> bool:
    """
    This function checks if a dataset has valid data. If it does, it returns True. If it does not, it returns False.

    :param algorithm: Algorithm to check for valid data
    :param field_name: Field name to check for valid data
    :return: Boolean that dictates if dataset has valid data
    :param project: Project to check for valid data
    """
    dataset_id = f"`{project}.{field_name}.{algorithm}`"

    # Create query to check if dataset has valid data
    select_query = f"""
    SELECT * FROM {dataset_id}
    order by date desc
# Alternative way of doing? Found this version online but haven't tested yet
#     WHERE
#         date >= DATE_SUB(CURRENT_DATE(), INTERVAL 1 DAY)
    """

    # Run query
    results = dbwriter.run_dml(select_query)

    # Check if dataset has valid data
    if results.total_rows > 0:
        # If dataset has valid data, return True
        print("Grabbed Data Successfully")
        return True
    else:
        # If dataset does not have valid data, return False
        print("No Data Found")
        return False


def fill_in_empty_tables_from_eos_database(directory: str, project: str):
    """
    This function fills in empty tables from the EOS database. It does this by checking if the dataset has valid data.
    """
    # Get all datasets from EOS database
    datasets = dbwriter.get_datasets(project)
    # Loop through datasets
    for dataset in datasets[0]:
        field_name = dataset.dataset_id
        algorithm = algorithm_list[0]
        algorithms_to_process = []

        # Check if dataset has valid data for NDVI algorithm
        for algorithm in algorithm_list:
            dataset_has_valid_data = check_if_dataset_has_valid_data(algorithm, field_name, project=project)
            if not dataset_has_valid_data:
                algorithms_to_process.append(algorithm)

        if algorithms_to_process:
            # If dataset does not have valid data, grab data from EOS database
            print(f"Data not found: {field_name} for {algorithm}")
            print("\tGrabbing KML Path and Coordinates")
            kml_path, coordinates = return_kml_file_path(directory=directory, field_name_database=field_name)
            if coordinates is None:
                print(f"Coordinates not found: {field_name}")
                continue
            print("\tGrabbing data from EOS database")
            get_eos_api_data(polygon_coordinates=coordinates, field_name=field_name, algorithms_to_process_list=algorithms_to_process)


def setup_retry_for_http_requests(url: str, type: str, payload='', header: dict = '') -> requests.Response:
    """
    Function sets up retry for http requests
    :param url: url to send request to
    :param type: type of request, either post or get
    :param payload: payload to send with request
    :param header: header to send with request
    :return: response from request
    """
    # Initialize session and variables for retry
    session = requests.Session()
    retry = Retry(total=retry_count, backoff_factor=backoff_factor)
    retries = 0
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    try:
        if type == 'post':
            # Send post request
            response = session.post(url, headers=header, data=payload)
            response.raise_for_status()
            print("Post Response:", response.text)
            return response

        elif type == 'get':
            # Send get request
            response = session.get(url)
            response.raise_for_status()
            # Check if response is empty
            if response.text:
                print("Successful Get Response")
                return response
            else:
                # Retry if response is empty up to retry_count
                print(f"Failed to get response, {response.text} is empty")
                while retries < retry_count:
                    # Increment retries and sleep for 30 seconds
                    retries += 1
                    time.sleep(30)
                    # Send get request again
                    response = session.get(url)
                    response.raise_for_status()
                    # Check if response is empty
                    if response.text:
                        # If response is not empty, return response
                        print(f"Successful Get Response after retry attempt {retries}:", response.text)
                        return response
                    else:
                        # If response is empty, retry again
                        print(f"Sleeping for 30 seconds to retry get response. Attempt: {retries}")
                print(f"Failed to get response after {retries} attempts")

    except requests.exceptions.RequestException as err:
        print("Error:", err)



def get_kml_file_paths(directory: str) -> list:
    """
    Function takes in a directory and returns a list of all the kml file paths in the directory
    :param directory: directory to search for kml files
    :return: list of kml file paths
    """
    # Create a list that will hold all KML File Paths
    kml_file_paths_list = []
    # Walk through the directory and grab all the kml file paths
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".kml"):
                file_path = os.path.join(root, file)
                kml_file_paths_list.append(file_path)
    return kml_file_paths_list


def write_crop_algorithm_data_to_db(dataset_id: str, table: str, data: dict, year: int, filename: str = "crop_index_data.csv", last_flight=False):
    """
    Function writes irr scheduling data into csv then creates a db table from csv

    :param last_flight: Boolean that dictates if this is the last flight of the year
    :param dataset_id: dataset id to write data to
    :param table: table to write data to
    :param data: data to write to csv
    :param year: year of the data
    :param filename: filename of the csv
    """
    # Write data to csv for db upload
    print('\t\t- writing data to csv')
    with open(filename, "w", newline='') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(data.keys())
        # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
        # This will add full null rows for any additional daily_switch list values
        writer.writerows(zip_longest(*data.values()))
    print('\t...Done - file: ' + filename)

    # Schema used to save algorithm data to db
    algorithm_schema = [
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("q1", "Float", mode="NULLABLE"),
        bigquery.SchemaField("q3", "Float", mode="NULLABLE"),
        bigquery.SchemaField("max", "Float", mode="NULLABLE"),
        bigquery.SchemaField("min", "Float", mode="NULLABLE"),
        bigquery.SchemaField("p10", "Float", mode="NULLABLE"),
        bigquery.SchemaField("p90", "Float", mode="NULLABLE"),
        bigquery.SchemaField("std", "Float", mode="NULLABLE"),
        bigquery.SchemaField("median", "Float", mode="NULLABLE"),
        bigquery.SchemaField("average", "Float", mode="NULLABLE"),
        bigquery.SchemaField("variance", "Float", mode="NULLABLE"),
        bigquery.SchemaField("cloud", "Float", mode="NULLABLE")
    ]
    print("\tWriting Data to DB")
    if year == 2022:
        project = project_2022
    elif year == 2023:
        project = project_2023
    else:
        return
    # Write data to db
    # if this is the last flight, do not overwrite the data just append to database
    if last_flight:
        dbwriter.write_to_table_from_csv(dataset_id, table, filename, algorithm_schema, project, overwrite=False)
    else:
        dbwriter.write_to_table_from_csv(dataset_id, table, filename, algorithm_schema, project, overwrite=True)


def write_eos_statistics_to_db(data, field_name: str, year: int, last_flight=False):
    """
    Function takes in a dictionary of data and writes it to the database
    :param last_flight: Boolean that dictates if this is the last flight of the year
    :param data: dictionary of data
    :param field_name: name of the field
    :param year: year of the data
    """

    # Get project based on year
    if year == 2022:
        project = project_2022
    elif year == 2023:
        project = project_2023
    else:
        # if no year matches than do write to db since there is not a correct project to write to
        return
    # Get bigquery client details
    client = dbwriter.grab_bq_client(project)
    field_name = dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
    datasets = dbwriter.get_datasets(project)
    dataset_found = False
    EOS_DATA = False
    # Set up dictionary to hold statistical data for each algorithm
    statistical_data_NDVI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_NDRE = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_MSAVI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                              'variance': [], 'cloud': []}
    statistical_data_NDWI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_RECI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_NDMI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_SAVI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_NDSI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_ARVI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_CCCI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'median': [], 'average': [],
                             'variance': [], 'cloud': []}
    statistical_data_EVI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'average': [], 'median': [],
                            'variance': [], 'cloud': []}
    statistical_data_GCI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'average': [], 'median': [],
                            'variance': [], 'cloud': []}
    statistical_data_NBR = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'average': [], 'median': [],
                            'variance': [], 'cloud': []}
    statistical_data_SIPI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'average': [], 'median': [],
                            'variance': [], 'cloud': []}
    statistical_data_MSI = {'date': [], 'q1': [], 'q3': [], 'max': [], 'min': [], 'p10': [], 'p90': [], 'std': [], 'average': [], 'median': [],
                            'variance': [], 'cloud': []}

    # Set up schema for algorithm data
    algorithm_schema = [
        bigquery.SchemaField("date", "DATE", mode="NULLABLE"),
        bigquery.SchemaField("q1", "Float", mode="NULLABLE"),
        bigquery.SchemaField("q3", "Float", mode="NULLABLE"),
        bigquery.SchemaField("max", "Float", mode="NULLABLE"),
        bigquery.SchemaField("min", "Float", mode="NULLABLE"),
        bigquery.SchemaField("p10", "Float", mode="NULLABLE"),
        bigquery.SchemaField("p90", "Float", mode="NULLABLE"),
        bigquery.SchemaField("std", "Float", mode="NULLABLE"),
        bigquery.SchemaField("median", "Float", mode="NULLABLE"),
        bigquery.SchemaField("average", "Float", mode="NULLABLE"),
        bigquery.SchemaField("variance", "Float", mode="NULLABLE"),
        bigquery.SchemaField("cloud", "Float", mode="NULLABLE")
    ]

    # Check if dataset exists in EOS database
    for dataset in datasets[0]:
        if dataset.dataset_id == field_name:
            print(f"Dataset found: {field_name}")
            dataset_found = True
            tables = client.list_tables(field_name)
            algorithm_tables_in_db = []
            for table in tables:
                algorithm_tables_in_db.append(table.table_id)
            # Check to see if algorithm table exists in dataset, if not create it
            for algorithm in algorithm_list:
                if not algorithm in algorithm_tables_in_db:
                    query_dataset = f"{project}.{field_name}.{algorithm}"
                    field_tables = bigquery.Table(query_dataset, schema=algorithm_schema)
                    field_tables = client.create_table(field_tables)  # Make an API request.
                    print(
                        "\t\tCreated table {}.{}.{}".format(field_tables.project, field_tables.dataset_id, field_tables.table_id)
                    )

    if not dataset_found:
        print(f"Dataset not found: {field_name}")
        dataset = dbwriter.create_dataset(dataset_id=field_name, project=project)
        # print(f"Created dataset: {field_name}")
        for algorithm in algorithm_list:
            query_dataset = f"{project}.{field_name}.{algorithm}"
            field_tables = bigquery.Table(query_dataset, schema=algorithm_schema)
            field_tables = client.create_table(field_tables)  # Make an API request.
            print(
                "\t\tCreated table {}.{}.{}".format(field_tables.project, field_tables.dataset_id, field_tables.table_id)
            )

    # Loop through data and save it to the correct algorithm dictionary
    try:
        for each_day in data['result']:
            # print(day)
            date = each_day['date']
            # print(f"Date: {date}")
            # Store date and cloud information in dictionary
            for algorithm in each_day['indexes'].keys():
                if algorithm == 'NDVI':
                    statistical_data_NDVI['date'].append(date)
                    statistical_data_NDVI['cloud'].append(each_day['cloud'])
                elif algorithm == 'NDRE':
                    statistical_data_NDRE['date'].append(date)
                    statistical_data_NDRE['cloud'].append(each_day['cloud'])
                elif algorithm == 'MSAVI':
                    statistical_data_MSAVI['date'].append(date)
                    statistical_data_MSAVI['cloud'].append(each_day['cloud'])
                elif algorithm == 'NDMI':
                    statistical_data_NDMI['date'].append(date)
                    statistical_data_NDMI['cloud'].append(each_day['cloud'])
                elif algorithm == 'SAVI':
                    statistical_data_SAVI['date'].append(date)
                    statistical_data_SAVI['cloud'].append(each_day['cloud'])
                elif algorithm == 'NDWI':
                    statistical_data_NDWI['date'].append(date)
                    statistical_data_NDWI['cloud'].append(each_day['cloud'])
                elif algorithm == 'RECI':
                    statistical_data_RECI['date'].append(date)
                    statistical_data_RECI['cloud'].append(each_day['cloud'])
                elif algorithm == 'NDSI':
                    statistical_data_NDSI['date'].append(date)
                    statistical_data_NDSI['cloud'].append(each_day['cloud'])
                elif algorithm == 'ARVI':
                    statistical_data_ARVI['date'].append(date)
                    statistical_data_ARVI['cloud'].append(each_day['cloud'])
                elif algorithm == 'CCCI':
                    statistical_data_CCCI['date'].append(date)
                    statistical_data_CCCI['cloud'].append(each_day['cloud'])
                elif algorithm == 'EVI':
                    statistical_data_EVI['date'].append(date)
                    statistical_data_EVI['cloud'].append(each_day['cloud'])
                elif algorithm == 'GCI':
                    statistical_data_GCI['date'].append(date)
                    statistical_data_GCI['cloud'].append(each_day['cloud'])
                elif algorithm == 'SIPI':
                    statistical_data_SIPI['date'].append(date)
                    statistical_data_SIPI['cloud'].append(each_day['cloud'])
                elif algorithm == 'MSI':
                    statistical_data_MSI['date'].append(date)
                    statistical_data_MSI['cloud'].append(each_day['cloud'])
                elif algorithm == 'NBR':
                    statistical_data_NBR['date'].append(date)
                    statistical_data_NBR['cloud'].append(each_day['cloud'])

                # Store statistical data in dictionary
                for crop_index_stat in each_day['indexes'][algorithm].keys():
                    if algorithm == 'NDVI':
                        statistical_data_NDVI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'NDRE':
                        statistical_data_NDRE[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'MSAVI':
                        statistical_data_MSAVI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'NDMI':
                        statistical_data_NDMI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'SAVI':
                        statistical_data_SAVI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'NDWI':
                        statistical_data_NDWI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'RECI':
                        statistical_data_RECI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'NDSI':
                        statistical_data_NDSI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'ARVI':
                        statistical_data_ARVI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'CCCI':
                        statistical_data_CCCI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'EVI':
                        statistical_data_EVI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'GCI':
                        statistical_data_GCI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'SIPI':
                        statistical_data_SIPI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'MSI':
                        statistical_data_MSI[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])
                    elif algorithm == 'NBR':
                        statistical_data_NBR[crop_index_stat].append(each_day['indexes'][algorithm][crop_index_stat])

    except Exception as e:
        print(e)
        print(f'Failed reading data for field: {field_name}')
        failed_fields.append(field_name)

    # Write data to db
    for algorithm in algorithm_list:
        if not EOS_DATA:
            if algorithm == 'NDVI' and statistical_data_NDVI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_NDVI, year=year, last_flight=last_flight)
            elif algorithm == 'NDRE' and statistical_data_NDRE['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_NDRE, year=year, last_flight=last_flight)
            elif algorithm == 'MSAVI' and statistical_data_MSAVI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_MSAVI, year=year, last_flight=last_flight)
            elif algorithm == 'NDMI' and statistical_data_NDMI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_NDMI, year=year, last_flight=last_flight)
            elif algorithm == 'SAVI' and statistical_data_SAVI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_SAVI, year=year, last_flight=last_flight)
            elif algorithm == 'NDWI' and statistical_data_NDWI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_NDWI, year=year, last_flight=last_flight)
            elif algorithm == 'RECI' and statistical_data_RECI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_RECI, year=year, last_flight=last_flight)
            elif algorithm == 'NDSI' and statistical_data_NDSI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_NDSI, year=year, last_flight=last_flight)
            elif algorithm == 'ARVI' and statistical_data_ARVI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_ARVI, year=year, last_flight=last_flight)
            elif algorithm == 'CCCI' and statistical_data_CCCI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_CCCI, year=year, last_flight=last_flight)
            elif algorithm == 'EVI' and statistical_data_EVI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_EVI, year=year, last_flight=last_flight)
            elif algorithm == 'GCI' and statistical_data_GCI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_GCI, year=year, last_flight=last_flight)
            elif algorithm == 'SIPI' and statistical_data_SIPI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_SIPI, year=year, last_flight=last_flight)
            elif algorithm == 'MSI' and statistical_data_MSI['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_MSI, year=year, last_flight=last_flight)
            elif algorithm == 'NBR' and statistical_data_NBR['date']:
                write_crop_algorithm_data_to_db(field_name, algorithm, statistical_data_NBR, year=year, last_flight=last_flight)


def get_eos_statistics(task_id: str, field_name: str, year: int, last_flight=False):
    """
    Get statistical data for field from the EOS API
    :param task_id: Task ID used to get statistical data for field from the EOS API
    :param field_name: Name of field to get statistics for
    :param year: Year of the data
    :param last_flight: Boolean that dictates if this is the last flight of the year
    """
    # Build get request URL using base url, task id, and api key
    url = f"{base_url_stats_get_request}{task_id}?api_key={API_KEY}"
    print(url)
    # Send Request and store response
    response = setup_retry_for_http_requests(url, type='get')
    time.sleep(MINUTE_IN_SECONDS / RATE_LIMIT)
    # print(response.status_code, response.json())
    try:
        if response.status_code == 200 or response.status_code == 202:
            # Successful request
            data = response.json()  # Assuming the response is in JSON format
            # Save results in Big Query
            write_eos_statistics_to_db(data, field_name, year, last_flight=last_flight)

        else:
            # Request encountered an error
            print("Request failed with status code:", response.status_code)
            response = response.json()
            print(response)

    except Exception as e:
        print(e)
        print(f'Failed reading data for field: {field_name}')
        failed_fields.append(field_name)


def get_coordinates_from_kml(file_path: str) -> tuple[list[list[float]], int]:
    """
    Get coordinates from a KML file
    :param file_path: Path to a KML file
    :return: A tuple containing a list of coordinates and the number of valid polygons in the KML file
    """
    # Get Root from KML structure
    tree = ET.parse(file_path)
    root = tree.getroot()

    coordinates = []
    valid_polygons = 0

    # Define the namespace mapping. There are two different types of namespace I have seen
    namespaces = [
        {"kml": "http://www.opengis.net/kml/2.2"},
        {"kml": "http://earth.google.com/kml/2.2"}
    ]

    for namespace in namespaces:
        # Find all Polygon elements
        polygons = root.findall(".//kml:Polygon", namespace)
        try:
            if polygons:
                for polygon in polygons:
                    # Find the outer LinearRing of the Polygon
                    outer_ring = polygon.find(".//kml:outerBoundaryIs/kml:LinearRing", namespace)

                    # Extract the coordinates from the LinearRing
                    try:
                        coordinates_elem = outer_ring.find(".//kml:coordinates", namespace)
                        if coordinates_elem is not None:
                            valid_polygons += 1
                            coordinates_str = coordinates_elem.text.strip()

                            # Split the coordinates string into individual points
                            points = coordinates_str.split()

                            # Extract longitude, latitude, and altitude (if available) from each point
                            for point in points:
                                values = point.split(",")

                                # Extract longitude and latitude
                                lon, lat = float(values[0]), float(values[1])

                                # Add long, lat to list of coordinates
                                coordinates.append([lon, lat])

                        # Found polygons, so break the loop
                        # Todo: Some KML Files might have multiple polygons, so we need to know how to handle those.
                        #  Low priority since I don't have any current examples
                    except AttributeError:
                        # print("Could not find coordinates due to empty polygon")
                        pass

        except AttributeError:
            print("Could not finding using the first namespace, attempting using the second namespace")

    return coordinates, valid_polygons


def get_eos_api_data(polygon_coordinates: list[list[float]], field_name: str, algorithms_to_process_list: list[str], year: int=2023,
                     start_date: str=None, end_date: str=None):
    """
    Function takes in a kml file and stores all satellite statistical data into the database
    :param algorithms_to_process_list: list of algorithms to get data for
    :param polygon_coordinates: list of coordinates that make up the polygon
    :param field_name: name of the field
    :param start_date: start date to get data for
    :param end_date: end date to get data for
    :param year: year to get data for
    """
    # Get today's date and use that with field name to create a unique reference name
    reference_name = f"{field_name}_{today}"
    planting_date = None
    uninstall_date = None

    # Loop through 2022 or 2023 pickle and assign planting and uninstall dates
    if year == 2022:
        growers = Decagon.open_pickle('2022_pickle.pickle', pickle_path_2022)
    elif year == 2023:
        growers = Decagon.open_pickle('2023_pickle.pickle', pickle_path_2023)
    else:
        return
    # Find field in pickle and assign planting and uninstall dates
    for grower in growers:
        for field in grower.fields:
            field_name_pickle = dbwriter.remove_unwanted_chars_for_db_dataset(field.name)
            # Make sure field is a tomato field
            if field_name_pickle == field_name and (field.loggers[0].crop_type == "Tomatoes" or field.loggers[0].crop_type == "tomatoes" or
                                             field.loggers[0].crop_type == "Tomato" or field.loggers[0].crop_type == "tomato"):
                if hasattr(field.loggers[0], 'planting_date'):
                    planting_date = (field.loggers[0].planting_date - datetime.timedelta(days=30)).strftime("%Y-%m-%d")
                if hasattr(field.loggers[0], 'uninstall_date'):
                    if field.loggers[-1].uninstall_date is not None:
                        uninstall_date = (field.loggers[0].uninstall_date + datetime.timedelta(days=30)).strftime("%Y-%m-%d")
                    else:
                        uninstall_date = today

    # If we have a start or end date, then use that instead of the planting and uninstall dates
    if start_date is not None:
        planting_date = start_date
    if end_date is not None:
        uninstall_date = end_date

    # If we have valid start and end dates, then set up the payload structure using the planting and uninstall dates
    if planting_date is not None or uninstall_date is not None:
        payload = {
            "type": "mt_stats",
            "params": {
                "date_start": planting_date,
                "date_end": uninstall_date,
                "bm_type":
                    algorithms_to_process_list,
                "reference": reference_name,
                "sensors": ["S2L2A"],
                "geometry": {
                    "type": "Polygon",
                    "coordinates": [polygon_coordinates]
                },
                "max_cloud_cover_in_aoi": 100
            }
        }

        # Convert payload into json format
        json_payload = json.dumps(payload)
        print(json_payload)

        # Send post request to get task id to view results
        post_response = setup_retry_for_http_requests(url_combined, type='post', payload=json_payload, header=header)
        time.sleep(MINUTE_IN_SECONDS / RATE_LIMIT)
        # Check for a successful request
        try:
            if post_response.status_code == 200 or post_response.status_code == 202:
                # Successful request
                # Decode response from json format
                result = post_response.json()
                # Process the data as needed
                # print(f"Successful post request: {result}")
                result_task_id = result['task_id']
                tasks_dict[field_name] = result_task_id


            else:
                # Request encountered an error
                print("Request failed with status code:", post_response.status_code)
                post_response = post_response.json()
                print(post_response)
        except Exception as e:
            print("Exception occurred during post request")
            print(e)
            print(post_response)
    else:
        print(f"Could not find planting date or uninstall date for {field_name} \n Skipping field")


def process_kml_directory(directory: str, algorithm_list_to_process: list[str], year: int = 2023, last_flight=False):
    """
    Function takes in a directory and processes all kml files in the directory for satellite statistical data gathering
    :param directory: directory path
    :param algorithm_list_to_process: list of algorithms to get data for
    :param year: year to get data for
    :param last_flight: Boolean that dictates if this is the last flight of the year
    """
    # Get all kml files in the directory
    kml_file_path_list = get_kml_file_paths(directory)
    # print(len(kml_file_path_list))
    # Loop through all kml files in the directory
    for kml_file_path in kml_file_path_list:
        # Get grower name and field name from file path
        grower_name = os.path.basename(os.path.dirname(kml_file_path))
        # Check that the folder name is not 'Ignore' or 'Multiple Fields'
        if not grower_name == 'Ignore' and not grower_name == 'Multiple Fields':
            # Get coordinates from KML file
            coordinates, polygon_len = get_coordinates_from_kml(kml_file_path)
            if coordinates is None:
                print(f"Could not find coordinates for {kml_file_path}")
            # Check that there isn't more than one polygon in the kml file
            elif not (polygon_len > 1):
                kml_file_name = os.path.basename(kml_file_path)
                # Remove the .kml extension
                kml_field_name = kml_file_name.split('.')[0]
                # Grab field name pickle using the directory path
                kml_field_name_pickle = grower_name + kml_field_name
                print(f"Working on Field: {kml_field_name_pickle}")
                kml_field_name_pickle = dbwriter.remove_unwanted_chars_for_db_dataset(kml_field_name_pickle)
                # Check if we want images since last flight
                if last_flight:
                    last_flight_date = get_last_date_of_flight_for_field(field_name=kml_field_name_pickle,
                                                                         algorithm_list_for_processing=algorithm_list_to_process, year=year)
                    if last_flight_date is None:
                        print(f"Could not find last flight date for {kml_field_name_pickle}")
                        continue
                    else:
                        get_eos_api_data(polygon_coordinates=coordinates, field_name=kml_field_name_pickle, algorithms_to_process_list=algorithm_list_to_process,
                                         year=year, start_date=last_flight_date, end_date=today)
                else:
                    get_eos_api_data(polygon_coordinates=coordinates, field_name=kml_field_name_pickle, algorithms_to_process_list=algorithm_list_to_process,
                                     year=year)


def return_kml_file_path(directory: str, field_name_database: str) -> tuple[str, list[list[float]]]:
    """
    Function takes in a directory and a field name and returns the kml file path for the field name
    :param directory: directory path
    :param field_name_database: field name to get kml file path for
    :return:
    """
    # Get all kml files in the directory
    coordinates = None
    kml_file_path_list = get_kml_file_paths(directory)
    # print(len(kml_file_path_list))
    # Loop through all kml files in the directory
    for kml_file_path in kml_file_path_list:
        # Get grower name and field name from file path
        grower_name = os.path.basename(os.path.dirname(kml_file_path))
        # Check that the folder name is not 'Ignore' or 'Multiple Fields'
        if not grower_name == 'Ignore' and not grower_name == 'Multiple Fields':
            kml_file_name = os.path.basename(kml_file_path)
            # Remove the .kml extension
            kml_field_name = kml_file_name.split('.')[0]
            kml_field_name_pickle = grower_name + kml_field_name
            kml_field_name_pickle = dbwriter.remove_unwanted_chars_for_db_dataset(kml_field_name_pickle)
            if kml_field_name_pickle == field_name_database:
                print(f"Found kml file for {field_name_database}")
                # Get coordinates from KML file
                coordinates, polygon_len = get_coordinates_from_kml(kml_file_path)
                if coordinates is None:
                    print(f"Could not find coordinates for {kml_file_path}")
                # Check that there isn't more than one polygon in the kml file
                elif not (polygon_len > 1):
                    return kml_file_path, coordinates
    # If we didn't find the kml file, then return None
    return "", coordinates


def delete_not_supported_algorithms_from_database(project: str):
    """
    Function deletes not supported algorithms from the database. Supported algorithms are the ones in the algorithm_list variable
    Algorithm List: ['NDVI', 'NDRE', 'MSAVI', 'NDSI', 'NDWI', 'RECI', 'NDMI', 'SAVI']

    """
    # Get list of datasets in database
    client = dbwriter.grab_bq_client(project)
    datasets = dbwriter.get_datasets(project)
    # Loop through all datasets
    for dataset in datasets[0]:
        print(f"Working on field: {dataset.dataset_id}")
        # Get list of tables in dataset
        tables = client.list_tables(dataset.dataset_id)
        # Loop through all tables in dataset
        for table in tables:
            # Check if table is in algorithm list
            # If not delete table
            if not table.table_id in algorithm_list:
                table_id = f"{project}.{dataset.dataset_id}.{table.table_id}"
                client.delete_table(table_id, not_found_ok=True)

def find_eos_api_statistics_for_field_list(fields_list_for_processing: list[str], algorithm_list_for_processing: list[str], directory: str, year: int,
                                           last_flight: bool = False):
    """
    Function takes in a list of fields and finds the statistical data for each field
    :param fields_list_for_processing: Fields to get statistical data for
    :param algorithm_list_for_processing: Algorithms to get statistical data for
    :param directory: Directory where all the kml files are stored
    :param year: Year of the data
    :param last_flight: Boolean that dictates if this is the last flight of the year
    """
    # Loop through all fields in the list
    for field in fields_list_for_processing:
        # Get kml file path and coordinates
        print(f"Data not found: {field}")
        print("\tGrabbing KML Path and Coordinates")
        kml_path, coordinates = return_kml_file_path(directory=directory, field_name_database=field)
        if coordinates is None:
            print(f"Coordinates not found: {field}")
            continue
        print("\tGrabbing data from EOS database")
        # Check if we want images since last flight
        if last_flight:
            last_flight_date = get_last_date_of_flight_for_field(field_name=field, algorithm_list_for_processing=algorithm_list_for_processing,
                                                                 year=year)
            if last_flight_date is None:
                print(f"Could not find last flight date for {field}")
                continue
            else:
                get_eos_api_data(polygon_coordinates=coordinates, field_name=field, algorithms_to_process_list=algorithm_list_for_processing,
                                 start_date=last_flight_date, end_date=today, year=year)
        else:
            get_eos_api_data(polygon_coordinates=coordinates, field_name=field, algorithms_to_process_list=algorithm_list_for_processing, year=year)

    # Process the task list
    process_task_list(year=year, last_flight=last_flight)


def process_all_fields_v2(satellite_image_directory: str, algorithm_list_to_process: list[str], year: int, last_flight=False):
    """
    Function processes all fields in the directory
    :param satellite_image_directory: Directory where all the kml files are stored
    :param algorithm_list_to_process: Algorithms to get statistical data for
    :param year: Year of the data
    :param last_flight: Boolean that dictates if this is the last flight of the year
    """
    # Get all kml files in the directory
    process_kml_directory(directory=satellite_image_directory, algorithm_list_to_process=algorithm_list_to_process, year=year, last_flight=last_flight)
    # Process the task list
    process_task_list(year=year, last_flight=last_flight)

def check_db_for_duplicate_algorithm_results(field_name: str, project: str):
    """
    Function checks the database for duplicate results for a given field name
    :param field_name: Field name to check for duplicates
    :param project: Project to check for duplicates
    """
    table_id_list = []
    duplicate = False
    sql_query = ""
    print(f"Checking {field_name}")
    # Store all algorithm tables dataset id information in table_id_list
    for algorithm in algorithm_list:
        table_id_list.append(f"{project}.{field_name}.{algorithm}")
    # Loop through all tables in table_id_list and create a sql query with all table_id's
    for table_id in table_id_list:
        sql_query += f"select * From {table_id}"
        index = table_id_list.index(table_id)
        list_end = len(table_id_list) - index
        if list_end > 1:
            sql_query += " Intersect Distinct "
    # print(sql_query)
    response = dbwriter.run_dml(sql_query, project=project)
    # print(response)
    # Check response and check if there is a date, if there is then there is a duplicate
    for row in response:
        if row.date is not None:
            duplicate = True
    if duplicate:
        print(f"Duplicate Found {field_name}")


def check_all_fields_db_for_duplicate_algorithm_results(project: str):
    """
    Function checks the database for duplicate results for all fields in the database
    :param project: Project to check for duplicates
    """
    datasets = dbwriter.get_datasets(project)
    for dataset in datasets[0]:
        field_name = dataset.dataset_id
        # Loop through all tables in dataset
        check_db_for_duplicate_algorithm_results(field_name, project=project)

def process_task_list(year: int, last_flight: bool = False):
    """
    Function processes the task list that was saved when the data was grabbed from the EOS API.
    This function will check to see the status of the task and if it is complete it will process the data and save it to the database
    :param last_flight: Boolean that dictates if this is the last flight of the year
    :param year: Year of the data
    """
    # Sleep for 4 minutes to give EOS time to process the data
    print(f'Tasks Dict: {tasks_dict}')
    time.sleep(240)
    # Loop through all tasks in the task list processing each individual task
    for tasks in tasks_dict:
        print(f"Working on {tasks}")
        # print(tasks)
        get_eos_statistics(task_id=tasks_dict[tasks], field_name=tasks, year=year, last_flight=last_flight)
    print("Done processing tasks")
    print(f"Failed Fields After Processing: {failed_fields}")
    # Pickle the failed fields list
    pickle_failed_fields_list(failed_fields)
    # Process the failed fields list.
    # TODO: Currently there is a bug where it endlessly retries the same fields over and over again
    # process_failed_fields_pickle(year=year, last_flight=last_flight)


def pickle_failed_fields_list(failed_fields_list: list[str]):
    """
    Function pickles the failed fields list
    :param failed_fields_list: List of fields that failed to process
    """
    print(f"Pickling failed fields list")
    with open('failed_fields_list.pkl', 'wb') as f:
        pickle.dump(failed_fields_list, f)
    print('Done pickling failed fields list')
    # Clear the failed fields list
    failed_fields_list.clear()


def process_failed_fields_pickle(year: int, last_flight: bool = False):
    """
    Function unpickles the failed fields list and processes them
    """
    failed_fields_list = open_eos_failed_fields_pickle()
    print(f"Failed Fields From Pickle: {failed_fields_list}")
    if year == datetime.datetime.today().year:
        directory = current_year_directory
    elif year == 2022:
        directory = directory_2022
    else:
        return

    find_eos_api_statistics_for_field_list(fields_list_for_processing=failed_fields_list, algorithm_list_for_processing=algorithm_list_part_1
                                           , directory=directory, year=year, last_flight=last_flight)
    find_eos_api_statistics_for_field_list(fields_list_for_processing=failed_fields_list, algorithm_list_for_processing=algorithm_list_part_2
                                           , directory=directory, year=year, last_flight=last_flight)
    find_eos_api_statistics_for_field_list(fields_list_for_processing=failed_fields_list, algorithm_list_for_processing=algorithm_list_part_3
                                           , directory=directory, year=year, last_flight=last_flight)

def process_data_from_planting_to_current(field_list: list[str]= None):
    """
    Function processes all data from the planting date to the current date
    :param field_list: List of fields to process
    """
    # Check to see if we have a field list, if we do only process the fields in the list
    if not field_list is None:
        find_eos_api_statistics_for_field_list(fields_list_for_processing=field_list, algorithm_list_for_processing=algorithm_list_part_1,
                                               directory=current_year_directory, year=2023)
        # find_eos_api_statistics_for_field_list(fields_list_for_processing=field_list, algorithm_list_for_processing=algorithm_list_part_2,
        #                                        directory=current_year_directory, year=2023)
        # find_eos_api_statistics_for_field_list(fields_list_for_processing=field_list, algorithm_list_for_processing=algorithm_list_part_3,
        #                                        directory=current_year_directory, year=2023)
    # Process the whole directory if we don't have a list
    else:
        process_all_fields_v2(satellite_image_directory=current_year_directory, algorithm_list_to_process=algorithm_list_part_1, year=2023)
        # process_all_fields_v2(satellite_image_directory=current_year_directory, algorithm_list_to_process=algorithm_list_part_2, year=2023)
        # process_all_fields_v2(satellite_image_directory=current_year_directory, algorithm_list_to_process=algorithm_list_part_3, year=2023)


def process_data_from_last_flight_to_current(field_list: list[str]=None):
    """
    Function processes all data from the last flight date to the current date
    :param field_list: List of fields to process
    """
    # Check to see if we have a field list, if we do only process the fields in the list
    if not field_list is None:
        find_eos_api_statistics_for_field_list(fields_list_for_processing=field_list, algorithm_list_for_processing=algorithm_list_part_1,
                                               directory=current_year_directory, year=2023, last_flight=True)
        # find_eos_api_statistics_for_field_list(fields_list_for_processing=field_list, algorithm_list_for_processing=algorithm_list_part_2,
        #                                        directory=current_year_directory, year=2023, last_flight=True)
        # find_eos_api_statistics_for_field_list(fields_list_for_processing=field_list, algorithm_list_for_processing=algorithm_list_part_3,
        #                                        directory=current_year_directory, year=2023, last_flight=True)
    # Process the whole directory if we don't have a list
    else:
        process_all_fields_v2(satellite_image_directory=current_year_directory, algorithm_list_to_process=algorithm_list_part_1, year=2023,
                              last_flight=True)
        # process_all_fields_v2(satellite_image_directory=current_year_directory, algorithm_list_to_process=algorithm_list_part_2, year=2023,
        #                       last_flight=True)
        # process_all_fields_v2(satellite_image_directory=current_year_directory, algorithm_list_to_process=algorithm_list_part_3, year=2023,
        #                       last_flight=True)


def get_last_date_of_flight_for_field(field_name: str, algorithm_list_for_processing: list[str], year: int):
    """
    Function gets the last date of the flight for the field
    :param field_name: Name of the field to get the last date of the flight for
    :param algorithm_list_for_processing: List of algorithms to get the last date of the flight for
    :param year: Year of the data
    :return:
    """
    date_list = []
    # Get project based on year
    if year == datetime.datetime.today().year:
        project = project_2023
    elif year == 2022:
        project = project_2022
    else:
        return
    # Get the first algorithm from the algorithm list
    algorithm = algorithm_list_for_processing[0]
    # All algorithm groups have the same amount of images and dates of flights, we just need to find the last flight for any algorithm in the group
    dataset_id = f"`{project}.{field_name}.{algorithm}`"

    # Create query to check for last flight date
    select_query = f"""
        SELECT date as day FROM {dataset_id} as t
        order by t.date desc
        """

    # Run query
    try:
        results = dbwriter.run_dml(select_query)
        for row in results:
            date_list.append(row.day)
        # Last Flight Date is going to be the first one in the list
        last_date = (date_list[0] + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    except Exception as e:
        print(e)
        return

    # Loop through results and add dates to date_list
    print(last_date)
    return last_date


def open_eos_failed_fields_pickle():
    """
    Function opens the eos pickle file and returns the failed fields list inside the pickle
    :return:
    """
    with open('failed_fields_list.pkl', 'rb') as f:
        failed_fields_list = pickle.load(f)
    return failed_fields_list

def check_algorithm_for_images(project: str, algorithm_list: list[str]):
    """
    Algorithm prints how many images each algorithm has in the database
    :param project: database project
    :param algorithm_list: algorithm list
    """
    client = dbwriter.grab_bq_client(project)
    datasets = dbwriter.get_datasets(project)
    # Loop through all datasets
    for dataset in datasets[0]:
        field_name = dataset.dataset_id
        print(f"Working on field: {dataset.dataset_id}")
        # Get list of tables in dataset
        tables = client.list_tables(field_name)
        # Loop through all tables in dataset
        for table in tables:
            algorithm_name = table.table_id
            if algorithm_name in algorithm_list:
                table_id = f"{project}.{field_name}.{algorithm_name}"
                sql_query = f"select count(date) as number_of_images FROM {table_id}"
                response = dbwriter.run_dml(sql_query, project=project)
                for row in response:
                    print(f'{algorithm_name}:{row.number_of_images}')
                    if row.number_of_images < 15:
                        print(f"Field {field_name} has less than 15 images")
                        print(f"\t{table_id}: {row.number_of_images}")

def get_kml_coordinates(directory: str, field_name: str):
    """
    Function takes in a field name and returns the coordinates for the field
    :param field_name:
    :param directory: directory path
    """
    # Get all kml files in the directory
    kml_file_path_list = get_kml_file_paths(directory)
    # print(len(kml_file_path_list))
    # Loop through all kml files in the directory
    for kml_file_path in kml_file_path_list:
        # Get grower name and field name from file path
        grower_name = os.path.basename(os.path.dirname(kml_file_path))
        # Check that the folder name is not 'Ignore' or 'Multiple Fields'
        if not grower_name == 'Ignore' and not grower_name == 'Multiple Fields':
            # Get coordinates from KML file
            kml_file_name = os.path.basename(kml_file_path)
            # Remove the .kml extension
            kml_field_name = kml_file_name.split('.')[0]
            if kml_field_name == field_name:
                coordinates, polygon_len = get_coordinates_from_kml(kml_file_path)
                if coordinates is None:
                    print(f"Could not find coordinates for {kml_file_path}")
                # Check that there isn't more than one polygon in the kml file
                elif not (polygon_len > 1):
                    return coordinates


# grower_object = {}
#
# grower_name_test = 'Matteoli Brothers'
# growers = Decagon.open_pickle('2023_pickle.pickle', pickle_path_2023)
# for grower in growers:
#     if grower.name == grower_name_test:
#         grower_object = {
#             'grower_name': grower.name,
#             'password': 'password',
#             'fields': []
#         }
#         for field in grower.fields:
#             coordinates = get_kml_coordinates(current_year_directory, field.nickname)
#             grower_object['fields'].append(
#                 {
#                     'field_name': field.nickname,
#                     'coordinates': coordinates
#                 })

# print(grower_object)

# Example usage
# Algorithm 3
# fields_list = ['Bone_Farms_LLCN42_N43', 'Knight_FarmsB5', 'Knight_FarmsB4', 'T_PCO4', 'Turlock_Fruit_Co1250', 'Mumma_Bros94', 'Mumma_Bros90N', 'Lucero_JNJC1_2_3_4', 'Bullseye_FarmsOE10', 'Lucero_LB1LB1', 'OPC15_4', 'OPC3_4', 'Lucero_Kern_TrinitasKRN_606']
# find_eos_api_statistics_for_field_list(fields_list, algorithm_list_part_3, current_year_directory, 2023)
# process_data_from_planting_to_current()
# Algorithm 1
# fields_list = []
# find_eos_api_statistics_for_field_list(fields_list, algorithm_list_part_1, current_year_directory, 2023)
# Algorithm 2
# fields_list = ['Bone_Farms_LLCN42_N43', 'Knight_FarmsNC2', 'Knight_FarmsB5', 'Lucero_BakersfieldTowerline', 'Turlock_Fruit_Co1250', 'Lucero_JNJC1_2_3_4', 'Bullseye_FarmsOE10', 'Dougherty_Bros4N', 'Dougherty_BrosT5', 'Lucero_LB1LB1', 'OPC3_4', 'Lucero_Kern_TrinitasKRN_605', 'Lucero_Kern_TrinitasKRN_606', 'Knight_FarmsLV1', 'Mumma_Bros7', 'Dougherty_Bros5', 'Lucero_Kern_TrinitasKRN_212', 'Lucero_Kern_TrinitasKRN_210']
# find_eos_api_statistics_for_field_list(fields_list, algorithm_list_part_2, current_year_directory, 2023)



# Process All Fields from planting to current. You can pass in a fields_list[] if you want only certain fields
# process_data_from_planting_to_current()

# Process All Fields from last flight to current. You can pass in a fields_list[] if you want only certain fields
# fields_list = ['Lucero_LB1LB1']
# fields_list = dbwriter.remove_unwanted_chars_for_db_dataset(fields_list[0])
# print(fields_list)
# Decagon.show_pickle('2023_pickle.pickle', pickle_path_2023)
# process_data_from_last_flight_to_current()

#Process any failed recent fields. Function works but logic is still a little wierd. Will be reworking this one at a later time.
# process_failed_fields_pickle(year=2023, last_flight=False)



