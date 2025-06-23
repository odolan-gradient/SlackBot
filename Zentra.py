import json
import time
from datetime import datetime

import requests
from dateutil import parser
from dateutil.tz import tzoffset

####################################################################
# Base Class for using the Zentra API
# Expand this class for additional API functionality
####################################################################

URL = "https://zentracloud.com/api/v4/get_readings/"
# TOKEN = "Token {TOKEN}".format(TOKEN="6d1f65835ec6fd0c7ec77266d6a74d7f5a7be6b1")  # jgarrido
TOKEN = "Token {TOKEN}".format(TOKEN="1f3ad8dbb4f3bd71399e21b769fe0eb1de97e041") # setup

API_TIME_SECONDS_LIMITER = 60
DIRECTORY_YEAR = "2025"
DXD_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Dxd Files\\"


class Zentra(object):

    def __init__(self):
        pass

    @staticmethod
    def get_all_readings(
            device_sn: str,
            start_date: str = None,
            end_date: str = None,
            start_mrid: int = None,
            end_mrid: int = None,
            output_format: str = 'json',
            per_page: int = 2000,
            sort_by: str = 'ascending',
            device_depth: bool = True,
            page_num: int = 1
    ):
        """
        Function to facilitate API v4 usage from Zentra.
        This function handles making all the sequential calls necessary to get all the data from the range requested
        back, regardless of how many pages this data spans.

        :param device_sn: ID of the logger you are wanting data from
        :param start_date: Data start date
            (Overrides start_mrid. Defaults to beginning of logger data)
        :param end_date: Data end date
            (Overrides end_mrid. Defaults to today)
        :param start_mrid: Data start mr_id
        :param end_mrid: Data end mr_id
        :param output_format: Return type of the data you get form the API. Defaults to json
        :param per_page: Number of results per page. Max is 2000, defaults to 2000
        :param sort_by: How you want the data sorted. Defaults to Ascending
        :param device_depth: Boolean to indicate if you want device_depth returned with the data. Defaults to True
        :param page_num: Page number you are requesting. Defaults to 1
        :return: response.ok: Boolean that indicates the success of the API call
        :return: all_data_returned: List of dictionary/ies from the returned API results, with 1 dictionary per return.
            [
                { API return },
                { API return }
            ]
        """

        api_calls_num = 1
        last_page_start_mrid = -999
        page_end_mrid = -999
        max_mrid = 0
        api_time_start = 0
        api_time_end = API_TIME_SECONDS_LIMITER
        all_data_returned = []
        errors = []
        page_number = page_num
        additional_buffer_time = 5  # seconds
        # Set up the offset information for Pacific Time Zone
        datetime_offset = tzoffset(None, -25200)  # -25200 seconds = -7 hours

        # Add that offset to the filter datetimes
        # end_date_dt = datetime.strptime(end_date, "%m-%d-%Y")

        # Try to parse with time first
        try:
            end_date_dt = datetime.strptime(end_date, '%m-%d-%Y %H:%M')
        except ValueError:
            # If that fails, try parsing without time
            end_date_dt = datetime.strptime(end_date, '%m-%d-%Y')

        end_date_dt = end_date_dt.replace(tzinfo=datetime_offset)
        page_end_date_dt = None

        # Loop to continue querying API until we get all the data we want
        # while page_end_mrid < max_mrid - 1:
        while page_end_date_dt is None or page_end_date_dt < end_date_dt:
            elapsed_time = api_time_end - api_time_start
            time_to_sleep = API_TIME_SECONDS_LIMITER - elapsed_time
            if time_to_sleep < 0:
                time_to_sleep = 0
            print(f'\tSleeping {round(time_to_sleep)} secs for API limiter...')
            time.sleep(time_to_sleep)

            print(
                f'\tAPI call # {api_calls_num}  |  Start Date: {start_date}  |  End Date: {end_date}  |  Page #{page_number}')
            api_time_start = time.time()
            response = Zentra.get_readings(
                device_sn,
                start_date=start_date,
                end_date=end_date,
                start_mrid=start_mrid,
                end_mrid=end_mrid,
                output_format=output_format,
                per_page=per_page,
                sort_by=sort_by,
                device_depth=device_depth,
                page_num=page_number
            )
            print(f'\t url: {response.url}')
            if response.ok:
                print('\t<- Good API return')
                content = json.loads(response.content)
                if len(content['data']) == 0:
                    print('\tAPI return successful but there is no data in the return')
                    break
                else:
                    all_data_returned.append(content)

                page_start_date_str = content['pagination']['page_start_date']
                page_start_date = parser.parse(page_start_date_str)
                page_end_date_str = content['pagination']['page_end_date']
                page_end_date = parser.parse(page_end_date_str)
                page_end_date_dt = page_end_date
                page_start_mrid = content['pagination']['page_start_mrid']
                page_end_mrid = content['pagination']['page_end_mrid']
                data_points_in_page = page_end_mrid - page_start_mrid
                max_mrid = content['pagination']['max_mrid']

                # print(f'\tMax MRID: {max_mrid}')
                print(f'')
                print(f'\tPage #{api_calls_num}')
                print(f'\t pg start date: {page_start_date}')
                print(f'\t pg end date: {page_end_date}')
                print(f'\t data req end date: {end_date}')
                print(f'\t data points in page: {data_points_in_page}')
                # print(f'\t pg start mrid: {page_start_mrid}')
                # print(f'\t pg end mrid: {page_end_mrid}/{max_mrid}')
                api_calls_num += 1
                page_number = api_calls_num
                api_time_end = time.time()
                elapsed_time = api_time_end - api_time_start
                print(f'\tv4 API call took {round(elapsed_time)} secs')
                # Subtracting the additional buffer time from api end time to simulate we need to add a
                # buffer amount of seconds
                api_time_end -= additional_buffer_time
            else:
                print(response.status_code)
                print(response.content)
                api_time_end = time.time()
                errors.append((response.status_code, response.content))

        print()
        print(f'All data received from {api_calls_num - 1} API calls')
        # print(f' Issues: {errors}')
        print()
        return response.ok, all_data_returned

    @staticmethod
    def get_readings(device_sn, start_date=None, end_date=None, start_mrid=None, end_mrid=None,
                     output_format='json', per_page=2000, sort_by='ascending', device_depth=True, page_num=1):
        """
        Function to facilitate API v4 usage from Zentra.
        This function calls the API for get_readings with all the parameters required

        :param device_sn: ID of the logger you are wanting data from
        :param start_date: Data start date
            (Overrides start_mrid. Defaults to beginning of logger data)
        :param end_date: Data end date
            (Overrides end_mrid. Defaults to today)
        :param start_mrid: Data start mr_id
        :param end_mrid: Data end mr_id
        :param output_format: Return type of the data you get form the API. Defaults to json
        :param per_page: Number of results per page. Max is 2000, defaults to 2000
        :param sort_by: How you want the data sorted. Defaults to Ascending
        :param device_depth: Boolean to indicate if you want device_depth returned with the data. Defaults to True
        :param page_num: Page number you are requesting. Defaults to 1
        :return: all_data_returned: List of dictionary/ies from the returned API results
        """

        # Setup parameters for the API call
        headers = {'content-type': 'application/json', 'Authorization': TOKEN}
        params = {
            'device_sn': device_sn,
            'output_format': output_format,
            'per_page': per_page,
            'sort_by': sort_by,
            'device_depth': device_depth,
            'page_num': page_num
        }

        # Handle optional parameters not already tackled through defaults
        if start_date is not None:
            params['start_date'] = start_date
        else:
            if start_mrid is not None:
                params['start_mrid'] = start_mrid
            else:
                params['start_mrid'] = 0
        if end_date is not None:
            params['end_date'] = end_date
        else:
            if end_mrid is not None:
                params['end_mrid'] = end_mrid
            else:
                end_date_dt = datetime.today()
                formatted_end_date = end_date_dt.strftime('%m-%d-%Y')
                params['end_date'] = formatted_end_date

        # Make the API call
        response = requests.get(URL, params=params, headers=headers)

        return response

    @staticmethod
    def get_env_model_data(device_sn, model_type='ETo', port_num=1, latitude: float = None):
        """
        Function to facilitate API v4 usage from Zentra for get env model data


        :param latitude: lat of the station
        :param port_num: Should always be 1 for an atmos 41
        :param model_type: Eto
        :param device_sn: ID of the logger you are wanting data from
        :return: all_data_returned: List of dictionary/ies from the returned API results
        """

        # Setup parameters for the API call
        et_url = "https://zentracloud.com/api/v3/get_env_model_data/"
        headers = {'content-type': 'application/json', 'Authorization': TOKEN}
        inputs_json = json.dumps({'elevation': 3, 'latitude': latitude, 'wind_measurement_height': 2})
        params = {
            'device_sn': device_sn,
            'model_type': model_type,
            'port_num': port_num,
            'inputs': inputs_json
        }

        # Make the API call
        response = requests.get(et_url, params=params, headers=headers)

        return response

    @staticmethod
    def get_settings(device_sn):
        """
        Function to facilitate API v1 usage from Zentra for get /settings to get geolocation and other metadata


        :param device_sn: ID of the logger you are wanting data from
        :return: response: API response
        """

        # Setup parameters for the API call
        et_url = "https://zentracloud.com/api/v1/settings"
        headers = {'content-type': 'application/json', 'Authorization': TOKEN}
        params = {
            'sn': device_sn,
        }

        # Make the API call
        response = requests.get(et_url, params=params, headers=headers)

        return response


    @staticmethod
    def structure_all_readings(all_data_returned):
        """
        Function to take data provided by the Zentra API and process it into a simpler dictionary organized by date,
        with each date containing the different values that we received from the API for that date.


        :param all_data_returned:
        :return: all_results = {
            9-25: {
                'Air Temperature': [],
                'Atmospheric Pressure': [],
                ...},
            9-26: {
                'Air Temperature': [],
                'Atmospheric Pressure': [],
                ...},
            ...
            }
        """
        all_results = {}
        if len(all_data_returned) == 0:
            return all_results

        device_name = all_data_returned[-1]["data"]["Air Temperature"][-1]["metadata"]["device_name"]
        device_name_date = all_data_returned[-1]["data"]["Air Temperature"][-1]["readings"][-1]["datetime"]

        print(f'\tLatest Device Name: {device_name} for date: {device_name_date}')

        # For each page we got back from the API
        for data_page in all_data_returned:

            # For each type of data value reading (Air Temperature, Atmospheric Pressure, Battery Percent, etc)
            for value_type in data_page["data"]:

                # Counter we may need for multiple lists of VWC values being returned when we have several VWC
                # sensors connected to the logger
                data_type_counter = 0
                depths_seen = []

                # For each potential list of values, for this value type  (most of the time should only be 1 group)
                # May have multiple lists if there were more than 1 sensor reading the same value type, for example
                # VWC sensors
                for data_point_group in data_page["data"][value_type]:

                    # Get data from metadata
                    port = data_point_group["metadata"]["port_number"]
                    sensor = data_point_group["metadata"]["sensor_name"]
                    units = data_point_group["metadata"]["units"]
                    depth = data_point_group["metadata"]["depth"]

                    # Default type key
                    value_type_key = value_type

                    # Logic case for scenario were we have multiple of the same sensors connected to a logger and are
                    # getting back multiple lists of data for a data type. Ex: VWC, Ec, etc
                    if len(data_page["data"][value_type]) > 1:
                        data_type_counter += 1
                        if depth != 'Not Set' and depth != '':
                            if depth not in depths_seen:
                                value_type_key = f'{value_type} {depth}'
                            else:
                                value_type_key = f'{value_type} {depth} {data_type_counter}'
                            depths_seen.append(depth)
                        else:
                            value_type_key = f'{value_type} {data_type_counter}'

                    # Get data from the readings
                    for data_point in data_point_group["readings"]:
                        error_flag = data_point["error_flag"]
                        error_description = data_point["error_description"]
                        date_str = data_point["datetime"]
                        date = parser.parse(date_str)

                        # Only add data that does not have an error flag triggered
                        if not error_flag:
                            if date not in all_results:
                                all_results[date] = {
                                    value_type_key: [data_point["value"]]
                                }
                            else:
                                if value_type_key not in all_results[date]:
                                    all_results[date][value_type_key] = [data_point["value"]]
                                else:
                                    all_results[date][value_type_key].append(data_point["value"])
                        else:
                            pass
                            # print(f'Error: {value_type} - Date:{date}   -    {error_description}')
                            # print(error_description)

        return all_results

    @staticmethod
    def filter_all_readings(
            all_data,
            filter_year=datetime.now().year,
            filter_install_date=datetime(datetime.now().year, day=1, month=1),
            filter_uninstall_date=datetime(datetime.now().year, day=31, month=12)
    ):
        """
        Function that takes the structured data from the API after restructuring it to have it by date, and filters
        the data to only keep data from within the parameters

        :param filter_uninstall_date:
        :param filter_install_date:
        :param filter_year:
        :param all_data:
        """
        filtered_data = {}
        dynamic_filtered_data = {}

        if len(all_data) == 0:
            return filtered_data, dynamic_filtered_data

        # Need to add tzoffset to the provided datetimes to make them aware, so we can compare with the dates from
        # the API which is aware. Cannot compare nonaware vs aware, and it's better to make them all aware to track
        # time zone information
        # Set up the offset information for Pacific Time Zone
        datetime_offset = tzoffset(None, -25200)  # -25200 seconds = -7 hours

        # Add that offset to the filter datetimes
        filter_install_date_aware = filter_install_date.replace(tzinfo=datetime_offset)
        filter_uninstall_date_aware = filter_uninstall_date.replace(tzinfo=datetime_offset)

        for date in all_data:
            #Filter by results from this year
            if date.year == filter_year and date >= filter_install_date_aware and date <= filter_uninstall_date_aware:
                if date not in filtered_data:
                    dynamic_filtered_data[date] = {}
                    filtered_data[date] = {
                        'Target Temperature': [],
                        'Air Temperature': [],
                        'Relative Humidity': [],
                        'VPD': [],
                        'Irrigation On Time': [],
                    }
                    current_date_data = all_data[date]

                    # Only grab the data we care about
                    # IR Sensor
                    if 'Target Temperature' in current_date_data:
                        filtered_data[date]['Target Temperature'] = current_date_data['Target Temperature']
                    else:
                        filtered_data[date]['Target Temperature'] = [None]

                    # VP4 / Atmos Sensor
                    if 'Air Temperature' in current_date_data:
                        filtered_data[date]['Air Temperature'] = current_date_data['Air Temperature']
                    else:
                        filtered_data[date]['Air Temperature'] = [None]

                    if 'Relative Humidity' in current_date_data:
                        filtered_data[date]['Relative Humidity'] = current_date_data['Relative Humidity']
                    else:
                        filtered_data[date]['Relative Humidity'] = [None]

                    if 'VPD' in current_date_data:
                        filtered_data[date]['VPD'] = current_date_data['VPD']
                    else:
                        filtered_data[date]['VPD'] = [None]

                    # VWC Sensor
                    # Filter keys that start with 'Water Content'
                    water_content_keys = [key for key in current_date_data.keys() if key.startswith('Water Content')]

                    if len(water_content_keys) > 0:
                        # Extract the numbers and sort them
                        # Use a lambda function to split and convert the number part
                        sorted_vwc_keys = sorted(water_content_keys, key=lambda x: int(x.split(' ')[2]))

                        for vwc_key in sorted_vwc_keys:
                            filtered_data[date][vwc_key] = current_date_data[vwc_key]

                    # Filter keys that start with 'Bulk EC'
                    bulk_ec_keys = [key for key in current_date_data.keys() if key.startswith('Bulk EC')]

                    if len(bulk_ec_keys) > 0:
                        # Extract the numbers and sort them
                        # Use a lambda function to split and convert the number part
                        sorted_ec_keys = sorted(bulk_ec_keys, key=lambda x: int(x.split(' ')[2]))

                        for ec_key in sorted_ec_keys:
                            filtered_data[date][ec_key] = current_date_data[ec_key]

                    # Switch Sensor
                    if 'Irrigation On Time' in current_date_data:
                        filtered_data[date]['Irrigation On Time'] = current_date_data['Irrigation On Time']
                    else:
                        filtered_data[date]['Irrigation On Time'] = [None]

                    # Grab data dynamically, regardless of what data is in all_data
                    for data_type in all_data[date]:
                        if data_type not in dynamic_filtered_data[date]:
                            dynamic_filtered_data[date][data_type] = [all_data[date][data_type]]
                        else:
                            dynamic_filtered_data[date][data_type].append(all_data[date][data_type])

        return filtered_data, dynamic_filtered_data

    @staticmethod
    def organize_all_readings(all_results_filtered):
        """
        Function to take filtered results and organize them in the same format that our current processing pipeline
        expects

        converted_results = {"dates": [], "canopy temperature": [], "ambient temperature": [], "rh": [], "vpd": [],
                             "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [], "vwc_3_ec": [],
                             "daily gallons": [], "daily switch": []}

        :param all_results_filtered:
        :return:
        """
        converted_results = {"dates": [], "canopy temperature": [], "ambient temperature": [], "rh": [], "vpd": [],
                             "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [], "vwc_3_ec": [],
                             "daily gallons": [], "daily switch": []}

        for date in all_results_filtered:
            date_no_tz_info = date.replace(tzinfo=None)
            converted_results["dates"].append(date_no_tz_info)

            date_dict = all_results_filtered[date]
            if "Target Temperature" in date_dict:
                target_temp = date_dict["Target Temperature"][-1]
            else:
                target_temp = None
            converted_results["canopy temperature"].append(target_temp)

            if "Air Temperature" in date_dict:
                air_temp = date_dict["Air Temperature"][-1]
            else:
                air_temp = None
            converted_results["ambient temperature"].append(air_temp)

            if "Relative Humidity" in date_dict:
                rh = date_dict["Relative Humidity"][-1]
            else:
                rh = None
            converted_results["rh"].append(rh)

            if "VPD" in date_dict:
                vpd = date_dict["VPD"][-1]
            else:
                vpd = None
            converted_results["vpd"].append(vpd)

            # Handle VWC Sensors
            # Multiple VWCs
            if "Water Content 1" in date_dict:
                vwc_1 = date_dict["Water Content 1"][-1]
            else:
                vwc_1 = None
            converted_results["vwc_1"].append(vwc_1)

            if "Water Content 2" in date_dict:
                vwc_2 = date_dict["Water Content 2"][-1]
            else:
                vwc_2 = None
            converted_results["vwc_2"].append(vwc_2)

            if "Water Content 3" in date_dict:
                vwc_3 = date_dict["Water Content 3"][-1]
            else:
                vwc_3 = None
            converted_results["vwc_3"].append(vwc_3)

            # # Dynamic way to grab VWC
            # # Step 1: Filter keys that start with 'Water Content'
            # water_content_keys = [key for key in date_dict.keys() if key.startswith('Water Content')]
            #
            # if len(water_content_keys) > 1:
            #     # Step 2: Extract the numbers and sort them
            #     # Use a lambda function to split and convert the number part
            #     sorted_keys = sorted(water_content_keys, key=lambda x: int(x.split(' ')[2]))
            #
            #     vwc_counter = 0
            #     for skey in sorted_keys:
            #         vwc_counter += 1
            #         vwc = date_dict[skey][-1]
            #         converted_results[f'vwc_{vwc_counter}'].append(vwc)
            # else:
            #     converted_results['vwc_1'].append(None)
            #     converted_results['vwc_2'].append(None)
            #     converted_results['vwc_3'].append(None)

            if "Bulk EC 1" in date_dict:
                vwc_1_ec = date_dict["Bulk EC 1"][-1]
            else:
                vwc_1_ec = None
            converted_results["vwc_1_ec"].append(vwc_1_ec)

            if "Bulk EC 2" in date_dict:
                vwc_2_ec = date_dict["Bulk EC 2"][-1]
            else:
                vwc_2_ec = None
            converted_results["vwc_2_ec"].append(vwc_2_ec)

            if "Bulk EC 3" in date_dict:
                vwc_3_ec = date_dict["Bulk EC 3"][-1]
            else:
                vwc_3_ec = None
            converted_results["vwc_3_ec"].append(vwc_3_ec)

            # # Dynamic way to grab EC
            # # Multiple ECs
            # # Step 1: Filter keys that start with 'Bulk EC'
            # ec_keys = [key for key in date_dict.keys() if key.startswith('Bulk EC')]
            #
            # if len(ec_keys) > 0:
            #     # Step 2: Extract the numbers and sort them
            #     # Use a lambda function to split and convert the number part
            #     sorted_ec_keys = sorted(ec_keys, key=lambda x: int(x.split(' ')[2]))
            #
            #     ec_counter = 0
            #     for skey in sorted_ec_keys:
            #         ec_counter += 1
            #         ec = date_dict[skey][-1]
            #         converted_results[f'vwc_{ec_counter}_ec'].append(ec)
            # else:
            #     converted_results['vwc_1_ec'].append(None)
            #     converted_results['vwc_2_ec'].append(None)
            #     converted_results['vwc_3_ec'].append(None)

            if "Irrigation On Time" in date_dict:
                switch = date_dict["Irrigation On Time"][-1]
            else:
                switch = None
            converted_results["daily switch"].append(switch)

        return converted_results

    @staticmethod
    def get_and_filter_all_data(
            device_sn,
            start_date,
            end_date,
            filter_year=datetime.now().year,
            filter_install_date=datetime(datetime.now().year, day=1, month=1),
            filter_uninstall_date=datetime(datetime.now().year, day=31, month=12)
    ):
        """
        Pipeline helper function to query data from Zentra, structure it, filter it, and then put it in the same
        organized format that our current processing pipeline expects

        :param filter_uninstall_date:
        :param filter_install_date:
        :param filter_year:
        :param device_sn:
        :param start_date:
        :param end_date:
        :return:
        """
        response_success = False
        all_raw_data_returned = {}
        all_results_structured = {}
        all_results_filtered = {}
        all_results_dynamically_filtered = {}
        organized_results = {"dates": [], "canopy temperature": [], "ambient temperature": [], "rh": [], "vpd": [],
                             "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [], "vwc_3_ec": [],
                             "daily gallons": [], "daily switch": []}

        print(f'Grabbing all data from Zentra...')
        response_success, all_raw_data_returned = Zentra.get_all_readings(
            device_sn,
            start_date,
            end_date
        )

        if response_success:
            print('<- Got all data')
            print()

            # If we actually get data back, continue the pipeline
            if len(all_raw_data_returned) != 0:
                print('Structuring data...')
                all_results_structured = Zentra.structure_all_readings(all_raw_data_returned)
                print('<- Structured data')
                print()

                print('Filtering data...')
                all_results_filtered, all_results_dynamically_filtered = Zentra.filter_all_readings(
                    all_results_structured,
                    filter_year,
                    filter_install_date,
                    filter_uninstall_date
                )
                print('<- Filtered data')
                print()

                print('Organizing data...')
                organized_results = Zentra.organize_all_readings(all_results_filtered)
                print('<- Organized data')
                print()

        return response_success, all_raw_data_returned, all_results_structured, all_results_filtered, all_results_dynamically_filtered, organized_results


# URL="https://protect.checkpoint.com/v2/r01/___https://zentracloud.com/api/v1/settings"___.YzJ1Om1vcm5pbmdzdGFyOmM6bzo2M2I3ZDA4MjAxMTZiZWFjMTFjNzViOWQzNDE4MDg2Nzo3OjNmYjY6OWE2M2QwZDYxMWQzNmUwMDJhMGJhNTcwNmY3NmUzZTg1ZjIzMjk4MTZjYzkxYzI2YmQ4OTZhNzlhZjM3MmUzZDp0OlQ6Tg;
# params = {"sn":"z6-01247"}
# headers = {"Authorization":TOKEN}
# payload = requests.get(URL,headers=headers,params=params).json()
# print(payload)
# print(payload["device"]["measurement_settings"]) # reading interval in seconds
# print(payload["device"]["measurement_settings"][0]) # current reading interval in seconds
# print(payload["device"]["locations"]) # positions
# print(payload["device"]["locations"][0]) # current position
# res = Zentra.get_settings('z6-02013')
# print(res)