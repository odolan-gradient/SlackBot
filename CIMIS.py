import csv
from datetime import timedelta, datetime
from itertools import zip_longest
from math import radians, sqrt, sin, cos, asin

import requests
from requests.adapters import HTTPAdapter, Retry


# from requests.packages.urllib3.util.retry import Retry

def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

class CIMIS(object):
    """
    Class to facilitate calling the CIMIS API to get ETo for a station.

    Attributes:
        api_key_string: String for building URL
        api_key: String holding my key to be able to access the CIMIS API
        target_string: String for building URL
        start_date_string: String for building URL
        end_date_string: String for building URL
        data_items_string: String for building URL
        data_items: String holding what type of information we are requesting from the API
        initial_url: String for building URL
    """

    # api_key = '2532b920-c439-4be8-8d4c-c98be7c00f3f' #OLD ONE
    # api_key = 'dafc4a37-cdb3-4b58-a73f-d8990eef4321' #SECOND OLD ONE
    # api_key = '16d3a734-a110-4a59-bcbd-dacbf7c0a57a'  #THIRD - jgarrido@morningstarco.com
    api_key = '26e2a4f7-9606-4719-b509-4a0f49f9af6e'  # fourth one
    CIMIS_API_BASE_URL = 'http://et.water.ca.gov/api/data'
    cimisStations = {}
    all_current_stations_data_dicts_list = []

    def get_eto(self, cimis_stations: list[str], start_date: str, end_date: str, data_items: list[str] = ['day-eto']):
        """
        Function to get ETo from CIMIS API.

        :param data_items: List of data parameters in string format that we want to get data for
        :param cimis_stations: List of CIMIS station numbers in string format that we want to pull information from
        :param start_date: String for the start date from which to pull information
        :param end_date: String for the end date from which to pull information
        :return: eto:
        """


        # Join the strings lists with commas into a single string for API call
        data_items_string = ', '.join(data_items)
        cimis_stations_string = ', '.join(cimis_stations)

        # API call to CIMIS
        url = (f'{self.CIMIS_API_BASE_URL}?appKey={self.api_key}&targets={cimis_stations_string}&startDate={start_date}'
               f'&endDate={end_date}&dataItems={data_items_string}')
        content = None
        print(f'\t\tQuerying CIMIS API = {url}')
        try:
            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 443, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            http = requests.Session()
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            response = http.get(url)

            # OLD METHOD WITHOUT RETRY OR BACKOFF
            # response = requests.get(url, timeout=120)

            if response.ok:
                print(f'\t\tSuccessful API CIMIS call for station/s - {cimis_stations_string}')
                content = response.json()
            else:
                print(f'\t\tFailed API CIMIS call for station/s - {cimis_stations_string}')
                print(response.json())

        except Exception as error:
            print(f'\tERROR in CIMIS call for station - {cimis_stations_string}')
            print(error)
        except requests.exceptions.Timeout:
            print('Timeout')
        except requests.exceptions.TooManyRedirects:
            print('Too many redirects')
        except requests.exceptions.RequestException as e:
            print('ERROR')
            print(e)

        return content


    def get_all_stations_et_data(self, all_current_cimis_stations, start_date, end_date):
        """
        Loop through all current cimis stations and call CIMIS API to get the ET information for each station
        between two given parameter dates startDate and endDate

        :param all_current_cimis_stations:
        :param start_date: Date in string format '2021-08-25'
        :param end_date: Date in string format '2021-08-25'
        :return: all_current_stations_data_dicts_list - List of station dictionaries with all ET data in the following format
        [{'station': '2', 'dates': ['2021-08-25'], 'eto': ['0.24']},{...},...]
        """

        all_current_stations_data_dicts_dict = {}
        start_date_datetime = datetime.strptime(start_date, "%Y-%m-%d")
        end_date_datetime = datetime.strptime(end_date, "%Y-%m-%d")
        end_date_datetime = end_date_datetime + timedelta(days=1)
        dates = []
        for single_date in daterange(start_date_datetime, end_date_datetime):
            # print(single_date.strftime("%Y-%m-%d"))
            dates.append(single_date.strftime("%Y-%m-%d"))

        # Convert the station number integers to strings
        cimis_station_list = [str(station.station_number) for station in all_current_cimis_stations if station.active and not station.updated]
        data_requested = ['day-eto']


        # Call the API to get the ET data for all stations
        etos = self.get_eto(cimis_station_list, start_date, end_date, data_requested)

        if etos is None:
            print("ETo is none, Issue with API Call")
        else:
            all_current_stations_data_dicts_dict = self.fill_all_stations_et_data_dict(etos, all_current_cimis_stations, dates)

        return all_current_stations_data_dicts_dict

    def getDictForStation(self, cimisStation, startDate, endDate):
        """
        Grabs eto for specific cimis station and fills ET dictionary f
        :param cimisStation:
        :param startDate:
        :param endDate:
        :return:
        """
        dicts = []
        etos = self.get_eto([cimisStation], startDate, endDate)
        if etos is not None:
            dictio = self.fill_et_dict(cimisStation, etos)
        else:
            dictio = None
        return dictio

    def fill_et_dict(self, station, etos):
        """
        Populate dictionary with et data from CIMIS API

        :param station: String number of the cimis station
        :param etos: ETo data
        :return: dictionary with ETo data populated
        """

        vals = {'station': station,
                'dates': [],
                'eto': []}

        try:
            if etos['Data']['Providers'][0]['Records']:
                data = etos['Data']['Providers'][0]['Records']
                for each in data:
                    vals['dates'].append(each['Date'])
                    vals['eto'].append(each['DayEto']['Value'])
        except Exception as error:
            print(f'\tERROR in fill_et_dict')
            vals['dates'].append(None)
            vals['eto'].append(None)
            print(etos)
            print(error)
        except requests.exceptions.Timeout:
            print('Timeout')
        except requests.exceptions.TooManyRedirects:
            print('Too many redirects')
        except requests.exceptions.RequestException as e:
            print('ERROR')
            print(e)
        return vals


    def fill_et_dict_latest_value(self, station, dates, lastEtValue):
        """
        Populate dictionary with et data from CIMIS API

        :param station: String number of the cimis station
        :return: dictionary with ETo data populated
        """

        vals = {'station': station,
                'dates': [],
                'eto': []}

        for date in dates:
            vals['dates'].append(date)
            vals['eto'].append(lastEtValue)

        return vals

    def test_writing_to_db_all_et_data(etData):
        if etData != None:
            filename = 'all et.csv'
            print('- writing data to csv')
            keys = ["dates", "eto"]
            justEtData = {key: etData[key] for key in keys}

            with open(filename, "w", newline='') as outfile:
                writer = csv.writer(outfile)
                writer.writerow(justEtData.keys())
                writer.writerows(zip_longest(*justEtData.values()))
            print('...Done - file: ' + filename)
            #
            # self.dbwriter.write_to_table_from_csv(dataset_id, table_id, filename, schema)

    def get_list_of_active_eto_stations(self):
        content = self.get_all_station_data()
        if content is None:
            return None

        active_eto_stations = []
        for station in content['Stations']:
            if station['IsEtoStation'] == 'True' and station['IsActive'] == 'True':
                active_eto_stations.append(station)
        return active_eto_stations

    def get_all_station_data(self):
        url = 'http://et.water.ca.gov/api/station'
        content = None
        try:
            retry_strategy = Retry(
                total=3,
                backoff_factor=2,
                status_forcelist=[429, 443, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            http = requests.Session()
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            response = http.get(url)

            if response.ok:
                print(f'\t\tSuccessful API CIMIS call for all stations info')
                content = response.json()
            else:
                print(f'\t\tFailed API CIMIS call for all stations info')
                print(response.json())

        except Exception as error:
            print(f'\t\tERROR in CIMIS call for all stations')
            print(error)
        except requests.exceptions.Timeout:
            print('Timeout')
        except requests.exceptions.TooManyRedirects:
            print('Too many redirects')
        except requests.exceptions.RequestException as e:
            print('ERROR')
            print(e)
        return content

    def get_historical_data_for_new_station(self, station: str, years: int = 5):
        """
        Call CIMIS API for eto data for new station that doesnt have an ET table in the DB
        :param station:
        :param years: How many past years of historical data we want, defaulted as 5
        :return: Dictionary of dates and etos for past 5 years of a station
        """
        current_date = datetime.today()
        results = {}
        print(f"\t\tPulling historical ET data for station {station} for past 5 years")

        for year in range(0, years):
            if station == 0: break
            new_year = current_date.year - year - 1
            start_date = str(datetime(new_year, 1, 1).date())
            end_date = str(datetime(new_year, 12, 31).date())

            station_data = self.getDictForStation(station, start_date, end_date)
            # Check to make sure its not a bad API call - bad API call will be eto = [none] dates = [none]
            # A station with no data will still give the dates and a list of eto = [None...]
            if station_data is None:
                print("\t\tStation data came back empty - API call fail likely")
                results = None
                break

            # Delete the leap year day to keep even number of days for each year
            for date_str in station_data['dates']:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
                if date_obj.month == 2 and date_obj.day == 29:
                    index = station_data['dates'].index(date_str)
                    del station_data['dates'][index]

            # For each year add to the dictionary that will represent the Hist ET table
            results[f'Year_{new_year}'] = station_data["dates"]
            results[f'Year_{new_year}_ET'] = station_data["eto"]

        return results

    def check_station_validity(self, station_data: dict, valid_points: int = 330) -> bool:
        """
        Check a stations data we receive from CIMIS for validity (at least 30 data points each year)
        :param valid_points: Amount of data points from each year needed for station to be valid
        :param station_data: Dictionary of previous years ET data for station
        :return: bool whether station data is valid
        """
        # Look for only the ET not the dates
        search_string = 'ET'
        filtered_keys = {key: value for key, value in station_data.items() if search_string in key}
        if len(filtered_keys) > 0:
            for year in filtered_keys.values():
                # If for this year there are less than valid_points, return Invalid
                valid = sum(1 for value in year if value is not None)
                if valid < valid_points:
                    print(f'Insufficient Data from CIMIS: Valid points for given year is {valid} which is less than {valid_points}\n')
                    return False
            else:
                return True
        return False

    def get_average_et(self, station_data) -> list:
        """
        For a given stations historical data we average each day across the years
        :param station_data:
        :return: A list of averages by date
        """
        if not station_data:
            return []

        # Filter to only get the ET value rows not the dates
        # TODO: pass in just_et_dict from check_validity
        search_string = 'ET'
        just_et_dict = {key: value for key, value in station_data.items() if search_string in key}

        if not just_et_dict:
            return []

        # Calculate the maximum length of the lists to determine the number of rows to process
        max_length = max(len(values) for values in just_et_dict.values() if values)

        # Prepare a list to store the sums and counts for each index
        sums = [0] * max_length
        counts = [0] * max_length

        # Iterate over each years values in the filtered dictionary
        for values in just_et_dict.values():
            for i in range(len(values)):
                if values[i] is not None:
                    sums[i] += float(values[i])
                    counts[i] += 1

        # Calculate the averages for each index
        averages = []
        for sum_val, count in zip(sums, counts):
            if count > 0:
                averages.append(sum_val / count)
            else:
                averages.append(None)  # Append None if no values were added at this index

        return averages

    def new_et_station_data(self, et_station: str) -> (bool, dict):
        """
        Function gets historical CIMIS data for the station creating an average column
        :param et_station: Et Station
        :return: bool whether station data is valid
        """
        station_is_valid = False

        new_et_results = self.get_historical_data_for_new_station(et_station)

        # Is there any data or was there an API fail and are we checking validity
        if new_et_results:
            station_is_valid = self.check_station_validity(new_et_results)
            averages = self.get_average_et(new_et_results)
            new_et_results['Average'] = averages

        return station_is_valid, new_et_results

    def get_closest_station(self, stations, lat, long, stations_to_skip, max_range=9999):
        """

        :param stations:
        :param lat:
        :param long:
        :param stations_to_skip:
        :param max_range:
        :return:
        """

        latitude = radians(float(lat))
        longitude = radians(float(long))
        best_cimis_station = None
        shortest_distance_recorded = 9999
        for station in stations:
            station_number = station["StationNbr"]
            if station_number not in stations_to_skip:
                distance = get_distance(station, latitude, longitude)

                if distance > max_range:
                    continue

                if distance < shortest_distance_recorded:
                    shortest_distance_recorded = distance
                    best_cimis_station = station_number

        return best_cimis_station

    def get_closest_station_in_county(self, station_number, stations_to_skip, cached_stations, active_stations):
        """
        Get closest cimis station in county, if one is found test its data validity, if none valid return None and
         updated stations info dict with the CIMIS API data that was validated
        :param station_number:
        :param stations_to_skip:
        :param cached_stations:
        :return:
        """
        county_stations, station_data = get_county_stations(station_number, active_stations)
        county = station_data['County']

        print(f"\tLooking for closest station in {county} county to station {station_data['StationNbr']}")

        lat_parts = station_data['HmsLatitude'].split('/')
        long_parts = station_data['HmsLongitude'].split('/')
        lat = lat_parts[1].strip()
        long = long_parts[1].strip()

        station_is_valid = False
        best_cimis_station = None

        if county_stations is not None:
            while station_is_valid is False:
                best_cimis_station = self.get_closest_station(county_stations, lat, long, stations_to_skip)

                if best_cimis_station is None:
                    return None

                # If we found a county station we try to grab its ET data
                else:
                    print(f'\n\t\tChecking station {best_cimis_station} validity')
                    station_is_valid, station_results = self.new_et_station_data(best_cimis_station)
                    cached_stations[best_cimis_station] = station_results
                    stations_to_skip.append(best_cimis_station)

        print(f'\t\tStation {best_cimis_station} data is valid\n')
        return best_cimis_station

    def get_closest_valid_station(self, latitude: float, longitude: float, stations_to_skip, stations_info: dict, active_stations,
                                  max_range: int = 80):
        """
        Get closest cimis station in range, if one is found test its data validity, if none valid return None and
         updated stations info dict with the CIMIS API data that was validated
        :param latitude:
        :param longitude:
        :param stations_to_skip:
        :param max_range:
        :param stations_info:
        :return:
        """
        print(f'Looking for closest station in range')

        station_is_valid = False
        best_cimis_station = None

        if active_stations is not None:
            while station_is_valid is False:
                best_cimis_station = self.get_closest_station(active_stations, latitude, longitude,
                                                              stations_to_skip, max_range)

                if best_cimis_station is None:
                    print('\tNo closest station in range found')
                    return None

                # If we found a county station we try to grab its ET data
                else:
                    print(f'Closest station found {best_cimis_station}, validating')
                    station_is_valid, station_results = self.new_et_station_data(best_cimis_station)
                    stations_info[best_cimis_station] = station_results
                    stations_to_skip.append(best_cimis_station)

        return best_cimis_station

    def fill_all_stations_et_data_dict(self, etos, all_current_cimis_stations, dates):
        dict_of_stations = {}
        for station in all_current_cimis_stations:
            dict_of_stations[station.station_number] = {'station': station, 'dates': [], 'eto': []}
        try:
            print('\tETo Data From CIMIS API call:')
            for eto_data_point in etos['Data']['Providers'][0]['Records']:
                station_number = eto_data_point['Station']
                eto_data_point_date = eto_data_point['Date']
                eto_data_point_value = eto_data_point['DayEto']['Value']

                dict_of_stations[station_number]['dates'].append(eto_data_point_date)
                dict_of_stations[station_number]['eto'].append(eto_data_point_value)
                if eto_data_point_value is not None:
                    dict_of_stations[station_number]['station'].latest_eto_value = eto_data_point_value
                    dict_of_stations[station_number]['station'].latest_eto_date = eto_data_point_date
                else:
                    print(f'\t\tStation: {station_number} -> Date: {eto_data_point_date} got a None value for ET')
            for station in dict_of_stations:
                print(f'\t\tStation: {station} -> ')
                print(f'\t\t\tDates: {dict_of_stations[station]["dates"]}')
                print(f'\t\t\tETos: {dict_of_stations[station]["eto"]}')

        except Exception as error:
            print('ERROR in fill_all_stations_et_data_dict')
            print(error)
            print(etos)

        # Checking for data completion and filling in missing data
        for each_station in dict_of_stations:
            if len(dates) == len(dict_of_stations[each_station]['dates']):
                dict_of_stations[each_station]['station'].updated = True
            else:
                # No longer filling in missing data as now we are processing 10 days of et at a time and don't want
                # to overwrite 9 days of good data with 1 day of old data if we have a bad call
                # # Fill in missing data
                # for date in dates:
                #     if date not in dict_of_stations[each_station]['dates']:
                #         dict_of_stations[each_station]['dates'].append(date)
                #         dict_of_stations[each_station]['eto'].append(dict_of_stations[each_station]['station'].latest_eto_value)
                pass

        return dict_of_stations


def get_county_stations(station_number: str, all_stations):
    """
    Gets all CIMIS stations in the county
    :param station_number: The station number to find the county.
    :param all_stations: The dictionary containing all stations.
    :return: A tuple containing a dictionary of stations in the county and the station data for the given station number.
    """
    station_data = None
    county = None
    county_stations = {}
    # First, find the county of the given station number
    if all_stations:
        for station in all_stations:

            # Adding to the county list
            if station['County'] in county_stations:
                county_stations[station['County']].append(station)
            # First time adding to dict
            else:
                county_stations[station['County']] = [station]
            # Get county
            if station['StationNbr'] == station_number:
                county = station['County']
                station_data = station

    if county is None:
        # If no such station number exists, return the original dictionary and None
        return all_stations, None

    # Filter out stations not in the county
    correct_county_stations = county_stations[county]
    return correct_county_stations, station_data


def get_distance(station, latitude, longitude):
    """

    :param station: CIMIS station data
    :param latitude:
    :param longitude:
    :return:
    """
    station_latitude = str(station["HmsLatitude"])
    station_longitude = str(station["HmsLongitude"])
    decimal_lat = float(station_latitude.split("/ ")[1])
    decimal_long = float(station_longitude.split("/ ")[1])
    rad_lat = radians(decimal_lat)
    rad_long = radians(decimal_long)

    distance_lat = rad_lat - latitude
    distance_long = rad_long - longitude

    a = sin(distance_lat / 2) ** 2 + cos(rad_lat) * cos(latitude) * sin(distance_long / 2) ** 2
    c = 2 * asin(sqrt(a))

    # Radius of earth in KM is 6371 and in Miles is 3956
    r = 3956
    distance = c * r
    return distance

cimis = CIMIS()

# res = cimis.get_closest_station_in_county('139', ['143'], {}, active_stations)
# print()
lat = 35.0816049
long = -119.1245581

stations_to_skip = []
#
# active_stations = cimis.get_list_of_active_eto_stations()
# res3 = cimis.get_closest_station(active_stations, lat, long, stations_to_skip)

# print(res3)