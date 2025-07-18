from __future__ import annotations

import datetime
import json
import time
import xml.etree.ElementTree as ET
from collections import defaultdict
from collections import deque
from datetime import date, timedelta, datetime
from os import path
from typing import Any

import requests
from dateutil import tz
from google.cloud import bigquery
from requests.adapters import HTTPAdapter, Retry

from CropCoefficient import CropCoefficient
from CwsiProcessor import CwsiProcessor
from DBWriter import DBWriter
from Field import Field
from Grower import Grower
from Notifications import Notification_SensorError, Notification_TechnicianWarning
from SharedPickle import get_dxd_file
from Soils import Soil
from Technician import Technician
from Thresholds import Thresholds
from Zentra import Zentra

DIRECTORY_YEAR = "2025"
DXD_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR + "\\Dxd Files\\"


####################################################################
# Base Class for a logger in a field                               #
####################################################################
def read_last_time(dxd_file: str):
    """
    Read and return the last time information was pulled from a dxd file.

    The Last Time information was pulled indicates when the last time the API was used to pull information

    :param dxd_file:
        File we want to get the last time from
    :return:
        if found return:
            last_time - datetime obj
            last_timestamp - timestamp
        else 0
    """
    txt = "no data found"
    with open(dxd_file) as file:
        data = json.load(file)
        if "created" in data:
            last_time_ts = data['device']['timeseries'][-1]['configuration']['values'][-1][0]
            return last_time_ts

    return 0


class Logger(object):
    """
    Class to hold information for 1 logger installed in the field.

    This class will encompass everything a logger needs to do to update its information. That includes
        downloading the new dxds, converting them, processing the information, and then writing it out
        to its Google Sheet.

    Attributes:
        id: String to hold the logger's ID
        password: String to hold the logger's password
        prev_day_gallons: Number to hold the previous day gallons. This value is needed and used to calculate what
            the next day's gallons are
        prev_day_switch: Number to hold the previous day switch minutes. This value is needed and used to calculate
            what the next day's switch minutes are
        daily_switch: List to hold values for daily switch
        ports: Dictionary to hold the different port values
        cwsi_processor: CwsiProcessor Object used to process the information from the logger
    """

    def __init__(
            self,
            id: str,
            password: str,
            name: str,
            crop_type: str,
            soil_type: str,
            gpm: float,
            irrigation_set_acres: float,
            logger_direction: str,
            install_date: datetime.date,
            lat: str = '',
            long: str = '',
            grower: Grower = None,
            field: Field = None,
            planting_date: datetime.date = None,
            rnd: bool = False,
            active: bool = True,
            nickname: str = ''
    ) -> object:
        """

        :param install_date:
        :param id:
        :param password:
        :param name:
        :param crop_type:
        :param field_capacity:
        :param wilting_point:
        :param gpm:
        :param irrigation_set_acres:
        :param grower:
        :param field:
        :param planting_date:
        :param rnd:
        """
        if len(nickname) > 0:
            self.nickname = nickname
        else:
            self.nickname = name

        self.id = id
        self.password = password
        self.name = name
        self.nickname = nickname
        self.grower = grower
        self.field = field
        self.cwsi_processor = CwsiProcessor()
        self.dbwriter = DBWriter()
        self.prev_day_switch = 0
        self.prev_day_gallons = 0
        self.updated = False
        self.crashed = False
        self.crop_coefficient = CropCoefficient()

        if self.id[0] == 'z':
            self.model = 'z6'
        else:
            self.model = 'unknown'
        if isinstance(planting_date, str):
            ptdate = datetime.strptime(planting_date, '%m/%d/%Y').date()
            self.planting_date = ptdate
        else:
            self.planting_date = planting_date
        self.crop_type = crop_type
        self.soil = Soil(soil_type)
        if gpm is not None:
            self.gpm = float(gpm)
        if irrigation_set_acres is not None:
            self.irrigation_set_acres = float(irrigation_set_acres)
        self.prev_mrid = 0
        self.prev_last_data_date = None
        self.rnd = rnd
        self.active = active
        self.lat = lat
        self.long = long
        self.logger_direction = logger_direction
        self.gdd_total = 0
        self.crop_stage = 'NA'
        self.ir_active = False
        self.ports = {}
        self.install_date = install_date
        self.broken = False
        self.uninstall_date = None
        self.consecutive_ir_values = deque()
        self.irrigation_ledger = {}

    def __repr__(self):
        return f'Logger Name: {self.name}, Nickname: {self.nickname}, Active: {self.active}, Crop Type: {self.crop_type}, Soil: {self.soil.soil_type}, IR Active: {self.ir_active}'

    def to_string(self):
        """
        Function used to print out output to screen.

        Print out the Logger sheet name, id, password, sheet url,
            sheet id, prev day gallons, prev day switch
        :return:
        """
        name_str = f'Name: {str(self.name)}'
        grower_str = f'Grower: {str(self.grower.name)}'
        id_str = f'ID: {str(self.id)}'
        active_str = f'Active: {str(self.active)}'
        if not hasattr(self, 'crop_type'):
            if hasattr(self, 'cropType'):
                self.crop_type = self.cropType
        crop_str = f'Crop: {str(self.crop_type)}'
        prev_day_switch_str = f'Prev Day Switch: {str(self.prev_day_switch)}'
        if not hasattr(self, 'gdd_total'):
            self.gdd_total = 'No GDD'
        gdd_str = f'GDD Total: {str(self.gdd_total)}'
        gpm_str = f'GPM: {str(self.gpm)}'
        soil_type_str = f'Soil Type: {str(self.soil.soil_type)}'
        updated_str = f'Updated: {str(self.updated)}'
        if not hasattr(self, 'crop_stage'):
            self.crop_stage = 'No Crop Stage'
        if not hasattr(self, 'irrigation_set_acres'):
            if hasattr(self, 'acres'):
                self.irrigation_set_acres = self.acres
        if not hasattr(self, 'crashed'):
            self.crashed = False
        if not hasattr(self, 'rnd'):
            self.rnd = False
        if not hasattr(self, 'ir_active'):
            self.ir_active = None
        install_date_str = f'Install Date: {str(self.install_date)}'
        formatted_consecutive_psi = [round(tup[0], 2) for tup in self.consecutive_ir_values]
        formatted_consecutive_sdd = [round(tup[1], 2) for tup in self.consecutive_ir_values]
        formatted_consecutive_dates = [f"{d.month}-{d.day}-{d.year}" for _, _, d in self.consecutive_ir_values]
        # formatted_consecutive_psi = [t[0] for t in self.consecutive_ir_values]
        consect_psi_str = f'Consec PSI: {formatted_consecutive_psi}'
        consect_dates_str = f'Consec Dates: {formatted_consecutive_dates}'

        print('....................................................................')
        print(f'\t{name_str:30} | Nickname: {str(self.nickname)}')
        print(f'\t{grower_str:30} | Field: {str(self.field.name)}')
        print(f'\t{id_str:30} | Password: {str(self.password)}')
        print(f'\t{active_str:30} | Broken: {str(self.broken)}')
        print(f'\t{install_date_str:30} | Planting Date: {str(self.planting_date)}')
        print(f'\t{crop_str:30} | R&D: {str(self.rnd)}')
        print(f'\t{prev_day_switch_str:30} | IR Active: {self.ir_active}')
        print(f'\t{consect_psi_str:30} | Consect SDD: {formatted_consecutive_sdd}')
        print(f'\t{consect_dates_str:30}')
        print(f'\t{gdd_str:30} | Crop Stage: {self.crop_stage}')
        print(f'\t{gpm_str:30} | Acres: {str(self.irrigation_set_acres)}')
        print(f'\t{soil_type_str:30} | FC: {str(self.soil.field_capacity)}   WP: {str(self.soil.wilting_point)}')
        print(f'\t{updated_str:30} | Crashed: {str(self.crashed)}')
        print('....................................................................')

    def get_logger_data(
            self,
            specific_mrid: int = None,
            subtract_from_mrid: int = 0,
            mr_id_location: str = DXD_DIRECTORY,
            dxd_save_location: str = DXD_DIRECTORY,
            specific_file_name: str = None,
            zentra_api_version: str = 'v1',
            specific_start_date: str = None,
            specific_end_date: str = None,
    ) -> tuple[bool, dict | None] | tuple[bool, Any, Any, Any, Any, Any]:
        """
        Download dxd files from decagon API containing all the logger information and store them.
        Call on the corresponding API for z6 loggers.

        Access information for the API is hard coded - email, user_password, url
        Files are stored in the OutputFolder with the Logger ID as the name. For example:
            z6-12345.dxd

        :param specific_mrid:
        :param subtract_from_mrid:
        :param mr_id_location:
        :param dxd_save_location:
        :param specific_file_name:
        :param zentra_api_version:
        :param specific_start_date: String date in the format: m-d-Y H:M
        :param specific_end_date: String date in the format: m-d-Y H:M
        :return:
        """

        if zentra_api_version == 'v1':
            mr_id = 0
            mr_id_file_path = ''
            dxd_save_location_file_path = ''
            if specific_file_name is None:
                specific_file_name = self.id + '.dxd'
            else:
                specific_file_name = specific_file_name + '.dxd'

            if self.crashed:
                print("\tCrashed so running with previous MRID: {0}".format(self.prev_mrid))
                mr_id = self.prev_mrid
                # Resetting the crashed boolean
                self.crashed = False
                mr_id_to_set_back = 0
            else:
                dxd_file = get_dxd_file(specific_file_name)
                file_mrid = self.get_mrid(dxd_file)

                print(f'\tDXD mrid = {file_mrid}')
                mr_id_to_set_back = file_mrid
                if specific_mrid is not None:
                    # We passed in a specific_mrid
                    mr_id = specific_mrid
                    print(
                        f"\t-->> Running from specific MRID: {str(specific_mrid)}"
                    )
                elif subtract_from_mrid > 0:
                    # We passed in a subtract_from_mrid
                    mr_id = file_mrid - subtract_from_mrid
                    print(
                        f"\t-->> Subtracting from MRID: {str(file_mrid)} - {str(subtract_from_mrid)} = {str(mr_id)}"
                    )
                else:
                    # No special pass in's, just default run
                    mr_id = file_mrid
                    mr_id_to_set_back = 0
                    self.prev_mrid = mr_id

            if mr_id < 0:
                print(f'\tMRID is negative {mr_id}, so setting to 0')
                mr_id = 0

            # # TEMPORARY HARDCODE SUBTRACT TO GET 21 DAYS OF DATA
            # if self.crop_type in ['almonds', 'Almonds', 'almond']:
            #     mr_id = mr_id - (24*21)
            # ######################
            api_time_start = time.time()
            response = self.zentra_api_call(mr_id)
            converted_raw_data = None

            if response.ok:
                response_success = True
                api_time_end = time.time()
                elapsed_time = api_time_end - api_time_start
                print(f'v1 API call took {round(elapsed_time)} secs')
                parsed_json = json.loads(response.content)
                self.write_dxd_to_drive(dxd_save_location_file_path, parsed_json, mr_id_to_set_back)

                print('Reading data-')
                raw_dxd = get_dxd_file(specific_file_name)
                raw_data = self.get_all_ports_information(raw_dxd)
                print('-Finished')
                print()
                converted_raw_data = raw_data

            else:
                print('\tResponse not OK')
                response_success = False
            return response_success, converted_raw_data
        # #
        # if zentra_api_version == 'v1':
        #     mr_id = 0
        #     mr_id_file_path = ''
        #     dxd_save_location_file_path = ''
        #     if specific_file_name is None:
        #         specific_file_name = self.id + '.dxd'
        #     else:
        #         specific_file_name = specific_file_name + '.dxd'
        #     if path.exists(mr_id_location):
        #         mr_id_file_path = mr_id_location + specific_file_name
        #     if path.exists(dxd_save_location):
        #         dxd_save_location_file_path = dxd_save_location + specific_file_name
        #
        #     if self.crashed:
        #         print("\tCrashed so running with previous MRID: {0}".format(self.prev_mrid))
        #         mr_id = self.prev_mrid
        #         # Resetting the crashed boolean
        #         self.crashed = False
        #         mr_id_to_set_back = 0
        #     else:
        #         if self.is_file(mr_id_file_path):
        #             file_mrid = self.read_mrid(mr_id_file_path)
        #         else:
        #             file_mrid = 0
        #         print(f'\tDXD mrid = {file_mrid}')
        #         mr_id_to_set_back = file_mrid
        #         if specific_mrid is not None:
        #             # We passed in a specific_mrid
        #             mr_id = specific_mrid
        #             print(
        #                 f"\t-->> Running from specific MRID: {str(specific_mrid)}"
        #             )
        #         elif subtract_from_mrid > 0:
        #             # We passed in a subtract_from_mrid
        #             mr_id = file_mrid - subtract_from_mrid
        #             print(
        #                 f"\t-->> Subtracting from MRID: {str(file_mrid)} - {str(subtract_from_mrid)} = {str(mr_id)}"
        #             )
        #         else:
        #             # No special pass in's, just default run
        #             mr_id = file_mrid
        #             mr_id_to_set_back = 0
        #             self.prev_mrid = mr_id
        #
        #     if mr_id < 0:
        #         print(f'\tMRID is negative {mr_id}, so setting to 0')
        #         mr_id = 0
        #
        #     # # TEMPORARY HARDCODE SUBTRACT TO GET 21 DAYS OF DATA
        #     # if self.crop_type in ['almonds', 'Almonds', 'almond']:
        #     #     mr_id = mr_id - (24*21)
        #     # ######################
        #     api_time_start = time.time()
        #     response = self.zentra_api_call(mr_id)
        #     converted_raw_data = None
        #
        #     if response.ok:
        #         response_success = True
        #         api_time_end = time.time()
        #         elapsed_time = api_time_end - api_time_start
        #         print(f'v1 API call took {round(elapsed_time)} secs')
        #         parsed_json = json.loads(response.content)
        #         self.write_dxd_file(dxd_save_location_file_path, parsed_json, mr_id_to_set_back)
        #
        #         print('Reading data-')
        #         raw_dxd = self.read_dxd()
        #         raw_data = self.get_all_ports_information(raw_dxd)
        #         print('-Finished')
        #         print()
        #         converted_raw_data = raw_data
        #
        #     else:
        #         print('\tResponse not OK')
        #         response_success = False
        #     return response_success, converted_raw_data

        elif zentra_api_version == 'v4':
            device_sn = self.id
            current_year = datetime.now().year
            dxd_save_location_file_path = ''

            if specific_file_name is None:
                file_name = self.id + '_v4.dxd'
            else:
                file_name = specific_file_name + '.dxd'
            if path.exists(dxd_save_location):
                dxd_save_location_file_path = dxd_save_location + file_name

            # Handle crashed logger runs
            if self.crashed:
                print(f"\tCrashed so running from last data date: {self.prev_last_data_date}")
                start_date = self.prev_last_data_date
                current_datetime_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
                end_date = current_datetime_dt.strftime("%m-%d-%Y %H:%M")
                # Resetting the crashed boolean
                self.crashed = False
                date_to_set_back = None
            else:
                # Go grab last data's date from dxd
                if self.is_file(dxd_save_location_file_path):
                    default_start_date = self.read_last_data_date(dxd_save_location_file_path)
                    print(f'\tDXD last data date = {default_start_date}')
                    date_to_set_back = default_start_date
                else:
                    start_date_dt = datetime(current_year, 1, 1, 0, 0)
                    default_start_date = start_date_dt.strftime("%m-%d-%Y %H:%M")
                    date_to_set_back = None
                    print(f'\tNo DXD file, start date set to default = {default_start_date}')

                # Set start date
                if specific_start_date is not None:
                    start_date = specific_start_date
                    print(
                        f"\t-->> Running from specific start date: {str(start_date)}"
                    )
                else:
                    start_date = default_start_date
                    self.prev_last_data_date = start_date
                    date_to_set_back = None

                # Set end date
                if specific_end_date is not None:
                    end_date = specific_end_date
                else:
                    current_datetime_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
                    end_date = current_datetime_dt.strftime("%m-%d-%Y %H:%M")

            (
                response_success,
                all_raw_data_returned,
                all_results_structured,
                all_results_filtered,
                all_results_dynamically_filtered,
                organized_results
            ) = Zentra.get_and_filter_all_data(
                device_sn, start_date, end_date)

            # Write raw data return to dxd
            if len(all_raw_data_returned) != 0:
                self.write_dxd_file(
                    dxd_save_location_file_path,
                    all_raw_data_returned,
                    date_to_set_back=date_to_set_back
                )
            return (response_success, all_raw_data_returned, all_results_structured, all_results_filtered,
                    all_results_dynamically_filtered, organized_results)
        return None

    def write_dxd_file(self, file_path, data_json, mr_id_to_set_back: int = 0, date_to_set_back: str = None):
        try:
            write_dxd_start_time = time.time()

            print('\tWriting dxd file...')

            if mr_id_to_set_back > 0:
                print(f'\tMRID was adjusted by special pass in (subtract or specific mrid)...')
                print(f'\tModifying file MRID with the set back MRID: {mr_id_to_set_back}')
                if "created" in data_json:
                    # Modify the MRID to the new value
                    data_json['device']['timeseries'][-1]['configuration']['values'][-1][1] = mr_id_to_set_back

            if date_to_set_back is not None:
                print(f'\tStart Date was adjusted by special pass in (specific start date)...')
                print(f'\tModifying file last data date with the set back date: {date_to_set_back}')
                if "pagination" in data_json[-1]:
                    data_json[-1]['pagination']['page_end_date'] = date_to_set_back

            with open(file_path, 'w', encoding="utf8") as fd:
                json.dump(data_json, fd, indent=4, sort_keys=True, default=str)

            write_dxd_end_time = time.time()
            print("----------FINISHED----------")
            write_dxd_elapsed_time_seconds = write_dxd_end_time - write_dxd_start_time

            write_dxd_elapsed_time_hours = int(write_dxd_elapsed_time_seconds // 3600)
            write_dxd_elapsed_time_minutes = int((write_dxd_elapsed_time_seconds % 3600) // 60)
            write_dxd_elapsed_time_seconds = int(write_dxd_elapsed_time_seconds % 60)
            print(f"\tWrite DXD execution time: {write_dxd_elapsed_time_hours}:"
                  + f"{write_dxd_elapsed_time_minutes}:"
                  + f"{write_dxd_elapsed_time_seconds} (hours:minutes:seconds)")
            print()
        except Exception as error:
            print('\tERROR in writing dxd file for json data for z6')
            self.updated = False
            print(error)

    def write_dxd_to_drive(
            self,
            file_id: str,
            data_json: dict,
            mr_id_to_set_back: int = 0,
            date_to_set_back: str = None,
    ):
        """
        Applies your MRID / date tweaks to data_json, then uploads it
        back into the existing Drive file identified by file_id.
        """
        start = time.time()
        print("\tWriting DXD to Drive…")

        # — your existing JSON‑tweaks —
        if mr_id_to_set_back > 0 and "created" in data_json:
            print(f"\tAdjusting MRID → {mr_id_to_set_back}")
            data_json['device']['timeseries'][-1]['configuration']['values'][-1][1] = mr_id_to_set_back

        if date_to_set_back is not None and isinstance(data_json, list):
            print(f"\tAdjusting page_end_date → {date_to_set_back}")
            if "pagination" in data_json[-1]:
                data_json[-1]['pagination']['page_end_date'] = date_to_set_back

        # — serialize to JSON bytes —
        payload = json.dumps(data_json, indent=4, sort_keys=True, default=str).encode('utf-8')
        media = MediaIoBaseUpload(io.BytesIO(payload),
                                  mimetype='application/json',
                                  resumable=True)

        # — upload via Drive API —
        svc = get_drive_service()
        svc.files().update(
            fileId=file_id,
            media_body=media,
            supportsAllDrives=True
        ).execute()

        # — timing log —
        elapsed = time.time() - start
        h, r = divmod(elapsed, 3600)
        m, s = divmod(r, 60)
        print(f"\tDone in {int(h)}h{int(m)}m{int(s)}s\n")
    def zentra_api_call(self, mr_id):
        print('\tCalling Zentra API...')
        url = 'https://zentracloud.com/api/v1/readings'
        params = {'user': 'jgarrido@morningstarco.com', 'user_password': 'Mexico1012', 'sn': self.id,
                  'device_password': self.password, 'start_mrid': mr_id}
        # OLD METHOD BUILDING WHOLE STRING BY HAND
        # email_lead = '?user='
        # email = 'jgarrido@morningstarco.com'
        # user_password_lead = '&user_password='
        # user_password = 'Mexico1012'
        # id_lead = '&sn='
        # id = self.id
        # password_lead = '&device_password='
        # password = self.password
        # mrid_lead = '&start_mrid='
        # built_url = url + email_lead + email + \
        #             user_password_lead + user_password + \
        #             id_lead + id + \
        #             password_lead + password
        # if mr_id > 0:
        #     built_url = built_url + mrid_lead + str(mr_id)
        try:
            retry_strategy = Retry(
                total=7,
                backoff_factor=2,
                status_forcelist=[429, 443, 500, 502, 503, 504]
            )
            adapter = HTTPAdapter(max_retries=retry_strategy)
            http = requests.Session()
            http.mount("https://", adapter)
            http.mount("http://", adapter)

            print(f"\tMRID for API Call: {mr_id}")

            response = http.get(url, params=params)
            print("\t-->> Request URL: " + str(response.url))

            # OLD METHOD WITHOUT RETRY OR BACKOFF
            # response = requests.get(url, timeout=120)

            if response.ok:
                print(f'\tSuccessful METER API')

        except Exception as error:
            print('\tERROR in making z6 data request to METER')
            self.updated = False
            print(error)
        except requests.exceptions.Timeout:
            print('\tTimeout in making z6 data request to METER')
            self.updated = False
        except requests.exceptions.TooManyRedirects:
            print('\tToo many redirects in making z6 data request to METER')
            self.updated = False
        except requests.exceptions.RequestException as e:
            print('\tERROR in making z6 data request to METER')
            self.updated = False
            print(e)
        return response

    def is_file(self, file: str) -> bool:
        """
        Check if a file is a file or not.

        :param file:
        :return:
            True if it is a file
            None if it is not
        """
        try:
            with open(file):
                pass
            return True
        except IOError as e:
            print("Unable to open file %s" % file)
            return None

    def read_mrid(self, file_path: str):
        """
        Read and return the MRID from a dxd file.

        MRID is a number that indicates how current the information in the dxd is. Everytime a download occurs,
        the MRID is set to a newer number. Each new download just requires the MRID + 1 to get the latest info
        since the last download.

        :param file_path:
            File we want to get the MRID from
        :return:
            MRID if found, 0 if not
        """
        with open(file_path) as file:
            data = json.load(file)
            if "created" in data:
                mrid = data['device']['timeseries'][-1]['configuration']['values'][-1][1]
                self.prev_mrid = mrid
                return mrid
        return 0
    def get_mrid(self, data):
        '''
        Meant for serverless dxd grab so no file path just pass the data object
        :param data:
        :return:
        '''
        if "created" in data:
            mrid = data['device']['timeseries'][-1]['configuration']['values'][-1][1]
            self.prev_mrid = mrid
            return mrid
        return 0

    def read_last_data_date(self, file_path: str):
        """
        Read and return the last data point date from a dxd file.

        :param file_path:
            File we want to get the date from
        :return:
            date if found, first day of this year if not
        """
        with open(file_path) as file:
            raw_data = json.load(file)
            if "pagination" in raw_data[-1]:
                last_data_date = raw_data[-1]['pagination']['page_end_date']
                # self.prev_mrid = mrid
                return last_data_date
            else:
                current_year = datetime.now().year
                last_data_date_dt = datetime(current_year, 1, 1, 0, 0)
                last_data_date = last_data_date_dt.strftime("%m-%d-%Y %H:%M")
                return last_data_date

    def read_ereset(self, dxd_file):
        """
        Read and return the eReset from a dxd file.

        eReset is a number that indicates how many times the logger has been reset. This is useful information
        because each time a logger is reset, the gallons and switch counters get reset to 0 as well which can affect
        the total tallies.

        :param dxd_file:
            File we want to get the eReset from
        :return:
            eReset if found, 0 if not
        """
        txt = "no data found"
        doc = ET.parse(dxd_file)
        root = doc.getroot()
        for element in root.iter():
            if 'Status' in element.tag:
                ereset = int(element.get('eReset'))
                return ereset
        return 0

    def read_battery_level(self, zentra_api_version: str = 'v1'):
        """
        Read and return the Battery level from a dxd file.

        Battery is a number that indicates how much battery is left in the logger batteries.

        :param dxd_file:
            File we want to get the Battery level from
        :return:
            Battery level if found, None if not
        """
        if zentra_api_version == 'v1':
            file_name = DXD_DIRECTORY + self.id + '.dxd'
            dxd_file = get_dxd_file(file_name)
            data = json.load(dxd_file)
            if "created" in data:
                battery_level = data['device']['timeseries'][-1]['configuration']['values'][-1][-2][0]['value']
                return battery_level

        elif zentra_api_version == 'v4':
            if path.exists(DXD_DIRECTORY):
                file_name = DXD_DIRECTORY + self.id + '_v4.dxd'

            with open(file_name) as file:
                data_json = json.load(file)
                if "data" in data_json[-1]:
                    battery_level = data_json[-1]['data']['Battery Percent'][-1]['readings'][-1]['value']
                    return battery_level
        return None

    def read_dxd(self, dxd_save_location: str = DXD_DIRECTORY, file_name: str = None, ):
        """
        Read the dxd file and check the response.

        :return:
            Dictionary with raw dates and raw values
        """
        result = None
        if file_name is None:
            file_name = self.id + '.dxd'
        else:
            file_name = file_name + '.dxd'

        if path.exists(dxd_save_location):
            file_path = dxd_save_location + file_name

        raw_dxd = "no data found"
        with open(file_path) as file:
            raw_dxd = json.load(file)
        if raw_dxd == "no data found":
            return None
        return raw_dxd

    def get_all_ports_information(self, raw_dxd: dict, specific_year: int = datetime.now().year) -> dict:
        """
        Read the dxd file and returns the information in all ports.

        :param specific_year:
        :param raw_dxd:
            Dxd file to read
        :return:
            Dictionary holding the raw values in each port
        """
        raw_dxd_dict = None
        converted_results = {"dates": [], "canopy temperature": [], "ambient temperature": [], "rh": [], "vpd": [],
                             "vwc_1": [], "vwc_2": [], "vwc_3": [], "vwc_1_ec": [], "vwc_2_ec": [], "vwc_3_ec": [],
                             "daily gallons": [], "daily switch": []}
        data_from_previous_years = False
        min_number_of_values_for_timeseries = 5

        if "device" in raw_dxd:
            all_data_series = []
            for ind, timeseries in enumerate(raw_dxd['device']['timeseries']):
                # Check that the timeseries has at least 5 values
                if len(timeseries['configuration']['values']) >= min_number_of_values_for_timeseries:
                    # Grab start and end of timeseries
                    timeseries_start_date_timestamp = timeseries['configuration']['values'][0][0]
                    timeseries_end_date_timestamp = timeseries['configuration']['values'][-1][0]

                    # Special check because some timeseries seem to get 1 random value at the very end from an
                    # early year. When this happens, we just grab the date for the 2nd to last value instead of
                    # the last
                    if timeseries_end_date_timestamp < timeseries_start_date_timestamp:
                        # start = self.convert_timestamp_to_local_datetime(timeseries_start_date_timestamp)
                        # end = self.convert_timestamp_to_local_datetime(timeseries_end_date_timestamp)
                        #
                        # print('Error: END DATE < START DATE')
                        # print(self.field.name)
                        # print(self.name)
                        # print(f'{end} < {start}')
                        # print()
                        timeseries_end_date_timestamp = timeseries['configuration']['values'][-2][0]

                    # Convert those start and end timestamps to datetimes
                    timeseries_start_date_datetime = self.convert_timestamp_to_local_datetime(
                        timeseries_start_date_timestamp)
                    timeseries_start_date_datetime_year = timeseries_start_date_datetime.year

                    timeseries_end_date_datetime = self.convert_timestamp_to_local_datetime(
                        timeseries_end_date_timestamp)
                    timeseries_end_date_datetime_year = timeseries_end_date_datetime.year

                    if timeseries_start_date_datetime_year <= specific_year <= timeseries_end_date_datetime_year:
                        # Grab the port configuration of this particular timeseries
                        ports = self.get_ports(timeseries['configuration']['sensors'])

                        # Check if ports.keys() has every one of the required ports
                        required_ports = [1, 2, 3, 4, 5, 6]
                        has_all_ports = True

                        for port_num in required_ports:
                            if port_num not in ports.keys():
                                has_all_ports = False
                                break

                        # If it has all required ports and each port is set to the correct sensor:
                        if (has_all_ports and
                                ports[1] in ['Infra Red'] and
                                ports[2] in ['VP4', 'Atmos 14'] and
                                ports[3] in ['GS1', 'Terros 10', 'GS3', 'Terros 12'] and
                                ports[4] in ['GS1', 'Terros 10', 'GS3', 'Terros 12'] and
                                ports[5] in ['GS1', 'Terros 10', 'GS3', 'Terros 12'] and
                                ports[6] in ['Switch']):

                            # If we passed all our checks, this is a valid timeseries we care about so append it along with
                            # its ports to all_data_series as a tuple (timeseries['configuration], ports)
                            all_data_series.append((timeseries['configuration'], ports))
                        else:
                            print(f"Error not a standard configuration: {ports}")

            # For each tuple (chapter, ports) in all the chapters and ports that have data we care about
            for data_series, ports in all_data_series:
                # print(ports)
                data_index_offset, ir_port, vp4_port, vwc1_port, vwc2_port, vwc3_port, switch_port = self.get_sensor_port_indexes(
                    ports)
                sensor_data_indexes = self.get_sensor_individual_data_indexes()

                # Grab data
                for data_point in data_series['values']:
                    data_from_previous_years = False

                    timestamp = (data_point[0])

                    # Bandaid fix for Meter randomly giving us data from 1970 and crashing our local timestamp conversion. wtf
                    if timestamp > 0:
                        datapoint_datetime = self.convert_timestamp_to_local_datetime(timestamp)

                        # Check and only add data that is from current year going forward
                        d_year = datapoint_datetime.year
                        if d_year == specific_year or (datetime.now().month == 1 and datetime.now().day == 1):

                            # Check and only grab data from install date going forward
                            # This is to avoid grabbing data from a logger that was moved to another field from the
                            # previous field it was installed in
                            if self.install_date <= datapoint_datetime.date():
                                converted_results["dates"].append(datapoint_datetime)

                                # Grabbing IR data
                                if ir_port is not None:
                                    ir_temp_value = data_point[ir_port][sensor_data_indexes['ir temp']]['value']
                                    if ir_temp_value == 'None' or ir_temp_value == '':
                                        ir_temp_value = None
                                    converted_results["canopy temperature"].append(ir_temp_value)
                                else:
                                    converted_results["canopy temperature"].append(None)

                                # Grabbing VP4/Atmos 14 data
                                if vp4_port is not None:
                                    vp4_air_temp_value = data_point[vp4_port][sensor_data_indexes['vp4 air temp']][
                                        'value']
                                    if vp4_air_temp_value == 'None' or vp4_air_temp_value == '':
                                        vp4_air_temp_value = None
                                    converted_results["ambient temperature"].append(vp4_air_temp_value)

                                    vp_rh_value = data_point[vp4_port][sensor_data_indexes['vp4 rh']]['value']
                                    if vp_rh_value == 'None' or vp_rh_value == '':
                                        vp_rh_value = None
                                    converted_results["rh"].append(vp_rh_value)

                                    vp_vpd_value = data_point[vp4_port][sensor_data_indexes['vp4 vpd']]['value']
                                    if vp_vpd_value == 'None' or vp_vpd_value == '':
                                        vp_vpd_value = None
                                    converted_results["vpd"].append(vp_vpd_value)
                                else:
                                    converted_results["ambient temperature"].append(None)
                                    converted_results["rh"].append(None)
                                    converted_results["vpd"].append(None)

                                # Grabbing VWC 1 data
                                if vwc1_port is not None:
                                    vwc1_value = data_point[vwc1_port][sensor_data_indexes['vwc volumetric']]['value']
                                    if vwc1_value == 'None' or vwc1_value == '':
                                        vwc1_value = None
                                    converted_results["vwc_1"].append(vwc1_value)

                                    if ports[vwc1_port - data_index_offset] in ['GS3', 'Terros 12']:
                                        vwc1_ec_value = data_point[vwc1_port][sensor_data_indexes['vwc ec']]['value']
                                        if vwc1_ec_value == 'None' or vwc1_ec_value == '':
                                            vwc1_ec_value = None
                                        converted_results["vwc_1_ec"].append(vwc1_ec_value)
                                    else:
                                        converted_results["vwc_1_ec"].append(None)
                                else:
                                    converted_results["vwc_1"].append(None)

                                # Grabbing VWC 2 data
                                if vwc2_port is not None:
                                    vwc2_value = data_point[vwc2_port][sensor_data_indexes['vwc volumetric']]['value']
                                    if vwc2_value == 'None' or vwc2_value == '':
                                        vwc2_value = None
                                    converted_results["vwc_2"].append(vwc2_value)

                                    if ports[vwc2_port - data_index_offset] in ['GS3', 'Terros 12']:
                                        vwc2_ec_value = data_point[vwc2_port][sensor_data_indexes['vwc ec']]['value']
                                        if vwc2_ec_value == 'None' or vwc2_ec_value == '':
                                            vwc2_ec_value = None
                                        converted_results["vwc_2_ec"].append(vwc2_ec_value)
                                    else:
                                        converted_results["vwc_2_ec"].append(None)
                                else:
                                    converted_results["vwc_2"].append(None)

                                # Grabbing VWC 3 data
                                if vwc3_port is not None:
                                    vwc3_value = data_point[vwc3_port][sensor_data_indexes['vwc volumetric']]['value']
                                    if vwc3_value == 'None' or vwc3_value == '':
                                        vwc3_value = None
                                    converted_results["vwc_3"].append(vwc3_value)

                                    if ports[vwc3_port - data_index_offset] in ['GS3', 'Terros 12']:
                                        vwc3_ec_value = data_point[vwc3_port][sensor_data_indexes['vwc ec']]['value']
                                        if vwc3_ec_value == 'None' or vwc3_ec_value == '':
                                            vwc3_ec_value = None
                                        converted_results["vwc_3_ec"].append(vwc3_ec_value)
                                    else:
                                        converted_results["vwc_3_ec"].append(None)
                                else:
                                    converted_results["vwc_3"].append(None)

                                # Grabbing Switch data
                                if switch_port is not None:
                                    switch_value = data_point[switch_port][sensor_data_indexes['switch minutes']][
                                        'value']
                                    if switch_value == 'None' or switch_value == '':
                                        switch_value = None
                                    converted_results["daily switch"].append(switch_value)
                                else:
                                    converted_results["daily switch"].append(None)
                            # else:
                            #     print('Ignored some data from earlier in the year')
                        else:
                            data_from_previous_years = True

            if data_from_previous_years:
                print("Ignored some data from previous years")
        return converted_results

    def get_all_ports_information_weather_stations(self, raw_dxd: dict,
                                                   specific_year: int = datetime.now().year) -> dict:
        """
        Read the dxd file and returns the information in all ports.

        :param specific_year:
        :param raw_dxd:
            Dxd file to read
        :return:
            Dictionary holding the raw values in each port
        """
        raw_dxd_dict = None
        converted_results = {"dates": [], "solar radiation": [], "precipitation": [], "lightning activity": [],
                             "lightning distance": [],
                             "wind direction": [], "wind speed": [], "gust speed": [], "ambient temperature": [],
                             "relative humidity": [], "atmospheric pressure": [],
                             "x axis level": [], "y axis level": [], "vpd": []}
        data_from_previous_years = False
        min_number_of_values_for_timeseries = 5

        if "device" in raw_dxd:
            all_data_series = []
            for ind, timeseries in enumerate(raw_dxd['device']['timeseries']):
                # Check that the timeseries has at least 5 values
                if len(timeseries['configuration']['values']) >= min_number_of_values_for_timeseries:
                    # Grab start and end of timeseries
                    timeseries_start_date_timestamp = timeseries['configuration']['values'][0][0]
                    timeseries_end_date_timestamp = timeseries['configuration']['values'][-1][0]

                    # Special check because some timeseries seem to get 1 random value at the very end from an
                    # early year. When this happens, we just grab the date for the 2nd to last value instead of
                    # the last
                    if timeseries_end_date_timestamp < timeseries_start_date_timestamp:
                        # start = self.convert_timestamp_to_local_datetime(timeseries_start_date_timestamp)
                        # end = self.convert_timestamp_to_local_datetime(timeseries_end_date_timestamp)
                        #
                        # print('Error: END DATE < START DATE')
                        # print(self.field.name)
                        # print(self.name)
                        # print(f'{end} < {start}')
                        # print()
                        timeseries_end_date_timestamp = timeseries['configuration']['values'][-2][0]

                    # Convert those start and end timestamps to datetimes
                    timeseries_start_date_datetime = self.convert_timestamp_to_local_datetime(
                        timeseries_start_date_timestamp)
                    timeseries_start_date_datetime_year = timeseries_start_date_datetime.year

                    timeseries_end_date_datetime = self.convert_timestamp_to_local_datetime(
                        timeseries_end_date_timestamp)
                    timeseries_end_date_datetime_year = timeseries_end_date_datetime.year

                    if timeseries_start_date_datetime_year <= specific_year <= timeseries_end_date_datetime_year:
                        # Grab the port configuration of this particular timeseries
                        ports = self.get_ports(timeseries['configuration']['sensors'])

                        # Check if ports.keys() has every one of the required ports
                        required_ports = [2]
                        has_all_ports = True

                        for port_num in required_ports:
                            if port_num not in ports.keys():
                                has_all_ports = False
                                break

                        # If it has all required ports and each port is set to the correct sensor:
                        if has_all_ports and ports[2] in ['Atmos 41']:

                            # If we passed all our checks, this is a valid timeseries we care about so append it along with
                            # its ports to all_data_series as a tuple (timeseries['configuration], ports)
                            all_data_series.append((timeseries['configuration'], ports))
                        else:
                            print(f"Error not a standard configuration: {ports}")

            # For each tuple (chapter, ports) in all the chapters and ports that have data we care about
            for data_series, ports in all_data_series:
                # print(ports)
                data_index_offset, atmos_41_port = self.get_sensor_port_indexes_weather_stations(ports)
                sensor_data_indexes = self.get_sensor_individual_data_indexes_weather_stations()

                # Grab data
                for data_point in data_series['values']:

                    data_from_previous_years = False

                    timestamp = (data_point[0])

                    # Bandaid fix for Meter randomly giving us data from 1970 and crashing our local timestamp conversion. wtf
                    if timestamp > 0:
                        datapoint_datetime = self.convert_timestamp_to_local_datetime(timestamp)

                        # Check and only add data that is from current year going forward
                        d_year = datapoint_datetime.year
                        if d_year == specific_year or (datetime.now().month == 1 and datetime.now().day == 1):

                            '''
                            {"dates": [], "solar radiation": [], "precipitation": [], "lightning activity": [],
                             "lightning distance": [],
                             "wind direction": [], "wind speed": [], "gust speed": [], "ambient temperature": [],
                             "relative humidity": [], "atmospheric pressure": [],
                             "x axis level": [], "y axis level": [], "vpd": []}
                            '''
                            # Check and only grab data from install date going forward
                            # This is to avoid grabbing data from a logger that was moved to another field from the
                            # previous field it was installed in
                            try:
                                if self.install_date <= datapoint_datetime.date():
                                    converted_results["dates"].append(datapoint_datetime)

                                    if atmos_41_port is not None:
                                        solar_radiation_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'solar radiation']]['value']
                                        precipitation_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'precipitation']]['value']
                                        lightning_activity_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'lightning activity']]['value']
                                        lightning_distance_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'lightning distance']]['value']
                                        wind_direction_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'wind direction']]['value']
                                        wind_speed_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'wind speed']]['value']
                                        gust_speed_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'gust speed']]['value']
                                        ambient_temperature_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'air temperature']][
                                            'value']
                                        relative_humidity_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'relative humidity']][
                                            'value']
                                        atmospheric_pressure_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'atmospheric pressure']][
                                            'value']
                                        x_axis_level_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'x axis level']]['value']
                                        y_axis_level_value = data_point[atmos_41_port][sensor_data_indexes[
                                            'y axis level']]['value']
                                        vpd_value = data_point[atmos_41_port][sensor_data_indexes['vpd']]['value']

                                        if solar_radiation_value == 'None' or solar_radiation_value == '':
                                            solar_radiation_value = None
                                        if precipitation_value == 'None' or precipitation_value == '':
                                            precipitation_value = None
                                        if lightning_activity_value == 'None' or lightning_activity_value == '':
                                            lightning_activity_value = None
                                        if lightning_distance_value == 'None' or lightning_distance_value == '':
                                            lightning_distance_value = None
                                        if wind_direction_value == 'None' or wind_direction_value == '':
                                            wind_direction_value = None
                                        if wind_speed_value == 'None' or wind_speed_value == '':
                                            wind_speed_value = None
                                        if gust_speed_value == 'None' or gust_speed_value == '':
                                            gust_speed_value = None
                                        if ambient_temperature_value == 'None' or ambient_temperature_value == '':
                                            ambient_temperature_value = None
                                        if relative_humidity_value == 'None' or relative_humidity_value == '':
                                            relative_humidity_value = None
                                        if atmospheric_pressure_value == 'None' or atmospheric_pressure_value == '':
                                            atmospheric_pressure_value = None
                                        if x_axis_level_value == 'None' or x_axis_level_value == '':
                                            x_axis_level_value = None
                                        if y_axis_level_value == 'None' or y_axis_level_value == '':
                                            y_axis_level_value = None
                                        if vpd_value == 'None' or vpd_value == '':
                                            vpd_value = None

                                        converted_results["solar radiation"].append(solar_radiation_value)
                                        converted_results["precipitation"].append(precipitation_value)
                                        converted_results["lightning activity"].append(lightning_activity_value)
                                        converted_results["lightning distance"].append(lightning_distance_value)
                                        converted_results["wind direction"].append(wind_direction_value)
                                        converted_results["wind speed"].append(wind_speed_value)
                                        converted_results["gust speed"].append(gust_speed_value)
                                        converted_results["ambient temperature"].append(ambient_temperature_value)
                                        converted_results["relative humidity"].append(relative_humidity_value)
                                        converted_results["atmospheric pressure"].append(atmospheric_pressure_value)
                                        converted_results["x axis level"].append(x_axis_level_value)
                                        converted_results["y axis level"].append(y_axis_level_value)
                                        converted_results["vpd"].append(vpd_value)
                                    else:
                                        converted_results["solar radiation"].append(None)
                                        converted_results["precipitation"].append(None)
                                        converted_results["lightning activity"].append(None)
                                        converted_results["lightning distance"].append(None)
                                        converted_results["wind direction"].append(None)
                                        converted_results["wind speed"].append(None)
                                        converted_results["gust speed"].append(None)
                                        converted_results["ambient temperature"].append(None)
                                        converted_results["relative humidity"].append(None)
                                        converted_results["atmospheric pressure"].append(None)
                                        converted_results["x axis level"].append(None)
                                        converted_results["y axis level"].append(None)
                                        converted_results["vpd"].append(None)
                            except Exception as e:
                                print(f"Error: {e}")
                                print()
                        else:
                            data_from_previous_years = True

            if data_from_previous_years:
                print("Ignored some data from previous years")
        return converted_results

    def get_sensor_port_indexes(self, ports):
        ir_port = None
        vp4_port = None
        vwc1_port = None
        vwc2_port = None
        vwc3_port = None
        switch_port = None
        vwc_ports = []
        # Offset to account for Zentra data return having a timestamp in index 0, mrid in index 1,
        # and 1 extra value in index 2. Actual sensors don't start until index 3. Using an offset of 2
        # since the port data starts at 1 already, not from 0. So 1 + 2 = 3 as the first sensor data index.
        data_index_offset = 2
        for key in ports:
            value = ports[key]
            if ir_port is None and value == 'Infra Red':
                ir_port = key + data_index_offset
            elif vp4_port is None and value in ['VP4', 'Atmos 14']:
                vp4_port = key + data_index_offset
            elif value in ['GS1', 'GS3', 'Terros 10', 'Terros 12']:
                vwc_ports.append(key)
            elif switch_port is None and value == 'Switch':
                switch_port = key + data_index_offset

        # VWC ports grabbed after in a sorted list to ensure we assign them in the correct order. The first port with
        # a VWC sensor will be vwc1_port, the second will be vwc2_port, and so on until we have 3 ports for VWCs
        for key in sorted(vwc_ports):
            if vwc1_port is None:
                vwc1_port = key + data_index_offset
            elif vwc2_port is None:
                vwc2_port = key + data_index_offset
            elif vwc3_port is None:
                vwc3_port = key + data_index_offset

        return data_index_offset, ir_port, vp4_port, vwc1_port, vwc2_port, vwc3_port, switch_port

    def get_sensor_port_indexes_weather_stations(self, ports):
        atmos_41_port = None
        # Offset to account for Zentra data return having a timestamp in index 0, mrid in index 1,
        # and 1 extra value in index 2. Actual sensors don't start until index 3. Using an offset of 2
        # since the port data starts at 1 already, not from 0. So 1 + 2 = 3 as the first sensor data index.
        data_index_offset = 2
        for ind, key in enumerate(ports):
            value = ports[key]
            if atmos_41_port is None and value == 'Atmos 41':
                # Can't use key as the value to designate the atmos_41_port because the API doesn't respect that.
                # It uses the index to position the values, not the key for some stupid reason
                # atmos_41_port = key + data_index_offset
                atmos_41_port = ind + 1 + data_index_offset

        return data_index_offset, atmos_41_port

    def get_ports(self, time_series, dxd_save_location: str = DXD_DIRECTORY) -> dict:
        """
        Read the dxd file and return information on what sensor is connected to each port.

        :return:
            Dictionary with the port and what sensor is connected to each port
                {'number': value, ...}
        """
        ports = {}
        for sensor in time_series:
            sensor_type = self.sensor_name(sensor["sensor_number"])
            # ports[sensor["port"]] = sensor["sensor_number"]
            ports[sensor["port"]] = sensor_type

        return ports

    def remove_duplicate_data(self, raw_data: dict, ports: dict) -> dict:
        print('Looking for duplicates-')
        duplicates = []
        duplicate_dates = []
        converted_results = {"dates": [], "canopy temperature": [], "ambient temperature": [], "rh": [], "vpd": [],
                             "vwc 8": [], "vwc 16": [], "vwc 24": [], "daily gallons": [], "daily switch": []}
        for ind, val in enumerate(raw_data["dates"]):
            # print('Checking' + str(val))
            if val in converted_results["dates"]:
                duplicates.append(ind)
                duplicate_dates.append(val)
            else:
                converted_results["dates"].append(raw_data["dates"][ind])
                converted_results["canopy temperature"].append(raw_data["canopy temperature"][ind])
                converted_results["ambient temperature"].append(raw_data["ambient temperature"][ind])
                converted_results["rh"].append(raw_data["rh"][ind])
                converted_results["vpd"].append(raw_data["vpd"][ind])
                converted_results["vwc 8"].append(raw_data["vwc 8"][ind])
                converted_results["vwc 16"].append(raw_data["vwc 16"][ind])
                converted_results["vwc 24"].append(raw_data["vwc 24"][ind])
                if self.switch_connected(ports):
                    converted_results["daily switch"].append(raw_data["daily switch"][ind])

        if duplicates:
            print("-Duplicates found at:")
            print(duplicates)
        else:
            print("-No duplicates found")
        # print(duplicate_dates)        #To show duplicate dates
        print()
        return converted_results

    def remove_duplicate_data_2(self, raw_data: dict):
        duplicates = []
        duplicate_dates = []

        tally = defaultdict(list)
        for i, item in enumerate(raw_data):
            tally[item].append(i)
        returned = ((key, locs) for key, locs in tally.items()
                    if len(locs) > 1)

        for dup in returned:
            print(dup)
            duplicate_dates.append(dup[-1][-1])
        print(duplicate_dates)

    def remove_out_of_order_data(self, raw_data: dict, ports: dict) -> dict:
        print('Looking for out of order data-')
        mark_for_removal = []
        out_of_order = []
        latest = raw_data["dates"][0]
        for ind, i in enumerate(raw_data["dates"]):
            if latest > i:
                mark_for_removal.append(ind)
                out_of_order.append(i)
            else:
                latest = i

        if mark_for_removal:
            print("-Out of order data found at:")
            print(mark_for_removal)
            print(out_of_order)
            print("--Removing out of order data")

            # Reversing the list that is marked for removal so that when we start removing those indeces we don't
            # have issues with later indeces being changed from the deletion of earlier ones
            mark_for_removal.reverse()
            for ind in mark_for_removal:
                del raw_data["dates"][ind]
                del raw_data["canopy temperature"][ind]
                del raw_data["ambient temperature"][ind]
                del raw_data["rh"][ind]
                del raw_data["vpd"][ind]
                del raw_data["vwc 8"][ind]
                del raw_data["vwc 16"][ind]
                del raw_data["vwc 24"][ind]
                if self.switch_connected(ports):
                    del raw_data["daily switch"][ind]

            print("--Removed")
        else:
            print("-No out of order data found")
        print()

        return raw_data

    def sensor_name(self, sensor: int) -> str:
        """
        Function to return the sensor name given its numeric value.

        Used for printing what sensor we are working on.
        :param sensor: Number assigned to the type of sensor
        :return:
            String with the sensor name
        """
        return {
            64: 'Infra Red',
            67: 'Infra Red',
            68: 'Infra Red',
            102: 'VP4',
            123: 'Atmos 14',
            241: 'GS1',
            238: 'Terros 10',
            119: 'GS3',
            103: 'Terros 12',
            180: 'Flow Meter',
            183: 'Flow Meter',
            220: 'Switch',
            221: 'Switch',
            133: 'Battery',
            134: 'Logger',
            93: 'Atmos 41'
        }.get(sensor, 'Other')

    ##########################################################
    # Converts decagon date into standard date               #
    ##########################################################
    def convert_dates(self, raw_date):
        """
        Function to convert decagon date into standard date.

        :param raw_date:
        :return:
            datetime_val: datetime object with the converted date
        """

        converted_date = time.gmtime(raw_date + 946684800)
        datetime_val = datetime(*converted_date[:6])
        return datetime_val

    def convert_datetime_to_timestamp(self, raw_datetime):
        return datetime.timestamp(raw_datetime)

    def convert_last_download_time_to_datetime(self, raw_time):
        return datetime.strptime(raw_time, '%Y-%m-%dT%H:%M:%SZ')

    def convert_utc_timestamp_to_utc_datetime(self, raw_timestamp):
        utc_dt = datetime.utcfromtimestamp(raw_timestamp)
        return utc_dt

    def convert_utc_datetime_to_local_datetime(self, utc_raw_time):
        from_zone = tz.tzutc()
        to_zone = tz.tzlocal()

        utc = utc_raw_time.replace(tzinfo=from_zone)
        pacific = utc.astimezone(to_zone)
        return pacific

    def convert_timestamp_to_local_datetime(self, timestamp):
        time_start = self.convert_utc_timestamp_to_utc_datetime(timestamp)
        time_local_start = self.convert_utc_datetime_to_local_datetime(time_start)
        dt = time_local_start.replace(tzinfo=None)
        return dt

    def flow_meter_connected(self, raw_ports: dict) -> bool:
        """
        Function to determine whether a Flow Meter is connected based on its sensor number.

        :param raw_ports: Dictionary with the ports information for the logger. We want to check port 5 for
            a flow meter
        :return:
            Boolean indicating if a flow meter is connected
        """

        # 180 and 183 are the sensor numbers for flow meters
        if 6 in raw_ports.keys():
            if raw_ports[6] == 180 or raw_ports[6] == 183:
                return True
            else:
                return False
        else:
            return False

    def switch_connected(self, raw_ports: dict) -> bool:
        """
        Function to determine whether a Pressure Switch is connected based on its sensor number.

        :param raw_ports: Dictionary with the ports information for the logger. We want to check port 5 for
            a pressure switch
        :return:
            Boolean indicating if a pressure switch is connected
        """

        # 220 and 221 are the sensor numbers for pressure switches
        if 6 in raw_ports.keys():
            if raw_ports[6] == 'Switch':
                return True
            else:
                return False
        else:
            return False

    def vp4_connected(self, raw_ports: dict) -> bool:
        """
        Function to determine whether a VP4 is connected based on its sensor number.

        :param raw_ports: Dictionary with the ports information for the logger. We want to check port 2 for a VP4
        :return:
            Boolean indicating if a vp4 is connected
        """
        ## Bandaid for Casey
        if 2 not in raw_ports:
            if raw_ports[3] == 102 or 123:
                return True
        if raw_ports[2] == 102 or 123:
            return True
        else:
            return False

    def delete_last_day(self, data: dict) -> dict:
        """
        Function to delete the last row of data if it is from today.

        We only want to write information from the previous day. If some information is in the DXD from today because
            download is at midnight, that data needs to be deleted and not written into the GSheet.
        Since we only care about data at the hottest time of the day, we won't lose any important data by doing this
            deletion other than possibly losing gallons or switch minutes if irrigation was on at midnight.
        In this case, we will simply subtract the gallons or minutes from the previous values in the logger so the next
            time it check if the new value is higher it is artificially inflated with the gallons lost in the midnight data
            deletion
        :param data:
        :return:
        """
        print('\tDelete Last Day')
        todayRaw = date.today()
        lastElement = len(data["dates"]) - 1
        if lastElement >= 0:
            if data["dates"][lastElement].date() == todayRaw:
                print("\tDeleting extra same day data")
                # try:
                #     if self.switch_connected(self.ports):
                #         print('Switch totals: ')
                #         print(self.daily_switch)
                #         if len(self.daily_switch) >= 1:
                #             last_swi = self.daily_switch[-1]
                #             del self.daily_switch[-1]
                #         else:
                #             last_swi = 0
                #         print('Leftover switch total: ' + str(last_swi))
                #
                #         self.prev_day_switch = last_swi
                # except Exception as error:
                #     print('\tSome error in delete_last_day - water part')
                #     print(error)

                try:
                    for key in data.keys():
                        if data[key]:
                            del data[key][lastElement]
                except Exception as error:
                    print('\tSome error in delete_last_day - results part Z6')
                    print(error)
        return data

    def get_kc(self, data: dict) -> dict:
        """
        Function to get the kc for a crop

        :param data: Dictionary holding all the data that is still missing kc info
        :return: data with kc information added
        """
        self.crop_coefficient = CropCoefficient()
        data['kc'] = []
        for data_point in data["dates"]:
            kc = self.crop_coefficient.get_kc(
                self.crop_type.lower(), data_point.date(),
                planting_date=self.planting_date
            )
            if kc is None:
                data['kc'].append(0)
            else:
                data['kc'].append(kc)

        # print()
        # print('\tGot KC data:')
        # print('\t', data['kc'])
        # print()
        return data

    def update_eto(self, latest_et: float):
        print('   Updating eto for logger:' + str(self.id))
        self.dbwriter = DBWriter()
        yesterdayRaw = date.today() - timedelta(1)
        yesterday = '{0}-{1}-{2}'.format(yesterdayRaw.year, yesterdayRaw.month, yesterdayRaw.day)
        yesterday = "'" + yesterday + "'"
        # print(yesterday)
        # print(latest_et)
        logger_name = self.name
        field_name = self.field.name
        field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
        project = self.dbwriter.get_db_project(self.crop_type)
        dataset_id = project + '.' + field_name + '.' + logger_name
        dataset_id = "`" + dataset_id + "`"
        if latest_et == None:
            latest_et = 0
        dml_statement = "UPDATE " + str(dataset_id) + ' SET eto = ' + str(latest_et) + ' WHERE date = ' + yesterday \
            # '@yesterday'

        self.dbwriter.run_dml(dml_statement, project=project)

    def update_etc(self):
        print('   Updating etc for logger:' + str(self.id))
        self.dbwriter = DBWriter()
        yesterdayRaw = date.today() - timedelta(1)
        yesterday = '{0}-{1}-{2}'.format(yesterdayRaw.year, yesterdayRaw.month, yesterdayRaw.day)
        yesterday = "'" + yesterday + "'"
        logger_name = self.name
        field_name = self.field.name
        field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
        project = self.dbwriter.get_db_project(self.crop_type)
        dataset_id = project + '.' + field_name + '.' + logger_name
        dataset_id = "`" + dataset_id + "`"

        # TODO check if kc is null
        kc = self.get_kc_from_db()
        if kc != None:
            dml_statement = "UPDATE " + dataset_id + 'SET etc = eto * kc WHERE date = ' + yesterday
            self.dbwriter.run_dml(dml_statement, project=project)
        else:
            print("       No kc, can't calculate ETc")
        return kc

    def update_et_hours(self):
        self.dbwriter = DBWriter()
        yesterdayRaw = date.today() - timedelta(1)
        yesterday = '{0}-{1}-{2}'.format(yesterdayRaw.year, yesterdayRaw.month, yesterdayRaw.day)
        yesterday = "'" + yesterday + "'"
        logger_name = self.name
        field_name = self.field.name
        field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
        project = self.dbwriter.get_db_project(self.crop_type)
        dataset_id = project + '.' + field_name + '.' + logger_name
        dataset_id = "`" + dataset_id + "`"
        # etc_dml_statement = "SELECT etc FROM " + dataset_id + ' WHERE date = ' + yesterday
        # etc_dml_statement = "SELECT etc FROM " + dataset_id + ' WHERE etc is not NULL ORDER BY date DESC LIMIT 1'
        # print('Getting etc from DB')
        # etc_response = self.dbwriter.run_dml(etc_dml_statement)
        # for e in etc_response:
        #     etc = e["etc"]
        #     print(etc)
        if isinstance(self.irrigation_set_acres, str):
            acres = float(self.irrigation_set_acres.replace(',', ''))
        elif isinstance(self.irrigation_set_acres, int):
            acres = float(self.irrigation_set_acres)
        elif isinstance(self.irrigation_set_acres, float):
            acres = self.irrigation_set_acres
        else:
            acres = 0
        if isinstance(self.gpm, str):
            gpm = float(self.gpm.replace(',', ''))
        elif isinstance(self.gpm, int):
            gpm = float(self.gpm)
        elif isinstance(self.gpm, float):
            gpm = self.gpm
        else:
            gpm = 0

        if gpm != 0:
            et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))

            print("Updating et_hours data for table: " + dataset_id)

            dml_statement = "UPDATE " + dataset_id + " SET et_hours = ROUND(" \
                            + str(et_hours_pending_etc_mult) + " * etc) " \
                            + " WHERE date = " + yesterday

            self.dbwriter.run_dml(dml_statement, project=project)
        else:
            print('   GPM is 0')

    def get_kc_from_db(self) -> float:
        print('   Checking kc for logger:' + str(self.id))
        self.dbwriter = DBWriter()
        yesterdayRaw = date.today() - timedelta(1)
        yesterday = '{0}-{1}-{2}'.format(yesterdayRaw.year, yesterdayRaw.month, yesterdayRaw.day)
        yesterday = "'" + yesterday + "'"
        logger_name = self.name
        field_name = self.field.name
        field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
        project = self.dbwriter.get_db_project(self.crop_type)
        dataset_id = project + '.' + field_name + '.' + logger_name
        dataset_id = "`" + dataset_id + "`"

        dml_statement = "SELECT kc FROM " + dataset_id + ' WHERE date = ' + yesterday
        kc_response = self.dbwriter.run_dml(dml_statement, project=project)
        kc = 0
        for e in kc_response:
            # print(e["eto"])
            kc = e["kc"]
        print('   Got KC = ' + str(kc))
        print()
        return kc

    def merge_et_db_with_logger_db_values(self):
        self.dbwriter = DBWriter()
        logger_name = self.name
        field_name = self.field.name
        field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
        project = self.dbwriter.get_db_project(self.crop_type)
        dataset_id = project + '.' + field_name + '.' + logger_name
        dataset_id = "`" + dataset_id + "`"
        et_id = "stomato-info.ET." + str(self.field.cimis_station)
        et_id = "`" + et_id + "`"

        acres = self.irrigation_set_acres
        gpm = self.gpm

        if gpm != 0:
            et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))

            print(f"\t\tUpdating all et data for table: {dataset_id} with ET station: {self.field.cimis_station}")

            dml_statement = "MERGE " + dataset_id + " T " \
                            + "USING " + et_id + " S " \
                            + "ON (T.date = S.date AND T.eto IS NULL)  " \
                            + "WHEN MATCHED THEN " \
                            + "UPDATE SET eto = s.eto, etc = s.eto * t.kc, et_hours = ROUND(" + str(
                et_hours_pending_etc_mult) + " * s.eto * t.kc)"
        else:
            print('   GPM is 0')
            dml_statement = "MERGE " + dataset_id + " T " \
                            + "USING " + et_id + " S " \
                            + "ON (T.date = S.date AND T.eto IS NULL)  " \
                            + "WHEN MATCHED THEN " \
                            + "UPDATE SET eto = s.eto, etc = s.eto * t.kc "
        self.dbwriter.run_dml(dml_statement, project=project)

    def grab_portal_data(self, data: dict) -> dict:
        """
        Function to grab just the last day of data from the dictionary data
         and assign it to a new dictionary called portal_data_dict and return that

        :param data: dict (str, list)
        :return portal_data_dict: dict (str, value)
        """
        data_keys = list(data.keys())
        portal_data_dict = dict.fromkeys(data_keys)
        for key in data_keys:
            if len(data[key]) > 0:
                portal_data_dict[key] = data[key][-1]
        return portal_data_dict

    def check_for_notifications_final_results(self, final_results_converted: dict, warnings: bool = True,
                                              errors: bool = True, zentra_api_version: str = 'v1'):
        """
        Function to check the daily values after conversion and processing to see if any of the numbers are
        outside of the thresholds

        :param errors:
        :param warnings:
        :param field_name:
        :param final_results_converted: Dictionary holding the values for the previous day
            final_results_converted = {"dates": [], "canopy temperature": [], "ambient temperature": [], "relative_humidity": [],
            "vpd": [], "vwc 18": [], "vwc 36": [], "sdd": [], "cwsi": [], "daily gallons": [], "daily switch": []}
        :return:
        """
        technician = self.field.grower.technician
        field_name = self.field.name

        thresholds = Thresholds()

        battery_level = self.read_battery_level(zentra_api_version=zentra_api_version)

        if errors:
            if battery_level < thresholds.battery_threshold and battery_level is not None:
                technician.all_notifications.add_notification(
                    Notification_SensorError(
                        datetime.now(),
                        field_name,
                        self,
                        "Battery",
                        "Battery Level of: " + str(battery_level) + " is less than " + str(thresholds.battery_threshold)
                    )
                )

        if final_results_converted["dates"]:
            for ind, date in enumerate(final_results_converted["dates"]):
                cwsi = final_results_converted["cwsi"][ind]
                vwc_1 = final_results_converted["vwc_1"][ind]
                vwc_2 = final_results_converted["vwc_2"][ind]
                vwc_3 = final_results_converted["vwc_3"][ind]
                # daily_switch = final_results_converted["daily_switch"][ind]
                sdd = final_results_converted["sdd"][ind]
                air_temperature = final_results_converted["ambient temperature"][ind]
                # TODO Check lowest temperature values
                canopy_temperature = final_results_converted["canopy temperature"][ind]
                relative_humidity = final_results_converted["rh"][ind]
                vpd = final_results_converted["vpd"][ind]

                if self.field.field_type != 'R&D':

                    self.vp4_notifications(field_name, date, air_temperature, relative_humidity, vpd, technician,
                                           thresholds, warnings=warnings, errors=errors)

                    if self.ir_active:
                        self.canopy_temperature_notifications(field_name, date, canopy_temperature, technician,
                                                              thresholds, warnings=warnings, errors=errors)

                        self.psi_notifications(field_name, date, cwsi, technician, thresholds, warnings=warnings,
                                               errors=errors)
                    else:
                        self.psi_not_active_notification(field_name, date, technician, warnings=warnings, errors=errors)

                    self.vwc_notifications(field_name, self.soil, date, vwc_1, vwc_2, vwc_3, technician, thresholds,
                                           warnings=warnings, errors=errors)

                    # TODO: make switch notifications, ie if vwc changes but no switch activity
                    # self.switch_notifications(field_name, self.soil, date, vwc_1, vwc_2, vwc_3, technician, thresholds,
                    #                        warnings=warnings, errors=errors)

    def check_for_notifications_all_data(self, all_data: dict):
        """
        Function to check the daily values after conversion and before processing to see if any of the numbers are
        None for the whole day. If they are, create a notification for the sensor

        :param all_data: Dictionary holding the values for all data from the API call for all hours
        """
        technician = self.field.grower.technician

        canopy_temperature_issue = False
        vp4_issue = False
        vwc_1_issue = False
        vwc_2_issue = False
        vwc_3_issue = False

        # Check if logger is sending data
        required_data_points = 2
        if len(all_data['dates']) < required_data_points:
            if all_data['dates']:
                dates_date = all_data['dates'][-1]
            else:
                dates_date = datetime.now() - timedelta(days=1)
            technician.all_notifications.add_notification(
                Notification_SensorError(
                    dates_date,
                    self.field.name,
                    self,
                    "Z6 Logger",
                    "We did not get any data for this logger. Signal issue? Connection issue? Got hit by a tractor issue?"
                )
            )

        # Loop through all values and check for None values
        for ind, dp in enumerate(all_data['dates']):
            if all_data['canopy temperature'][ind] is None:
                canopy_temperature_issue = True
                canopy_temperature_date = dp
            if (all_data['ambient temperature'][ind] is None or
                    all_data['rh'][ind] is None or
                    all_data['vpd'][ind] is None):
                vp4_issue = True
                vp4_date = dp
            if all_data['vwc_1'][ind] is None:
                vwc_1_issue = True
                vwc_1_date = dp
            if all_data['vwc_2'][ind] is None:
                vwc_2_issue = True
                vwc_2_date = dp
            if all_data['vwc_3'][ind] is None:
                vwc_3_issue = True
                vwc_3_date = dp

        # Canopy Temp
        if canopy_temperature_issue:
            technician.all_notifications.add_notification(
                Notification_SensorError(
                    canopy_temperature_date,
                    self.field.name,
                    self,
                    "Canopy Temp",
                    "Canopy Temp is showing None at some point in the day (Not necessarily at the hottest time). Connection issue?"
                )
            )

        # VP4
        if vp4_issue:
            technician.all_notifications.add_notification(
                Notification_SensorError(
                    vp4_date,
                    self.field.name,
                    self,
                    "VP4",
                    "VP4 is showing None at some point in the day (Not necessarily at the hottest time). Connection issue?"
                )
            )

        # VWC_1
        if vwc_1_issue:
            technician.all_notifications.add_notification(
                Notification_SensorError(
                    vwc_1_date,
                    self.field.name,
                    self,
                    "VWC_1",
                    "VWC_1 is showing None at some point in the day (Not necessarily at the hottest time). Connection issue?"
                )
            )

        # VWC_2
        if vwc_2_issue:
            technician.all_notifications.add_notification(
                Notification_SensorError(
                    vwc_2_date,
                    self.field.name,
                    self,
                    "VWC_2",
                    "VWC_2 is showing None at some point in the day (Not necessarily at the hottest time). Connection issue?"
                )
            )

        # VWC_3
        if vwc_3_issue:
            technician.all_notifications.add_notification(
                Notification_SensorError(
                    vwc_3_date,
                    self.field.name,
                    self,
                    "VWC_3",
                    "VWC_3 is showing None at some point in the day (Not necessarily at the hottest time). Connection issue?"
                )
            )

    def vwc_notifications(
            self,
            field_name: str,
            soil,
            date: datetime,
            vwc_1: float,
            vwc_2: float,
            vwc_3: float,
            technician: Technician,
            thresholds: Thresholds,
            warnings: bool = True,
            errors: bool = True
    ):
        if vwc_1 is not None:
            if vwc_1 < thresholds.error_vwc_lower or vwc_1 > thresholds.error_vwc_upper:
                if errors:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VWC_1",
                            f"VWC_1 is out of reasonable bounds: {vwc_1}. Connection issue?"
                        )
                    )
            elif vwc_1 < soil.very_low_upper:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_1",
                            f"!!!! VWC_1 in VERY LOW levels: {str(round(vwc_1, 1))} < {str(soil.very_low_upper)} for soil type: {soil.soil_type}"
                        )
                    )
            elif vwc_1 < soil.low_upper:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_1",
                            f"!! VWC_1 in LOW levels: {str(round(vwc_1, 1))} < {str(soil.low_upper)} for soil type: {soil.soil_type}"
                        )
                    )
            elif vwc_1 > soil.very_high_lower:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_1",
                            f"!!!! VWC_1 in VERY HIGH levels: {str(round(vwc_1, 1))} > {str(soil.very_high_lower)} for soil type: {soil.soil_type}"
                        )
                    )

        if vwc_2 is not None:
            if vwc_2 < thresholds.error_vwc_lower or vwc_2 > thresholds.error_vwc_upper:
                if errors:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VWC_2",
                            f"VWC_2 is out of reasonable bounds: {vwc_2}. Connection issue?"
                        )
                    )
            elif vwc_2 < soil.very_low_upper:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_2",
                            f"!!!! VWC_2 in VERY LOW levels: {str(round(vwc_2, 1))} < {str(soil.very_low_upper)} for soil type: {soil.soil_type}"
                        )
                    )
            elif vwc_2 < soil.low_upper:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_2",
                            f"!! VWC_2 in LOW levels: {str(round(vwc_2, 1))} < {str(soil.low_upper)} for soil type: {soil.soil_type}"
                        )
                    )
            elif vwc_2 > soil.very_high_lower:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_2",
                            f"!!!! VWC_2 in VERY HIGH levels: {str(round(vwc_2, 1))} > {str(soil.very_high_lower)} for soil type: {soil.soil_type}"
                        )
                    )

        if vwc_3 is not None:
            if vwc_3 < thresholds.error_vwc_lower or vwc_3 > thresholds.error_vwc_upper:
                if errors:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VWC_3",
                            f"VWC_3 is out of reasonable bounds: {vwc_3}. Connection issue?"
                        )
                    )
            elif vwc_3 < soil.very_low_upper:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_3",
                            f"!!!! VWC_3 in VERY LOW levels: {str(round(vwc_3, 1))} < {str(soil.very_low_upper)} for soil type: {soil.soil_type}"
                        )
                    )
            elif vwc_3 < soil.low_upper:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_3",
                            f"!! VWC_3 in LOW levels: {str(round(vwc_3, 1))} < {str(soil.low_upper)} for soil type: {soil.soil_type}"
                        )
                    )
            elif vwc_3 > soil.very_high_lower:
                if warnings:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "VWC_3",
                            f"!!!! VWC_3 in VERY HIGH levels: {str(round(vwc_3, 1))} > {str(soil.very_high_lower)} for soil type: {soil.soil_type}"
                        )
                    )
    def switch_notifications(
            self,
            field_name: str,
            soil,
            date: datetime,
            vwc_1: float,
            vwc_2: float,
            vwc_3: float,
            technician: Technician,
            thresholds: Thresholds,
            warnings: bool = True,
            errors: bool = True
    ):
        # table_results = SQLScripts.get_entire_table_points(field_name)
        if vwc_1 is not None:
            if vwc_1 < thresholds.error_vwc_lower or vwc_1 > thresholds.error_vwc_upper:
                if errors:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VWC_1",
                            f"VWC_1 is out of reasonable bounds: {vwc_1}. Connection issue?"
                        )
                    )


    def vp4_notifications(
            self,
            field_name: str,
            date: datetime,
            air_temp: float,
            relative_humidity: float,
            vpd: float,
            technician: Technician,
            thresholds: Thresholds,
            warnings: bool = True,
            errors: bool = True
    ):
        # VP4 NOTIFICATIONS ----
        if errors:
            # COMMENTING THIS OUT FOR NOW SINCE WE CHECK FOR NONES IN ALL VALUES BEFORE THIS AND THIS WAS CAUSING
            # DUPLICATE NOTIFICATIONS
            # Check for None values
            # vp4_values = [air_temp, relative_humidity, vpd]
            # vp4_error = False
            # if any(value is None or value == 'None' for value in vp4_values):
            #     vp4_error = True
            #     technician.all_notifications.add_notification(
            #         Notification_SensorError(
            #             date,
            #             field_name,
            #             self,
            #             "VP4",
            #             "VP4 is showing None. Connection issue?"
            #         )
            #     )

            # If we didn't get a None on the VP4 values, check for threshold errors
            # if not vp4_error:
            if air_temp is not None:
                if air_temp < thresholds.error_temp_lower:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VP4",
                            f"Air Temp of: {str(air_temp)} is < {str(thresholds.error_temp_lower)}"
                        )
                    )
                elif air_temp > thresholds.error_temp_upper:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VP4",
                            f"Air Temp of: {str(air_temp)} is > {str(thresholds.error_temp_upper)}"
                        )
                    )

            if relative_humidity is not None:
                if relative_humidity < thresholds.error_rh_lower:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VP4",
                            f"Relative Humidity of: {str(relative_humidity)} is < {str(thresholds.error_rh_lower)}"
                        )
                    )
                elif relative_humidity > thresholds.error_rh_upper:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VP4",
                            f"Relative Humidity of: {str(relative_humidity)} is > {str(thresholds.error_rh_upper)}"
                        )
                    )

            if vpd is not None:
                if vpd < thresholds.error_vpd_lower:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VP4",
                            f"VPD of: {str(vpd)} is < {str(thresholds.error_vpd_lower)}"
                        )
                    )
                elif vpd > thresholds.error_vpd_upper:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "VP4",
                            f"VPD of: {str(vpd)} is > {str(thresholds.error_vpd_upper)}"
                        )
                    )

    def canopy_temperature_notifications(
            self,
            field_name: str,
            date: datetime,
            canopy_temp: float,
            technician: Technician,
            thresholds: Thresholds,
            warnings: bool = True,
            errors: bool = True
    ):
        if errors:
            if canopy_temp is None:
                # Not reporting this anymore since its caught by the Notification checks in for All Data, not just
                # hottest time of day
                # technician.all_notifications.add_notification(
                #     Notification_SensorError(
                #         date,
                #         field_name,
                #         self,
                #         "Canopy Temp",
                #         "Canopy Temp is showing None. Connection issue?"
                #     )
                # )
                pass
            elif canopy_temp < thresholds.error_temp_lower:
                technician.all_notifications.add_notification(
                    Notification_SensorError(
                        date,
                        field_name,
                        self,
                        "Canopy Temp",
                        f"Canopy Temp of: {str(canopy_temp)} is < {str(thresholds.error_temp_lower)}"
                    )
                )
            elif canopy_temp > thresholds.error_temp_upper:
                technician.all_notifications.add_notification(
                    Notification_SensorError(
                        date,
                        field_name,
                        self,
                        "Canopy Temp",
                        f"Canopy Temp of: {str(canopy_temp)} is > {str(thresholds.error_temp_upper)}"
                    )
                )

    def psi_notifications(
            self,
            field_name: str,
            date: datetime,
            psi: float,
            technician: Technician,
            thresholds: Thresholds,
            warnings: bool = True,
            errors: bool = True
    ):
        if psi is None:
            # Not reporting this anymore since its caught by the Notification checks in for All Data, not just
            # hottest time of day
            # if errors:
            #     technician.all_notifications.add_notification(
            #         Notification_SensorError(
            #             date,
            #             field_name,
            #             self,
            #             "PSI",
            #             "PSI is showing None. Connection issue?"
            #         )
            #     )
            pass
        elif self.field.crop_type in ['Tomatoes', 'Tomato', 'tomatoes', 'tomato']:
            # Tomato PSI Notifications
            if warnings:
                if psi > thresholds.tomato_psi_danger:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "PSI",
                            f"!!!! PSI in CRITICAL HIGH levels: {str(round(psi, 1))} > {str(thresholds.tomato_psi_danger)}"
                        )
                    )
        else:
            # Permanent Crop PSI Notifications
            if warnings:
                if psi > thresholds.permanent_psi_danger:
                    technician.all_notifications.add_notification(
                        Notification_TechnicianWarning(
                            date,
                            field_name,
                            self,
                            "PSI",
                            f"!!!! PSI in CRITICAL HIGH levels: {str(round(psi, 1))} > {str(thresholds.permanent_psi_danger)}"
                        )
                    )

    def psi_not_active_notification(
            self,
            field_name: str,
            date: datetime,
            technician: Technician,
            warnings: bool = True,
            errors: bool = True
    ):
        if errors:
            formatted_consecutive_psi = [round(tup[0], 2) for tup in self.consecutive_ir_values]
            formatted_consecutive_sdd = [round(tup[1], 2) for tup in self.consecutive_ir_values]
            if self.field.crop_type in ['Tomatoes', 'Tomato', 'tomatoes', 'tomato']:
                # Tomato PSI Notifications
                planting_date_plus_70 = self.planting_date + timedelta(days=70)
                if date.date() >= planting_date_plus_70:
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "PSI",
                            f"It's been more than 70 days from planting date and IR is not active. Date: {date.date()} >= Planting + 70: {planting_date_plus_70}. Should it be on? The last 3 psi values were: {formatted_consecutive_psi}. Last 3 sdd values were: {formatted_consecutive_sdd}"
                        )
                    )

            elif self.field.crop_type in ['Pistachio', 'pistachio', 'Pistachios', 'pistachios']:
                # Pistachio PSI Notifications
                # ir_start_date = datetime(date.year, 5, 1).date()
                ir_start_date = Thresholds.pistachio_start_date
                ir_start_date_plus_15 = ir_start_date + timedelta(days=15)
                # changing the window to be much smaller so it doesnt send
                # out errors after techs have disabled the IR manually in later season
                if (4 < date.month < 11) and (date.date() >= ir_start_date_plus_15):
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "PSI",
                            f"It's been more than 15 days from activation month and IR is not active. Date: {date.date()} >= March 1 + 15: {ir_start_date_plus_15}. Should it be on? The last 3 psi values were: {formatted_consecutive_psi}. Last 3 sdd values were: {formatted_consecutive_sdd}"
                        )
                    )

            elif self.field.crop_type in ['Almond', 'almond', 'Almonds', 'almonds']:
                # Almond PSI Notifications
                # ir_start_date = datetime(date.year, 5, 1).date()
                ir_start_date = Thresholds.almond_start_date
                ir_start_date_plus_15 = ir_start_date + timedelta(days=15)
                if (2 < date.month < 10) and (14 < date.day) and (date.date() >= ir_start_date_plus_15):
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            field_name,
                            self,
                            "PSI",
                            f"It's been more than 15 days from activation month and IR is not active. Date: {date.date()} >= May 1 + 15: {ir_start_date_plus_15}. Should it be on? The last 3 psi values were: {formatted_consecutive_psi}. Last 3 sdd values were: {formatted_consecutive_sdd}"
                        )
                    )
            else:
                # Other crops
                pass

    def update(
            self,
            cimis_stations_pickle,
            write_to_db: bool = False,
            check_for_notifications: bool = False,
            specific_mrid: int = None,
            subtract_from_mrid: int = 0,
            check_updated: bool = False,
            zentra_api_version: str = 'v1',
            specific_start_date: str = None,
            specific_end_date: str = None,
    ) -> dict:
        """
        Function to update the information from each Logger

        :param specific_start_date: String date in the format: m-d-Y H:M
        :param specific_end_date: String date in the format: m-d-Y H:M
        :param zentra_api_version:
        :param end_date:
        :param start_date:
        :param api_version:
        :param write_to_db:
        :param check_for_notifications:
        :param specific_mrid:
        :param subtract_from_mrid:
        :param check_updated:
        :param subtract_from_mrid: Int used to subtract a specific amount from the logger MRIDs for API calls
        :return:
        """
        if self.active:
            try:
                portal_data = None
                if self.updated:
                    print('\tLogger: ' + self.id + '  already updated. Skipping...')
                    return portal_data
                else:
                    print(
                        '========================================================================================================================'
                    )
                    print('LOGGER updating: ')
                    self.to_string()
                    print(
                        '------------------------------------------------------------------------------------------------------------------------'
                    )

                    # Download dxd files from Decagon API
                    print()
                    print('Downloading Data into DXD files-')
                    # response_success, converted_raw_data
                    results = self.get_logger_data(
                        specific_mrid=specific_mrid,
                        subtract_from_mrid=subtract_from_mrid,
                        zentra_api_version=zentra_api_version,
                        specific_start_date=specific_start_date,
                        specific_end_date=specific_end_date
                    )

                    if len(results) == 2:
                        response_success, converted_raw_data = results
                    elif len(results) == 6:
                        response_success, all_raw_data_returned, all_results_structured, all_results_filtered, all_results_dynamically_filtered, converted_raw_data = results
                    else:
                        response_success = False
                        print('ERROR Unexpected number of values returned from Logger.get_logger_data()')

                    # print('-METER API Quota Delay')
                    # time.sleep(20)
                    print('-Finished')
                    print()

                    # # Read data in dxd files
                    if response_success:
                        if check_for_notifications:
                            # Check for notifications
                            try:
                                print('Checking for notifications on all data-')
                                self.check_for_notifications_all_data(converted_raw_data)
                                print('\t-Finished')
                            except Exception as e:
                                print("Error in Logger check_for_notifications_all_data - " + self.name)
                                print("Error type: " + str(e))

                        # Process data
                        print('Processing data-')
                        print()
                        print('\tAll Results Converted -> Before Processing: ')
                        for key, values in converted_raw_data.items():
                            print('\t', key, " : ", values)
                            if len(values) > 5:
                                print()

                        # Update irrigation ledger
                        print('\tCleaning irrigation ledger')
                        self.cwsi_processor.clean_irrigation_ledger(self.irrigation_ledger)

                        print('\tUpdating irrigation ledger')
                        self.cwsi_processor.update_irrigation_ledger(converted_raw_data, self.irrigation_ledger)
                        ################### TEMP FIX FOR 15 MINUTE INTERVAL ##########################
                        # self.irrigation_ledger = {}
                        # self.cwsi_processor.update_irrigation_ledger_2(converted_raw_data, self.irrigation_ledger)
                        # self.cwsi_processor.update_irrigation_ledger_3(converted_raw_data, self.irrigation_ledger)
                        ##############################################################################
                        print('\t Ledger after update:')
                        self.show_irrigation_ledger()
                        print('\t ...done')
                        print()

                        print('\tGetting hottest and coldest temperatures')
                        highest_temp_values_ind, lowest_temp_values_ind, _ = \
                            self.cwsi_processor.get_highest_and_lowest_temperature_indexes(converted_raw_data)

                        print('\tFinal Results processing')
                        final_results_converted = self.cwsi_processor.final_results(
                            converted_raw_data, highest_temp_values_ind, lowest_temp_values_ind, self
                        )

                        print('\tLedger after final results:')
                        self.show_irrigation_ledger()

                        print()
                        print("\tFinal Results Converted -> After Processing:")
                        for key, values in final_results_converted.items():
                            print('\t', key, " : ", values)
                            if len(values) > 5:
                                print()

                        # Check irrigation_ledger for delayed completed ledger date switch lists and update db
                        self.check_and_update_delayed_ledger_filled_lists()
                        print()
                        print('\tLedger after delayed update:')
                        self.show_irrigation_ledger()

                        # Getting kc
                        final_results_converted = self.get_kc(final_results_converted)

                        # Get ET data
                        # final_results_converted = self.get_et(final_results_converted, cimis_stations_pickle)

                        # Delete last row of data if it is from today
                        final_results_converted = self.delete_last_day(final_results_converted)

                        if self.crop_type.lower() == 'tomatoes' or self.crop_type.lower() == 'tomato':
                            final_results_converted = self.calculate_total_gdd_and_crop_stage(final_results_converted)
                        print('-Finished')
                        print()

                        # PLUG IN AI ENGINE THROUGH CWSI PROCESSOR
                        # final_results_converted = self.cwsi_processor.irrigation_ai_processing(final_results_converted, self)

                        print()
                        print('\tFinal Results before DB write-')
                        for key, values in final_results_converted.items():
                            print('\t', key, " : ", values)
                            if len(values) > 5:
                                print()

                        # Grab only last day data for portal
                        print()
                        print('\tGrab Portal Data:')
                        portal_data = self.grab_portal_data(final_results_converted)
                        print('\t-Finished')

                        if check_for_notifications:
                            # Check for notifications
                            try:
                                print()
                                print('\tChecking for Notifications on final results')
                                self.check_for_notifications_final_results(final_results_converted, warnings=False,
                                                                           zentra_api_version=zentra_api_version)
                                print('\t-Finished')
                            except Exception as e:
                                print("Error in Logger check_for_notifications_final_results - " + self.name)
                                print("Error type: " + str(e))

                        # Write data to DB
                        if write_to_db:
                            # self.dbwriter = DBWriter()
                            # self.dbwriter.create_dataset(self.grower.name + '_' + self.field.name)

                            # the database writes for logger
                            try:
                                if final_results_converted["dates"]:
                                    print()
                                    print('\tWriting to DB-')
                                    # Get project, dataset and table names
                                    project = self.dbwriter.get_db_project(self.crop_type)
                                    dataset_id = self.field.name
                                    table_id = self.name

                                    schema = [
                                        bigquery.SchemaField("logger_id", "STRING"),
                                        bigquery.SchemaField("date", "DATE"),
                                        bigquery.SchemaField("time", "STRING"),
                                        bigquery.SchemaField("canopy_temperature", "FLOAT"),
                                        bigquery.SchemaField("canopy_temperature_celsius", "FLOAT"),
                                        bigquery.SchemaField("ambient_temperature", "FLOAT"),
                                        bigquery.SchemaField("ambient_temperature_celsius", "FLOAT"),
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
                                        bigquery.SchemaField("sdd_celsius", "FLOAT"),
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
                                        bigquery.SchemaField("lowest_ambient_temperature_celsius", "FLOAT"),
                                        bigquery.SchemaField("gdd", "FLOAT"),
                                        bigquery.SchemaField("crop_stage", "STRING"),
                                        bigquery.SchemaField("id", "STRING"),
                                        bigquery.SchemaField("planting_date", "DATE"),
                                        bigquery.SchemaField("variety", "STRING"),
                                    ]

                                    # Check if the data we have is new or already exists in the DB
                                    db_dates = self.dbwriter.grab_specific_column_table_data(
                                        dataset_id,
                                        table_id,
                                        project,
                                        'date'
                                    )
                                    if db_dates is not None:
                                        db_dates_list = [row[0] for row in db_dates]
                                    else:
                                        db_dates_list = []

                                    # Prepping data to be written
                                    self.cwsi_processor.prep_data_for_writting_db(final_results_converted, self,
                                                                                  db_dates_list)

                                    self.dbwriter.write_to_table_from_csv(
                                        dataset_id, table_id, 'data.csv', schema, project
                                    )

                                    ####################################
                                    # Commenting this out as updating logger tables with ET data is going to be part of
                                    # the ET Update pipeline
                                    # print('\tMerging ET table with Logger table')
                                    # # Merge ET table with Logger table
                                    # self.merge_et_db_with_logger_db_values()
                                    ####################################
                                    print('\t-Finished')

                                else:
                                    print('\tNothing new to write to DB')
                            except Exception as e:
                                print("\tError in logger db write - " + self.id)
                                print("\tError type: " + str(e))
                        self.updated = True
                    else:
                        print('\tResponse not successful')
                        self.updated = False

                    print()
                    print(
                        '-------------------------------------Logger ' + self.id + ' Done------------------------------------------------------------'
                    )
                    print()
                    self.to_string()
                    print(
                        '========================================================================================================================'
                    )
                    print()
                    print()
                    print()
                    print()

                    return portal_data

                # ------------------------------------HERE uncomment for trycatch
            except Exception as e:
                print("Error in logger update - " + self.id)
                print("Error type: " + str(e))
                self.crashed = True
                # -----------------------------------HERE uncomment for trycatch
        else:
            print('Logger - {} not active'.format(self.id))

    def show_irrigation_ledger(self):
        print('\tIrrigation Ledger: ')
        for date_key, switch_list in self.irrigation_ledger.items():
            print(f'\t\t{date_key} -> {switch_list}')

    def check_and_update_delayed_ledger_filled_lists(self):
        dates_to_remove = []
        technician = self.field.grower.technician
        for date, switch_list in self.irrigation_ledger.items():
            if None not in switch_list:
                switch_sum = sum(switch_list)
                dates_to_remove.append(date)
                if switch_sum > 0:
                    print(f'\tDelayed switch data for date: {date}...updating DB')
                    print(f'\tSwitch sum: {switch_sum}')
                    self.dbwriter.update_overflow_switch_irr_hours_for_date_by_replacing(
                        self,
                        switch_sum,
                        date
                    )

                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date,
                            self.field.name,
                            self,
                            "Delayed Irrigation Hours Updated",
                            f"Updated irrigation hours for date {date}. There may have been a lack of data reported for"
                            + f" this date when originally processed so hours have been updated now that the full 24"
                            + f" hours of irrigation are accounted for."
                        )
                    )

        for date in dates_to_remove:
            del self.irrigation_ledger[date]

    def get_ir_date(self):
        current_year = datetime.now().year
        crop = self.crop_type
        if crop == 'Almonds':
            return date(current_year, 3, 1)
        elif crop == 'Pistachios':
            return date(current_year, 5, 1)
        elif crop == 'Tomatoes':
            planting_date = self.planting_date
            return planting_date + timedelta(days=30)

    def should_ir_be_active(self, date_to_check: datetime.date = datetime.today().date()) -> bool:
        print(f'\t\tChecking if IR should be active for {date_to_check}:')

        # Checking to make sure ir_active is False before the other checks so that we don't turn off IR on permanent
        # crops if it's already on. For tomatoes, ir_active will always be false if we come into this function.
        if self.ir_active is False:
            # Do we have at least 3 tuples of values?
            if len(self.consecutive_ir_values) < 3:
                print(f'\t\t\t Not enough consecutive values to activate yet: {len(self.consecutive_ir_values)}')
                return False

            # Are the dates in those tuples consecutive dates?
            sorted_dq_dates = sorted([item[2] for item in self.consecutive_ir_values])
            dates_are_consecutive = self.are_dates_consecutive(sorted_dq_dates)
            if not dates_are_consecutive:
                print(f'\t\t\t Dates are NOT consecutive: {sorted_dq_dates}')
                return False

        if self.crop_type.lower() in ['tomato', 'tomatoes']:
            psi_threshold_high = 1.6
            sdd_threshold = -5.0
            planting_date_plus_30 = self.planting_date + timedelta(days=30)

            # If its On keep it on - there's no off switch for tomatoes
            if self.ir_active:
                return True

            # If its Off should it be On
            if self.ir_active is False:
                if date_to_check > planting_date_plus_30:
                    for psi_val, sdd_val, _ in self.consecutive_ir_values:
                        if psi_val > psi_threshold_high or sdd_val > sdd_threshold:
                            print(f'\t\t\t SI values did not pass:')
                            print(f'\t\t\t PSI -> {psi_val} > {psi_threshold_high}?')
                            print(f'\t\t\t SDD -> {sdd_val} > {sdd_threshold}?')
                            return False
                    else:
                        # IR Should be turned on
                        technician = self.field.grower.technician
                        # Tomato PSI Turning on Notification
                        technician.all_notifications.add_notification(
                            Notification_SensorError(
                                date_to_check,
                                self.field.name,
                                self,
                                "PSI",
                                f"PSI just turned on for {self.name}"
                            )
                        )
                        print('\t IR Turning On')
                        return True

        elif self.crop_type.lower() in ['almond', 'almonds']:
            psi_threshold_high = 0.5

            # SDD checks were not working well since trees turn on earlier in the year so cold temps can lead to
            # deceivingly low SDDs. Off for now
            # sdd_threshold = -3.0

            # almond window: March 15 – September 30
            # start_date = date(date.today().year, 3, 14)  # March 14
            # end_date = date(date.today().year, 10, 1)  # October 1
            start_date = Thresholds.almond_start_date
            end_date = Thresholds.almond_end_date

            # IF its on check if it should turn off
            if self.ir_active:
                if date_to_check > end_date:
                    return False
                else:
                    return True

            # If its Off should it turn on
            elif self.ir_active is False:
                if start_date < date_to_check < end_date:
                    for psi_val, _, _ in self.consecutive_ir_values:
                        if psi_val > psi_threshold_high:  # or sdd_val > sdd_threshold:
                            return False
                    # if IR is off and did not fail checks turn IR ON
                    # IR Should be turned on
                    technician = self.field.grower.technician
                    # PSI Turning on Notification
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date_to_check,
                            self.field.name,
                            self,
                            "PSI",
                            f"PSI just turned on for {self.name}"
                        )
                    )
                    print('\t IR Turning On')
                    return True
                else:
                    return False

        elif self.crop_type.lower() in ['pistachio', 'pistachios']:
            psi_threshold_high = 0.5

            # SDD checks were not working well since trees turn on earlier in the year so cold temps can lead to
            # deceivingly low SDDs. Off for now
            # sdd_threshold = -3.0

            # pistachio window: April 15 – Nov 1
            start_date = Thresholds.pistachio_start_date
            end_date = Thresholds.pistachio_end_date

            # IF its on check if it should turn off
            if self.ir_active:
                if date_to_check > end_date:
                    return False
                else:
                    return True

            # If its Off should it turn on
            elif self.ir_active is False:
                if start_date < date_to_check < end_date:
                    for psi_val, _, _ in self.consecutive_ir_values:
                        if psi_val > psi_threshold_high:  # or sdd_val > sdd_threshold:
                            return False
                    # if IR is off and did not fail checks turn IR ON
                    # IR Should be turned on
                    technician = self.field.grower.technician
                    # PSI Turning on Notification
                    technician.all_notifications.add_notification(
                        Notification_SensorError(
                            date_to_check,
                            self.field.name,
                            self,
                            "PSI",
                            f"PSI just turned on for {self.name}"
                        )
                    )
                    print('\t IR Turning On')
                    return True
                else:
                    return False

            # # If date_to_check's date is between May and November (Active pistachio tree cycle)
            # if start_date < date_to_check < end_date:
            #     # if within months check if IR is On. If on, leave it on
            #     if self.ir_active:
            #         return True
            #     # if IR has not been turned on see if it passes checks
            #     else:
            #         # return False  # waiting for technician confirmation to turn on
            #         for psi_val, _, _ in self.consecutive_ir_values:
            #             if psi_val > psi_threshold_high:  # or sdd_val > sdd_threshold:
            #                 return False
            #         # if IR is off and did not fail checks turn IR ON
            #         print('\t IR Turning On')
            #         return True
            # # if outside the months turn IR off always
            # else:
            #     return False

        elif self.crop_type.lower() == 'dates' or self.crop_type.lower() == 'date':
            # La Quinta requested to always see canopy temp and SDD
            # print('\t\tIR Active')
            return True
        return False

    def are_dates_consecutive(self, dates_to_check):
        """
        Function to check if dates in a list are consecutive

        :param dates_to_check:
        :return:
        """
        # Check if the differences between consecutive dates are exactly one day
        for i in range(1, len(dates_to_check)):
            if dates_to_check[i] - dates_to_check[i - 1] != timedelta(days=1):
                return False
        return True

    def deactivate(self):
        print('\t\t\tDeactivating Logger {}...'.format(self.id))
        self.active = False
        if self.uninstall_date is None:
            self.uninstall_date = datetime.now().date()
        print('\t\t\tDone')

    def calculate_total_gdd_and_crop_stage(self, final_results_converted: dict) -> dict:
        print('\tCalculating GDDS and Crop Stage:')
        accumulated_gdd = previous_gdd = self.gdd_total

        crop_stage = 'NA'
        for gdd in final_results_converted['gdd']:
            accumulated_gdd += gdd
            crop_stage = self.cwsi_processor.get_crop_stage(accumulated_gdd)
            final_results_converted['crop stage'].append(crop_stage)
        self.gdd_total = accumulated_gdd
        self.crop_stage = crop_stage
        print(
            f'\t GDD before: {str(previous_gdd)} -> GDD after: {str(self.gdd_total)} -> Crop Stage: {self.crop_stage}'
        )
        print()
        return final_results_converted

    def recalculate_total_gdd(self):
        pass

    def get_sensor_individual_data_indexes(self):

        get_sensor_individual_data_indexes = {}
        # Dictionary to hold what index the data is being stored in from the API return for Zentra v1
        get_sensor_individual_data_indexes['ir temp'] = 0
        get_sensor_individual_data_indexes['ir body temp'] = 1
        get_sensor_individual_data_indexes['vp4 air temp'] = 0
        get_sensor_individual_data_indexes['vp4 rh'] = 1
        get_sensor_individual_data_indexes['vp4 pressure'] = 2
        get_sensor_individual_data_indexes['vp4 vpd'] = 3
        get_sensor_individual_data_indexes['vwc volumetric'] = 0
        get_sensor_individual_data_indexes['vwc soil temp'] = 1
        get_sensor_individual_data_indexes['vwc ec'] = 2
        get_sensor_individual_data_indexes['switch minutes'] = 0

        return get_sensor_individual_data_indexes

    def get_sensor_individual_data_indexes_weather_stations(self):
        """

        :return:
        """

        get_sensor_individual_data_indexes = {}
        # Dictionary to hold what index the data is being stored in from the API return for Zentra v1
        get_sensor_individual_data_indexes['solar radiation'] = 0
        get_sensor_individual_data_indexes['precipitation'] = 1
        get_sensor_individual_data_indexes['lightning activity'] = 2
        get_sensor_individual_data_indexes['lightning distance'] = 3
        get_sensor_individual_data_indexes['wind direction'] = 4
        get_sensor_individual_data_indexes['wind speed'] = 5
        get_sensor_individual_data_indexes['gust speed'] = 6
        get_sensor_individual_data_indexes['air temperature'] = 7
        get_sensor_individual_data_indexes['relative humidity'] = 8
        get_sensor_individual_data_indexes['atmospheric pressure'] = 9
        get_sensor_individual_data_indexes['x axis level'] = 10
        get_sensor_individual_data_indexes['y axis level'] = 11
        get_sensor_individual_data_indexes['max precip rate'] = 12
        get_sensor_individual_data_indexes['rh sensor temp'] = 13
        get_sensor_individual_data_indexes['vpd'] = 14

        return get_sensor_individual_data_indexes

    def set_broken(self):
        self.broken = True
        self.active = False
        self.uninstall_date = datetime.now().date()

    def update_ir_consecutive_data(self, cwsi: float, sdd: float, date):
        """
        Function to handle adding consecutive IR data to a loggers .consecutive_ir_values deque to be able to analyze
        whether an IR should turn on or not

        :param date: Datetime of the date we want to append to the que
        :param sdd: Float of the SDD we want to append to the que
        :param cwsi: Float of the CWSI we want to append to the que
        """
        date = date.date()
        if cwsi is not None and sdd is not None and date is not None:
            date_already_in_dq = self.is_date_in_deque(date, self.consecutive_ir_values)
            if date_already_in_dq:
                print(f'\t\tSkipping adding consec IR data for {date} because its already in the dq')
            else:
                # print(f'\t\tAdding consecutive IR data to dq')
                self.consecutive_ir_values.append((cwsi, sdd, date))
        if len(self.consecutive_ir_values) > 3:
            self.consecutive_ir_values.popleft()

    def is_date_in_deque(self, date, dq):
        """
        Function to check if a date is in a dq

        :param date: datetime.date()
        :param dq: [(cwsi, sdd, date), (cwsi, sdd, date), (cwsi, sdd, date)]
        :return:
        """
        for _, _, consec_date in dq:
            if consec_date == date:
                return True
        return False

    def get_et(self, final_results_converted, cimis_stations_pickle):
        # Check - If we only need yesterday's value, we can grab it from the cimis station pickle. If we need more than
        # yesterday's value, we need grab it from the database.
        final_results_converted['eto'] = []
        final_results_converted['etc'] = []
        final_results_converted['et_hours'] = []

        if len(final_results_converted['dates']) == 1:
            # Grab et data from latest cimis station pickle
            final_results_converted = self.update_et_values_from_cimis_pickle(cimis_stations_pickle,
                                                                              final_results_converted)
        elif len(final_results_converted['dates']) > 1:
            # Merge et data from et db
            final_results_converted = self.update_et_values_from_et_db(final_results_converted)
        return final_results_converted

    def update_et_values_from_cimis_pickle(self, cimis_stations_pickle, final_results_converted):
        # Grab data from latest cimis station pickle
        print("\tUpdating ET data from pickle")
        try:
            cimis_station = self.field.cimis_station
            latest_eto = 0
            for station in cimis_stations_pickle:
                if station.station_number == cimis_station:
                    latest_eto = station.latest_eto_value
                    break
            # calculate etc from eto * kc
            acres = self.irrigation_set_acres
            gpm = self.gpm

            kc = final_results_converted['kc'][0]
            latest_eto = float(latest_eto)
            etc = latest_eto * kc  # etc = eto * kc
            et_hours = None
            # calculate et_hours
            if gpm != 0:
                # et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))
                # et_hours = round(et_hours_pending_etc_mult * latest_eto * kc)
                et_hours = round(etc * ((449 * acres) / (gpm * 0.85)))
            final_results_converted['eto'].append(latest_eto)
            final_results_converted['etc'].append(etc)
            final_results_converted['et_hours'].append(et_hours)
            print(
                f"\tGot ET data from pickle: ETo: {latest_eto}, kc: {kc}, ETc: {etc}, ET Hours: {et_hours}, Acres: {acres}, GPM: {gpm}")
        except Exception as error:
            print("Error in logger ET data grab from pickle - " + self.name)
            print("Error type: " + str(error))
            print(
                f"ET data from pickle: ETo: {latest_eto}, kc: {kc}, ETc: {etc}, ET Hours: {et_hours}, Acres: {acres}, GPM: {gpm}")
        return final_results_converted

    def update_et_values_from_et_db(self, final_results_converted):
        print("\tUpdating ET data from DB")
        try:
            field_name = self.field.name
            field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(field_name)
            project = 'stomato-info'
            dataset_id = f"{project}.ET.{str(self.field.cimis_station)}"
            dataset_id = "`" + dataset_id + "`"

            acres = self.irrigation_set_acres
            gpm = self.gpm

            start_date = final_results_converted['dates'][0].strftime('%Y-%m-%d')
            end_date = final_results_converted['dates'][-1].strftime('%Y-%m-%d')

            dml_statement = f"SELECT * FROM {dataset_id} WHERE date BETWEEN DATE(\'{start_date}\') AND DATE(\'{end_date}\') ORDER BY date ASC"
            result = self.dbwriter.run_dml(dml_statement, project=project)

            et_db_dates = []
            et_db_etos = []
            for row in result:
                et_db_dates.append(row['date'])
                et_db_etos.append(row['eto'])

            for ind, data_date in enumerate(final_results_converted['dates']):
                eto = None
                etc = None
                et_hours = None

                if data_date.date() in et_db_dates:
                    eto = et_db_etos[et_db_dates.index(data_date.date())]
                    kc = final_results_converted['kc'][ind]
                    etc = eto * kc  # etc = eto * kc
                    if gpm != 0:
                        et_hours_pending_etc_mult = ((449 * acres) / (gpm * 0.85))
                        et_hours = round(et_hours_pending_etc_mult * eto * kc)
                    else:
                        et_hours = None

                final_results_converted['eto'].append(eto)
                final_results_converted['etc'].append(etc)
                final_results_converted['et_hours'].append(et_hours)
            print(f"\tGot ET data from DB:")
            print(f"\t\tETO: {final_results_converted['eto']}")
            print(f"\t\tETC: {final_results_converted['etc']}")
            print(f"\t\tET Hours: {final_results_converted['et_hours']}")
        except Exception as error:
            print("Error in logger ET data grab from DB - " + self.name)
            print("Error type: " + str(error))
        return final_results_converted


def convert_datetime_to_timestamp(raw_datetime):
    return datetime.timestamp(raw_datetime)


def convert_utc_timestamp_to_utc_datetime(raw_timestamp):
    utc_dt = datetime.utcfromtimestamp(raw_timestamp)
    return utc_dt


def convert_utc_datetime_to_local_datetime(utc_raw_time):
    from_zone = tz.tzutc()
    to_zone = tz.tzlocal()

    utc = utc_raw_time.replace(tzinfo=from_zone)
    pacific = utc.astimezone(to_zone)
    return pacific

# timestamp = 1659988800
# timestamp = 1659985200 #8-8-22
# timestamp = 1655154000 #6-13-22
# meter_time = convert_utc_timestamp_to_utc_datetime(timestamp)
# time_local = convert_utc_datetime_to_local_datetime(meter_time)
# time_local = time_local.replace(tzinfo=None)
# print(time_local.date())
# print(type(time_local.date()))
