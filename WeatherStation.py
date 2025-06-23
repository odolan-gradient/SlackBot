import pickle
from datetime import datetime, timedelta
from os import path

from Logger import Logger
from Zentra import Zentra


class WeatherStation(Logger):
    def __init__(
            self,
            id: str,
            password: str,
            name: str,
            crop_type: str,
            install_date: datetime.date = None,
            lat: str = '',
            long: str = '',
            planting_date: datetime.date = None,
            start_date: datetime.date = None,
            end_date: datetime.date = None,
            station_type: str = 'Weather Station',
            cimis_station: str = None,
    ):
        super().__init__(id, password, name, crop_type, None, None, None, None, install_date, lat=lat, long=long, planting_date=planting_date)
        if start_date is not None:
            self.start_date = start_date
        elif planting_date is not None:
            self.start_date = planting_date
        elif install_date is not None:
            current_year = datetime.now().year
            if install_date.year < current_year:
                self.start_date = datetime(current_year, 1, 1).date()
            self.start_date = install_date
        else:
            current_year = datetime.now().year
            first_day_of_year = datetime(current_year, 1, 1).date()
            self.start_date = first_day_of_year

        if end_date is not None:
            self.end_date = end_date
        elif self.uninstall_date is not None:
            self.end_date = self.uninstall_date
        else:
            current_year = datetime.now().year
            self.end_date = datetime(current_year, 12, 31).date()

        self.station_type = station_type
        self.cimis_station = cimis_station


    def __repr__(self):
        return f'Weather Station: {self.name}, Active: {self.active}, Crop Type: {self.crop_type}, Station Type: {self.station_type}'
    @staticmethod
    def open_weather_station_pickle():
        if path.exists("H:\\Shared drives\\Stomato\\2024\\Pickle\\2024_weather_station.pickle"):
            with open("H:\\Shared drives\\Stomato\\2024\\Pickle\\2024_weather_station.pickle", 'rb') as f:
                content = pickle.load(f)
            return content
    @staticmethod
    def write_weather_station_pickle(data):
        if path.exists("H:\\Shared drives\\Stomato\\2024\\Pickle\\"):
            with open("H:\\Shared drives\\Stomato\\2024\\Pickle\\2024_weather_station.pickle", 'wb') as f:
                pickle.dump(data, f)

    def get_eto_values(self):
        # Example format of eto_values from Zentra:
        # {'datetime': '2025-01-04 23:59:59-08:00', 'error_description': None, 'error_flag': False,
        #  'timestamp_utc': 1736063999, 'tz_offset': -28800, 'value': 0.0478}
        eto_values = {}
        zentra = Zentra()
        response = zentra.get_env_model_data(self.id, latitude=float(self.lat))
        print(f'\t url: {response.url}')
        if response.status_code:
            print(response.reason)
        if response.ok:
            print('\t<- Good API return')
            content = response.json()
            eto_values = content['data']['readings']
        return eto_values

    def get_weather_readings(self, start_date=None, end_date=None):
        # current_datetime_dt = datetime.now().replace(minute=0, second=0, microsecond=0)
        # end_date = current_datetime_dt.strftime("%m-%d-%Y %H:%M")
        now = datetime.now()
        if start_date is None:
            start_date = now.date() - timedelta(days=30)
            start_date_ymd = start_date.strftime("%Y-%m-%d %H:%M")
            start_date_mdy = start_date.strftime("%m-%d-%Y %H:%M")

        else:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            start_date_ymd = start_date.strftime("%Y-%m-%d %H:%M")
            start_date_mdy = start_date.strftime("%m-%d-%Y %H:%M")

        if end_date is None:
            end_date_ymd = now.strftime("%Y-%m-%d %H:%M")
            end_date_mdy = now.strftime("%m-%d-%Y %H:%M")

        # response = Zentra.get_and_filter_all_data(
        #         device_sn=self.id,
        #         start_date=start_date_mdy,
        #         end_date=end_date_mdy,
        #     )
        res = self.get_logger_data(zentra_api_version='v4')
        return res[6]  # organized results
        # if response.status_code:
        #     print(response.reason)
        # if response.ok:
        #     print('\t<- Good API return')
        #     content = response.json()
        #     print(content)