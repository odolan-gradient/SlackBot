from datetime import datetime

from Logger import Logger


class WeatherStation(Logger):
    def __init__(
            self,
            id: str,
            password: str,
            name: str,
            crop_type: str,
            install_date: datetime.date,
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
