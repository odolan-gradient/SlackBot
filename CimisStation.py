import pickle
from os import path


class CimisStation(object):

    def __init__(self, station_number='', latest_eto_value='', updated=False):
        self.station_number = station_number
        self.latest_eto_value = latest_eto_value
        self.updated = updated
        self.active = True

    def to_string(self):
        print(f'CIMIS STATION')
        print(f'Station #: {self.station_number}')
        print(f'Latest ETo: {str(self.latest_eto_value)}')
        print(f'Updated: {str(self.updated)}')
        print()

    def __repr__(self):
        return f'Station #: {self.station_number}, Latest ETo: {str(self.latest_eto_value)}, Updated: {str(self.updated)}'

    def removeStation(self, station_to_be_removed):
        foundStation = False
        cimisStationsPickle = self.open_cimis_station_pickle()
        for index, station in enumerate(cimisStationsPickle):
            if station.station_number == station_to_be_removed:
                print("Removing Station: ", station.station_number)
                stationIndex = index
                foundStation = True
        if foundStation:
            cimisStationsPickle.pop(stationIndex)
        self.write_cimis_station_pickle(data=cimisStationsPickle)

    def create_station_and_add_to_pickle(self, station_to_be_added):
        cimis_station_pickle = self.open_cimis_station_pickle()
        new_cimis_station = CimisStation(station_number=station_to_be_added)
        cimis_station_pickle.append(new_cimis_station)
        self.write_cimis_station_pickle(data=cimis_station_pickle)

    def showCimisStations(self):
        cimisStationsPickle = self.open_cimis_station_pickle()
        for stations in cimisStationsPickle:
            print(stations.station_number, ": ", stations.latest_eto_value)

    def return_list_of_stations(self):
        cimisStationsPickle = self.open_cimis_station_pickle()
        stationList = []
        for stations in cimisStationsPickle:
            stationList.append(stations.station_number)
        return stationList

    def get_latest_eto_specific_station(self, station_number):
        cimisStationsPickle = self.open_cimis_station_pickle()
        list_of_stations = self.return_list_of_stations()
        if station_number in list_of_stations:
            for stations in cimisStationsPickle:
                if stations.station_number == station_number:
                    return stations.latest_eto_value

    def open_cimis_station_pickle(self):
        if path.exists("H:\\Shared drives\\Stomato\\2024\\Pickle\\cimisStation.pickle"):
            with open("H:\\Shared drives\\Stomato\\2024\\Pickle\\cimisStation.pickle", 'rb') as f:
                content = pickle.load(f)
            return content

    def write_cimis_station_pickle(self, data):
        if path.exists("H:\\Shared drives\\Stomato\\2024\\Pickle\\"):
            with open("H:\\Shared drives\\Stomato\\2024\\Pickle\\cimisStation.pickle", 'wb') as f:
                pickle.dump(data, f)

    def check_for_new_cimis_stations(self, stomato_pickle):
        cimis_stations_list = self.return_list_of_stations()
        added_cimis_stations_list = []
        for g in stomato_pickle:
            for f in g.fields:
                if f.cimis_station not in cimis_stations_list and f.cimis_station not in added_cimis_stations_list:
                    print("Cimis Station not in Cimis Station List \n\tAdding Station to list: ", f.cimis_station)
                    self.create_station_and_add_to_pickle(f.cimis_station)
                    added_cimis_stations_list.append(f.cimis_station)

        print("Done checking for new cimis stations")

    def return_cimis_station_active_status(self, cimis_station_number):
        cimis_stations_pickle = self.open_cimis_station_pickle()
        for stations in cimis_stations_pickle:
            if stations.station_number == cimis_station_number:
                return stations.active
        return None

    def return_inactive_cimis_stations_list(self):
        inactive_cimis_stations_list = []
        cimis_stations_pickle = self.open_cimis_station_pickle()
        for stations in cimis_stations_pickle:
            if not stations.active:
                inactive_cimis_stations_list.append(stations.station_number)
        return inactive_cimis_stations_list

    def deactivate_cimis_station(self, station_number: str):
        """
        :type station_number: station number to be deactivated
        :param station_number:
        """
        cimis_stations_pickle = self.open_cimis_station_pickle()
        for stations in cimis_stations_pickle:
            if stations.station_number == station_number:
                stations.active = False
                print(f"Deactivating Station: {station_number}")
        self.write_cimis_station_pickle(data=cimis_stations_pickle)

    def activate_cimis_station(self, station_number: str):
        """
        :type station_number: station number to be activated
        :param station_number:
        """
        cimis_stations_pickle = self.open_cimis_station_pickle()
        for stations in cimis_stations_pickle:
            if stations.station_number == station_number:
                stations.active = True
                print(f"Activating Station: {station_number}")
        self.write_cimis_station_pickle(data=cimis_stations_pickle)



#
# cimisStations = CimisStation()
# cimisStations = cimisStations.open_cimis_station_pickle()
# for f in cimisStations:
#     print(f.station_number)
