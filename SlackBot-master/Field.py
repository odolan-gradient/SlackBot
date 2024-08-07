import csv
import uuid
from datetime import datetime
from datetime import timedelta
from itertools import zip_longest

import numpy as numpy
from google.cloud import bigquery

# import Decagon
from CwsiProcessor import CwsiProcessor
from DBWriter import DBWriter
from Grower import Grower
from Notifications import AllNotifications
from Thresholds import Thresholds
from WeatherProcessor import WeatherProcessor

DATABASE_YEAR = '2024'
FIELD_PORTALS_BIGQUERY_PROJECT = 'growers-' + DATABASE_YEAR


class Field(object):
    """
    Class to hold information for 1 field where we have a water management trial.

    Attributes:
        cimis_station: String with the value of the CIMIS station that
            corresponds to the field and from which we will get ET info
        name: String of the field name
        grower: String of the grower name for the field
        weather_processor: WeatherProcessor object used to call
            weather API's to get the forecast and icons
    """

    def __init__(self,
                 name: str,
                 loggers: list,
                 lat: str,
                 long: str,
                 cimis_station: str,
                 acres: float,
                 crop_type: str,
                 grower: Grower = None,
                 active: bool = True,
                 report_url: str = 'https://i.imgur.com/04UdmBH.png',
                 preview_url: str = 'https://i.imgur.com/04UdmBH.png',
                 nickname: str = '',
                 field_type: str = 'Commercial',
                 ):
        """
        Inits Field class with the following parameters:

        :param acres:
        :param preview_url:
        :param report_url:
        :param name:
        :param loggers:
        :param lat:
        :param long:
        :param cimis_station:
        :param grower:
        """

        self.loggers = loggers
        self.id = uuid.uuid4()
        self.lat = lat
        self.long = long
        self.cimis_station = cimis_station
        self.name = name
        self.grower = grower
        self.dbwriter = DBWriter()
        self.cwsi_processor = CwsiProcessor()
        # if grower.name == 'Sugal Chile':
        #     self.weather_processor = WeatherProcessor(self.lat, self.long, use_celsius=True)
        # else:
        self.weather_processor = WeatherProcessor(self.lat, self.long)
        self.all_notifications = AllNotifications()
        self.updated = False
        self.weather_crashed = False
        self.active = active
        self.report_url = report_url
        self.preview_url = preview_url
        self.acres = acres
        self.crop_type = crop_type
        self.net_yield = None
        self.paid_yield = None
        self.field_type = field_type

        if len(nickname) > 0:
            self.nickname = nickname
        else:
            self.nickname = self.name.split(self.grower.name)[-1]
        if grower is not None:
            self.field_string = self.grower.name + " - " + self.name

    def __repr__(self):
        return f'Field: {self.nickname}, Active: {self.active}, # of Loggers: {len(self.loggers)}'

    def check_successful_updated_loggers(self):
        successful_loggers = 0
        number_of_active_loggers, number_of_inactive_loggers = self.get_number_of_active_loggers()
        for logger in self.loggers:
            if logger.active and logger.updated:
                successful_loggers += 1
        if successful_loggers == number_of_active_loggers:
            print(f"\t\tAll loggers for Field {self.name} successful! ")
            print(f"\t\tSuccess: {successful_loggers}/ Active: {number_of_active_loggers}")
            self.updated = True
        else:
            print("\t\t{0}/{1} loggers updated successfully".format(successful_loggers, number_of_active_loggers))
            self.updated = False

    def add_logger(self, logger):
        self.loggers.append(logger)

    def add_loggers(self, loggers: list):
        for logger in loggers:
            self.loggers.append(logger)

    def to_string(self, include_loggers: bool = True):
        """
        Function used to print out output to screen. Prints out the field name, grower,
            location GSheetURL, gSheetEtName, gSheetWeatherName, gSheetWeatherIconName,
            cimisStation.
            Then it calls on its plots list and has each object in the list call its own toString function
        :return:
        """
        if not hasattr(self, 'cimis_station'):
            if hasattr(self, 'cimisStation'):
                self.cimis_station = self.cimisStation

        if not hasattr(self, 'report_url'):
            self.report_url = 'No URL'

        if not hasattr(self, 'crop_type'):
            self.crop_type = self.loggers[0].crop_type

        field_str = f'Field: {str(self.name)}'
        location_str = f'Location: ({str(self.lat)},{str(self.long)})'
        active_str = f'Active: {str(self.active)}'
        crop_type_str = f'Crop Type: {str(self.crop_type)}'
        net_yield_str = f'Yield --> Net T/A: {str(self.net_yield)}'


        print('---------------------------------------------------------------------------------------------------')
        print(f'\t{field_str:40} | Grower: {str(self.grower.name)}')
        print(f'\t{location_str:40} | CimisStation: {str(self.cimis_station)}')
        print(f'\t{active_str:40} | Updated: {str(self.updated)}')
        print(f'\t{crop_type_str:40} | Field Type: {self.field_type}')
        print(f'\t{net_yield_str:40} | Paid T/A: {str(self.paid_yield)}')
        print(f'\tReport URL: {str(self.report_url)}')
        print()
        if include_loggers:
            for logger in self.loggers:
                logger.to_string()
        print('---------------------------------------------------------------------------------------------------')

    def update(
            self,
            cimis_stations_pickle,
            get_weather: bool = False,
            get_data: bool = False,
            write_to_portal: bool = False,
            write_to_db: bool = False,
            check_for_notifications: bool = False,
            check_updated: bool = False,
            subtract_from_mrid: int = 0,
            specific_mrid = None
    ):
        """
        Function used to update each fields information. This function will be called every day.
        This function then calls the update function on each of its plots[]

        :param subtract_from_mrid: Int used to subtract a specific amount from the logger MRIDs for API calls
        :param cimis_stations_pickle:
        :param get_weather: Boolean that dictates if we want to get the fields weather forecast
        :param get_data: Boolean that dictates if we want to get the logger data
        :param write_to_portal:
        :param write_to_db:
        :param check_for_notifications:
        :param check_updated:
        :return:
        """
        if self.active:
            # self.field_string = self.grower.name + " - " + self.name
            print()

            if self.updated and not self.weather_crashed:
                print('\tField: ' + self.name + '  already updated. Skipping...')
            else:
                print(f'FIELD updating: {str(self.grower.name)} - {self.name} ->')

                # UPDATE WEATHER
                if get_weather:
                    try:
                        forecast = self.get_weather_forecast()
                        self.weather_crashed = False
                        # weatherData, weatherIconData = self.get_weather_forecast()
                    except Exception as error:
                        print("\tError in Field get_weather - " + self.name)
                        print("\tError type: " + str(error))
                        forecast = []
                        self.weather_crashed = True
                else:
                    print('\tNot doing Weather')

                # Write weather data to DB
                if write_to_db and get_weather:
                    if not forecast:
                        print('\tNo weather data')
                    else:
                        try:
                            print('\tPreparing weather data to be written to DB-')
                            # Prep data for writing to DB
                            weather_schema, weather_filename = self.prep_weather_data_for_writing_to_db(forecast)

                            # Prep DB
                            self.prep_db_for_weather(forecast)

                            # Write to DB
                            print()
                            print('\tWriting weather data to DB')
                            field_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(self.name)
                            project = self.dbwriter.get_db_project(self.loggers[0].crop_type)
                            self.dbwriter.write_to_table_from_csv(
                                field_name, 'weather_forecast', weather_filename, weather_schema, project
                            )
                        except Exception as error:
                            print("Error in Field Weather DB Write - " + self.name)
                            print("Error type: " + str(error))

                if not self.updated:
                    # Get data
                    if get_data:
                        #
                        # For each logger, call its update function

                        field_loggers_portal_data = {}
                        for logger in self.loggers:
                            logger_portal_data = logger.update(cimis_stations_pickle, write_to_db=write_to_db,
                                                               check_for_notifications=check_for_notifications,
                                                               check_updated=check_updated,
                                                               subtract_from_mrid=subtract_from_mrid)  # do logger updates
                            if logger_portal_data is not None:
                                field_loggers_portal_data[logger.id] = logger_portal_data
                        self.check_successful_updated_loggers()
                    else:
                        print('\tNot doing Data')
                        print()

                    # Handling all portal data (Field Portal and Logger Portal) - Processing and writing to portal
                    if write_to_portal and get_data:
                        try:
                            self.cwsi_processor = CwsiProcessor()
                            if field_loggers_portal_data:
                                new_data_to_write_to_portal = False
                                for logger in self.loggers:
                                    if logger.active and logger.id in field_loggers_portal_data:
                                        if field_loggers_portal_data[logger.id]['dates']:
                                            # Intentional way of setting new_data_to_write_to_portal that sets it to True if
                                            # any of the loggers do have data that needs to be written, even if loggers after
                                            # that don't have new data
                                            new_data_to_write_to_portal = new_data_to_write_to_portal or True
                                        else:
                                            new_data_to_write_to_portal = new_data_to_write_to_portal or False

                                if new_data_to_write_to_portal:
                                    print()
                                    print('\t>>> Handling ', self.name, ' Portal Data')

                                    field_averages_table_schema = [bigquery.SchemaField("order", "FLOAT"),
                                                                   bigquery.SchemaField("field", "STRING"),
                                                                   bigquery.SchemaField("crop_type", "STRING"),
                                                                   bigquery.SchemaField("crop_image", "STRING"),
                                                                   bigquery.SchemaField("soil_moisture_num", "FLOAT"),
                                                                   bigquery.SchemaField("soil_moisture_desc", "STRING"),
                                                                   bigquery.SchemaField("si_num", "FLOAT"),
                                                                   bigquery.SchemaField("si_desc", "STRING"),
                                                                   bigquery.SchemaField("report", "STRING"),
                                                                   bigquery.SchemaField("preview", "STRING")]

                                    logger_portal_table_schema = field_averages_table_schema.copy()
                                    logger_portal_table_schema.append(
                                        bigquery.SchemaField("logger_name", "STRING")
                                    )
                                    logger_portal_table_schema.append(
                                        bigquery.SchemaField("logger_direction", "STRING")
                                    )
                                    logger_portal_table_schema.append(
                                        bigquery.SchemaField("location", "STRING")
                                    )

                                    #  Grab field portal data average
                                    print('\t\tGrabbing field average data...')
                                    average_field_portal_data = self.average_field_portal_data(field_loggers_portal_data)

                                    # Process field portal data average
                                    print('\t\tProcessing field portal data...')
                                    processed_average_field_portal_data = self.cwsi_processor.process_data_for_writing_db_portal(
                                        average_field_portal_data, self
                                    )

                                    # Write field portal data
                                    print('\t\tWriting field portal data...')
                                    grower_name = self.dbwriter.remove_unwanted_chars_for_db_dataset(self.grower.name)
                                    field_averages_portal_dataset_id = FIELD_PORTALS_BIGQUERY_PROJECT + '.' + grower_name + '.field_averages'
                                    logger_portal_dataset_id = FIELD_PORTALS_BIGQUERY_PROJECT + '.' + grower_name + '.loggers'

                                    # Check field portal table for field
                                    value_exists = self.check_if_row_value_exists_in_table(
                                        field_averages_portal_dataset_id, 'field', self.nickname, FIELD_PORTALS_BIGQUERY_PROJECT
                                    )
                                    # If it's there, replace specific column values
                                    if value_exists:
                                        # update_portal_table_value(self, dbw, dataset_id, column_name, value_name, processed_portal_data)
                                        self.update_portal_table_value(
                                            field_averages_portal_dataset_id, 'field', self.nickname,
                                            processed_average_field_portal_data
                                        )
                                    else:
                                        # If not, write new row for field
                                        self.write_portal_row(
                                            field_averages_table_schema, grower_name, processed_average_field_portal_data,
                                            'field_averages'
                                        )

                                    # Process logger portal data
                                    print()
                                    print('\t\tProcessing logger portal data...')
                                    processed_logger_portal_data = []
                                    for logger in self.loggers:
                                        if logger.id in field_loggers_portal_data:
                                            if logger.nickname != '':
                                                logger_name = logger.nickname
                                            else:
                                                logger_name = logger.name
                                            processed_logger_portal_data.append(
                                                self.cwsi_processor.process_data_for_writing_db_portal(
                                                    field_loggers_portal_data[logger.id],
                                                    self,
                                                    logger_name=logger_name,
                                                    logger_direction=logger.logger_direction,
                                                    logger_lat=float(logger.lat),
                                                    logger_long=float(logger.long)
                                                )
                                            )
                                    print('\t\tWriting logger portal data...')
                                    for logger_processed_data in processed_logger_portal_data:
                                        value_exists = self.check_if_row_value_exists_in_table(
                                            logger_portal_dataset_id, 'logger_name', logger_processed_data["logger_name"],
                                            FIELD_PORTALS_BIGQUERY_PROJECT
                                        )
                                        # If it's there, replace specific column values
                                        if value_exists:
                                            # update_portal_table_value(self, dbw, dataset_id, column_name, value_name, processed_portal_data)
                                            self.update_portal_table_value(
                                                logger_portal_dataset_id, 'logger_name', logger_processed_data["logger_name"],
                                                logger_processed_data
                                            )
                                        else:
                                            # If not, write new row for field
                                            self.write_portal_row(
                                                logger_portal_table_schema, grower_name, logger_processed_data, 'loggers'
                                            )
                                    print('\t<<< Done with ', self.name, ' Portal Data')
                                else:
                                    print('\tNothing new to write to portal')
                        except Exception as error:
                            print("Error in field portal data - " + self.name)
                            print("Error type: " + str(error))
                    print(str(self.grower.name) + ' - ' + str(self.name) + ' Done Updating-')
                    print()
        else:
            print('Field - {} not active'.format(self.name))

    def write_portal_row(self, table_schema, grower_name: str, processed_portal_data: dict, table_name: str):
        print('\t\t\tdata not already in table')
        print('\t\t\tWriting new row')
        filename = 'portal_data.csv'
        print('\t\t\t- writing data to csv')
        with open(filename, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(processed_portal_data.keys())
            # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
            # This will add full null rows for any additional daily_switch list values
            writer.writerow(processed_portal_data.values())
        print('\t\t\t...Done - file: ' + filename)
        self.dbwriter.write_to_table_from_csv(
            grower_name, table_name, filename, table_schema, project=FIELD_PORTALS_BIGQUERY_PROJECT
        )

    def update_portal_table_value(self, dataset_id: str, column_name: str, value_name: str,
                                  processed_portal_data: dict):
        print('\t\t\tData already found in table')
        print('\t\t\tUpdating table values')

        # DML statement doesn't like None for update, so we changed to 'null'
        if processed_portal_data["si_num"] is None:
            processed_portal_data["si_num"] = 'null'
        if processed_portal_data["soil_moisture_num"] is None:
            processed_portal_data["soil_moisture_num"] = 'null'

        if 'location' in processed_portal_data:
            # This is a logger portal update
            dml = "UPDATE " + dataset_id + " as t SET t.order = " + str(processed_portal_data["order"]) \
                  + ", t.soil_moisture_num = " + str(processed_portal_data["soil_moisture_num"]) \
                  + ", t.soil_moisture_desc = '" + str(processed_portal_data["soil_moisture_desc"]) + "'" \
                  + ", t.si_num = " + str(processed_portal_data["si_num"]) \
                  + ", t.si_desc = '" + str(processed_portal_data["si_desc"]) + "'" \
                  + ", t.crop_image = '" + str(processed_portal_data["crop_image"]) + "'" \
                  + ", t.report = '" + str(processed_portal_data["report"]) + "'" \
                  + ", t.preview = '" + str(processed_portal_data["preview"]) + "'" \
                  + ", t.field = '" + str(processed_portal_data["field"]) + "'" \
                  + ", t.location = '" + str(processed_portal_data["location"]) + "'" \
                  + " WHERE t." + column_name + " = '" + str(value_name) + "'"
        else:
            # This is a field portal update
            dml = "UPDATE " + dataset_id + " as t SET t.order = " + str(processed_portal_data["order"]) \
              + ", t.soil_moisture_num = " + str(processed_portal_data["soil_moisture_num"]) \
              + ", t.soil_moisture_desc = '" + str(processed_portal_data["soil_moisture_desc"]) + "'" \
              + ", t.si_num = " + str(processed_portal_data["si_num"]) \
              + ", t.si_desc = '" + str(processed_portal_data["si_desc"]) + "'" \
              + ", t.crop_image = '" + str(processed_portal_data["crop_image"]) + "'" \
              + ", t.report = '" + str(processed_portal_data["report"]) + "'" \
              + ", t.preview = '" + str(processed_portal_data["preview"]) + "'" \
              + ", t.field = '" + str(processed_portal_data["field"]) + "'" \
              + " WHERE t." + column_name + " = '" + str(value_name) + "'"
        # print('\t\t\t  - ' + str(dml))
        self.dbwriter.run_dml(dml, project=FIELD_PORTALS_BIGQUERY_PROJECT)

    def check_if_row_value_exists_in_table(self, dataset_id: str, column_name: str, value_name: str,
                                           project: str) -> bool:
        dml = "SELECT " + column_name + " FROM " + dataset_id + " WHERE " + column_name + " = '" + value_name + "'"
        result = self.dbwriter.run_dml(dml, project=project)
        if len(list(result)) >= 1:
            return True
        return False

    def average_field_portal_data(self, all_loggers_portal_data: dict) -> dict:
        field_loggers = list(all_loggers_portal_data.keys())
        data_keys = list(all_loggers_portal_data[field_loggers[0]].keys())
        average_field_portal_data = dict.fromkeys(data_keys)

        for each_logger in all_loggers_portal_data:
            if all_loggers_portal_data[each_logger]['dates'] is not None:
                average_field_portal_data['dates'] = all_loggers_portal_data[each_logger]['dates']
            break

        canopy_temps = []
        ambient_temps = []
        cwsis = []
        sdds = []
        rhs = []
        vpds = []
        vwc_1s = []
        vwc_2s = []
        vwc_3s = []
        vwc_1_ecs = []
        vwc_2_ecs = []
        vwc_3_ecs = []
        daily_switchs = []
        kcs = []

        for logger_data in all_loggers_portal_data:
            canopy_temperature = all_loggers_portal_data[logger_data]['canopy temperature']
            ambient_temperature = all_loggers_portal_data[logger_data]['ambient temperature']
            cwsi = all_loggers_portal_data[logger_data]['cwsi']
            sdd = all_loggers_portal_data[logger_data]['sdd']
            rh = all_loggers_portal_data[logger_data]['rh']
            vpd = all_loggers_portal_data[logger_data]['vpd']
            vwc_1 = all_loggers_portal_data[logger_data]['vwc_1']
            vwc_2 = all_loggers_portal_data[logger_data]['vwc_2']
            vwc_3 = all_loggers_portal_data[logger_data]['vwc_3']
            vwc_1_ec = all_loggers_portal_data[logger_data]['vwc_1_ec']
            vwc_2_ec = all_loggers_portal_data[logger_data]['vwc_2_ec']
            vwc_3_ec = all_loggers_portal_data[logger_data]['vwc_3_ec']
            daily_switch = all_loggers_portal_data[logger_data]['daily switch']
            if 'kc' in all_loggers_portal_data[logger_data].keys():
                kc = all_loggers_portal_data[logger_data]['kc']
            else:
                kc = None

            if canopy_temperature is not None:
                canopy_temps.append(canopy_temperature)
            if ambient_temperature is not None:
                ambient_temps.append(ambient_temperature)
            if cwsi is not None:
                cwsis.append(cwsi)
            if sdd is not None:
                sdds.append(sdd)
            if rh is not None:
                rhs.append(rh)
            if vpd is not None:
                vpds.append(vpd)
            if vwc_1 is not None:
                vwc_1s.append(vwc_1)
            if vwc_2 is not None:
                vwc_2s.append(vwc_2)
            if vwc_3 is not None:
                vwc_3s.append(vwc_3)
            if vwc_1_ec is not None:
                vwc_1_ecs.append(vwc_1_ec)
            if vwc_2_ec is not None:
                vwc_2_ecs.append(vwc_2_ec)
            if vwc_3_ec is not None:
                vwc_3_ecs.append(vwc_3_ec)
            if daily_switch is not None:
                daily_switchs.append(daily_switch)
            if kc is not None:
                kcs.append(kc)

        if len(canopy_temps) > 0:
            average_field_portal_data['canopy temperature'] = numpy.mean(canopy_temps)
        if len(ambient_temps) > 0:
            average_field_portal_data['ambient temperature'] = numpy.mean(ambient_temps)
        if len(cwsis) > 0:
            average_field_portal_data['cwsi'] = numpy.mean(cwsis)
        if len(sdds) > 0:
            average_field_portal_data['sdd'] = numpy.mean(sdds)
        if len(rhs) > 0:
            average_field_portal_data['rh'] = numpy.mean(rhs)
        if len(vpds) > 0:
            average_field_portal_data['vpd'] = numpy.mean(vpds)
        if len(vwc_1s) > 0:
            average_field_portal_data['vwc_1'] = numpy.mean(vwc_1s)
        if len(vwc_2s) > 0:
            average_field_portal_data['vwc_2'] = numpy.mean(vwc_2s)
        if len(vwc_3s) > 0:
            average_field_portal_data['vwc_3'] = numpy.mean(vwc_3s)
        if len(vwc_1_ecs) > 0:
            average_field_portal_data['vwc_1_ec'] = numpy.mean(vwc_1_ecs)
        if len(vwc_2_ecs) > 0:
            average_field_portal_data['vwc_2_ec'] = numpy.mean(vwc_2_ecs)
        if len(vwc_3_ecs) > 0:
            average_field_portal_data['vwc_3_ec'] = numpy.mean(vwc_3_ecs)
        if len(daily_switchs) > 0:
            average_field_portal_data['daily switch'] = numpy.mean(daily_switchs)
        if len(kcs) > 0:
            average_field_portal_data['kc'] = numpy.mean(kcs)

        return average_field_portal_data

    def get_weather_forecast(self) -> list:
        """
        Function used to get the weather forecast. Uses the weatherProcessor class to call the yahoo
            weather API and get the forecast for the next 5 days
        :return:
            weatherMatrix: Matrix with weather forecast ready to be written to GSheet
            weatherIconMatrix: Matrix with corresponding weather icons for forecast ready to be written to GSheet
        """
        print()
        # ## Uncomment to use Open Weather API
        # print('\tGetting weather forecast using Open Weather-')
        # forecast = self.weatherProcessor.open_weather_forecast()
        # ##

        ## Uncomment to use Apple Weather Kit API
        print('\tGetting weather forecast using Apple Weather Kit-')
        forecast = self.weather_processor.apple_forecast()
        ##

        print()

        return forecast

    def prep_weather_data_for_writing_to_db(self, forecast: list[dict]) -> (list, str):
        weather_schema = [
            bigquery.SchemaField("date", "DATE"),
            bigquery.SchemaField("day", "STRING"),
            bigquery.SchemaField("order", "FLOAT"),
            bigquery.SchemaField("temp", "FLOAT"),
            bigquery.SchemaField("rh", "FLOAT"),
            bigquery.SchemaField("vpd", "FLOAT"),
            bigquery.SchemaField("icon", "STRING"),
        ]
        weather_filename = 'weather forecast.csv'
        order = 0

        weather_data = {"date": [], "day": [], "order": [], "temp": [], "rh": [], "vpd": [], "icon": []}
        for ind, data_point in enumerate(forecast):
            date = data_point['time']
            date_string_format = date.strftime("%Y-%m-%d")
            date_day = date.strftime('%a')
            max_temp = data_point['max_temp']
            relative_humidity = data_point['humidity']
            vpd = data_point['vpd']
            forecastText = data_point['icon']
            relative_humidity_percentage = relative_humidity * 100
            order = ind + 1

            weather_data["date"].append(date_string_format)
            weather_data["day"].append(date_day)
            weather_data["order"].append(order)
            weather_data["temp"].append(round(max_temp, 1))
            weather_data["rh"].append(round(relative_humidity_percentage, 1))
            weather_data["vpd"].append(round(vpd, 1))
            weather_data["icon"].append(forecastText)

        weather_data = self.add_extra_blank_days(forecast, order, weather_data)

        print('\t\tWriting weather data to csv')
        with open(weather_filename, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(weather_data.keys())
            # Using zip_longest because dict rows are uneven length due to daily_switch algo issue
            # This will add full null rows for any additional daily_switch list values
            writer.writerows(zip_longest(*weather_data.values()))
        print('\t\tDone - file: ' + weather_filename)
        print()

        return weather_schema, weather_filename

    def add_extra_blank_days(self, forecast: list, order: int, weather_data: dict):

        current_forecast_days = len(weather_data["date"])
        days_to_add = 10 - current_forecast_days

        if days_to_add > 0:
            for x in range(days_to_add):
                extra_day = (forecast[-1]['time']) + timedelta(days=x + 1)
                extra_day_string = extra_day.strftime("%Y-%m-%d")
                weather_data["date"].extend([extra_day_string])
                weather_data["day"].extend([None])
                weather_data["order"].extend([order + x + 1])
                weather_data["temp"].extend([None])
                weather_data["rh"].extend([None])
                weather_data["vpd"].extend([None])
                weather_data["icon"].extend([None])

        return weather_data

    def prep_db_for_weather(self, forecast: list):
        dbw = DBWriter()
        field_name = dbw.remove_unwanted_chars_for_db_dataset(self.name)
        project = dbw.get_db_project(self.loggers[0].crop_type)
        table_exsists = dbw.check_if_table_exists(field_name, 'weather_forecast', project=project)
        if table_exsists:
            # Change the order of the previous day to 99
            print('\t\tChanging weather order from prev day to 99')
            self.change_order_from_previous_day(forecast)
            # Delete all data that is between the new ranges for forecast
            print('\t\tRemoving redundant data')
            self.remove_data_that_is_about_to_be_updated(forecast)

    def change_order_from_previous_day(self, forecast: list):
        dbw = DBWriter()
        field = dbw.remove_unwanted_chars_for_db_dataset(self.name)
        first_date = (forecast[0]['time'])
        # first_date_string = first_date.strftime("%Y-%m-%d")
        previous_date = first_date - timedelta(days=1)
        previous_date_string = previous_date.strftime("%Y-%m-%d")
        project = dbw.get_db_project(self.loggers[0].crop_type)
        dml = "UPDATE `" + project + "." + field + ".weather_forecast` as t SET t.order = 99.0 WHERE date <= '" + previous_date_string + "'"
        dbw.run_dml(dml, project=project)

    def remove_data_that_is_about_to_be_updated(self, forecast: list):
        dbw = DBWriter()
        field = dbw.remove_unwanted_chars_for_db_dataset(self.name)
        first_date = (forecast[0]['time'])
        last_date = (forecast[-1]['time']) + timedelta(days=2)
        first_date_string = first_date.strftime("%Y-%m-%d")
        last_date_string = last_date.strftime("%Y-%m-%d")
        project = dbw.get_db_project(self.loggers[0].crop_type)
        dml = "DELETE `" + project + "." + field + ".weather_forecast` " \
                                                   "WHERE date BETWEEN DATE('" + first_date_string + "') AND DATE('" + last_date_string + "') "
        # print(dml)
        dbw.run_dml(dml, project=project)

    def update_et_tables(self):
        # latest_et = self.get_latest_et()
        for logger in self.loggers:
            print('\tUpdating et values in Logger table...')
            try:
                logger.merge_et_db_with_logger_db_values()
            except Exception as err:
                print("ET Did not update for this logger")
                print(err)

    def get_number_of_active_loggers(self) -> (int, int):
        active_loggers = 0
        inactive_loggers = 0
        for logger in self.loggers:
            if logger.active:
                active_loggers += 1
            else:
                inactive_loggers += 1
        return active_loggers, inactive_loggers

    def check_for_notifications(self, weather_data):
        """
        Function to check for notifications related to the weather forecast data.

        :param weather_data:
            2-dimensional list of the weather forecast data before it is written to GSheets. Format of the list is:
            [
                [day],
                [temperature highs],
                [temperature lows]
            ]

        :return:
        """
        thresholds = Thresholds()
        weather_threshold = thresholds.weather_threshold
        consecutive_temps = thresholds.consecutive_temps
        all_weather_results = {"days": [],
                               "temps": []}
        consecutive_weather_results = {"days": [],
                                       "temps": []}

        i = 0
        j = 0
        length = len(weather_data[1])
        while i < length:
            if int(weather_data[1][i]) >= weather_threshold:
                all_weather_results["temps"].insert(j, [int(weather_data[1][i])])
                all_weather_results["days"].insert(j, [weather_data[0][i]])
                i += 1
                while i < len(weather_data[1]):
                    if int(weather_data[1][i]) >= weather_threshold:
                        all_weather_results["temps"][j].append(int(weather_data[1][i]))
                        all_weather_results["days"][j].append(weather_data[0][i])
                        i += 1
                    else:
                        break
                j += 1
            i += 1
        for ind, i in enumerate(all_weather_results["temps"]):
            if len(i) >= consecutive_temps:
                consecutive_weather_results["temps"].append(all_weather_results["temps"][ind])
                consecutive_weather_results["days"].append(all_weather_results["days"][ind])

        if consecutive_weather_results["temps"]:
            self.all_notifications.add_notification(
                datetime.now(), str(self.grower.name) + " - " + self.name,
                self.field_string, "Weather", consecutive_weather_results,
                weather_threshold, "were greater than"
            )

    def deactivate(self):
        print('Deactivating Field {}...'.format(self.name))
        self.active = False
        for logger in self.loggers:
            logger.deactivate()
        print('Done')
