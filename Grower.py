import uuid

from google.cloud import bigquery

from DBWriter import DBWriter
from Notifications import AllNotifications
from Technician import Technician

YEAR = '2025'
FIELD_PORTALS_BIGQUERY_PROJECT = f'growers-{YEAR}'


class Grower(object):
    """
    Class to hold information for a grower

    Attributes:
            name: String variable to hold the name of the Grower
            email: String variable to hold the grower email for notification purposes
            id: UUID4 unique string id
            fields: A list of Field objects for each Field we have our system in with this grower
    """

    def __init__(self, name: str, fields: list, technician: Technician, email: str = '', region: str = '',
                 active: bool = True):
        """
        Inits Grower class with the following parameters:

        :param name:
        :param email:
        :param fields:
        :param region:
        """
        self.name = name
        self.email = email
        self.portalGSheetURL = ''
        self.id = uuid.uuid4()
        self.fields = fields
        self.region = region
        self.technician = technician
        self.updated = False
        self.active = active
        self.all_notifications = AllNotifications()

    def __repr__(self):
        return f'Grower: {self.name}, Active: {self.active}, # of Fields: {len(self.fields)}'

    def check_successful_updated_fields(self):
        successfulFields = 0
        number_of_active_fields, number_of_inactive_fields = self.get_number_of_active_fields()
        for f in self.fields:
            if f.updated and f.active:
                successfulFields = successfulFields + 1
        if successfulFields == number_of_active_fields:
            print("All fields for Grower {0} successful! ".format(self.name))
            print("{0}/{1}".format(successfulFields, number_of_active_fields))
            self.updated = True
        else:
            print("{0}/{1} fields updated successfully".format(successfulFields, number_of_active_fields))
            self.updated = False

    def get_number_of_active_fields(self) -> (int, int):
        active_fields = 0
        inactive_fields = 0
        for field in self.fields:
            if field.active:
                active_fields += 1
            else:
                inactive_fields += 1
        return active_fields, inactive_fields

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
            specific_mrid: int = None,
            zentra_api_version: str = 'v1',
            specific_start_date: str = None,
            specific_end_date: str = None,
    ):
        """
        Function used to update each fields information. This function will be called every day.
        This function then calls the update function on each of its plots[]

        :param specific_mrid:
        :param specific_start_date: String date in the format: m-d-Y H:M
        :param specific_end_date: String date in the format: m-d-Y H:M
        :param zentra_api_version:
        :param subtract_from_mrid: Int used to subtract a specific amount from the logger MRIDs for API calls
        :param cimis_stations_pickle:
        :param check_updated:
        :param write_to_db:
        :param write_to_portal:
        :param get_et: Boolean that dictates if we want to get the field Et
        :param get_weather: Boolean that dictates if we want to get the fields weather forecast
        :param get_data: Boolean that dictates if we want to get the logger data
        :param check_for_notifications: Boolean that dictates if we want to process notifications
        :return:
        """

        if self.active:
            if self.updated:
                print('\tGrower: ' + self.name + '  already updated. Skipping...')
            else:
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
                print(f'GROWER updating: {str(self.name)} ->')
                print()

                if write_to_portal:
                    try:
                        print('Setting up Portal Tables')
                        self.setup_portal_tables()
                    except Exception as e:
                        print("Error in Grower Update - Setting up Portal Tables" + self.name)
                        print("Error type: " + str(e))

                for field in self.fields:
                    field.update(
                        cimis_stations_pickle,
                        get_weather=get_weather,
                        get_data=get_data,
                        write_to_portal=write_to_portal,
                        write_to_db=write_to_db,
                        check_for_notifications=check_for_notifications,
                        check_updated=check_updated,
                        subtract_from_mrid=subtract_from_mrid,
                        specific_mrid=specific_mrid,
                        zentra_api_version=zentra_api_version,
                        specific_start_date=specific_start_date,
                        specific_end_date=specific_end_date,
                    )

                self.check_successful_updated_fields()

                print()
                print('>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>')
                print()
        else:
            print('Grower - {} not active'.format(self.name))
            # except Exception as e:
            #     print("Error in Grower Update - " + f.name)
            #     print("Error type: " + str(e))
            #     self.all_notifications.create_error_notification(datetime.now(), f, "Decagon API Error")

    def setup_portal_tables(self):
        dbwriter = DBWriter()
        grower_name = dbwriter.remove_unwanted_chars_for_db_dataset(self.name)
        # Create grower dataset for portal data
        dbwriter.create_dataset(grower_name, project=FIELD_PORTALS_BIGQUERY_PROJECT)
        field_averages_table_exists = dbwriter.check_if_table_exists(
            grower_name, 'field_averages',
            project=FIELD_PORTALS_BIGQUERY_PROJECT
        )
        if not field_averages_table_exists:
            field_averages_table_schema = [
                bigquery.SchemaField("order", "FLOAT"),
                bigquery.SchemaField("field", "STRING"),
                bigquery.SchemaField("crop_type", "STRING"),
                bigquery.SchemaField("crop_image", "STRING"),
                bigquery.SchemaField("soil_moisture_num", "FLOAT"),
                bigquery.SchemaField("soil_moisture_desc", "STRING"),
                bigquery.SchemaField("si_num", "FLOAT"),
                bigquery.SchemaField("si_desc", "STRING"),
                bigquery.SchemaField("report", "STRING"),
                bigquery.SchemaField("preview", "STRING")
            ]
            table = dbwriter.create_table(
                grower_name, 'field_averages', field_averages_table_schema,
                project=FIELD_PORTALS_BIGQUERY_PROJECT
            )

        loggers_table_exists = dbwriter.check_if_table_exists(
            grower_name, 'loggers',
            project=FIELD_PORTALS_BIGQUERY_PROJECT
        )
        if not loggers_table_exists:
            loggers_table_schema = [
                bigquery.SchemaField("order", "FLOAT"),
                bigquery.SchemaField("field", "STRING"),
                bigquery.SchemaField("crop_type", "STRING"),
                bigquery.SchemaField("crop_image", "STRING"),
                bigquery.SchemaField("soil_moisture_num", "FLOAT"),
                bigquery.SchemaField("soil_moisture_desc", "STRING"),
                bigquery.SchemaField("si_num", "FLOAT"),
                bigquery.SchemaField("si_desc", "STRING"),
                bigquery.SchemaField("report", "STRING"),
                bigquery.SchemaField("preview", "STRING"),
                bigquery.SchemaField("logger_name", "STRING"),
                bigquery.SchemaField("logger_direction", "STRING"),
                bigquery.SchemaField("location", "STRING"),
            ]
            table = dbwriter.create_table(
                grower_name, 'loggers', loggers_table_schema,
                project=FIELD_PORTALS_BIGQUERY_PROJECT
            )

    def to_string(self, include_fields: bool = True):
        """
        Function used to print out output to screen. Prints out the Plot type.
        Then this calls on its loggers list and has each object in the list call its own toString function
        :return:
        """
        tech_str = f'Tech: {str(self.technician.name)}'
        region_str = f'Region: {self.region}'
        print()
        print(
            '*****************************************************************************************************************************'
        )
        print(f'\tGrower: {self.name}')
        print(f'\t{tech_str:40} | Active: {str(self.active)}')
        print(f'\t{region_str:40} | Updated: {str(self.updated)}')
        print()
        if include_fields:
            for f in self.fields:
                f.to_string()

    def deactivate(self):
        print('Deactivating Grower {}...'.format(self.name))
        self.active = False
        for field in self.fields:
            field.deactivate()
        print('Done')
