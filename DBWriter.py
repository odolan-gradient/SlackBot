import json
import os
from pathlib import Path

from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.oauth2 import service_account

####################################################################
# Base Class for writing Decagon sensor data to Google BigQuery    #
# Expand this class for changes to what is written                 #
####################################################################
DIRECTORY_YEAR = "2024"
DATABASE_YEAR = DIRECTORY_YEAR
LOGGERS_BIGQUERY_PROJECT = 'stomato-' + DATABASE_YEAR
LOGGERS_PERMANENT_BIGQUERY_PROJECT = 'stomato-permanents'


class DBWriter(object):

    def __init__(self):
        # self.client = bigquery.Client()
        pass

    def remove_unwanted_chars_for_db_dataset(self, dataset_id):
        dataset_id = dataset_id.replace('-', '_')
        dataset_id = dataset_id.replace(' ', '_')
        dataset_id = dataset_id.replace('/', '_')
        dataset_id = dataset_id.replace(',', '_')
        dataset_id = dataset_id.replace('&', '_')
        dataset_id = dataset_id.replace(':', '_')
        dataset_id = dataset_id.replace('\\', '_')
        dataset_id = dataset_id.replace('|', '_')
        dataset_id = dataset_id.replace('.', '')
        return dataset_id

    def remove_unwanted_chars_for_db_table(self, table_id):
        table_id = table_id.replace('&', '_')
        return table_id

    def grab_bq_client(self, my_project):
        # First, try to get the path to the credentials file
        directory_path = Path().absolute()
        credentials_path = Path.joinpath(directory_path, 'credentials_file.json')
        try:
            if credentials_path and os.path.exists(credentials_path):
                # If the file exists, use it
                credentials = service_account.Credentials.from_service_account_file(
                    credentials_path,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )
            else:
                # If the file doesn't exist, look for credentials in an environment variable
                credentials_json = os.environ.get('BQ_GOOGLE_CREDENTIALS')
                if not credentials_json:
                    raise ValueError(
                        "Neither BQ_GOOGLE_CREDENTIALS file nor GOOGLE_CREDENTIALS_JSON environment variable is set")

                # Parse the JSON string from the environment variable
                credentials_info = json.loads(credentials_json)
                credentials = service_account.Credentials.from_service_account_info(
                    credentials_info,
                    scopes=["https://www.googleapis.com/auth/cloud-platform"],
                )

            client = bigquery.Client(credentials=credentials, project=my_project)
            return client

        except FileNotFoundError:
            raise FileNotFoundError(f"Credentials file not found at {credentials_path}")
        except json.JSONDecodeError:
            raise ValueError("Invalid JSON in GOOGLE_CREDENTIALS_JSON environment variable")
        except ValueError as e:
            raise ValueError(f"Error with credentials: {str(e)}")

    def write_to_table_from_csv(self, dataset_id, table_id, filename, schema, project, overwrite=False):
        # print('\t- writing to table')
        if project == '' or project is None:
            print('Error: Empty/None project in write_to_table_from_csv')
            print(' Not writing anything')
        else:
            client = self.grab_bq_client(my_project=project)

            # Checking if the dataset already exists, if it doesn't create it
            dataset_id = self.remove_unwanted_chars_for_db_dataset(dataset_id)
            table_id = self.remove_unwanted_chars_for_db_table(table_id)
            try:
                client.get_dataset(dataset_id)  # Make an API request.
                # print("\t  Dataset {} already exists".format(dataset_id))
            except NotFound:
                # print("\t  Dataset {} is not found".format(dataset_id))
                self.create_dataset(dataset_id, project=project)

            full_table_id = client.project + '.' + dataset_id + '.' + table_id

            job_config = bigquery.LoadJobConfig(
                schema=schema,
                skip_leading_rows=1,
                # The source format defaults to CSV, so the line below is optional.
                source_format=bigquery.SourceFormat.CSV,
            )
            if overwrite:
                job_config = bigquery.LoadJobConfig(
                    schema=schema,
                    skip_leading_rows=1,
                    write_disposition='WRITE_TRUNCATE',
                    # The source format defaults to CSV, so the line below is optional.
                    source_format=bigquery.SourceFormat.CSV,
                )

            with open(filename, "rb") as source_file:
                job = client.load_table_from_file(source_file, full_table_id, job_config=job_config)

            job.result()  # Waits for the job to complete.

            table = client.get_table(full_table_id)  # Make an API request.
            print(
                "\t\tLoaded {} rows and {} columns to {}".format(
                    table.num_rows, len(table.schema), full_table_id
                )
            )
            print()

    def create_dataset(self, dataset_id, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)

        # Set dataset_id to the ID of the dataset to create.
        dataset_id = "{}.{}".format(client.project, dataset_id)
        # dataset_id = dataset_id

        # Construct a full Dataset object to send to the API.
        dataset = bigquery.Dataset(dataset_id)

        # Specify the geographic location where the dataset should reside.
        dataset.location = "US"

        # Send the dataset to the API for creation, with an explicit timeout.
        # Raises google.api_core.exceptions.Conflict if the Dataset already
        # exists within the project.
        try:
            dataset = client.create_dataset(dataset, timeout=30)  # Make an API request.
            print("\tCreated dataset {}.{}".format(client.project, dataset.dataset_id))
        except Exception as e:
            pass
            # Not printing e because its ugly in Logs and shows a whole long 409 message
            # print(e)

    def delete_dataset(self, dataset_id, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)

        dataset_id = project + '.' + dataset_id

        # Use the delete_contents parameter to delete a dataset and its contents.
        # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
        client.delete_dataset(
            dataset_id, delete_contents=True, not_found_ok=True
        )  # Make an API request.

        print("Deleted dataset '{}'.".format(dataset_id))

    def delete_all_datasets(self, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)

        datasets = self.list_datasets(project=project)
        dataset_ids = []
        print('Deleting datasets...')
        for dataset in datasets:
            dataset_ids.append(dataset.dataset_id)

        for id in dataset_ids:
            # Use the delete_contents parameter to delete a dataset and its contents.
            # Use the not_found_ok parameter to not receive an error if the dataset has already been deleted.
            client.delete_dataset(
                id, delete_contents=True, not_found_ok=True
            )  # Make an API request.
            print("Deleted dataset '{}'.".format(id))

    def list_datasets(self, project='stomato'):
        # Construct a BigQuery client object.
        datasets, project = self.get_datasets(project=project)

        if datasets:
            print("Datasets in project {}:".format(project))
            for dataset in datasets:
                print("\t{}".format(dataset.dataset_id))
        else:
            print("{} project does not contain any datasets.".format(project))
        return datasets

    def get_datasets(self, project='stomato'):
        client = self.grab_bq_client(my_project=project)
        datasets = list(client.list_datasets())  # Make an API request.
        project = client.project
        return datasets, project

    def list_tables(self, dataset_id, project='stomato'):
        tables = self.get_tables(dataset_id, project=project)

        print("Tables contained in '{}':".format(dataset_id))
        for table in tables:
            print("\t{}.{}.{}".format(table.project, table.dataset_id, table.table_id))
        print()
        return tables

    def get_tables(self, dataset_id, project='stomato'):
        client = self.grab_bq_client(my_project=project)
        dataset_id = project + '.' + str(dataset_id)
        tables = client.list_tables(dataset_id)
        return tables

    def run_dml(self, dml, job_config=None, project='stomato'):
        # print('\t\t Running DML...')  # Data Manipulation Language
        # print(f'\t\t {dml}')
        client = self.grab_bq_client(my_project=project)
        dml_statement = dml
        query_job = client.query(dml_statement, job_config)  # API request
        result = query_job.result()  # Waits for statement to finish
        # print('\t\t Done with DML')
        return result

    @staticmethod
    def get_db_project(crop_type: str) -> str:
        """
        Get the correct Google Cloud Big Query project you need for writing to the DB depending on the crop_type.
        If it is a yearly changing crop like tomatoes or peppers, you use a project with the name: stomato-CURRENT_YEAR.
        If it is a permanent crop like almonds or pistachios, you use a project with the name: stomato-permanents.
        :param crop_type: String of the crop type. ex: 'Tomatoes' / 'Almonds' / 'Pistachios' / etc
        :return: String for the correct project to use. ex: 'stomato-2023' / 'stomato-permanents'
        """
        crop = crop_type.lower()[0:4]
        return {
            'toma': LOGGERS_BIGQUERY_PROJECT,
            'pepp': LOGGERS_BIGQUERY_PROJECT,
            'almo': LOGGERS_PERMANENT_BIGQUERY_PROJECT,
            'pist': LOGGERS_PERMANENT_BIGQUERY_PROJECT,
            'date': LOGGERS_PERMANENT_BIGQUERY_PROJECT,
            'squa': LOGGERS_BIGQUERY_PROJECT,
            'cher': LOGGERS_PERMANENT_BIGQUERY_PROJECT,
            'onio': LOGGERS_BIGQUERY_PROJECT,
            'wate': LOGGERS_BIGQUERY_PROJECT,
            'corn': LOGGERS_BIGQUERY_PROJECT,
        }.get(crop, '')

    def return_query_dict(self, dml, col1, col2, project, job_config=None):
        # Function returns in a dictionary format 2 columns of DB data. Col 1 is the key and col 2 is the value
        # print('    -----Running DML...')  # Data Manipulation Language
        client = self.grab_bq_client(my_project=project)
        dml_statement = dml
        query_job = client.query(dml_statement, job_config)  # API request
        queryDict = {}
        for row in query_job:
            queryCol1 = row[col1]
            # if isinstance(queryCol1, datetime.date):
            #     queryCol1 = queryCol1.strftime('%Y-%m-%d')
            queryDict[queryCol1] = row[col2]
        # print('    -----Done with DML')
        return queryDict

    def add_new_column_to_table(self, dataset_id, table_name, column_name, value_type, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)

        table_id = project + '.' + dataset_id + "." + table_name

        table = client.get_table(table_id)  # Make an API request.

        original_schema = table.schema
        new_schema = original_schema[:]  # Creates a copy of the schema.
        new_schema.append(bigquery.SchemaField(column_name, value_type))

        table.schema = new_schema
        table = client.update_table(table, ["schema"])  # Make an API request.

        if len(table.schema) == len(original_schema) + 1 == len(new_schema):
            print("A new column has been added.")
        else:
            print("The column has not been added.")

    def add_ai_columns_to_all_tables(self, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)
        datasets = dbwriter.get_datasets(project=project)
        for d in datasets[0]:
            if d.dataset_id == 'ET':
                continue
            tables = dbwriter.get_tables(d.dataset_id, project=project)
            for t in tables:
                table_id = project + '.' + d.dataset_id + "." + t.table_id

                table = client.get_table(table_id)  # Make an API request.

                original_schema = table.schema
                new_schema = original_schema[:]  # Creates a copy of the schema.
                if len(original_schema) < 26:
                    new_schema.append(bigquery.SchemaField('phase1_adjustment', 'FLOAT'))
                    new_schema.append(bigquery.SchemaField('phase1_adjusted', 'FLOAT'))
                    new_schema.append(bigquery.SchemaField('phase2_adjustment', 'FLOAT'))
                    new_schema.append(bigquery.SchemaField('phase2_adjusted', 'FLOAT'))
                    new_schema.append(bigquery.SchemaField('phase3_adjustment', 'FLOAT'))
                    new_schema.append(bigquery.SchemaField('phase3_adjusted', 'FLOAT'))

                    table.schema = new_schema
                    table = client.update_table(table, ["schema"])  # Make an API request.
                    print('Table {} - {} does not have AI columns'.format(d.dataset_id, t.table_id))
                    print('Schema length: {}'.format(len(original_schema)))
                else:
                    print('Table {} - {} already has AI columns'.format(d.dataset_id, t.table_id))
                    print('Schema length: {}'.format(len(original_schema)))

                if len(table.schema) == len(original_schema) + 6 == len(new_schema):
                    print("AI columns have been added.")
                else:
                    print("The column has not been added.\n")
        print('Done')

    def add_gdd_columns_to_all_tables(self, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)
        datasets = dbwriter.get_datasets(project=project)
        for d in datasets[0]:

            if d.dataset_id == 'ET' or d.dataset_id == 'Meza' or d.dataset_id == 'Historical_ET':
                continue
            print(d.dataset_id)
            if d.dataset_id == 'Nees_AI':
                tables = dbwriter.get_tables(d.dataset_id, project=project)
                for t in tables:

                    if t.table_id == 'weather_forecast' or 'Irr_Scheduling' in t.table_id or 'z6' in t.table_id or '5G' in t.table_id:
                        continue
                    print(t.table_id)
                    table_id = project + '.' + d.dataset_id + "." + t.table_id

                    table = client.get_table(table_id)  # Make an API request.

                    original_schema = table.schema
                    new_schema = original_schema[:]  # Creates a copy of the schema.
                    if len(original_schema) < 40:
                        new_schema.append(bigquery.SchemaField('lowest_ambient_temperature', 'FLOAT'))
                        new_schema.append(bigquery.SchemaField('gdd', 'FLOAT'))
                        new_schema.append(bigquery.SchemaField('crop_stage', 'STRING'))
                        new_schema.append(bigquery.SchemaField('id', 'STRING'))
                        new_schema.append(bigquery.SchemaField('planting_date', 'DATE'))
                        new_schema.append(bigquery.SchemaField('variety', 'STRING'))

                        table.schema = new_schema
                        table = client.update_table(table, ["schema"])  # Make an API request.
                        print('Table {} - {} does not have GDD columns'.format(d.dataset_id, t.table_id))
                        print('Schema length: {}'.format(len(original_schema)))
                    else:
                        print('Table {} - {} already has GDD columns'.format(d.dataset_id, t.table_id))
                        print('Schema length: {}'.format(len(original_schema)))

                    if len(table.schema) == len(original_schema) + 6 == len(new_schema):
                        print("GDD columns have been added.")
                    else:
                        print("The column has not been added.\n")
        print('Done')

    def delete_all_temp_tables(self, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)
        datasets = dbwriter.get_datasets(project=project)
        for d in datasets[0]:
            if d.dataset_id == 'ET' or d.dataset_id == 'Historical_ET':
                continue
            # print(d.dataset_id)
            tables = dbwriter.get_tables(d.dataset_id, project=project)
            for t in tables:
                if '_temp' in t.table_id:
                    client.delete_table(t.table_id, not_found_ok=True)
                    print("Deleted table '{}'.".format(t.table_id))
        print('Done')

    def merge_all_tables_for_gdd(self, project='stomato'):
        # Construct a BigQuery client object.
        client = self.grab_bq_client(my_project=project)
        datasets = dbwriter.get_datasets(project=project)
        for d in datasets[0]:
            if d.dataset_id == 'ET' or d.dataset_id == 'Historical_ET':
                continue
            # print(d.dataset_id)
            if d.dataset_id == 'Nees_AI':
                tables = dbwriter.get_tables(d.dataset_id, project=project)
                for t in tables:
                    if '_temp' in t.table_id:
                        table_id = t.table_id[:-5]
                        print(f'Merging {d.dataset_id} === {table_id} with {t.table_id}')
                        dml_statement = "MERGE `" + project + "." + d.dataset_id + "." + table_id + "` T " \
                                        + "USING `" + project + "." + d.dataset_id + "." + t.table_id + "` S " \
                                        + "ON T.date = S.date " \
                                        + "WHEN MATCHED THEN " \
                                        + "UPDATE SET " \
                                          "lowest_ambient_temperature = s.lowest_ambient_temperature, " \
                                          "gdd = s.gdd," \
                                          "crop_stage = s.crop_stage, " \
                                          "id = s.id, " \
                                          "planting_date = s.planting_date"

                        result = dbwriter.run_dml(dml_statement, project=project)

        print('Done')

    def list_db(self, project='stomato'):
        datasets, project = dbwriter.get_datasets(project=project)
        for dataset in datasets:
            if dataset.dataset_id == 'ET':
                print(dataset.dataset_id)
                dbwriter.list_tables(dataset.dataset_id, project=project)

    def get_specific_dataset(self, dataset, project='stomato'):
        datasets, project = self.get_datasets(project=project)
        for ds in datasets:
            if ds.dataset_id == dataset:
                print('Found dataset ' + str(dataset))
                return ds

    def check_if_table_exists(self, dataset, table, project='stomato'):
        table_id = project + '.' + dataset + '.' + table
        client = self.grab_bq_client(my_project=project)
        try:
            client.get_table(table_id)  # Make an API request.
            print("\tTable {} already exists.".format(table_id))
            return True
        except NotFound:
            print("\tTable {} is not found.".format(table_id))
            return False
    def check_if_dataset_exists(self, dataset, project='stomato-2024'):
        dataset_id = project + '.' + dataset
        client = self.grab_bq_client(my_project=project)
        try:
            client.get_dataset(dataset_id)  # Make an API request.
            print("\tDataset {} already exists.".format(dataset_id))
            return True
        except NotFound:
            print("\tDataset {} is not found.".format(dataset_id))
            return False

    def create_table(self, dataset_id, table_id, schema, project='stomato'):
        client = self.grab_bq_client(my_project=project)
        table_id = project + "." + dataset_id + "." + table_id
        table = bigquery.Table(table_id, schema=schema)
        try:
            table = client.create_table(table)
            print(
                "Created table {}.{}.{}".format(table.project, table.dataset_id, table.table_id)
            )
        except Exception as e:
            print(e)

    def update_overflow_switch_irr_hours_for_date_by_adding(self, gpm, acres, field, logger, overflow_switch_minutes, date):
        # Create Logger object to rename field to DB style
        field_db = dbwriter.remove_unwanted_chars_for_db_dataset(field)
        # Calculate daily hours, flow, and inches using Switch Data
        overflow_switch_hours = overflow_switch_minutes / 60
        flow = round((overflow_switch_minutes * float(gpm)) / float(acres))
        dailyInches = flow / 27154
        project = dbwriter.get_db_project(logger.crop_type)
        # Set up and run Query
        try:
            dml = "UPDATE `" + project + "." + str(field_db) + "." + str(logger.name) + "`" \
                  + " SET daily_switch = daily_switch + " + str(
                overflow_switch_minutes
                ) + ", daily_hours = daily_hours + " + \
                  str(overflow_switch_hours) + ", daily_inches = daily_inches +" \
                  + str(dailyInches) + " WHERE date = '" + str(date) + "'"
            dbwriter.run_dml(dml, project=project)
        except Exception as error_message:
            print(error_message)
            print("Error occured when updating overflow switch data")

    def update_overflow_switch_irr_hours_for_date_by_replacing(self, logger, overflow_switch_minutes, date):
        field = logger.field.name
        gpm = logger.gpm
        acres = logger.irrigation_set_acres
        # Create Logger object to rename field to DB style
        field_db = dbwriter.remove_unwanted_chars_for_db_dataset(field)
        # Calculate daily hours, flow, and inches using Switch Data
        overflow_switch_hours = overflow_switch_minutes / 60
        flow = round((overflow_switch_minutes * float(gpm)) / float(acres))
        dailyInches = flow / 27154
        project = dbwriter.get_db_project(logger.crop_type)
        # Set up and run Query
        try:
            dml = f"UPDATE `{project}.{str(field_db)}.{str(logger.name)}`" \
                  + f" SET daily_switch = {str(overflow_switch_minutes)}"\
                  + f", daily_hours = {str(overflow_switch_hours)}"\
                  + f", daily_inches = {str(dailyInches)} WHERE date = '{str(date)}'"
            dbwriter.run_dml(dml, project=project)
        except Exception as error_message:
            print(error_message)
            print("Error occured when updating overflow switch data")

    def add_up_whole_column(self, column_name: str, logger) -> float:
        column_sum = 0
        field_db = self.remove_unwanted_chars_for_db_dataset(logger.field.name)
        project = self.get_db_project(logger.crop_type)

        dml = "SELECT SUM(" + column_name + ") FROM `" + str(project) + "." + str(field_db) + "." + str(
            logger.name) + "`"
        result = self.run_dml(dml, project=project)
        for r in result:
            column_sum = r[0]
        return column_sum

    def grab_all_table_data(self, dataset_id, table_id, project, order_by='date', order='DESC'):
        dataset_id = self.remove_unwanted_chars_for_db_dataset(dataset_id)
        table_id = self.remove_unwanted_chars_for_db_table(table_id)

        result = None
        dml = f"SELECT * FROM `{project}.{dataset_id}.{table_id}` ORDER BY {order_by} {order}"
        try:
            result = self.run_dml(dml, project=project)
        except Exception as error:
            print(f"Error in DBWriter grab_all_table_data - {project}.{dataset_id}.{table_id}")
            print("Error type: " + str(error))
        return result

    def grab_specific_column_table_data(self, dataset_id, table_id, project, column_name, order_by='date', order='DESC'):
        dataset_id = self.remove_unwanted_chars_for_db_dataset(dataset_id)
        table_id = self.remove_unwanted_chars_for_db_table(table_id)

        result = None
        dml = f"SELECT {column_name} FROM `{project}.{dataset_id}.{table_id}` ORDER BY {order_by} {order}"
        try:
            result = self.run_dml(dml, project=project)
        except Exception as error:
            print(f"Error in DBWriter grab_specific_column_table_data - {project}.{dataset_id}.{table_id} for column {column_name}")
            print("Error type: " + str(error))
        return result


dbwriter = DBWriter()
