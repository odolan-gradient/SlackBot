import DBWriter
import SharedPickle

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

    def run_dml(self, dml, job_config=None, project='stomato'):
        # print('\t\t Running DML...')  # Data Manipulation Language
        # print(f'\t\t {dml}')
        client = self.grab_bq_client(my_project=project)
        dml_statement = dml
        query_job = client.query(dml_statement, job_config)  # API request
        result = query_job.result()  # Waits for statement to finish
        # print('\t\t Done with DML')
        return result
class CwsiProcessor(object):
    def __init__(self):
        pass
def change_logger_soil_type(logger_name: str, field_name: str, grower_name: str, new_soil_type: str):
    """
    Single function to change the soil type for a logger in both the pickle and the db

    :param logger_name:
    :param field_name:
    :param grower_name:
    :param new_soil_type:
    """
    print(f'Changing soil type for logger: {logger_name} to {new_soil_type}')

    growers = SharedPickle.open_pickle()
    dbw = DBWriter()

    # Change soil type in the pickle
    print('-Changing soil type in the pickle')
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            print('\tFound logger...changing')
                            old_soil_type = logger.soil.soil_type
                            logger.soil.set_soil_type(new_soil_type)
                            field_capacity = logger.soil.field_capacity
                            wilting_point = logger.soil.wilting_point
                            crop_type = logger.crop_type
    SharedPickle.write_pickle(growers)
    print('\tDone with pickle')

    # Change soil type parameters in the DB
    print('-Changing soil type in the db')
    field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field_name)
    db_project = dbw.get_db_project(crop_type)
    dml = (f'UPDATE `{db_project}.{field_name_db}.{logger_name}` '
           f'SET field_capacity = {field_capacity}, wilting_point = {wilting_point} '
           f'WHERE TRUE')
    result = dbw.run_dml(dml)
    print(f'\tDone with DB')
    print()
    print(f'Soil type for {logger_name} changed from {old_soil_type} to {new_soil_type}')
    print()