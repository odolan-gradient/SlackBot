from EmailProcessor import EmailProcessor
#Testing new Decaon API for z6 loggers
em = EmailProcessor()
em.auth_login()

# line = 'z6-563782'
# print(line)
# line = re.sub('\-','',line)
# print(line)
# def query_stackoverflow():
#     client = bigquery.Client()
#     query_job = client.query(
#         """
#         SELECT
#           CONCAT(
#             'https://stackoverflow.com/questions/',
#             CAST(id as STRING)) as url,
#           view_count
#         FROM `bigquery-public-data.stackoverflow.posts_questions`
#         WHERE tags like '%google-bigquery%'
#         ORDER BY view_count DESC
#         LIMIT 10"""
#     )
#     results = query_job.result()  # Waits for job to complete.
#     for row in results:
#         print("{} : {} views".format(row.url, row.view_count))
# if __name__ == "__main__":
#     query_stackoverflow()


# config = {
#     'user': 'root',
#     'password': 'a0ssdD#A9nWh',
#     'host': '34.122.132.236',
#     'client_flags': [ClientFlag.SSL],
#     'ssl_ca': 'ssl/server-ca.pem',
#     'ssl_cert': 'ssl/client-cert.pem',
#     'ssl_key': 'ssl/client-key.pem'
# }
#
# # now we establish our connection
# cnxn = mysql.connector.connect(**config)
# #
# cursor = cnxn.cursor()  # initialize connection cursor


# cursor.execute('CREATE DATABASE stomato_test')  # create a new 'testdb' database
# cnxn.close()  # close connection because we will be reconnecting to testdb

# config['database'] = 'stomato_test'  # add new database to config dict
#
# cnxn = mysql.connector.connect(**config)
# cursor = cnxn.cursor()

# cursor.execute(" CREATE TABLE z602012 ("
#                " id VARCHAR(255) NOT NULL,"
#                " date VARCHAR(255),"
#                " air_temp FLOAT(4,2),"
#                " canopy_temp FLOAT(4,2),"
#                " sdd FLOAT(4,2),"
#                " relative_humidity FLOAT(4,2),"
#                " vpd FLOAT(4,2),"
#                " wsi FLOAT(4,2),"
#                " vwc1 FLOAT(4,2),"
#                " vwc2 FLOAT(4,2),"
#                " vwc3 FLOAT(4,2),"
#                " switch_hours FLOAT(4,2),"
#                " kc FLOAT(4,2),"
#                " eto FLOAT(4,2),"
#                " etc FLOAT(4,2),"
#                " et_hours FLOAT(4,2),"
#                " switch_inches FLOAT(4,2),"
#                " gpm FLOAT(4,2),"
#                " acres FLOAT(4,2),"
#                " field_capacity FLOAT(4,2),"
#                " wilting_point FLOAT(4,2),"
#                " soil_type VARCHAR(255) )")
#
# cnxn.commit()  # this commits changes to the database


# Trying using pandas
# data = pd.read_csv("C:\\Users\\javie\\Projects\\S-TOMAto\\Dummy Data\\opc56NE.csv")
# data = data.where((pd.notnull(data)), None)
# print(list(data.to_records(index=False)))
# Running into issues with data type conversion

# Trying using simple lists from csv
# with open("C:\\Users\\javie\\Projects\\S-TOMAto\\Dummy Data\\opc56NEw99.csv", newline='') as f:
#     reader = csv.reader(f)
#     data = [tuple(row) for row in reader]
# print(data)
# for r in data:
#     print(r)
# #
# query = "INSERT INTO z602012 (id, day, air_temp, canopy_temp, sdd, relative_humidity, vpd, wsi, vwc1, " \
#         "vwc2, vwc3, switch_hours, kc, eto, etc, et_hours, switch_inches, gpm, " \
#         "acres, field_capacity, wilting_point, soil_type) " \
#         "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
# for r in data:
#     # print(r)
#     cursor.execute(query, r)
# cursor.executemany(query, data)
#
# cnxn.commit()  # and commit changes

# def update_value_for_date(field, logger, date, value_name, value):
#     dml = 'UPDATE `stomato.' + str(field) + '.' + str(logger) + '`'\
#     + ' SET ' + str(value_name) + ' = ' + str(value)\
#     + " WHERE date = '" + str(date) + "'"
#     print(dml)
#
# dml = 'SELECT date, vpd, vwc_1, vwc_2, vwc_3, ' \
#       'field_capacity, wilting_point, psi, et_hours ' \
#       'FROM `stomato.RKB_RanchField_71516.z6-01864` ' \
#       'WHERE et_hours is not NULL ORDER BY date DESC'
# dbwriter = DBWriter()
# expertSys = IrrigationRecommendationExpert()
# result = dbwriter.run_dml(dml)
#
# applied_finals = {}
# applied_finals['date'] = []
# applied_finals['base'] = []
# applied_finals['final_rec'] = []
# applied_finals['adjustmented_values'] = []
# applied_finals['adjustment_steps'] = []
#
# for r in result:
#     date = r[0]
#     vpd = r[1]
#     vwc_1 = r[2]
#     vwc_2 = r[3]
#     vwc_3 = r[4]
#     # vwc =(vwc_1 + vwc_2)/2
#     field_capacity = r[5]
#     wilting_point = r[6]
#     psi = r[7]
#     et = r[8]
#     rec = expertSys.make_recommendation(vpd, psi, field_capacity, wilting_point, vwc_1)
#     applied_final, applied_steps = expertSys.apply_recommendations(et, rec)
#     applied_finals['date'].append(date)
#     applied_finals['base'].append(et)
#     applied_finals['final_rec'].append(applied_final)
#     applied_finals['adjustmented_values'].append(applied_steps)
#     applied_finals['adjustment_steps'].append(rec.recommendation_info)
#
#     print('Updating adjusted info')
#     dml = 'UPDATE `stomato.RKB_RanchField_71516.z6-01864`' + \
#           'SET ' \
#             'phase1_adjustment = ' + str(rec.recommendation_info[0]) + \
#             ', phase1_adjusted = ' + str(applied_steps[0]) + \
#             ', phase2_adjustment = ' + str(rec.recommendation_info[1]) + \
#             ', phase2_adjusted = ' + str(applied_steps[1]) + \
#             ', phase3_adjustment = ' + str(rec.recommendation_info[2]) + \
#             ', phase3_adjusted = ' + str(applied_steps[2]) + \
#           " WHERE date = '" + str(date) + "'"
#     dbwriter.run_dml(dml)
#     print()
#     # print(dml)
# print('Fully Done')

# dml = 'UPDATE `stomato.RKB_RanchField_71516.z6-01864` \
#       'SET phase1_adjustment = , phase1_adjusted = , phase2_adjustment = , phase2_adjusted = , phase3_adjustment = , phase3_adjusted = ' \
#       'WHERE date = '


#
# update_value_for_date('Bone_Farms_LLCF7', 'z6-03447', '2021-04-07', 'ambient_temperature', 84.65)

# var = 5
# if var < 10:
#     print('Less than 10')
# elif var < 20:
#     print('Less than 20')
# elif var < 50:
#     print('Less than 50')
# message = "Attached is a txt file with possible logger issues  (beta)"
# email = EmailProcessor()
# email.send_email_v2(["jgarrido@morningstarco.com", "javierationmex@gmail.com"], "Field Technician Notifications (beta) -  ", message)

# from google.cloud import bigquery
#
# client = bigquery.Client()
#
# gcs_uri = 'gs://cloud-samples-data/bigquery/us-states/us-states.json'
#
# dataset = client.create_dataset('us_states_dataset')
# table = dataset.table('us_states_table')
#
# job_config = bigquery.job.LoadJobConfig()
# job_config.schema = [
#     bigquery.SchemaField('name', 'STRING'),
#     bigquery.SchemaField('post_abbr', 'STRING'),
# ]
# job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
#
# load_job = client.load_table_from_uri(gcs_uri, table, job_config=job_config)
#
# print('JSON file loaded to BigQuery')





# from google.cloud import bigquery
#
# # Construct a BigQuery client object.
# client = bigquery.Client(project='Stomato')
#
# table_id = "stomato-295321.test_dataset.test_table"
# job_config = bigquery.job.LoadJobConfig()
# job_config.source_format = bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
#
# uri = "C:\\Users\\javie\\Google Drive\\Morning Star R&D\\S-TOMAto\\2020\\Dxd Files\\z6-07275.dxd"
#
# load_job = client.load_table_from_uri(
#     uri,
#     table_id,
#     location="US",  # Must match the destination dataset location.
#     job_config=job_config,
# )  # Make an API request.
#
# load_job.result()  # Waits for the job to complete.
#
# destination_table = client.get_table(table_id)
# print("Loaded {} rows.".format(destination_table.num_rows))




#
# def test(name='', age=''):
#     print(name)
#     print(age)
#
# test(age='25', name='Javier')

# data = pd.read_csv("C:\\Users\\javie\\Projects\\S-TOMAto\\Dummy Data\\space_missions.csv")
# data = data.where((pd.notnull(data)), None)
# # first we setup our query
# query = ("INSERT INTO space_missions (company_name, location, datum, detail, status_rocket, rocket, status_mission) "
#          "VALUES (%s, %s, %s, %s, %s, %s, %s)")
#
# # then we execute with every row in our dataframe
# cursor.executemany(query, list(data.to_records(index=False)))
# cnxn.commit()  # and commit changes

# cursor.execute("SELECT * FROM space_missions LIMIT 5")
# out = cursor.fetchall()
# for row in out:
#     print(row)








# subprocess.run(["cloud_sql_proxy.exe", "-instances=stomato:us-west2:root=tcp:3306", "-credential_file=stomato-09026d7c64db.json"])
#
# print('yes')
# connection = pymysql.connect(host='35.236.122.76',
#                              user='username',
#                              password='username',
#                              db='test_db')
# print(connection)




#
# with open('Logger IDs and Planting Dates.csv', newline='') as csvfile:
#     reader = csv.DictReader(csvfile)
#     for row in reader:
#         print(row)

# with open('Logger IDs and Planting Dates.csv', mode='r') as infile:
#     reader = csv.reader(infile)
#     with open('coors_new.csv', mode='w') as outfile:
#         writer = csv.writer(outfile)
#         mydict = {rows[0]:rows[1] for rows in reader}
#
# print(mydict)
#
# for key, value in mydict.items():
#     if value == '':
#         value = None
#     else:
#         dt = datetime.datetime.strptime(value, '%m/%d/%Y')
#         value = dt.date()
#     print(key, ' -> ', value)