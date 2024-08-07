import json
import pprint

import requests

api_key_string = 'appKey='
api_key = '2532b920-c439-4be8-8d4c-c98be7c00f3f'
target_string = '&targets='
targets = "95340"
start_date_string = '&startDate='
start_date = '2016-03-12'
end_date_string = '&endDate='
end_date = '2016-03-19'
data_items_string = '&dataItems='
data_items = 'day-eto'
initial_url = 'http://et.water.ca.gov/api/data?'
url = initial_url + api_key_string + api_key + target_string + targets + start_date_string + start_date + end_date_string + end_date + data_items_string + data_items + ';prioritizeSCS=N'
print(url)
request = requests.get(url)
print(request)
jsonResponse = json.loads(request.text)

#req = requests.get('http://et.water.ca.gov/api/data?appKey=2532b920-c439-4be8-8d4c-c98be7c00f3f&targets=2,8,127&startDate=2010-01-01&endDate=2010-01-05')
print(jsonResponse)
print('Eto is: ' + jsonResponse['Data']['Providers'][0]["Records"][0]["DayEto"]["Value"])
#print(request[])
etList = []
i = 0
for eachRecord in jsonResponse['Data']['Providers'][0]:
    etList.append(jsonResponse['Data']['Providers'][0]["Records"][i]["DayEto"]["Value"])
    i = i = 1
pprint.pprint(jsonResponse)
print(etList)