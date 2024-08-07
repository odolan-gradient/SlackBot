# OLD VERSION
# from weather import Weather, Unit
# from datetime import datetime
# weather = Weather(unit=Unit.FAHRENHEIT)
#
# # lookup = weather.lookup_by_location('turlock, ca')
# lookup = weather.lookup(2508762)
# condition = lookup.condition()
# print(condition.text())
#
#
# forecasts = lookup.forecast()
# f = forecasts[0:7]
# print (f)
# for forecast in f:
#     date = datetime.strptime(forecast.date(), '%d %b %Y')
#     print("Date: " + str(date.strftime('%a')))
#     print("Forecast: " + forecast.text())
#     print("High: " + forecast.high())
#     print("Low: " + forecast.low())
#     print()



#NEW YAHOO VERSION - NOT WORKING
"""
Weather API Python sample code
Copyright 2019 Oath Inc. Licensed under the terms of the zLib license see https://opensource.org/licenses/Zlib for terms.
$ python --version
Python 2.7.10
"""
# import time, uuid, urllib, urllib.request, urllib.parse
# import hmac, hashlib
# from base64 import b64encode
#
# """
# Basic info
# """
# url = 'https://weather-ydn-yql.media.yahoo.com/forecastrss'
# method = 'GET'
# app_id = 'v61GPN30'
# consumer_key = 'dj0yJmk9NnpzSFhJdEhkcllGJnM9Y29uc3VtZXJzZWNyZXQmc3Y9MCZ4PWEw'
# consumer_secret = 'caee60b19973aa895a8cbf92cd6604d565878287'
# concat = '&'
# query = {'location': 'turlock,ca', 'format': 'json'}
# oauth = {
#     'oauth_consumer_key': consumer_key,
#     'oauth_nonce': uuid.uuid4().hex,
#     'oauth_signature_method': 'HMAC-SHA1',
#     'oauth_timestamp': str(int(time.time())),
#     'oauth_version': '1.0'
# }
#
# """
# Prepare signature string (merge all params and SORT them)
# """
# merged_params = query.copy()
# merged_params.update(oauth)
# sorted_params = [k + '=' + urllib.parse.quote(merged_params[k], safe='') for k in sorted(merged_params.keys())]
# signature_base_str =  method + concat + urllib.parse.quote(url, safe='') + concat + urllib.parse.quote(concat.join(sorted_params), safe='')
#
# """
# Generate signature
# """
# composite_key = urllib.parse.quote(consumer_secret, safe='') + concat
# oauth_signature = b64encode(hmac.new(composite_key, signature_base_str, hashlib.sha1).digest())
#
# """
# Prepare Authorization header
# """
# oauth['oauth_signature'] = oauth_signature
# auth_header = 'OAuth ' + ', '.join(['{}="{}"'.format(k,v) for k,v in oauth.iteritems()])
#
# """
# Send request
# """
# url = url + '?' + urllib.urlencode(query)
# request = urllib.Request(url)
# request.add_header('Authorization', auth_header)
# request.add_header('X-Yahoo-App-Id', app_id)
# response = urllib.urlopen(request).read()
# print(response)




# OPENWEATHER VERSION - NOT WORKING
# Python program to find current
# weather details of any city
# using openweathermap api

# import required modules
# import requests, json
#
# # Enter your API key here
# api_key = "91abc03a7e1748b767c16fccd701c7ec"
#
# # base_url variable to store url
# base_url = "http://api.openweathermap.org/data/2.5/weather?"
#
# # Give city name
# city_name = input("Enter city name : ")
#
# # complete_url variable to store
# # complete url address
# complete_url = base_url + "appid=" + api_key + "&q=" + city_name
#
# # get method of requests module
# # return response object
# response = requests.get(complete_url)
#
# # json method of response object
# # convert json format data into
# # python format data
# x = response.json()
#
# print(x)
#
# # Now x contains list of nested dictionaries
# # Check the value of "cod" key is equal to
# # "404", means city is found otherwise,
# # city is not found
# if x["cod"] != "404":
#
# 	# store the value of "main"
# 	# key in variable y
# 	y = x['main']
#
# 	# store the value corresponding
# 	# to the "temp" key of y
# 	current_temperature = y["temp"]
#
# 	# store the value corresponding
# 	# to the "pressure" key of y
# 	current_pressure = y["pressure"]
#
# 	# store the value corresponding
# 	# to the "humidity" key of y
# 	current_humidiy = y["humidity"]
#
# 	# store the value of "weather"
# 	# key in variable z
# 	z = x["weather"]
#
# 	# store the value corresponding
# 	# to the "description" key at
# 	# the 0th index of z
# 	weather_description = z[0]["description"]
#
# 	# print following values
# 	print(" Temperature (in kelvin unit) = " +
# 					str(current_temperature) +
# 		"\n atmospheric pressure (in hPa unit) = " +
# 					str(current_pressure) +
# 		"\n humidity (in percentage) = " +
# 					str(current_humidiy) +
# 		"\n description = " +
# 					str(weather_description))
#
# else:
# 	print(" City Not Found ")


#
#
# import pyowm
#
# owm = pyowm.OWM('91abc03a7e1748b767c16fccd701c7ec')  # You MUST provide a valid API key
#
# # Have a pro subscription? Then use:
# # owm = pyowm.OWM(API_key='your-API-key', subscription_type='pro')
#
# # Search for current weather in London (Great Britain)
# observation = owm.weather_at_place('London,GB')
# forecast = owm.three_hours_forecast('95380')
# f = forecast.get_forecast()
# for weather in f:
#       print (weather.get_reference_time('iso'),weather.get_status())
#
# w = observation.get_weather()
# print(w)                      # <Weather - reference time=2013-12-18 09:20,
#                               # status=Clouds>
#
# # Weather details
# print(w.get_wind())                  # {'speed': 4.6, 'deg': 330}
# w.get_humidity()              # 87
# w.get_temperature('celsius')  # {'temp_max': 10.5, 'temp': 9.7, 'temp_min': 9.0}
#
# # Search current weather observations in the surroundings of
# # lat=22.57W, lon=43.12S (Rio de Janeiro, BR)
# observation_list = owm.weather_around_coords(-22.57, -43.12)
import json
import pprint
from time import time

import jwt
import requests

with open('AuthKey_R6H438Y9R3.p8', 'r') as key_file:
    key = key_file.read()
current_time = int(time())
team_id = 'NJBH8DAQ4G'
service_id = 'stomato.weather'
expiry_time = current_time + 3600
payload = {
    'iss': team_id,
    'iat': current_time,
    'exp': expiry_time,
    'sub': service_id
}
headers = {
    "kid": 'R6H438Y9R3',
    "id": f"{team_id}.{service_id}"
}
token = jwt.encode(payload, key, algorithm='ES256', headers=headers)


language = "en"
latitude = '38.6544264'
longitude = '-121.7767128'
url = f"https://weatherkit.apple.com/api/v1/weather/{language}/{latitude}/{longitude}"
timezone = "America/New_York"
dataSets: list[str] = ["currentWeather", "forecastDaily"]
print(url)
headers = {
    "Authorization": f"Bearer {token}"
}
params = {
    "timezone": timezone,
    "dataSets": "forecastDaily"
}
response = requests.get(url, headers=headers, params=params)
print(response)
data = json.loads(response.content)
# forecast = data["forecastDaily"]
forecast = data['forecastDaily']['days']
for entry in forecast:
    icon = entry['conditionCode'].lower()
    pprint.pprint(icon)


# DarkSky Weather API
# API KEY
# fa279d8056b586cb53754b18de671431
# Meza Lat Long: 37.054699, -120.809713


# url = 'https://api.darksky.net/forecast/'
# key = 'fa279d8056b586cb53754b18de671431'
# lat = '37.054699'
# lng = '-120.809713'
# builturl = url + key + '/' + lat + ',' + lng
# print(builturl)
#
# r = requests.get(builturl)
# parsed_json = json.loads(r.content)
# pp = pprint.PrettyPrinter()
# # pp.pprint(parsed_json)
# print()
# forecast = parsed_json['daily']['data']
# print (forecast)
# print(len(forecast))
# print()
# for d in parsed_json['daily']['data']:
# 	tm = int(d['time'])
# 	# print(datetime.utcfromtimestamp(tm).strftime('%a -- %Y-%m-%d'))
# 	print(datetime.utcfromtimestamp(tm).strftime('%a'))
# 	print("Summary: " + str(d['summary']))
# 	print("Temp High: " + str(d['temperatureHigh']))
# 	print("Relative Humidity: " + str(d['humidity'] * 100) + '%')
#
#
# 	tempF = d['temperatureHigh']
# 	tempC = (tempF - 32) * 5.0/9.0
# 	saturation_vapor_pressure = 610.7 * 10 ** ((7.5 * tempC) / (237.7 + tempC))
# 	vapor_pressure_deficit = (((1 - d['humidity']) * saturation_vapor_pressure) * 0.001)
# 	# vapor_pressure = (saturation_vapor_pressure * d['humidity']/100) / 100
#
# 	print("VPD: " + str(vapor_pressure_deficit))
#
# 	print()
#
# pp.pprint (parsed_json['daily']['data'])
# print()








# forecasted = parsed_json['daily']
#
# print(forecasted['data'])