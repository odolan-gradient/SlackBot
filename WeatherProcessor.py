import json
from datetime import datetime, timezone
from time import time

import jwt
import requests
from requests.adapters import HTTPAdapter, Retry


class WeatherProcessor (object):
    """
    Class to help in processing weather requests

    """
    lat = 0
    long = 0
    use_celsius = False

    def __init__(self, lat, long, use_celsius=False):
        self.lat = lat
        self.long = long
        self.use_celsius = use_celsius

    def open_weather_forecast(self):
        """
        Function to get the weather forecast from Open Weather's API

        :return:
            forecast:
        """
        key = "91abc03a7e1748b767c16fccd701c7ec"
        url = "https://api.openweathermap.org/data/2.5/onecall?lat=%s&lon=%s&appid=%s&units=imperial&exclude=minutely,hourly" % (
        self.lat, self.long, key)
        builtUrl = url

        retry_strategy = Retry(
            total=7,
            backoff_factor=2,
            status_forcelist=[429, 443, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        response = http.get(builtUrl)

        if response.ok:
            print(f'\tSuccessful Weather API')

        data = json.loads(response.text)
        forecast = data["daily"]

        #Clean up the API forecast and only grab the values we care about
        converted_forecast = self.converted_forecast_from_openweather_api(forecast)

        return converted_forecast

    def apple_forecast(self) -> list:
        token = self.get_apple_weather_token()
        language = "en"
        latitude = self.lat
        longitude = self.long
        url = f"https://weatherkit.apple.com/api/v1/weather/{language}/{latitude}/{longitude}"
        timezone = "America/New_York"

        headers = {
            "Authorization": f"Bearer {token}"
        }
        params = {
            # "timezone": timezone,
            "dataSets": "forecastDaily"
        }

        retry_strategy = Retry(
            total=7,
            backoff_factor=2,
            status_forcelist=[429, 443, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        response = http.get(url, headers=headers, params=params)

        # response = requests.get(url, headers=headers, params=params)

        if response.ok:
            print(f'\tSuccessful Weather API')
        data = json.loads(response.content)
        forecast = data['forecastDaily']['days']

        converted_forecast = self.converted_forecast_from_apple_weather_kit(forecast)

        return converted_forecast

    def weather2020_forecast(self, start_date, end_date):
        api_key = 'fb9447f1cf7e97d1519022e573b5c5f0'
        language = "en"
        latitude = self.lat
        longitude = self.long
        units = 'imperial'
        url = f"https://api.weather2020.com/forecasts?"
        timezone = "America/New_York"

        headers = {
            "X-API-Key": api_key
        }
        params = {
            "lat": latitude,
            "lon": longitude,
            "units": units,
            "end_date": end_date,
            "start_date": start_date
        }

        retry_strategy = Retry(
            total=7,
            backoff_factor=2,
            status_forcelist=[429, 443, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http = requests.Session()
        http.mount("https://", adapter)
        http.mount("http://", adapter)

        response = http.get(url, headers=headers, params=params)

        # response = requests.get(url, headers=headers, params=params)

        if response.ok:
            print(f'\tSuccessful Weather API')
        data = json.loads(response.content)
        # forecast = data['forecastDaily']['days']
        #
        # converted_forecast = self.converted_forecast_from_apple_weather_kit(forecast)

        return data

    def get_apple_weather_token(self):
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
        return token

    def converted_forecast_from_darksky_api(self, forecast: dict) -> list:
        """
        Grab the API response, clean it up and add our required fields to converted_forecast so that the
        return is always standardized regardless of the API used to grab the data

        :param forecast:
        :return: converted_forecast
        """
        converted_forecast = []
        for entry in forecast:
            time = datetime.utcfromtimestamp(entry['time'])
            max_temp = entry['temperatureHigh']
            humidity = entry['humidity']
            vpd = self.calculate_vpd(humidity, max_temp)
            icon = entry['icon']
            icon_link = self.icons(icon)

            converted_forecast.append({'time': time, 'max_temp': max_temp, 'humidity': humidity, 'vpd': vpd, 'icon': icon_link})
        return converted_forecast

    def converted_forecast_from_openweather_api(self, forecast: dict) -> list:
        """
        Grab the API response, clean it up and add our required fields to converted_forecast so that the
        return is always standardized regardless of the API used to grab the data

        :param forecast:
        :return: converted_forecast
        """
        converted_forecast = []

        for entry in forecast:
            time = datetime.utcfromtimestamp(entry["dt"])
            max_temp = entry["temp"]["max"]
            humidity = entry["humidity"]/100
            vpd = self.calculate_vpd(humidity, max_temp)
            icon = entry["weather"][0]["main"]
            icon_link = self.icons(icon)

            converted_forecast.append({'time': time, 'max_temp': max_temp, 'humidity': humidity, 'vpd': vpd, 'icon': icon_link})
        return converted_forecast

    def converted_forecast_from_apple_weather_kit(self, forecast) -> list:
        converted_forecast = []
        for entry in forecast:

            time_iso = entry['forecastStart'][:-1]
            time = datetime.fromisoformat(time_iso).astimezone(timezone.utc)
            max_temp_celsius = entry['temperatureMax']
            if self.use_celsius:
                max_temp = max_temp_celsius
            else:
                max_temp = self.celsius_to_farenheit(max_temp_celsius)
            humidity = entry['daytimeForecast']['humidity']
            vpd = self.calculate_vpd(humidity, max_temp)
            icon = entry['conditionCode']
            icon_link = self.icons(icon)

            converted_forecast.append(
                {'time': time, 'max_temp': max_temp, 'humidity': humidity, 'vpd': vpd, 'icon': icon_link}
                )
        return converted_forecast

    def celsius_to_farenheit(self, temperature: float) -> float:
        return temperature * (9/5) + 32

    def calculate_vpd(self, humidity: float, max_temp: float):
        if self.use_celsius:
            tempC = max_temp
        else:
            tempC = (max_temp - 32) * 5.0 / 9.0
        saturation_vapor_pressure = 610.7 * 10 ** ((7.5 * tempC) / (237.7 + tempC))
        vpd = (((1 - humidity) * saturation_vapor_pressure) * 0.001)
        vpd = round(vpd, 1)
        return vpd

    def time_machine_forecast(self, latitude, longitude, start_date, end_date):
        """
        Function to get the weather forecast from a specific time.
        This function still relies on darksky's API which will be deprecated in 2023

        :param days: Int to dictate how many days of forecast we want

        :return:
            forecast:
        """

        api_key = 'HWXBYQUGGBYZW5TV3JUJY7Z7W'
        base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/"
        date_range = f"{start_date.strftime('%Y-%m-%d')}/{end_date.strftime('%Y-%m-%d')}"
        location = f"{latitude},{longitude}"

        params = {
            'key': api_key,
            'include': 'obs',
            'unitGroup': 'us',
        }

        response = requests.get(f"{base_url}{location}/{date_range}", params=params)
        data = response.json()

        if response.status_code == 200:
            weather_data = data
        else:
            print(f"Error: {data['message']}")

        return weather_data

    def icons(self, forecast_text: str) -> str:
        # Icon text was used based on the following documentations:
        # https://developer.apple.com/documentation/weatherkit/weathercondition
        # https://gist.github.com/mikesprague/048a93b832e2862050356ca233ef4dc1

        #Actual URL:
        # https://icons.wxug.com/i/c/k/partlycloudy.gif


        # # Hosting on GDrive
        # defUrl = 'https://drive.google.com/uc?id='
        # sun = defUrl + '1-oRgXFDnEbxRba6Yg1AvCvOHQwigAq4s'
        # sunCloud = defUrl + '1o-6627qagko-lP2Ew9v-E5DmRnVcVq8O'
        # cloud = defUrl + '1AAgq9vPEEqF10ypETzaXBH7ikHixLKlt'
        # cloudRain = defUrl + '12FtQFhUHcLgCwPnHjpNGwqk-xpV-tdn7'
        # cloudThunder = defUrl + '1DW92lTnHEl9NqvJAa3f6ZAYZ8TVOe0u-'
        # fog = defUrl + '1H3VlsbCyQVvYRK_s7Z0AdV78hqZKE-2a'
        # moon = defUrl + '1EGCNhZve2BEC4kPuA-8v4_aaR5JNbLVq'
        # snow = defUrl + '1xhaN9jOWN0vPzzAA9IZNpJxMpDVYQ5zP'

        # Hosted on imgur.com
        defUrl = 'https://i.imgur.com/'
        sun = defUrl + '8enBPX2.gif'
        sun_cloud = defUrl + 'q9T5REp.gif'
        cloud = defUrl + 'CS7hqtE.gif'
        cloud_rain = defUrl + 'xPvUxi9.gif'
        cloud_thunder = defUrl + 'MftuoKp.gif'
        fog = defUrl + 'Auj4HFo.gif'
        moon = defUrl + '9l3gYmC.gif'
        snow = defUrl + 'f9p0QYu.gif'
        flurries = defUrl + 'Ek7ri9T.gif'
        forecast_text = forecast_text.lower()

        # print('Forecast text: ', forecastText)

        return {
            'blizzard': snow,
            'blowingdust': fog,
            'blowingsnow': snow,
            'breezy': cloud,
            'clear': sun,
            'clear-day': sun,
            'clear-night': sun,
            'clouds': cloud,
            'cloudy': cloud,
            'drizzle': cloud_rain,
            'flurries': flurries,
            'fog': fog,
            'foggy': fog,
            'freezingdrizzle': flurries,
            'freezingrain': flurries,
            'frigid': flurries,
            'hail': flurries,
            'haze': fog,
            'heavyrain': cloud_rain,
            'heavysnow': snow,
            'hot': sun,
            'hurricane': fog,
            'isolatedthunderstorms': cloud_thunder,
            'mostlyclear': sun,
            'mostlycloudy': cloud,
            'partlycloudy': sun_cloud,
            'partly-cloudy-day': sun_cloud,
            'partly-cloudy-night': sun_cloud,
            'rain': cloud_rain,
            'scatteredthunderstorms': cloud_thunder,
            'sleet': cloud_rain,
            'smoky': fog,
            'snow': snow,
            'strongstorms': cloud_thunder,
            'sunflurries': flurries,
            'sunshowers': cloud_rain,
            'thunderstorm': cloud_thunder,
            'thunderstorms': cloud_thunder,
            'tropicalstorm': cloud_thunder,
            'windy': fog,
            'wind': fog,
            'wintrymix': flurries,
        }.get(forecast_text, moon)

        # OLD ICONS
        # defUrl = 'https://openweathermap.org/img/w/'
        # sun = defUrl + '01d.png'
        # sunCloud = defUrl + '02d.png'
        # cloud = defUrl + '03d.png'
        # clouds = defUrl + '04d.png'
        # cloudRain = defUrl + '09d.png'
        # cloudRainSun = defUrl + '10d.png'
        # cloudThunder = defUrl + '11d.png'
        # fog = defUrl + '50.png'
        # moon = defUrl + '01n.png'
        #
        # return {
        #     'Sunny': sun,
        #     'Hot': sun,
        #     'Mostly Sunny': sun,
        #     'Partly Cloudy': sunCloud,
        #     'Fair': sunCloud,
        #     'Cloudy': cloud,
        #     'Mostly Cloudy': clouds,
        #     'Showers': cloudRain,
        #     'Scattered Showers': cloudRainSun,
        #     'Isolated Thunderstorms': cloudThunder,
        #     'Scattered Thunderstorms': cloudThunder,
        #     'Thundershowers': cloudThunder,
        #     'Foggy': fog,
        #     'Haze': fog,
        #     'Smoky': fog,
        #     'Windy': fog,
        #     'Cold': fog,
        #     'Rain': cloudRain
        # }.get(forecastText, moon)
