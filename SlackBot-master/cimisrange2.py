import csv
from itertools import zip_longest
from math import sin, cos, sqrt, atan2, radians

import numpy as np
import pandas as pd

from CIMIS import CIMIS

# Load CSV files
# station1_data = pd.read_csv("C:\\Users\\odolan\\PycharmProjects\\Stomato\\124.csv")
# station2_data = pd.read_csv("C:\\Users\\odolan\\PycharmProjects\\Stomato\\126.csv")
stations = ['143', '124']
start_date = '2023-01-01'
end_date = '2024-04-01'
c = CIMIS()
for station in stations:
    dicts = c.getDictForStation(station, start_date, end_date)
    if dicts is not None:
        filename = f'{station}.csv'
        print('- writing data to csv')
        keys = ["dates", "eto"]
        justEtData = {key: dicts[key] for key in keys}

        with open(filename, "w", newline='') as outfile:
            writer = csv.writer(outfile)
            writer.writerow(justEtData.keys())
            writer.writerows(zip_longest(*justEtData.values()))
        print('...Done - file: ' + filename)
station1_data = pd.read_csv(f'{stations[0]}.csv')
station2_data = pd.read_csv(f'{stations[1]}.csv')

# Merge data on date column
merged_data = pd.merge(station1_data, station2_data, on="dates")

# Calculate correlation coefficient
correlation = np.corrcoef(merged_data["eto_x"], merged_data["eto_y"])[0, 1]

print("Correlation coefficient between the two stations:", correlation)


def calculate_distance(lat1, lon1, lat2, lon2):
    # Radius of the Earth in miles
    R = 3958.8  # Earth radius in miles

    # Convert latitude and longitude from degrees to radians
    lat1_rad = radians(lat1)
    lon1_rad = radians(lon1)
    lat2_rad = radians(lat2)
    lon2_rad = radians(lon2)

    # Compute the change in coordinates
    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    # Haversine formula
    a = sin(dlat / 2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))

    # Calculate the distance
    distance = R * c
    return distance

# Coordinates for the two points
lat1 = 36.997444
lon1 = -121.99676
lat2 = 36.890056
lon2 = -120.73141

# Calculate the distance
distance = calculate_distance(lat1, lon1, lat2, lon2)
print("The distance between the two points is approximately:", distance, "miles")
