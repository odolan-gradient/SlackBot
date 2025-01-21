import pandas as pd
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch
import requests
from shapely.wkt import loads
import matplotlib.pyplot as plt
from shapely.geometry import Polygon

from shapely.ops import orient
from pykml import parser


import geopandas as gpd
import plotly.express as px
import plotly.io as pio
from PIL import Image
import io

class AllSoils(object):
    def __init__(self):
        sand = Soil(soil_type='Sand', field_capacity=10, wilting_point=5)
        loamy_sand = Soil(soil_type='Loamy Sand', field_capacity=12, wilting_point=5)
        sandy_loam = Soil(soil_type='Sandy Loam', field_capacity=18, wilting_point=8)
        sandy_clay_loam = Soil(soil_type='Sandy Clay Loam', field_capacity=27, wilting_point=17)
        loam = Soil(soil_type='Loam', field_capacity=28, wilting_point=14)
        sandy_clay = Soil(soil_type='Sandy Clay', field_capacity=36, wilting_point=25)
        silt_loam = Soil(soil_type='Silt Loam', field_capacity=31, wilting_point=11)
        silt = Soil(soil_type='Silt', field_capacity=30, wilting_point=6)
        clay_loam = Soil(soil_type='Clay Loam', field_capacity=36, wilting_point=22)
        silty_clay_loam = Soil(soil_type='Silty Clay Loam', field_capacity=38, wilting_point=22)
        silty_clay = Soil(soil_type='Silty Clay', field_capacity=41, wilting_point=27)
        clay = Soil(soil_type='Clay', field_capacity=42, wilting_point=30)

        self.soils = [sand, loamy_sand, sandy_loam, sandy_clay_loam, loam, sandy_clay, silt_loam, silt, clay_loam,
                      silty_clay_loam, silty_clay, clay]

    def graph(self):
        # Define custom color ranges in RGB
        red = (211 / 255, 47 / 255, 47 / 255)
        orange = (255 / 255, 143 / 255, 0 / 255)
        yellow = (255 / 255, 204 / 255, 0 / 255)
        green = (56 / 255, 142 / 255, 60 / 255)

        # Assign colors to the range category
        very_low = red
        low = orange
        below_optimum = yellow
        optimum = green
        high = orange
        very_high = red

        cmap = ListedColormap([very_low, low, below_optimum, optimum, high, very_low])

        soil_types = [soil.soil_type for soil in self.soils]

        fig, ax = plt.subplots(figsize=(10, 6))  # Increase the width of the figure

        for soil in self.soils:
            # Calculate the heights of the categories based on bounds
            heights = [soil.bounds[i + 1] - soil.bounds[i] for i in range(0, len(soil.bounds) - 1, 2)]

            # Plot the stacked bar for each category (excluding the "Highest" range)
            bottom = 0
            bar_width = 0.5
            x_pos = soil_types.index(soil.soil_type) * (bar_width * 2)
            for i, height in enumerate(heights):
                ax.bar(x_pos, height, width=bar_width, bottom=bottom, color=cmap.colors[i])
                bottom += height

            # Add field_capacity and wilting_point as horizontal lines
            ax.hlines(
                soil.field_capacity, x_pos - bar_width / 2, x_pos + bar_width / 2, colors='black', linestyles='dashed',
                label='Field Capacity'
            )
            ax.hlines(
                soil.wilting_point, x_pos - bar_width / 2, x_pos + bar_width / 2, colors='black', linestyles='dotted',
                label='Wilting Point'
            )
            ax.hlines(
                soil.very_high_lower, x_pos - bar_width / 2, x_pos + bar_width / 2, colors='black', linestyles='solid',
                label='Pre Saturation'
            )

            # Add numerical values for field_capacity and wilting_point lines
            ax.text(
                x_pos + bar_width / 2 + 0.05, soil.field_capacity, f'{soil.field_capacity}', va='center', fontsize=9,
                fontdict={'weight': 'bold'}
            )
            ax.text(
                x_pos + bar_width / 2 + 0.05, soil.wilting_point, f'{soil.wilting_point}', va='center', fontsize=9,
                fontdict={'weight': 'bold'}
            )
            ax.text(
                x_pos + bar_width / 2 + 0.05, soil.very_high_lower, f'{soil.very_high_lower}', va='center', fontsize=8)

        # Customize the plot
        ax.set_xticks([i * bar_width * 2 for i in range(len(soil_types))])
        ax.set_xticklabels(soil_types, rotation=90)
        ax.set_ylabel('Volumetric Water Content (0% - 50%)')
        ax.set_title('Volumetric Water Content Ranges by Soil Type')
        plt.tight_layout()

        # Add colorbar
        sm = plt.cm.ScalarMappable(cmap=cmap, norm=plt.Normalize(0, 100))
        sm.set_array([])
        ticks = [(i + 0.5) * 100 / len(cmap.colors) for i in range(len(cmap.colors))]
        boundaries = [i * 100 / len(cmap.colors) for i in range(len(cmap.colors) + 1)]
        cbar = plt.colorbar(sm, ticks=ticks, boundaries=boundaries, ax=ax)
        cbar.ax.set_yticklabels(['Very Low', 'Low', 'Below Optimum', 'Optimum', 'High', 'Very High'])

        # Create a legend for field_capacity, wilting_point, and high_lower
        legend_elements_lines = [
            Patch(facecolor='none', edgecolor='black', linestyle='solid', label='Pre Saturation'),
            Patch(facecolor='none', edgecolor='black', linestyle='dashed', label='Field Capacity'),
            Patch(facecolor='none', edgecolor='black', linestyle='dotted', label='Wilting Point')
        ]

        # Add the legend for the lines outside the plot area
        leg_lines = ax.legend(handles=legend_elements_lines, bbox_to_anchor=(0, 1.12), loc='center left', fontsize=10)
        ax.add_artist(leg_lines)

        plt.subplots_adjust(top=0.85, bottom=0.25)

        # Add custom legend at the bottom
        legend_elements = [
            Patch(facecolor=cmap(0), label="VERY HIGH = 'Very High Soil Moisture'"),
            Patch(facecolor=cmap(0.8), label="HIGH = 'High Soil Moisture'"),
            Patch(facecolor=cmap(0.6), label="OPTIMUM = 'Optimum Moisture'"),
            Patch(facecolor=cmap(0.4), label="BELOW OPTIMUM = 'Below Optimum'"),
            Patch(facecolor=cmap(0.2), label="LOW = 'Low Moisture Levels'"),
            Patch(facecolor=cmap(0), label="VERY LOW = 'Very Low Moisture'")
        ]

        # Add the custom legend below the plot
        leg = ax.legend(handles=legend_elements, bbox_to_anchor=(1.07, -0.27), loc='center', ncol=1, fontsize=8)
        ax.add_artist(leg)

        # Show the plot
        plt.show()


class Soil(object):
    """
    Class to hold an individual Soil. This class has variables for soil type, field capacity, wilting point, as well
    as the lower and upper bounds of each of its thresholds: lowest, low, optimum, high
    """

    MINIMUM = 0
    SATURATION = 55

    VERY_LOW = 'Very Low Moisture'
    LOW = 'Low Moisture Levels'
    BELOW_OPT = 'Below Optimum'
    OPTIMUM = 'Optimum Moisture'
    HIGH = 'High Soil Moisture'
    VERY_HIGH = 'Very High Soil Moisture'
    INCORRECT = 'Incorrect Value'

    SOIL_TYPES = ['Sand', 'Loamy Sand', 'Sandy Loam', 'Sandy Clay Loam', 'Loam', 'Sandy Clay', 'Silt Loam', 'Silt',
                  'Clay Loam', 'Silty Clay Loam', 'Silty Clay', 'Clay']

    def __init__(self, soil_type: str = '', field_capacity: float = -1, wilting_point: float = -1):
        # If we aren't passed in a soil type, go look it up based on field_capacity and wilting_point
        if not soil_type:
            self.soil_type = self.soil_type_lookup(field_capacity, wilting_point)
        else:
            if soil_type in self.SOIL_TYPES:
                self.soil_type = soil_type

        # If we aren't passed in a field capacity, go look it up based on soil_type
        if field_capacity < 0:
            self.field_capacity = self.field_capacity_lookup(soil_type)
        else:
            self.field_capacity = field_capacity

        # If we aren't passed in a wilting point, go look it up based on soil_type
        if wilting_point < 0:
            self.wilting_point = self.wilting_point_lookup(soil_type)
        else:
            self.wilting_point = wilting_point
        hello = self.set_bounds()

    def set_bounds(self) -> None:
        """
        Sets the soil type bounds for Very High, High, Optimum, Below Optimum, Low and Very Low based on
        the soil type of the field capacity/wilting point

        """
        # Grab the lower and upper bounds of each of its thresholds depending on soil type
        self.very_low_lower, self.very_low_upper = self.get_very_low()
        self.low_lower, self.low_upper = self.get_lows()
        self.below_optimum_lower, self.below_optimum_upper = self.get_below_optimum()
        self.optimum_lower, self.optimum_upper = self.get_optimums()
        self.high_lower, self.high_upper = self.get_highs()
        self.very_high_lower, self.very_high_upper = self.get_very_highs()
        # Get a list of the boundary values for each range
        self.bounds = [
            self.very_low_lower, self.very_low_upper,
            self.low_lower, self.low_upper,
            self.below_optimum_lower, self.below_optimum_upper,
            self.optimum_lower, self.optimum_upper,
            self.high_lower, self.high_upper,
            self.very_high_lower, self.very_high_upper
        ]

    def __repr__(self):
        return f'Soil Type: {self.soil_type}, Field Capacity: {self.field_capacity}, Wilting Point: {self.wilting_point}'

    def set_soil_type(self, soil_type: str):
        """
        Setter for soil_type that finds and also changes the field_capacity and wilting_point accordingly

        :param soil_type:
        """
        self.soil_type = soil_type
        self.field_capacity = self.field_capacity_lookup(soil_type)
        self.wilting_point = self.wilting_point_lookup(soil_type)
        self.set_bounds()

    def set_field_capacity_wilting_point(self, field_capacity: float, wilting_point: float):
        """
        Setter for field_capacity and wilting_point that also finds and sets soil_type up correctly

        :param field_capacity:
        :param wilting_point:
        """
        self.soil_type = self.soil_type_lookup(field_capacity, wilting_point)
        self.field_capacity = field_capacity
        self.wilting_point = wilting_point
        self.set_bounds()

    def soil_type_lookup(self, field_capacity: float, wilting_point: float) -> str:
        """
        Lookup soil type based on field capacity and wilting point

        :param field_capacity: Float of the field capacity
        :param wilting_point: Float of the wilting point
        :return: String of the soil type
        """

        if field_capacity == 10 and wilting_point == 5:
            return 'Sand'
        elif field_capacity == 12 and wilting_point == 5:
            return 'Loamy Sand'
        elif field_capacity == 18 and wilting_point == 8:
            return 'Sandy Loam'
        elif field_capacity == 27 and wilting_point == 17:
            return 'Sandy Clay Loam'
        elif field_capacity == 28 and wilting_point == 14:
            return 'Loam'
        elif field_capacity == 36 and wilting_point == 25:
            return 'Sandy Clay'
        elif field_capacity == 36 and wilting_point == 22:
            return 'Clay Loam'
        elif field_capacity == 31 and wilting_point == 11:
            return 'Silt Loam'
        elif field_capacity == 30 and wilting_point == 6:
            return 'Silt'
        elif field_capacity == 38 and wilting_point == 22:
            return 'Silty Clay Loam'
        elif field_capacity == 41 and wilting_point == 27:
            return 'Silty Clay'
        elif field_capacity == 42 and wilting_point == 30:
            return 'Clay'
        else:
            closest = self.find_closest_soil_type(field_capacity, wilting_point)
            return closest

    def get_very_low(self) -> tuple[int, int]:
        """
        Get the lowest range limits, lower and upper

        :return: Tuple (lower_limit, upper_limit)
        """
        return {
            'Sand': (self.MINIMUM, 6),
            'Loamy Sand': (self.MINIMUM, 6),
            'Sandy Loam': (self.MINIMUM, 10),
            'Sandy Clay Loam': (self.MINIMUM, 19),
            'Loam': (self.MINIMUM, 16),
            'Sandy Clay': (self.MINIMUM, 28),
            'Silt Loam': (self.MINIMUM, 15),
            'Silt': (self.MINIMUM, 9),
            'Clay Loam': (self.MINIMUM, 25),
            'Silty Clay Loam': (self.MINIMUM, 25),
            'Silty Clay': (self.MINIMUM, 30),
            'Clay': (self.MINIMUM, 32)
        }.get(self.soil_type, (999, 999))

    def get_lows(self) -> tuple[int, int]:
        """
        Get the low range limits, lower and upper

        :return: Tuple (lower_limit, upper_limit)
        """
        return {
            'Sand': (6, 7),
            'Loamy Sand': (6, 8),
            'Sandy Loam': (10, 13),
            'Sandy Clay Loam': (19, 22),
            'Loam': (16, 21),
            'Sandy Clay': (28, 31),
            'Silt Loam': (15, 22),
            'Silt': (9, 19),
            'Clay Loam': (25, 29),
            'Silty Clay Loam': (25, 30),
            'Silty Clay': (30, 34),
            'Clay': (32, 36)
        }.get(self.soil_type, (999, 999))

    def get_below_optimum(self) -> tuple[int, int]:
        """
        Get the low range limits, lower and upper

        :return: Tuple (lower_limit, upper_limit)
        """
        return {
            'Sand': (7, 8),
            'Loamy Sand': (8, 9),
            'Sandy Loam': (13, 16),
            'Sandy Clay Loam': (22, 24),
            'Loam': (21, 25),
            'Sandy Clay': (31, 33),
            'Silt Loam': (22, 28),
            'Silt': (19, 27),
            'Clay Loam': (29, 32),
            'Silty Clay Loam': (30, 34),
            'Silty Clay': (34, 38),
            'Clay': (36, 39)
        }.get(self.soil_type, (999, 999))

    def get_optimums(self) -> tuple[int, int]:
        """
        Get the optimum range limits, lower and upper

        :return: Tuple (lower_limit, upper_limit)
        """
        return {
            'Sand': (8, 26),
            'Loamy Sand': (9, 26),
            'Sandy Loam': (16, 31),
            'Sandy Clay Loam': (24, 36),
            'Loam': (25, 36),
            'Sandy Clay': (33, 41),
            'Silt Loam': (28, 39),
            'Silt': (27, 37),
            'Clay Loam': (32, 41),
            'Silty Clay Loam': (34, 43),
            'Silty Clay': (38, 45),
            'Clay': (39, 45)
        }.get(self.soil_type, (999, 999))

    def get_highs(self) -> tuple[int, int]:
        """
        Get the high range limits, lower and upper

        :return: Tuple (lower_limit, upper_limit)
        """
        return {
            'Sand': (26, 40),
            'Loamy Sand': (26, 40),
            'Sandy Loam': (31, 40),
            'Sandy Clay Loam': (36, 40),
            'Loam': (36, 40),
            'Sandy Clay': (41, 41),
            'Silt Loam': (39, 40),
            'Silt': (37, 40),
            'Clay Loam': (41, 41),
            'Silty Clay Loam': (43, 43),
            'Silty Clay': (45, 45),
            'Clay': (45, 45)
        }.get(self.soil_type, (999, 999))

    def get_very_highs(self) -> tuple[int, int]:
        """
        Get the high range limits, lower and upper

        :return: Tuple (lower_limit, upper_limit)
        """
        return {
            'Sand': (40, self.SATURATION),
            'Loamy Sand': (40, self.SATURATION),
            'Sandy Loam': (40, self.SATURATION),
            'Sandy Clay Loam': (40, self.SATURATION),
            'Loam': (40, self.SATURATION),
            'Sandy Clay': (41, self.SATURATION),
            'Silt Loam': (40, self.SATURATION),
            'Silt': (40, self.SATURATION),
            'Clay Loam': (41, self.SATURATION),
            'Silty Clay Loam': (43, self.SATURATION),
            'Silty Clay': (45, self.SATURATION),
            'Clay': (45, self.SATURATION)
        }.get(self.soil_type, (999, 999))

    @staticmethod
    def field_capacity_lookup(soil_type: str) -> int:
        """

        :param soil_type:
        :return:
        """
        return {
            'Sand': 10,
            'Loamy Sand': 12,
            'Sandy Loam': 18,
            'Sandy Clay Loam': 27,
            'Loam': 28,
            'Sandy Clay': 36,
            'Silt Loam': 31,
            'Silt': 30,
            'Clay Loam': 36,
            'Silty Clay Loam': 38,
            'Silty Clay': 41,
            'Clay': 42
        }.get(soil_type, 999)

    @staticmethod
    def wilting_point_lookup(soil_type: str) -> int:
        """

        :param soil_type:
        :return:
        """
        return {
            'Sand': 5,
            'Loamy Sand': 5,
            'Sandy Loam': 8,
            'Sandy Clay Loam': 17,
            'Loam': 14,
            'Sandy Clay': 25,
            'Silt Loam': 11,
            'Silt': 6,
            'Clay Loam': 22,
            'Silty Clay Loam': 22,
            'Silty Clay': 27,
            'Clay': 30
        }.get(soil_type, 999)

    @staticmethod
    def find_closest_soil_type(field_capacity: float, wilting_point: float) -> str:
        """
        Method that finds the closest soil type based on field capacity and wilting point.

        :param field_capacity: Int of the field capacity
        :param wilting_point: Int of the wilting point
        :return: closest: String of the closes soil type
        """

        # Dictionary of the Soil Types with the sum of the difference between the passed in field_capacity
        # and the soil type field capacity and the difference between the passed in wilting_point and the soil type
        # wilting point as the values.
        closest_soil_type = {'Sand': ((abs(field_capacity - 10)) + (abs(wilting_point - 5))),
                             'Loamy Sand': ((abs(field_capacity - 12)) + (abs(wilting_point - 5))),
                             'Sandy Loam': ((abs(field_capacity - 18)) + (abs(wilting_point - 8))),
                             'Sandy Clay Loam': ((abs(field_capacity - 27)) + (abs(wilting_point - 17))),
                             'Loam': ((abs(field_capacity - 28)) + (abs(wilting_point - 14))),
                             'Sandy Clay': ((abs(field_capacity - 36)) + (abs(wilting_point - 25))),
                             'Silt Loam': ((abs(field_capacity - 31)) + (abs(wilting_point - 11))),
                             'Silt': ((abs(field_capacity - 30)) + (abs(wilting_point - 6))),
                             'Clay Loam': ((abs(field_capacity - 36)) + (abs(wilting_point - 22))),
                             'Silty Clay Loam': ((abs(field_capacity - 38)) + (abs(wilting_point - 22))),
                             'Silty Clay': ((abs(field_capacity - 41)) + (abs(wilting_point - 27))),
                             'Clay': ((abs(field_capacity - 42)) + (abs(wilting_point - 30)))}
        closest = min(closest_soil_type, key=closest_soil_type.get)
        return closest

    def find_vwc_range_description(self, vwc: float) -> str:
        """
        Method to return the appropriate vwc description based on ranges for the soil type

        :param vwc: Float of the volumetric water content to check the range for
        :return: String of the range description
        """
        if vwc is None:
            return self.INCORRECT

        if self.very_low_lower <= vwc < self.very_low_upper:
            return self.VERY_LOW
        elif self.low_lower <= vwc < self.low_upper:
            return self.LOW
        elif self.below_optimum_lower <= vwc < self.below_optimum_upper:
            return self.BELOW_OPT
        elif self.optimum_lower <= vwc < self.optimum_upper:
            return self.OPTIMUM
        elif self.high_lower <= vwc < self.high_upper:
            return self.HIGH
        elif self.very_high_lower <= vwc < self.very_high_upper:
            return self.VERY_HIGH
        else:
            return self.INCORRECT

    def find_vwc_range_range(self, vwc: float) -> str:
        """
        Method to return the appropriate vwc description based on ranges for the soil type

        :param vwc: Float of the volumetric water content to check the range for
        :return: String of the range description
        """
        if vwc is None:
            return self.INCORRECT

        if self.very_low_lower <= vwc < self.very_low_upper:
            return 'very low'
        elif self.low_lower <= vwc < self.low_upper:
            return 'low'
        elif self.below_optimum_lower <= vwc < self.below_optimum_upper:
            return 'below optimum'
        elif self.optimum_lower <= vwc < self.optimum_upper:
            return 'optimum'
        elif self.high_lower <= vwc < self.high_upper:
            return 'high'
        elif self.very_high_lower <= vwc < self.very_high_upper:
            return 'very high'
        else:
            return 'incorrect'

def get_soil_type_from_coords(latitude, longitude):
    """
    Grabs soil type from ADA API given lat, long
    :param latitude:
    :param longitude:
    :return:
    """
    print('Getting soil type')

    point_wkt = f"POINT({longitude} {latitude})"
    # SQL query to get soil texture information
    query = f"""
    SELECT mu.muname, c.localphase
    FROM mapunit AS mu
    JOIN component AS c ON c.mukey = mu.mukey
    JOIN chorizon AS ch ON ch.cokey = c.cokey
    WHERE mu.mukey IN (
            SELECT DISTINCT mukey
            FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('{point_wkt}')
        )
    """

    # SDA request payload
    request_payload = {
        "format": "JSON+COLUMNNAME+METADATA",
        "query": query
    }

    sda_url = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(sda_url, json=request_payload, headers=headers)

    # soil types intentionally formatted, longest to shortest length
    # so when we check if soil_type in response_string
    soil_types = ['Silty Clay Loam', 'Sandy Clay Loam',
                  'Sandy Clay', 'Silt Loam', 'Clay Loam',
                  'Silty Clay', 'Loamy Sand', 'Sandy Loam',
                  'Sand', 'Clay', 'Loam', 'Silt']

    if response.status_code == 200:
        data = response.json()
        if "Table" in data:
            # the row in the json containing soil texture information
            if data["Table"][2]:
                texture_line = data["Table"][2][0]
                lowercase_second_texture = None
                if data["Table"][2][1]:
                    second_texture_descrip = data["Table"][2][1] #local phase is a backuup soil description
                    lowercase_second_texture = second_texture_descrip.lower()

                lowercase_texture = texture_line.lower()

                matched_soil_type = None

                # Iterate through the list of soil types and check for a match
                for soil_type in soil_types:
                    if soil_type.lower() in lowercase_texture:
                        matched_soil_type = soil_type
                        print(f'Found soil type: {lowercase_texture}')
                        break
                    elif lowercase_second_texture:  # if localphase exists
                        if soil_type.lower() in lowercase_second_texture:
                            matched_soil_type = soil_type
                            print(f'Found soil type: {lowercase_second_texture}')
                            break

                return matched_soil_type
        else:
            print("No soil information found for the given coordinates.")
    else:
        print(f"Error: {response.status_code}, {response.text}")

def get_soil_types_from_area(polygon_coords):
    """
    Retrieves soil types within a polygon defined by a list of latitude/longitude points.
    :param polygon_coords: List of (latitude, longitude) tuples defining the polygon.
    :return: List of soil types in the area.
    """
    # Construct a POLYGON WKT from the coordinates
    polygon_wkt = f"POLYGON(({', '.join(f'{lon} {lat}' for lat, lon in polygon_coords)}, {polygon_coords[0][1]} {polygon_coords[0][0]}))"

    query = f"""
    SELECT mu.muname, mp.mupolygongeo, c.localphase
    FROM mapunit AS mu
    JOIN component AS c ON c.mukey = mu.mukey
    JOIN mupolygon AS mp ON mu.mukey = mp.mukey
    WHERE mu.mukey IN (
        SELECT DISTINCT mukey
        FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('{polygon_wkt}')
    );
    """

    # SDA request payload
    request_payload = {
        "format": "JSON+COLUMNNAME+METADATA",
        "query": query
    }

    sda_url = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
    headers = {'Content-Type': 'application/json'}

    # Execute the POST request to SDA
    response = requests.post(sda_url, json=request_payload, headers=headers)
    if response.status_code == 200:
        data = response.json()
        processed_data = process_mupolygon_data(data['Table'])
        gdf = graph_data(processed_data, polygon_coords)
        gdf_plotted = plot_soil_on_mapbox(gdf, polygon_coords)
        return gdf_plotted

def get_soil_type(muname, localphase):

    soil_types = ['Silty Clay Loam', 'Sandy Clay Loam',
                  'Sandy Clay', 'Silt Loam', 'Clay Loam',
                  'Silty Clay', 'Loamy Sand', 'Sandy Loam',
                  'Sand', 'Clay', 'Loam', 'Silt']
    texture_line = muname
    second_texture_descrip = localphase
    lowercase_texture = texture_line.lower()

    lowercase_second_texture = None
    if second_texture_descrip:  # if localphase
        lowercase_second_texture = second_texture_descrip.lower()

    matched_soil_type = None

    # Iterate through the list of soil types and check for a match
    for soil_type in soil_types:
        if soil_type.lower() in lowercase_texture:
            matched_soil_type = soil_type
            print(f'Found soil type: {lowercase_texture}')
            break
        elif lowercase_second_texture:  # if localphase exists
            if soil_type.lower() in lowercase_second_texture:
                matched_soil_type = soil_type
                print(f'Found soil type: {lowercase_second_texture}')
                break
    return matched_soil_type

def process_mupolygon_data(data):
    """
    Processes the soil polygon data to extract meaningful structures for analysis.
    """
    rows = data[2:]  # Skipping metadata row at index 1


    processed_data = []
    for row in rows:

        muname = row[0]
        geometry_wkt = row[1]
        localphase = row[2]
        soil_type = get_soil_type(muname, localphase)

        try:
            geometry = loads(geometry_wkt)  # Convert WKT to a Shapely geometry
        except Exception as e:
            print(f"Error processing WKT for {soil_type}: {e}")
            geometry = None

        processed_data.append({
            "muname": muname,
            "geometry": geometry
        })
    return processed_data


def graph_data(data, polygon_coords):
    # Convert data to GeoDataFrame
    gdf = gpd.GeoDataFrame(data, crs="EPSG:4326")

    # Define your area of interest as a Polygon
    defined_area = Polygon([(lon, lat) for lat, lon in polygon_coords])
    defined_area_gdf = gpd.GeoDataFrame(geometry=[defined_area], crs="EPSG:4326")

    # Clip soil polygons to the defined area
    gdf_clipped = gpd.clip(gdf, defined_area_gdf)

    # Assign unique colors to each 'muname'
    unique_soil_types = gdf_clipped["muname"].unique()
    cmap = plt.get_cmap("tab10")
    colors = {soil: cmap(i / len(unique_soil_types)) for i, soil in enumerate(unique_soil_types)}
    gdf_clipped["color"] = gdf_clipped["muname"].map(colors)

    # Plot the data
    fig, ax = plt.subplots(figsize=(10, 10))
    gdf_clipped.plot(ax=ax, color=gdf_clipped["color"], edgecolor="black")
    defined_area_gdf.boundary.plot(ax=ax, color="black", linewidth=1, label="Defined Area")

    # Add a custom legend to the right
    handles = [plt.Line2D([0], [0], color=colors[soil], lw=4, label=soil) for soil in unique_soil_types]
    ax.legend(
        handles=handles,
        title="Soil Types",
        loc="center left",
        bbox_to_anchor=(1, 0.8),
    )

    # Add labels and titles
    plt.title('Soil Map of Defined Polygon')
    plt.xlabel("Longitude")
    plt.ylabel("Latitude")

    # Remove x and y ticks
    ax.set_xticks([])
    ax.set_yticks([])

    # plt.show()
    return gdf_clipped


def plot_soil_on_mapbox(gdf, polygon_coords):
    """
    Plots soil polygons on a Mapbox map using Plotly, ensuring compatibility and visibility.

    Parameters:
    - gdf (GeoDataFrame): GeoDataFrame containing soil polygons with 'muname' and 'geometry'.
    - polygon_coords: List of (latitude, longitude) tuples defining the polygon.
    """
    # Ensure GeoDataFrame has the correct CRS
    gdf = gdf.to_crs("EPSG:4326")

    # Fix polygon orientation and ensure valid geometries
    gdf["geometry"] = gdf["geometry"].apply(
        lambda geom: orient(geom, sign=1.0) if geom.is_valid else geom.buffer(0)
    )

    # Define and clip the area of interest
    defined_area = gpd.GeoDataFrame(
        geometry=[Polygon([(lon, lat) for lat, lon in polygon_coords])],
        crs="EPSG:4326"
    )
    gdf_clipped = gpd.clip(gdf, defined_area)

    # Convert GeoDataFrame to GeoJSON
    geojson_data = gdf_clipped.__geo_interface__

    # Calculate the center of the map
    lats, lons = zip(*polygon_coords)
    center_lat = sum(lats) / len(lats)
    center_lon = sum(lons) / len(lons)

    # Plot the map
    fig = px.choropleth_mapbox(
        gdf_clipped,
        geojson=geojson_data,
        locations=gdf_clipped.index,
        color="muname",
        mapbox_style="carto-positron",
        center={"lat": center_lat, "lon": center_lon},
        zoom=14,
        opacity=0.8
    )

    # Add your Mapbox token
    # fig.update_layout(mapbox_accesstoken="pk.eyJ1IjoiZ3JhZGllbnRvbGxpZSIsImEiOiJjbTN4bm05N2wxaXAzMmlvYjZlczRjeWJ3In0.LGkbg4xjs8TZLOLu1rSJvA")
    #
    # save_mapbox_image(fig, 'mapbox_map.png')
    # Show map
    # fig.show()
    return gdf_clipped

def kml_to_polygon_wkt(kml_file_path):
    """
    Reads a KML file, extracts polygon coordinates, and converts them to a WKT format.

    Parameters:
    - kml_file_path (str): Path to the KML file.

    Returns:
    - str: WKT representation of the polygon.
    """
    with open(kml_file_path, 'r') as file:
        kml_content = file.read()

    # Parse the KML file
    kml_tree = parser.fromstring(kml_content)

    # Navigate to the Polygon coordinates (assumes the KML has a single Polygon)
    coordinates = kml_tree.Document.Placemark.Polygon.outerBoundaryIs.LinearRing.coordinates.text.strip()

    # Split coordinates into a list
    coord_list = coordinates.split()

    # Convert the coordinate strings to (longitude, latitude) tuples
    polygon_coords = []
    for coord in coord_list:
        lon, lat, *_ = map(float, coord.split(","))  # Extract longitude and latitude
        polygon_coords.append((lon, lat))

    # Create a Polygon object
    polygon = Polygon(polygon_coords)

    # Convert the Polygon to WKT
    polygon_wkt = polygon.wkt

    return polygon_wkt


def save_mapbox_image(fig, filename="mapbox_image.png", scale=2):
    """
    Saves a Mapbox map figure created with Plotly to an image file.

    Parameters:
    - fig (plotly.graph_objects.Figure): The Plotly figure object to save.
    - filename (str): The name of the output file (e.g., "mapbox_image.png").
    - scale (int): The scale factor for the image resolution.

    Returns:
    - None
    """
    try:
        # Convert the figure to an image bytes
        img_bytes = fig.to_image(format="png", scale=scale)

        # Open the image using PIL
        img = Image.open(io.BytesIO(img_bytes))

        # Save the image
        img.save(filename)
        print(f"Mapbox image saved as {filename}")
    except Exception as e:
        print(f"Error saving the mapbox image: {e}")


def simplify_polygon_coords(coords, tolerance=0.001):
    """
    Simplifies the polygon by reducing the number of coordinates while maintaining its shape.
    :param coords: List of (latitude, longitude) coordinates defining a polygon.
    :param tolerance: Tolerance for simplification (higher means more simplification).
    :return: Simplified list of coordinates.
    """
    # Create a Polygon from the coordinates
    polygon = Polygon(coords)

    # Simplify the polygon to reduce the number of points
    simplified_polygon = polygon.simplify(tolerance, preserve_topology=True)

    # Extract the simplified coordinates
    simplified_coords = list(simplified_polygon.exterior.coords)

    return simplified_coords


def extract_boundaries_from_kml(kml_path):
    """
    Extracts field boundaries (polygons) from a KML file and simplifies them.
    :param kml_path: Path to the KML file.
    :return: List of simplified polygon coordinates (latitude, longitude).
    """
    # Read the KML as a GeoDataFrame
    gdf = gpd.read_file(kml_path, driver='KML')

    boundaries = []
    for _, row in gdf.iterrows():
        if row.geometry.is_valid:
            if row.geometry.geom_type == 'Polygon':
                coords = [(coord[1], coord[0]) for coord in row.geometry.exterior.coords]

                simplified_coords = simplify_polygon_coords(coords)
                # Remove duplicate (first == last) coordinate
                if simplified_coords[0] == simplified_coords[-1]:
                    simplified_coords = simplified_coords[:-1]
                boundaries.append(simplified_coords)
            elif row.geometry.geom_type == 'MultiPolygon':
                for poly in row.geometry.geoms:
                    coords = [(coord[1], coord[0]) for coord in poly.exterior.coords]
                    simplified_coords = simplify_polygon_coords(coords)
                    # Remove duplicate (first == last) coordinate
                    if simplified_coords[0] == simplified_coords[-1]:
                        simplified_coords = simplified_coords[:-1]
                    boundaries.append(simplified_coords)

    return boundaries


def get_all_soil_data(kml_files):
    """
    Process multiple KML files to fetch soil data and generate GeoDataFrames.
    :param kml_files: List of KML file paths.
    :return: Combined GeoDataFrame of all soil data.
    """
    combined_gdf = gpd.GeoDataFrame()

    for kml_path in kml_files:
        print(f"Processing {kml_path}")
        # Extract boundaries from the KML file
        boundaries = extract_boundaries_from_kml(kml_path)

        for polygon_coords in boundaries:
            print(f'Processing {polygon_coords}')
            # Get soil data for the boundary
            gdf = get_soil_types_from_area(polygon_coords)

            # Combine into a single GeoDataFrame
            combined_gdf = pd.concat([combined_gdf, gdf], ignore_index=True)

    return combined_gdf


def plot_combined_soil_map(combined_gdf):
    """
    Plots the combined GeoDataFrame of soil data on a Mapbox map.
    :param combined_gdf: Combined GeoDataFrame containing soil data.
    """
    # Ensure combined_gdf is a GeoDataFrame if it's not one already
    if not isinstance(combined_gdf, gpd.GeoDataFrame):
        # Create a GeoDataFrame by defining the 'geometry' column
        combined_gdf = gpd.GeoDataFrame(combined_gdf, geometry="geometry")

    # Set the CRS if it hasn't been set already (assuming original CRS is 'EPSG:3857')
    if combined_gdf.crs is None:
        combined_gdf.set_crs("EPSG:3857", allow_override=True, inplace=True)

    # Convert the GeoDataFrame to the desired CRS (WGS84, EPSG:4326)
    combined_gdf = combined_gdf.to_crs("EPSG:4326")

    # Convert the GeoDataFrame to GeoJSON
    geojson_data = combined_gdf.__geo_interface__

    # Calculate map center (based on the bounds of the combined_gdf)
    bounds = combined_gdf.total_bounds  # [minx, miny, maxx, maxy]
    map_center = {
        "lat": (bounds[1] + bounds[3]) / 2,  # Midpoint latitude
        "lon": (bounds[0] + bounds[2]) / 2   # Midpoint longitude
    }

    # Set the zoom level based on the bounding box size
    zoom_level = 12 - (bounds[2] - bounds[0]) * 10  # Adjust zoom dynamically

    # Plot the map
    fig = px.choropleth_mapbox(
        combined_gdf,
        geojson=geojson_data,
        locations=combined_gdf.index,
        color="muname",  # Use soil types for coloring
        mapbox_style="carto-positron",
        center=map_center,
        zoom=zoom_level,  # Adjust zoom level based on bounds
        opacity=0.7
    )

    # Add your Mapbox token
    fig.update_layout(mapbox_accesstoken='sk.eyJ1IjoiZ3JhZGllbnRvbGxpZSIsImEiOiJjbTV5Z2RuZmgwajNvMmtvbndiaHYxNm1wIn0.U1uGDST-t6Pu5O2WTuoQnw')
    # save_mapbox_image(fig)
    fig.show()


polygon_coords = [
    (37.9395672, -121.6291727),  # top left
    (37.9328473, -121.6291727),  # bottom left
    (37.9328473, -121.6263780),  # bottom right
    (37.9395672, -121.6263780),  # top right
]

#1263 kml bounds
# polygon_coords = [(38.22001241, -121.62689789), (38.22948627, -121.6369282), (38.22928111, -121.63713793), (38.22701958, -121.6359769), (38.2253787, -121.63814319), (38.21413429, -121.62624929)]


# soil_types = get_soil_types_from_area(polygon_coords)
# print("Soil types in the area:", soil_types)

# Paths to KML files
# kml_files = ["1263.kml"]

# Define California boundaries (approximate)
# california_bounds = [-124.409591, 32.534156, -114.131211, 42.009518]  # [min_lon, min_lat, max_lon, max_lat]

# Process KML files and get combined soil data
# combined_soil_gdf = get_all_soil_data(kml_files)

# Plot the combined soil data on Mapbox
# plot_combined_soil_map(combined_soil_gdf)
# TODO add acres in each color

# default token
# mapbox_accesstoken="pk.eyJ1IjoiZ3JhZGllbnRvbGxpZSIsImEiOiJjbTN4bm05N2wxaXAzMmlvYjZlczRjeWJ3In0.LGkbg4xjs8TZLOLu1rSJvA"
# mine
# mapbox_token = 'sk.eyJ1IjoiZ3JhZGllbnRvbGxpZSIsImEiOiJjbTV5Z2RuZmgwajNvMmtvbndiaHYxNm1wIn0.U1uGDST-t6Pu5O2WTuoQnw'