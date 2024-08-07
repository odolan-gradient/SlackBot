import matplotlib.pyplot as plt
from matplotlib.colors import ListedColormap
from matplotlib.patches import Patch


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


