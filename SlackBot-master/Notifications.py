import datetime
import os
from abc import ABC, abstractmethod
from pathlib import Path

from EmailProcessor import EmailProcessor

DIRECTORY_YEAR = "2024"
NOTIFICATION_DIRECTORY = "H:\\Shared drives\\Stomato\\" + DIRECTORY_YEAR

class AllNotifications(object):
    """
    Class to hold and manage all notifications

    Attributes:
        notifications: A list of Notification objects
    """
    notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Notifications")
    tech_notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Tech_Notifications")
    logger_setup_notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Logger_Setup_Notifications")
    tech_field_warning_notif_folder = Path(
        f"{NOTIFICATION_DIRECTORY}\\Tech_Field_Warning_Notifications")

    def __init__(self):
        self.notifications = []
        # self.error_notifications = []
        # self.tech_notifications = []
        # self.tech_field_warning_notifications = []
        self.logger_setup_notification = []

        notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Notifications")
        tech_notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Tech_Notifications")
        logger_setup_notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Logger_Setup_Notifications")
        tech_field_warning_notif_folder = Path(
            f"{NOTIFICATION_DIRECTORY}\\Tech_Field_Warning_Notifications")

    def add_notification(self, notif):
        """

        :param notif:
        :return:
        """
        self.notifications.append(notif)
        return notif

    def clear_all_notifications(self):
        """
        Function to clear all notifications from the list. This will be called every day before running update
            to ensure we start with a clear list of notifications

        :return:
        """
        self.notifications = []
        # self.error_notifications = []
        # self.tech_notifications = []
        # self.logger_setup_notification = []
        # self.tech_field_warning_notifications = []

    # TODO move all writting to Notification class
    def write_all_notifications_to_txt(self, technician_name, grower_name):
        notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Notifications")

        now = datetime.datetime.today()
        if self.notifications:
            # Get the notification files and prepare them for writing for the current grower
            # Sensor Errors
            sensor_error_notif_folder = Path.joinpath(notif_folder, 'Sensor Error')
            sensor_error_file_name = technician_name + "_sensor_error_notifications_" + str(
                now.strftime("%m-%d-%y")
            ) + ".txt"
            sensor_error_file_path = sensor_error_notif_folder / sensor_error_file_name
            with open(sensor_error_file_path, 'a') as the_file:
                the_file.write(
                    f"\n==================================  {grower_name}  =================================="
                )
                the_file.write("\n")

            # Technician Warning
            # Disabling for the time being
            # tech_warning_notif_folder = Path.joinpath(notif_folder, 'Tech Warning')
            # tech_warning_file_name = technician_name + "_tech_warning_notifications_" + str(
            #     now.strftime("%m-%d-%y")
            # ) + ".txt"
            # tech_warning_file_path = tech_warning_notif_folder / tech_warning_file_name
            # with open(tech_warning_file_path, 'a') as the_file:
            #     the_file.write(
            #         "\n==================================  NEW GROWER  =================================="
            #     )
            #     the_file.write("\n")

            # Logger Setups
            logger_setups_notif_folder = Path.joinpath(notif_folder, 'Logger Setups')
            logger_setups_file_name = technician_name + "_logger_setups_notifications_" + str(
                now.strftime("%m-%d-%y")
            ) + ".txt"
            logger_setups_file_path = logger_setups_notif_folder / logger_setups_file_name
            with open(logger_setups_file_path, 'a') as the_file:
                the_file.write(
                    f"\n==================================  {grower_name}  =================================="
                )
                the_file.write("\n")

            # Write notifications to the files
            for ind, notification in enumerate(self.notifications):
                if notification.type == 'Sensor Error':
                    filepath = sensor_error_file_path
                # elif notification.type == 'Technician Warning':
                #     filepath = tech_warning_file_path
                elif notification.type == 'Logger Setups':
                    filepath = logger_setups_file_path
                notification.notify_to_txt_file(filepath)

    def write_all_notifications_to_html(self, technician_name, grower_name):
        notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Notifications")

        now = datetime.datetime.today()
        if self.notifications:
            # Sensor Errors
            sensor_error_notif_folder = Path.joinpath(notif_folder, 'Sensor Error')
            sensor_error_file_name = technician_name + "_sensor_error_notifications_" + str(
                now.strftime("%m-%d-%y")
            ) + ".html"
            sensor_error_file_path = sensor_error_notif_folder / sensor_error_file_name
            with open(sensor_error_file_path, 'a') as the_file:
                the_file.write("<p></p>\n")
                the_file.write(
                    f"<h3>======  {grower_name}  =====</h3>\n")
                the_file.write("<p></p>\n")

            # Technician Warning
            # Disabling for the time being
            # tech_warning_notif_folder = Path.joinpath(notif_folder, 'Tech Warning')
            # tech_warning_file_name = technician_name + "_tech_warning_notifications_" + str(
            #     now.strftime("%m-%d-%y")
            # ) + ".html"
            # tech_warning_file_path = tech_warning_notif_folder / tech_warning_file_name
            # with open(tech_warning_file_path, 'a') as the_file:
            #     the_file.write(
            #         "\n==================================  NEW GROWER  =================================="
            #     )
            #     the_file.write("\n")

            # Logger Setups
            logger_setups_notif_folder = Path.joinpath(notif_folder, 'Logger Setups')
            logger_setups_file_name = technician_name + "_logger_setups_notifications_" + str(
                now.strftime("%m-%d-%y")
            ) + ".html"
            logger_setups_file_path = logger_setups_notif_folder / logger_setups_file_name
            with open(logger_setups_file_path, 'a') as the_file:
                the_file.write("<p></p>\n")
                the_file.write(
                    f"<h3>======  {grower_name}  ======</h3>\n")
                the_file.write("<p></p>\n")

            for ind, notification in enumerate(self.notifications):
                if notification.type == 'Sensor Error':
                    filepath = sensor_error_file_path
                # elif notification.type == 'Technician Warning':
                #     filepath = tech_warning_file_path
                elif notification.type == 'Logger Setups':
                    filepath = logger_setups_file_path
                notification.notify_to_html_file(filepath)


    def write_all_notifications_to_html_v2(self, technician_name, grower_name):
        notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Notifications")

        now = datetime.datetime.today()
        if self.notifications:
            sensor_error_notifications = False
            tech_warning_notifications = False
            logger_setup_notifications = False
            for notification in self.notifications:
                if notification.type == 'Sensor Error':
                    sensor_error_notifications = True
                # elif notification.type == 'Technician Warning':
                #     tech_warning_notifications = True
                elif notification.type == 'Logger Setups':
                    logger_setup_notifications = True

            if sensor_error_notifications:
                # Sensor Errors
                sensor_error_notif_folder = Path.joinpath(notif_folder, 'Sensor Error')
                sensor_error_file_name = technician_name + "_sensor_error_notifications_" + str(
                    now.strftime("%m-%d-%y")
                ) + ".html"
                sensor_error_file_path = sensor_error_notif_folder / sensor_error_file_name
                with open(sensor_error_file_path, 'a') as the_file:
                    the_file.write("<p></p>\n")
                    the_file.write("<table>\n")
                    the_file.write(f"<caption style='font-size:200%;'>======  {grower_name}  ======</caption>\n")
                    the_file.write("<tr>\n")
                    the_file.write("<th>Field</th>\n")
                    the_file.write("<th>Logger</th>\n")
                    the_file.write("<th>Gradient Page</th>\n")
                    the_file.write("<th>Date</th>\n")
                    the_file.write("<th>Sensor</th>\n")
                    the_file.write("<th style='width : 250px;'>Issue</th>\n")
                    the_file.write("<th>Station Location</th>\n")
                    the_file.write("</th>\n")

            # if tech_warning_notifications:
            #     Technician Warning
            #     Disabling for the time being
            #     tech_warning_notif_folder = Path.joinpath(notif_folder, 'Tech Warning')
            #     tech_warning_file_name = technician_name + "_tech_warning_notifications_" + str(
            #         now.strftime("%m-%d-%y")
            #     ) + ".html"
            #     tech_warning_file_path = tech_warning_notif_folder / tech_warning_file_name
            #     with open(tech_warning_file_path, 'a') as the_file:
            #         the_file.write(
            #             "\n==================================  NEW GROWER  =================================="
            #         )
            #         the_file.write("\n")

            if logger_setup_notifications:
                # Logger Setups
                logger_setups_notif_folder = Path.joinpath(notif_folder, 'Logger Setups')
                logger_setups_file_name = technician_name + "_logger_setups_notifications_" + str(
                    now.strftime("%m-%d-%y")
                ) + ".html"
                logger_setups_file_path = logger_setups_notif_folder / logger_setups_file_name
                with open(logger_setups_file_path, 'a') as the_file:
                    the_file.write("<p></p>\n")
                    the_file.write("<table>\n")
                    the_file.write(f"<caption style='font-size:200%;'>======  New Field Pages  ======</caption>\n")
                    the_file.write("<tr>\n")
                    the_file.write("<th>Grower</th>\n")
                    the_file.write("<th>Field</th>\n")
                    the_file.write("<th>Date</th>\n")
                    the_file.write("<th>Gradient Page</th>\n")
                    the_file.write("<th>Issue</th>\n")
                    the_file.write("</th>\n")

            for ind, notification in enumerate(self.notifications):
                if notification.type == 'Sensor Error':
                    filepath = sensor_error_file_path
                # elif notification.type == 'Technician Warning':
                #     filepath = tech_warning_file_path
                elif notification.type == 'Logger Setups':
                    filepath = logger_setups_file_path
                notification.notify_to_html_file_v2(filepath)

            # Close table and add spacer and break
            if sensor_error_notifications:
                with open(sensor_error_file_path, 'a') as the_file:
                    the_file.write("</table>\n")
                    the_file.write("<p></p>\n")
                    the_file.write("<hr>\n")

            if logger_setup_notifications:
                with open(logger_setups_file_path, 'a') as the_file:
                    the_file.write("</table>\n")
                    the_file.write("<p></p>\n")
                    the_file.write("<hr>\n")


    def email_all_notifications(self, technician_name, technician_email, file_type='txt'):
        now = datetime.datetime.today()

        # Sensor Errors
        notif_folder = Path(f"{NOTIFICATION_DIRECTORY}\\Notifications")
        sensor_error_notif_folder = Path.joinpath(notif_folder, 'Sensor Error')
        sensor_error_file_name = technician_name + "_sensor_error_notifications_" + str(
            now.strftime("%m-%d-%y")
        ) + "." + file_type
        sensor_error_file_path = sensor_error_notif_folder / sensor_error_file_name
        with open(sensor_error_file_path, 'r') as fp:
            number_of_lines = len(fp.readlines())
            print('Total lines (Sensor Errs):', number_of_lines)
        filename = 'Sensor Errors.' + file_type
        message = f"Attached is a {file_type} file with possible logger issues"
        email = EmailProcessor()

        if os.path.exists(sensor_error_file_path):
            # if number_of_lines > 4:
            email.send_email_v3(
                technician_email, technician_name + " > Sensor Errs -  " + str(
                    datetime.datetime.now().strftime("%m/%d/%y %I:%M")
                ), message, sensor_error_file_path, filename
            )
            # else:
            #     print('File < 4 lines long, not emailing')

        # Technician Warnings
        # Disabling for the time being
        # tech_warning_notif_folder = Path.joinpath(notif_folder, 'Tech Warning')
        # tech_warning_file_name = technician_name + "_tech_warning_notifications_" + str(
        #     now.strftime("%m-%d-%y")
        # ) + ".txt"
        # tech_warning_file_path = tech_warning_notif_folder / tech_warning_file_name
        # with open(tech_warning_file_path, 'r') as fp:
        #     number_of_lines = len(fp.readlines())
        #     print('Total lines (Tech Warning):', number_of_lines)
        # filename = 'Technician Warnings.txt'
        # message = "Attached is a txt file with sensor values in dangerous ranges"
        # email = EmailProcessor()
        #
        # if os.path.exists(tech_warning_file_path):
        #     # if number_of_lines > 4:
        #     email.send_email_v3(
        #         technician_email, technician_name + " > Tech Warnings -  " + str(
        #             datetime.datetime.now().strftime("%m/%d/%y %I:%M")
        #         ), message, tech_warning_file_path, filename
        #     )
        #     # else:
        #     #     print('File < 4 lines long, not emailing')


class Notification(ABC):
    """
    Abstract class to be implemented by any further notification classes.
    """

    @property
    @abstractmethod
    def type(self):
        """
        Abstract variable required by all instantiations of Notification.
        This describes what type of notification the instance is.
        """
        pass

    @abstractmethod
    def notify_to_console(self, message=None):
        """
        Method to print to console a notification.
        :param message: Optional parameter to be added to a notification's console output
        """
        pass

    @abstractmethod
    def notify_to_txt_file(self, filename, message=None):
        """
        Method to write to file a notification.
        :param filename: Name of the file to write to
        :param message: Optional parameter to be added to the file writing
        """
        pass

    @abstractmethod
    def notify_to_html_file(self, filename, message=None):
        """
        Method to write to file a notification.
        :param filename: Name of the file to write to
        :param message: Optional parameter to be added to the file writing
        """
        pass


class Notification_SensorError(Notification):
    """
    Notification class for Sensor Errors. Any errors that involve the sensor being disconnected or not reading properly
    will be handled by this notification class
    """

    def __init__(self, date: datetime, field_name: str, logger: object, sensor: str, issue: str):
        self.date = date
        self.field = field_name
        self.logger = logger
        self.sensor = sensor
        self.issue = issue

    @property
    def type(self):
        return 'Sensor Error'

    def notify_to_console(self, message=None):
        print(
            "-----------------------------------------------------------------------" \
            "\n Field: " + str(self.field) + \
            "\n Logger: " + str(self.logger.name) + \
            "\n Logger ID: " + str(self.logger.id) + \
            "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
            "\n Sensor: " + str(self.sensor) + \
            "\n   -> " + str(self.issue) + \
            "\n   -> " + str(message) + \
            "\n Location: " + str(f"https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}") + \
            "\n-----------------------------------------------------------------------\n"
        )

    def notify_to_txt_file(self, filename, message=None):
        """
        Function to write the notification information out to a txt file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            if message:
                the_file.write(
                    "-----------------------------------------------------------------------" \
                    "\n Field: " + str(self.field) + \
                    "\n Logger: " + str(self.logger.name) + \
                    "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
                    "\n Sensor: " + str(self.sensor) + \
                    "\n   -> " + str(self.issue) + \
                    "\n   -> " + str(message) + \
                    "\n Location: " + str(f'{self.logger.lat},{self.logger.long}') + \
                    "\n-----------------------------------------------------------------------\n"
                )
            else:
                the_file.write(
                    "-----------------------------------------------------------------------" \
                    "\n Field: " + str(self.field) + \
                    "\n Logger: " + str(self.logger.name) + \
                    "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
                    "\n Sensor: " + str(self.sensor) + \
                    "\n   -> " + str(self.issue) + \
                    "\n Location: " + str(f'{self.logger.lat},{self.logger.long}') + \
                    "\n-----------------------------------------------------------------------\n"
                )

    def notify_to_html_file(self, filename, message=None):
        """
        Function to write the notification information out to a html file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            if message:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Logger: {str(self.logger.name)}</p>\n")
                the_file.write(f"<a href='{self.logger.field.report_url}' target='_blank'>Gradient Page</a>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Sensor: {str(self.sensor)}</p>\n")
                the_file.write(f"<p>-> {str(self.issue)}</p>\n")
                the_file.write(f"<p>-> {str(message)}</p>\n")
                the_file.write(
                    f"<a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Station Location</a>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
            else:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Logger: {str(self.logger.name)}</p>\n")
                the_file.write(f"<a href='{self.logger.field.report_url}' target='_blank'>Gradient Page</a>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Sensor: {str(self.sensor)}</p>\n")
                the_file.write(f"<p>-> {str(self.issue)}</p>\n")
                the_file.write(
                    f"<a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Station Location</a>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")

    def notify_to_html_file_v2(self, filename):
        """
        Function to write the notification information out to a html file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            the_file.write("<tr>\n")
            the_file.write(f"<td>{str(self.field)}</td>\n")
            the_file.write(f"<td>{str(self.logger.name)}</td>\n")
            the_file.write(f"<td><a href='{self.logger.field.report_url}' target='_blank'>Link</a></td>\n")
            the_file.write(f"<td>{str(self.date.strftime('%m/%d/%y'))}</td>\n")
            the_file.write(f"<td>{str(self.sensor)}</td>\n")
            the_file.write(f"<td>{str(self.issue)}</td>\n")
            the_file.write(f"<td><a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Google Maps</a></td>\n")
            the_file.write("</tr>\n")


class Notification_TechnicianWarning(Notification):
    """
    Class for Technician Warning Notifications. This will hold all warning that have to do with sensor data being
    in dangerous levels such as the VWC being close to Wilting Point.
    """

    def __init__(self, date, field, logger, sensor, issue):
        self.date = date
        self.field = field
        self.logger = logger
        self.sensor = sensor
        self.issue = issue

    @property
    def type(self):
        return 'Technician Warning'

    def notify_to_console(self, message=None):
        """
        Function to show the notification information out to console.

        :param message:
        """
        print(
            "-----------------------------------------------------------------------" \
            "\n Field: " + str(self.field) + \
            "\n Logger: " + str(self.logger.name) + \
            "\n Logger ID: " + str(self.logger.id) + \
            "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
            "\n Sensor: " + str(self.sensor) + \
            "\n   -> " + str(self.issue) + \
            "\n   -> " + str(message) + \
            "\n Location: " + str(f"https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}") + \
            "\n-----------------------------------------------------------------------\n"
        )

    def notify_to_txt_file(self, filename, message=None):
        """
        Function to write the notification information out to a file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            if message:
                the_file.write(
                    "-----------------------------------------------------------------------" \
                    "\n Field: " + str(self.field) + \
                    "\n Logger: " + str(self.logger.name) + \
                    "\n Logger ID: " + str(self.logger.id) + \
                    "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
                    "\n Sensor: " + str(self.sensor) + \
                    "\n   -> " + str(self.issue) + \
                    "\n   -> " + str(message) + \
                    "\n Location: " + str(f'{self.logger.lat},{self.logger.long}') + \
                    "\n-----------------------------------------------------------------------\n"
                )
            else:
                the_file.write(
                    "-----------------------------------------------------------------------" \
                    "\n Field: " + str(self.field) + \
                    "\n Logger: " + str(self.logger.name) + \
                    "\n Logger ID: " + str(self.logger.id) + \
                    "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
                    "\n Sensor: " + str(self.sensor) + \
                    "\n   -> " + str(self.issue) + \
                    "\n Location: " + str(f'{self.logger.lat},{self.logger.long}') + \
                    "\n-----------------------------------------------------------------------\n"
                )

    def notify_to_html_file(self, filename, message=None):
        """
        Function to write the notification information out to a html file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            if message:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Logger: {str(self.logger.name)}</p>\n")
                the_file.write(f"<p>Logger ID: {str(self.logger.id)}</p>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Sensor: {str(self.sensor)}</p>\n")
                the_file.write(f"<p>-> {str(self.issue)}</p>\n")
                the_file.write(f"<p>-> {str(message)}</p>\n")
                the_file.write(
                    f"<a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Station Location</a>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
            else:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Logger: {str(self.logger.name)}</p>\n")
                the_file.write(f"<p>Logger ID: {str(self.logger.id)}</p>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Sensor: {str(self.sensor)}</p>\n")
                the_file.write(f"<p>-> {str(self.issue)}</p>\n")
                the_file.write(
                    f"<a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Station Location</a>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")

    def notify_to_html_file_v2(self, filename, message=None):
        """
        Function to write the notification information out to a html file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            if message:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Logger: {str(self.logger.name)}</p>\n")
                the_file.write(f"<p>Logger ID: {str(self.logger.id)}</p>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Sensor: {str(self.sensor)}</p>\n")
                the_file.write(f"<p>-> {str(self.issue)}</p>\n")
                the_file.write(f"<p>-> {str(message)}</p>\n")
                the_file.write(
                    f"<a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Station Location</a>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
            else:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Logger: {str(self.logger.name)}</p>\n")
                the_file.write(f"<p>Logger ID: {str(self.logger.id)}</p>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Sensor: {str(self.sensor)}</p>\n")
                the_file.write(f"<p>-> {str(self.issue)}</p>\n")
                the_file.write(
                    f"<a href='https://www.google.com/maps/search/?api=1&query={self.logger.lat},{self.logger.long}' target='_blank'>Station Location</a>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")

class Notification_LoggerSetups(Notification):
    """
    Class for Logger Setups Notifications. This will hold all issues that have to do with incorrect logger setups
    and will send notifications for fields with webpages ready.
    """

    def __init__(self, date: datetime, grower: str, field: str, issue: str = "No Issue", page_link: str = ''):
        self.date = date
        self.field = field
        self.grower = grower
        self.issue = issue
        self.page_link = page_link


    @property
    def type(self):
        return 'Logger Setups'

    def notify_to_console(self, message=None):
        print(
            "-----------------------------------------------------------------------" \
            "\n Grower: " + str(self.grower) + \
            "\n Field: " + str(self.field) + \
            "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
            "\n   -> " + str(message) + \
            "\n-----------------------------------------------------------------------\n"
        )

    def notify_to_txt_file(self, filename, message=None):
        with open(filename, 'a') as the_file:
            if message:
                the_file.write(
                    "-----------------------------------------------------------------------" \
                    "\n Grower: " + str(self.grower) + \
                    "\n Field: " + str(self.field) + \
                    "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
                    "\n Issue: " + str(self.issue) + \
                    "\n   -> " + str(message) + \
                    "\n-----------------------------------------------------------------------\n"
                )
            else:
                the_file.write(
                    "-----------------------------------------------------------------------" \
                    "\n Grower: " + str(self.grower) + \
                    "\n Field: " + str(self.field) + \
                    "\n Date: " + str(self.date.strftime("%m/%d/%y")) + \
                    "\n Issue: " + str(self.issue) + \
                    "\n-----------------------------------------------------------------------\n"
                )

    def notify_to_html_file(self, filename, message=None):
        """
        Function to write the notification information out to a html file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            if message:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Grower: {str(self.grower)}</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Issue: {str(self.issue)}</p>\n")
                the_file.write(f"<p>-> {str(message)}</p>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
            else:
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")
                the_file.write(f"<p>Grower: {str(self.grower)}</p>\n")
                the_file.write(f"<p>Field: {str(self.field)}</p>\n")
                the_file.write(f"<p>Date: {str(self.date.strftime('%m/%d/%y'))}</p>\n")
                the_file.write(f"<p>Issue: {str(self.issue)}</p>\n")
                the_file.write("<p>-----------------------------------------------------------------------</p>\n")

    def notify_to_html_file_v2(self, filename):
        """
        Function to write the notification information out to a html file.

        :param filename:
        :param message:
        """
        with open(filename, 'a') as the_file:
            the_file.write("<tr>\n")
            the_file.write(f"<td>{str(self.grower)}</td>\n")
            the_file.write(f"<td>{str(self.field)}</td>\n")
            the_file.write(f"<td>{str(self.date.strftime('%m/%d/%y'))}</td>\n")
            if self.page_link:
                the_file.write(f"<td><a href='{self.page_link}' target='_blank'>Link</a></td>\n")
            if self.issue:
                the_file.write(f"<td></td>\n")
                the_file.write(f"<td>{str(self.issue)}</td>\n")
            the_file.write("</tr>\n")