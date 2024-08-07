from Notifications import AllNotifications

#NOT BEING USED ANYMORE

class Plot (object):
    """
    Class to hold information for 1 plot of a trial in a field

    Attributes:
            type: String variable to hold the name of the Plot. Usually Control or Water Management
            loggers: A list of Logger objects for each Logger we have installed in the plot
            hottestTimeOfDay: Dictionary used to hold what the hottest time of day and values are for loggers with VP4
                that need to share that information with loggers without VP4
    """

    type = ''
    loggers = []
    hottestTimeOfDay = {'dates': [], 'air temperature': [], 'rh': [], 'vpd': []}
    all_notifications = AllNotifications()

    def __init__(self, type, loggers):
        """
        Inits Plot class with the following parameters:
        :param type:
        :param loggers:
        hottestTimeOfDay is initialized blank
        """
        self.type = type
        self.loggers = loggers
        self.hottestTimeOfDay = {'dates': [], 'air temperature': [], 'rh': [], 'vpd': []}

    def addLogger(self, logger):
        self.loggers.append(logger)

    def update(self, write_to_sheet = False, write_to_portal_sheet = False, write_to_db = False, check_for_notifications = False):
        """
        Function used to update each plots information. This function will be called every day.
        This function then calls the update function on each of its loggers[]
        :return:
        """
        print('Plot: ' + str(self.type))
        print()
        for l in self.loggers:
            try:
                self.hottestTimeOfDay = l.update(write_to_sheet=write_to_sheet, write_to_db=write_to_db,
                                                 check_for_notifications=check_for_notifications)  #do logger updates
            except Exception as e:
                print("Error in Logger Update - " + l.name)
                print("Error type: " + str(e))
            #     self.all_notifications.create_error_notification(datetime.now(), l, "Decagon API Error")
        self.hottestTimeOfDay = {'dates': [], 'air temperature': [], 'rh': [], 'vpd': []}


    def toString(self):
        """
        Function used to print out output to screen. Prints out the Plot type.
        Then this calls on its loggers list and has each object in the list call its own toString function
        :return:
        """
        print('          | Plot: ' + self.type + ' | ')
        # print('   Hottest Time of Day: ')
        # print(self.hottestTimeOfDay)
        for l in self.loggers:
            l.to_string()
        print()





