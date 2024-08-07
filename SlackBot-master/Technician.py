from Notifications import AllNotifications

class Technician(object):
    def __init__(self, name, email):
        self.email = email
        self.name = name
        self.growers = []
        self.all_notifications = AllNotifications()
        self.notification_file_path = ''
        self.logger_setup_notification_file_path = ''

    def to_string(self):
        print('========================================================================')
        print('Technician: \t{}'.format(self.name))
        print('Email: \t{}'.format(self.email))
        print('Growers: ')
        counter = 0
        print('\t', end = '')
        for g in self.growers:
            print(g.name + ', ',end='')
            counter = counter + 1
            if counter == 4:
                print()
                print('\t', end = '')
                counter = 0
        print()
        print('========================================================================')
        print()