import os
import time

from apscheduler.schedulers.background import BackgroundScheduler

import UninstallFields


def removeFields():
    UninstallFields.remove_fields()


if __name__ == '__main__':
    scheduler = BackgroundScheduler()
    scheduler.add_job(removeFields, 'interval', seconds=60)
    scheduler.start()
    print('Press Ctrl+{0} to exit'.format('Break' if os.name == 'nt' else 'C'))

    try:
        # This is here to simulate application activity (which keeps the main thread alive).
        while True:
            time.sleep(2)

    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled but should be done if possible
        scheduler.shutdown()