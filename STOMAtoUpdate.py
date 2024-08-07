import time
from datetime import datetime

import Decagon

#  Update Information Call Params
#   (Get Et, Get Weather, Get Data, Write To Sheet, Write to Portal, Check for Notifications)
start_time = time.time()
Decagon.reset_updated_all()

Decagon.update_information(get_weather=True, get_data=True, write_to_portal=True, write_to_db=True,
                           check_for_notifications=True)

Decagon.show_pickle()
print()
Decagon.updated_run_report()

# Decagon.temp_ai_application()

now = datetime.today()
end_time = time.time()
elapsed_time_seconds = end_time - start_time
hours = int(elapsed_time_seconds // 3600)
minutes = int((elapsed_time_seconds % 3600) // 60)
seconds = int(elapsed_time_seconds % 60)

print('-------------------------------')
print('>>>>>>>>>>> D O N E <<<<<<<<<<<')
print("                                 - " + now.strftime("%m/%d/%y  %I:%M %p"))
print(f"STOMAtoUpdate.py execution time: {hours}:{minutes}:{seconds} (hours:minutes:seconds)")
print('-------------------------------')