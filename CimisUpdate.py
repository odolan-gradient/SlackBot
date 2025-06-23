import time
from datetime import datetime

import Decagon

start_time = time.time()
now = datetime.today()

print(">>>>>>>>>>>>>>>>>>>CIMIS Update<<<<<<<<<<<<<<<<<<<")
print("                                 - " + now.strftime("%m/%d/%y  %I:%M %p"))
Decagon.reset_updated_all()
Decagon.update_et_information(get_et=True, write_to_db=True)

Decagon.merge_et_logger_tables()

end_time = time.time()
elapsed_time_seconds = end_time - start_time
hours = int(elapsed_time_seconds // 3600)
minutes = int((elapsed_time_seconds % 3600) // 60)
seconds = int(elapsed_time_seconds % 60)
now = datetime.today()
print('-------------------------------')
print('>>>>>>>>>>> D O N E <<<<<<<<<<<')
print("                                 - " + now.strftime("%m/%d/%y  %I:%M %p"))
print(f"CimisUpdate.py execution time: {hours}:{minutes}:{seconds} (hours:minutes:seconds)")
print('-------------------------------')

