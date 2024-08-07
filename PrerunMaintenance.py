import LoggerSetups

print(' ------------Pre-run Maintenance------------')
print()

print(' Setting up notifications')
LoggerSetups.notification_setup()
print(' Done setting up notifications')

print(' >> Updating Preview and URLs for new fields')
LoggerSetups.check_for_missing_report_or_preview_in_pickle()
print(' Done updating preview and url')

print()
print(' Looking for Fields to Uninstall')
LoggerSetups.field_uninstall_process()
print(' Done Uninstalling Fields')

# Skipping these next two because I'm not entirely sure what they are for of if they are needed
# print()
# print(' Looking for Fields to Add to Technician Portal')
# UninstallFields.add_fields_to_technician_portal()
# print(' Done Adding Fields to Technician Portal')
#
# print()
# print(' Updating Stats for Technician Portal')
# UninstallFields.update_stats_for_acres()
# print(' Done Updating Stats for Technician Portal')

print()
print(' >> Checking for Broken Sensors')
LoggerSetups.logger_swap_process()
print(' Done checking for broken sensors')

# Old Logger Setups process
# print()
# print(' >> Logger Setups')
# LoggerSetups.setup_field()
# print(' Done setting up new fields')

print()
print(' >> Logger Setups Process 2.0')
LoggerSetups.logger_setups_process()
print('Done with Logger Setups Process 2.0')

print()
print(' >> Checking for New Cimis Stations to Add')
LoggerSetups.check_for_new_cimis_stations()
print(' Done adding new Cimis Stations')

print()
