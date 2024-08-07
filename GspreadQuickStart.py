import gspread
from oauth2client.service_account import ServiceAccountCredentials


# use creds to create a client to interact with the Google Drive API
scope = ['https://spreadsheets.google.com/feeds']
creds = ServiceAccountCredentials.from_json_keyfile_name('client_secret.json', scope)
client = gspread.authorize(creds)

# Find a workbook by name and open the first sheet
# Make sure you use the right name here.
#sheet = client.create("Test Creation")
sheet = client.open("Created Sheet").sheet1

# Extract and print all of the values
list_of_hashes = sheet.get_all_values()
print(list_of_hashes)
cell = sheet.find("Ending")
print("Found something at R%sC%s" % (cell.row, cell.col))
sheet.update_cell(cell.row, cell.col, "Ending")
sheet.update_cell(1, 5, "1231412313")