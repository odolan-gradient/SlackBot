import os
from collections import defaultdict

from dotenv import load_dotenv
from flask import Flask, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

import Decagon
import SQLScripts
import SharedPickle
import SheetsHandler
import Soils
from blocks import field_list_menu, logger_and_soil_list_menu, logger_and_toggle_menu, logger_and_dates_menu, \
    logger_select_block, main_menu, get_soil_menu, show_pickle_menu, use_prev_days_menu, grower_select_block, \
    get_field_location_menu, get_coord_location_menu, turn_on_psi_menu, change_soil_menu, logger_delete_menu, \
    modify_value_selector_menu, change_gpm_menu, logger_and_gpm_menu, run_field_menu
from slackFunctions import change_logger_soil_type, delete_psi_values_for_logger_and_range, bulk_toggle_psi, \
    delete_psi_values_for_specific_logger, update_gpm_irrigation_acres_for_logger

load_dotenv()

app = App(
    token=os.getenv("SLACK_BOT_TOKEN"),
    signing_secret=os.getenv("SLACK_SIGNING_SECRET")
)

flask_app = Flask(__name__)
handler = SlackRequestHandler(app)

user_selections = defaultdict(dict)

# Example lists for dropdown options
# growers = SharedPickle.open_pickle()
# grower_names = [grower.name for grower in growers]
# fields = [field.name for grower in growers for field in grower.fields]


@app.command("/menu")
@app.command("/test")
def main_menu_command(ack, body, respond):
    try:
        ack()
        # U06NJRAT1T2 Ollie
        # Javi 'U4KFKMH8C'
        if body['user_id'] in ['U4KFKMH8C']:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Show Pickle', 'Use Previous Days VWC', 'Add Grower Billing', 'Get Field Location',
                            'Change GPM / Irr Acres']

        # just Ollie
        elif body['user_id'] in ['U06NJRAT1T2']:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Delete PSI values', 'Show Pickle', 'Use Previous Days VWC',
                            'Modify Values', 'Add Grower Billing', 'Get Field Location', 'Change GPM / Irr Acres', 'Run Field']
        else:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Delete PSI values', 'Show Pickle', 'Add Grower Billing', 'Get Field Location',
                            'Change GPM / Irr Acres']
        main_menu(ack, respond, menu_options)
    except Exception as e:
        print(f"Error: {e}")
        respond("An error occurred while processing your request.")


# ----------------------------------------------------

@app.action("menu_select")
def handle_main_menu(ack, body, respond):
    ack()
    growers = SharedPickle.open_pickle()
    grower_names = [grower.name for grower in growers]
    menu_option = body['actions'][0]['selected_option']['value']
    if menu_option == 'Change Soil Type':
        change_soil_menu(ack, respond, grower_names)
    elif menu_option == 'Get Soil Type':
        get_soil_menu(ack, respond)
    elif menu_option == 'Toggle PSI':
        turn_on_psi_menu(ack, respond, grower_names)
    elif menu_option == 'Delete PSI values':
        delete_psi_menu(ack, respond, grower_names)
    elif menu_option == 'Show Pickle':
        show_pickle_menu(ack, respond, grower_names)
    elif menu_option == 'Use Previous Days VWC':
        use_prev_days_menu(ack, respond, grower_names)
    elif menu_option == 'Add Grower Billing':
        add_billing_menu(ack, respond, body, growers)
    elif menu_option == 'Get Field Location':
        get_field_location_menu(ack, respond)
    elif menu_option == 'Check if inside MS field':
        get_coord_location_menu(ack, respond)
    elif menu_option == 'Change GPM / Irr Acres':
        change_gpm_menu(ack,respond,grower_names)
    elif menu_option == 'Run Field':
        run_field_menu(ack, respond, grower_names)



@app.action("grower_select_psi")
@app.action("grower_select_change_soil")
@app.action("grower_select_show")
@app.action("grower_select_delete_psi")
@app.action("grower_select_prev_day")
@app.action("grower_select_billing")
@app.action("grower_select_modify_values")
@app.action("grower_select_gpm")
@app.action("grower_select_run_field")
def handle_grower_menu(ack, body, client, respond):
    ack()

    # Extract values from the actions
    grower_action_id = body['callback_id']
    selected_value = body['actions'][0]['selected_options'][0]['value']
    # Ampersands come in weird through slack
    selected_value = fix_ampersand(selected_value)
    print(selected_value)
    grower = SharedPickle.get_grower(selected_value)
    field_list = [field.name for field in grower.fields]
    username = body['user']['name']

    # Add to selections
    user_id = body['user']['id']
    user_selections[user_id]['grower'] = grower

    if grower_action_id == 'grower_select_psi':
        action_id = 'field_select_psi'
        field_list_menu(ack, respond, field_list, action_id)

    elif grower_action_id == 'grower_select_change_soil':
        action_id = 'field_select_change_soil'
        field_list_menu(ack, respond, field_list, action_id)

    elif grower_action_id == 'grower_select_delete_psi':
        action_id = 'field_select_delete_psi'
        field_list_menu(ack, respond, field_list, action_id)

    elif grower_action_id == 'grower_select_prev_day':
        action_id = 'field_select_prev_day'
        field_list_menu(ack, respond, field_list, action_id)

    elif grower_action_id == 'grower_select_modify_values':
        action_id = 'field_select_modify_values'
        field_list_menu(ack, respond, field_list, action_id)
    elif grower_action_id == 'grower_select_gpm':
           action_id = 'field_select_gpm'
           field_list_menu(ack, respond, field_list, action_id)
    elif grower_action_id == 'grower_select_run_field':
            # Show them the field picker for “Run Field”
            action_id = 'field_select_run_field'
            field_list_menu(ack, respond, field_list, action_id)
            return

    elif grower_action_id == 'grower_select_show':

        # SheetsHandler.log_request_to_sheet(request_name, username, info)

        grower = SharedPickle.get_grower(selected_value)
        pickle_contents = grower.to_string()

        if len(pickle_contents) > 16000:
            chunks = [pickle_contents[i:i + 16000] for i in range(0, len(pickle_contents), 16000)]
            for chunk in chunks:
                client.chat_postMessage(
                    channel=body['channel']['id'],
                    text=chunk,
                    as_user=True
                )
        else:
            client.chat_postMessage(
                channel=body['channel']['id'],
                text=pickle_contents,
                as_user=True
            )

        # Log the request to Google Sheets
        print('Showing pickle')
        request_name = 'Show Pickle'
        info = selected_value
        SheetsHandler.log_request_to_sheet(request_name, username, info)

@app.action("get_field_location")
def handle_field_location_lookup(ack, body, respond):
    ack()

    field_number = body['state']['values']['field_location_input']['submit_field_number']['value']

    try:
        coords = SharedPickle.get_coords_from_kml_folder(field_number)
        if coords:
            lat, lon = coords
            maps_link = f"https://www.google.com/maps?q={lat},{lon}"
            respond(f"📍 Here's the location for field `{field_number}`:\n<{maps_link}|View on Google Maps>")

            # Log the request to Google Sheets
            request_name = 'Get Field Location'
            info = [field_number, lat, lon]
            username = body['user']['name']
            SheetsHandler.log_request_to_sheet(request_name, username, info)
        else:
            respond(f"⚠️ Field `{field_number}` not found in any KML file.")
            # Log the request to Google Sheets
            request_name = 'Get Field Location'
            info = [field_number, 'No field found']
            username = body['user']['name']
            SheetsHandler.log_request_to_sheet(request_name, username, info)
    except Exception as e:
        respond(f"❌ Error while looking up field location: {e}")

@app.action("get_field_from_coord")
def handle_field_from_coordinate_lookup(ack, body, respond):
    ack()

    raw_coords = body['state']['values']['field_coord_input']['submit_field_coord']['value']
    if ',' in raw_coords:
        parts = raw_coords.split(',')
    else:
        parts = raw_coords.split()

    try:
        lat, lon = float(parts[0].strip()), float(parts[1].strip())
    except Exception:
        respond("❌ Please provide valid coordinates like `36.862627, -120.607836`")
        return

    try:
        matches = SharedPickle.get_kml_from_coordinate(lat, lon)

        if matches:
            msg = f"📍 Coordinate `{lat}, {lon}` is inside:\n" + "\n".join(f"• `{name}`" for name in matches)
        else:
            msg = f"❌ Coordinate `{lat}, {lon}` is not found inside any KML polygon."

        respond(msg)

        # Log the request to Google Sheets
        request_name = 'Check if inside MS field'
        info = [lat, lon, matches]
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)
    except Exception as e:
        respond(f"❌ Error while checking coordinates: {e}")

@app.action("get_soil_type")
def handle_get_soil(ack, body, respond):
    # Acknowledge command request
    ack()

    # Get the user who invoked the command
    raw_coords = body['state']['values']['soil_input']['soil_coordinates_input']['value']

    # Split coordinates by either space or comma
    if ',' in raw_coords:
        coords = raw_coords.split(',')
    else:
        coords = raw_coords.split()

    if len(coords) == 2:
        lat, long = coords
        soil, status = Soils.get_soil_type_from_coords(lat, long)

        # Log the request to Google Sheets
        request_name = 'Get Soil Type'
        info = [lat, long, soil]
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)

        respond(f"Soil type at {coords}: \n\t{soil}")
    else:
        respond("Error: Coordinates should be in the format 'latitude longitude' or 'latitude,longitude'.")


@app.action("soil_select")
@app.action("logger_select_change_soil")
def handle_soil_and_psi_selections(ack, body, respond):
    ack()
    user_id = body['user']['id']
    action_id = body['actions'][0]['action_id']

    # Instantiate the user selections
    if action_id == 'soil_select':
        selected_option = body['actions'][0]['selected_option']['value']
        user_selections[user_id][action_id] = selected_option

    elif action_id in ['logger_select_change_soil', 'logger_select_psi']:  # logger_select
        selected_options = body['actions'][0]['selected_options']
        user_selections[user_id][action_id] = [option['value'] for option in selected_options]

    # Check if both selections are complete
    if 'soil_select' in user_selections[user_id] and 'logger_select_change_soil' in user_selections[user_id]:
        loggers = user_selections[user_id]['logger_select_change_soil']
        soil_type = user_selections[user_id]['soil_select']
        soil_type = soil_type.split(' (')[0]  # get rid of FC and WP
        field = user_selections[user_id]['fields']
        grower = user_selections[user_id]['grower']
        grower_name = grower.name
        old_soil_type = ''
        for logger in loggers:
            old_soil_type = change_logger_soil_type(logger, field, grower_name, soil_type)
        response_text = f"Changing the following:\nLoggers: {', '.join(loggers)} to Soil Type: {soil_type}"
        respond(text=response_text)

        # Log the request to Google Sheets
        request_name = 'Change Soil Type'
        logger_str = ', '.join(loggers)
        info = f'{field} {logger_str} from {old_soil_type} to {soil_type}'
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)

        # Clear the selections for this user
        del user_selections[user_id]

    else:
        # If both selections aren't complete, don't respond yet
        pass


@app.action("logger_select_psi")
@app.action("psi_on")
@app.action("psi_off")
@app.action("psi_confirm")
def handle_toggle_psi(ack, body, respond):
    ack()
    user_id = body['user']['id']
    action = body['actions'][0]
    action_id = action.get('action_id', '')

    # Handle logger selection
    if action_id == 'logger_select_psi':
        selected_loggers = [opt['value'] for opt in action['selected_options']]
        user_selections[user_id]['logger_select_psi'] = selected_loggers

    # Handle on/off button
    elif action_id in ['psi_on', 'psi_off']:
        user_selections[user_id][action_id] = action['value']
        logger_list = user_selections[user_id].get('logger_select_psi', [])
        selected_state = 'on' if action_id == 'psi_on' else 'off'
        blocks = logger_and_toggle_menu(logger_list, preselected=logger_list, selected_state=selected_state)
        respond(blocks=blocks, replace_original=True)

    # Handle confirm
    elif action_id == 'psi_confirm':
        user_selections[user_id]['psi_confirm'] = True

    # If all required selections are made, process the PSI toggle
    if all(k in user_selections[user_id] for k in ['logger_select_psi', 'psi_confirm']) and \
       ('psi_on' in user_selections[user_id] or 'psi_off' in user_selections[user_id]):

        grower = user_selections[user_id]["grower"].name
        fields = user_selections[user_id]["fields"]
        loggers = user_selections[user_id]["logger_select_psi"]
        on_off = user_selections[user_id].get("psi_on") or user_selections[user_id].get("psi_off")

        # single pickle open/write, batch all toggles
        msgs = bulk_toggle_psi(grower, fields, loggers, on_off)
        respond(text="\n".join(msgs))
        SheetsHandler.log_request_to_sheet("Toggle PSI", body["user"]["name"], f"{fields} → {loggers} → {on_off}")
        user_selections.pop(user_id, None)



@app.action("logger_select_prev_day")
@app.action("start_date_select")
@app.action("end_date_select")
@app.action("vwc_depth_select")
def handle_prev_day_selections(ack, body, respond):
    ack()
    user_id = body['user']['id']
    action_id = body['actions'][0]['action_id']

    # Instantiate the user selections
    if action_id == 'logger_select_prev_day':  # Logger select
        selected_options = body['actions'][0]['selected_options']
        user_selections[user_id][action_id] = [option['value'] for option in selected_options]
    elif action_id in ['start_date_select', 'end_date_select']:
        selected_date = body['actions'][0]['selected_date']
        user_selections[user_id][action_id] = selected_date
    elif action_id in ['vwc_depth_select']:
        selected_vwc = body['actions'][0]['selected_options']
        user_selections[user_id][action_id] = [option['value'] for option in selected_vwc]

    # Check if all selections are complete
    if 'logger_select_prev_day' in user_selections[user_id] and 'start_date_select' in user_selections[
        user_id] and 'end_date_select' in user_selections[user_id] and 'vwc_depth_select' in user_selections[user_id]:
        loggers = user_selections[user_id]['logger_select_prev_day']
        start_date = user_selections[user_id]['start_date_select']
        end_date = user_selections[user_id]['end_date_select']
        # field = user_selections[user_id]['fields']
        field = user_selections[user_id]['fields'][0]
        grower = user_selections[user_id]['grower']
        grower_name = grower.name
        project = SharedPickle.get_project(field, grower_name)
        vwcs = user_selections[user_id]['vwc_depth_select']

        for logger in loggers:
            SQLScripts.update_vwc_for_date_range(project, field, logger, start_date, end_date, vwcs)
        response_text = f"Using previous day VWC for the following:\nLoggers: {', '.join(loggers)} at {vwcs}"
        respond(text=response_text)

        # Log the request to Google Sheets
        request_name = 'Use Previous Days VWC'
        info = ', '.join(loggers) + ', ' + ', '.join(map(str, vwcs))
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)

        # Clear the selections for this user
        del user_selections[user_id]

    else:
        # If both selections aren't complete, don't respond yet
        pass


@app.action("field_select_change_soil")
@app.action("field_select_psi")
@app.action("field_select_delete_psi")
@app.action("field_select_prev_day")
@app.action("field_select_modify_values")
@app.action("field_select_modify_values")
@app.action("field_select_gpm")
@app.action("field_select_run_field")
def handle_field_select(ack, body, respond):
    ack()
    print('Handling Field Select')
    action = body['actions'][0]
    action_id = action['action_id']
    user_id = body['user']['id']

    # Handle both single-select and multi-select cases
    if 'selected_option' in action:
        field_names = [fix_ampersand(action['selected_option']['value'])]
    elif 'selected_options' in action:
        field_names = [fix_ampersand(opt['value']) for opt in action['selected_options']]
    else:
        respond("No field selected.")
        return

    # Save all selected fields
    user_selections[user_id]['fields'] = field_names

    # Open the pickle once and get all requested Field objects
    growers = SharedPickle.open_pickle()
    fields = SharedPickle.get_fields(field_names, growers=growers)

    # Build your flat logger list, extend makes it so its all one list instead of list of lists with each inner list being each fields loggers
    logger_list = []
    for f in fields:
        logger_list.extend(l.name for l in f.loggers)
    # dedupe (gets rid of any duplicates)
    # dict.fromkeys makes a dict with the keys being the values of logger_list and their values set to None
    # concise way to remove dups since dict cant have dup keys
    logger_list = list(dict.fromkeys(logger_list))

    # Route based on action
    if action_id == 'field_select_change_soil':
        print('\t Field Select Change Soil')
        soil_types = [
            'Sand (10-5)', 'Loamy Sand (12-5)', 'Sandy Loam (18-8)', 'Sandy Clay Loam (27-17)',
            'Loam (28-14)', 'Sandy Clay (36-25)', 'Silt Loam (31-11)', 'Silt (30-6)',
            'Clay Loam (36-22)', 'Silty Clay Loam (38-22)', 'Silty Clay (41-27)', 'Clay (42-30)'
        ]
        logger_and_soil_list_menu(ack, respond, logger_list, soil_types)

    elif action_id == 'field_select_psi':
        print('\t Field Select PSI')
        user_selections[user_id]['logger_select_psi'] = logger_list
        blocks = logger_and_toggle_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)

    elif action_id == 'field_select_delete_psi':
        print('\t Field Delete PSI')
        user_selections[user_id]['logger_select_delete_psi'] = logger_list
        blocks = logger_delete_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)

    elif action_id == 'field_select_prev_day':
        print('\t Field Prev Day')
        logger_and_dates_menu(ack, respond, logger_list)

    elif action_id == 'field_select_modify_values':
        print('\t Field Modify Values')
        user_selections[user_id]['logger_select_modify_values'] = logger_list
        blocks = logger_select_block(logger_list, action_id)
        respond(blocks=blocks)
    elif action_id == 'field_select_gpm':
        user_selections[user_id]['fields'] = field_names
        blocks = logger_and_gpm_menu(ack, respond, logger_list)
        respond(blocks=blocks)
    elif action_id == 'field_select_run_field':
        # Save the selected field
        user_selections[user_id]['fields'] = field_names

        Decagon.only_certain_growers_fields_update(fields=field_names, get_data=True, get_weather=True, write_to_db=True, write_to_portal=True, subtract_from_mrid=200)
        # my_service.run_field(
        #     grower=user_selections[user_id]['grower'].name,
        #     field=field_names[0]
        # )

        respond(text=f"Ran field `{field_names[0]}` for grower `{user_selections[user_id]['grower'].name}`…")
        user_selections.pop(user_id, None)
        return

@app.action("logger_select_gpm")
@app.action("update_fields_select")
@app.action("confirm_gpm_update")
def handle_confirm_gpm(ack, body, respond):
    ack()
    user_id = body["user"]["id"]
    action_id = body["actions"][0]["action_id"]
    # store the picked loggers
    if action_id == "logger_select_gpm":
        selected = [opt["value"] for opt in body["actions"][0]["selected_options"]]
        user_selections[user_id]["logger_select_gpm"] = selected
        logger_and_gpm_menu(ack, respond, selected)
        return

    # 2) User checked which values to update
    if action_id == "update_fields_select":
        checked = {opt["value"] for opt in body["actions"][0]["selected_options"]}
        user_selections[user_id]["fields_to_update"] = checked
        return

    # 3) User pressed Confirm
    if action_id == "confirm_gpm_update":
        # pull everything back out
        grower     = user_selections[user_id]["grower"]
        field_name = user_selections[user_id]["fields"][0]
        loggers    = user_selections[user_id]["logger_select_gpm"]
        to_update  = user_selections[user_id].get("fields_to_update", set())
        vals       = body["state"]["values"]

        # only parse inputs if they asked for them
        new_gpm = None
        if "gpm" in to_update:
            raw = vals["gpm_input_block"]["gpm_input"]["value"]
            new_gpm = int(raw) if raw.isdigit() else None

        new_irrigation_acres = None
        if "irr_acres" in to_update:
            raw = vals["irr_input_block"]["irr_input"]["value"]
            new_irrigation_acres = int(raw) if raw.isdigit() else None

        # apply to each logger
        results = []
        for logger_name in loggers:
            results.append(
                update_gpm_irrigation_acres_for_logger(
                    grower_name=grower.name,
                    field_name=field_name,
                    logger_name=logger_name,
                    new_gpm=new_gpm,
                    new_irrigation_acres=new_irrigation_acres
                )
            )

        # send back all statuses and clear session
        respond(text="\n".join(results))
        SheetsHandler.log_request_to_sheet(
            "Change GPM/Irr Acres",
            body["user"]["name"],
            f"{results}"
        )
        user_selections.pop(user_id, None)
        return




@app.action("attribute_select")
def handle_attribute_select(ack, body, respond):
    ack()
    selected_attribute = body['actions'][0]['selected_options'][0]['value']
    user_id = body['user']['id']
    
    # Create input block for the value
    blocks = [
        {
            "type": "input",
            "element": {
                "type": "plain_text_input",
                "action_id": "value_input"
            },
            "label": {
                "type": "plain_text",
                "text": f"Enter new value for {selected_attribute}",
                "emoji": True
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Submit",
                        "emoji": True
                    },
                    "value": selected_attribute,
                    "action_id": "submit_value"
                }
            ]
        }
    ]
    respond(blocks=blocks)

@app.action("submit_value")
def handle_submit_value(ack, body, respond):
    ack()
    selected_attribute = body['actions'][0]['value']
    new_value = body['state']['values']['value_input']['value']
    user_id = body['user']['id']
    
    # Get the selected logger and date range from user selections
    logger_name = user_selections[user_id]['logger_select_modify_values']
    start_date = user_selections[user_id]['start_date']
    end_date = user_selections[user_id]['end_date']
    
    # TODO: Add code to update the value in the database
    # This will require creating a new function in SQLScripts.py
    
    response_text = f"Updating {selected_attribute} to {new_value} for logger {logger_name} from {start_date} to {end_date}"
    respond(text=response_text)
    
    # Clear the selections for this user
    del user_selections[user_id]

    # Extract values from the actions
    action_data = body['actions'][0]
    action_id = action_data.get('action_id', action_data.get('name'))
    callback_id = action_id
    selected_value = action_data['selected_options'][0]['value']
    fields_selected = [fix_ampersand(selected_value)]
    user_id = body['user']['id']
    user_selections[user_id]['fields'] = fields_selected
    # Extract values from the actions
    action_data = body['actions'][0]
    action_id = action_data.get('action_id', action_data.get('name'))
    callback_id = action_id
    selected_value = action_data['selected_options'][0]['value']
    fields_selected = [fix_ampersand(selected_value)]
    user_id = body['user']['id']
    user_selections[user_id]['fields'] = fields_selected

    # Build combined logger list from all selected fields
    logger_list = []
    for fname in fields_selected:
        field_obj = SharedPickle.get_field(fname)
        logger_list.extend([logger.name for logger in field_obj.loggers])
    logger_list = list(dict.fromkeys(logger_list))  # unique while preserving order
    if callback_id == 'field_select_change_soil':
        soil_types = ['Sand (10-5)', 'Loamy Sand (12-5)', 'Sandy Loam (18-8)', 'Sandy Clay Loam (27-17)',
                      'Loam (28-14)', 'Sandy Clay (36-25)', 'Silt Loam (31-11)', 'Silt (30-6)',
                      'Clay Loam (36-22)', 'Silty Clay Loam (38-22)', 'Silty Clay (41-27)', 'Clay (42-30)']
        logger_and_soil_list_menu(ack, respond, logger_list, soil_types)

    if callback_id == 'field_select_psi':
        user_selections[user_id]['logger_select_psi'] = logger_list
        blocks = logger_and_toggle_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)
        # logger_and_toggle_menu(ack, respond, logger_list)
    elif callback_id == 'field_select_delete_psi':
        # store full logger list
        user_selections[user_id]['logger_select_delete_psi'] = logger_list
        blocks = logger_delete_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)


    elif callback_id == 'field_select_prev_day':
        logger_and_dates_menu(ack, respond, logger_list)

    elif callback_id == 'field_select_modify_values':
        modify_value_selector_menu(respond, logger_list)

@app.action("logger_select_delete_psi")
@app.action("delete_start_date")
@app.action("delete_end_date")
@app.action("delete_confirm")
def handle_delete_psi(ack, body, respond):
    ack()
    user_id   = body["user"]["id"]
    action_id = body["actions"][0]["action_id"]

    # picked loggers
    if action_id == "logger_select_delete_psi":
        selected = [o["value"] for o in body["actions"][0]["selected_options"]]
        user_selections[user_id]["logger_select_delete_psi"] = selected
        blocks = logger_delete_menu(selected, preselected=selected)
        return respond(blocks=blocks, replace_original=True)

    # picked dates (optional)
    if action_id == "delete_start_date":
        user_selections[user_id]["delete_start_date"] = body["actions"][0]["selected_date"]
        return
    if action_id == "delete_end_date":
        user_selections[user_id]["delete_end_date"] = body["actions"][0]["selected_date"]
        return

    # confirmed delete
    if action_id == "delete_confirm":
        respond(
            text="⏳ Deleting PSI values, please wait…",
            replace_original=True
        )
        # fields into a list
        raw    = user_selections[user_id].get("fields") or user_selections[user_id].get("field")
        fields = raw if isinstance(raw, (list,tuple)) else [raw]

        grower    = user_selections[user_id]["grower"]
        to_delete = set(user_selections[user_id]["logger_select_delete_psi"])
        start     = user_selections[user_id].get("delete_start_date")
        end       = user_selections[user_id].get("delete_end_date")

        results = []
        growers = SharedPickle.open_pickle()
        for field_name in fields:
            # only delete loggers that are in this field (the Tony fix)
            field_obj = SharedPickle.get_field(field_name, growers=growers)
            actual = {l.name for l in field_obj.loggers}
            for lg in to_delete & actual:
                print(f'\t{lg}')
                if start and end:
                    results.append(delete_psi_values_for_logger_and_range(
                        grower.name, field_name, lg, start, end, growers=growers
                    ))
                else:
                    results.append(delete_psi_values_for_specific_logger(
                        grower.name, field_name, lg, growers=growers
                    ))

        respond(
            text="✅ Delete complete!",
            blocks=[{
                "type": "section",
                "text": {"type": "mrkdwn", "text": "\n".join(results)}
            }],
            replace_original=True
        )
        SheetsHandler.log_request_to_sheet(
            "Delete PSI Values",
            body["user"]["name"],
            f"{fields} → {list(to_delete)} ({start} to {end})"
        )
        user_selections.pop(user_id, None)


def get_selected_value(state, block_id, action_id):
    """Helper function to get selected value from state."""
    return state[block_id][action_id]['selected_option']['value']

def get_selected_values(state, action_id):
    """Helper function to get multiple selected values from state."""
    for block in state.values():
        if action_id in block:
            return [opt['value'] for opt in block[action_id]['selected_options']]
    return []


@app.action("modify_confirm")
def handle_modify_confirm(ack, body, respond):
    ack()
    user_id = body['user']['id']
    state = body['state']['values']

    try:
        loggers = get_selected_values(state, "logger_select_modify_values")
        attribute = get_selected_value(state, "attribute_select_block", "attribute_select_modify")
        new_value = state["value_input_block"]["value_input"]["value"]
        selected_date = state["date_picker_block"]["modify_date_select"]["selected_date"]

        grower = user_selections[user_id]['grower']
        field = user_selections[user_id]['fields'][0]

        for logger_name in loggers:
            SQLScripts.write_logger_value_to_db(
                grower=grower,
                field=field,
                logger=logger_name,
                date=selected_date,
                attribute=attribute,
                new_value=new_value
            )

        respond(f"✅ Updated `{attribute}` to `{new_value}` for {len(loggers)} logger(s) on {selected_date}")
    except Exception as e:
        respond(f"❌ Failed to apply modification: {str(e)}")


def fix_ampersand(text):
    return text.replace('&amp;', '&')


# def handle_billing(ack, body, respond):


def add_billing_menu(ack, respond, body, growers):
    ack()
    # action_id = 'grower_select_billing'
    try:
        # growers = SharedPickle.open_pickle()
        result = SheetsHandler.billing_report_new_tab_v2(growers)
        link = 'https://docs.google.com/spreadsheets/d/137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k/edit?gid=0#gid=0'
        if result:
            respond(f'Added current growers to the sheet\nView here: {link}')
        elif not result:
            respond(f'Something went wrong contact your handsome software engineers')

        # Log the request to Google Sheets
        request_name = 'Add Grower Billing'
        # TODO
        # info = body.username
        #Error: 'dict' object has no attribute 'username' let Ollie know por favor
        SheetsHandler.log_request_to_sheet(request_name, 'need to fix', '')
    except Exception as e:
        respond(f'Error: {e} let Ollie know por favor')
    # response = {
    #     "response_type": "in_channel",
    #     "text": "Add Grower to Billing Sheet",
    #     "attachments": [
    #         grower_select_block(grower_names, action_id)
    #     ]
    # }

    # respond(response)


# def toggle_psi(grower_name, field_names, logger_name, psi_toggle, growers):
#     response_text = ''
#     for grower in growers:
#         if grower.name == grower_name:
#             for field in grower.fields:
#                 if field.name in field_names:
#                     for logger in field.loggers:
#                         if logger.name == logger_name:
#                             if psi_toggle == 'off':  # if On
#                                 response_text += f'Turned Off IR for {logger.name}\n'
#                                 logger.ir_active = False
#                             if psi_toggle == 'on':
#                                 response_text += f'Turned On IR for {logger.name}\n'
#                                 logger.ir_active = True
#     SharedPickle.write_pickle(growers)
#     return response_text


def delete_psi_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_delete_psi'

    response = {
        "response_type": "in_channel",
        "text": "Select Grower to Delete PSI:",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


# Entry point for Google Cloud Functions
@flask_app.route('/slack/events', methods=['POST'])
def slack_events():
    return handler.handle(request)


# The function that Google Cloud Functions will call
def slack_bot(request):
    return slack_events()


#  to run locally (not needed for Cloud Functions)
# need to be running the file for Slack API to accept the ngrok http url
if __name__ == "__main__":
    flask_app.run(port=int(os.getenv("PORT", 3000)))

# if __name__ == "__main__":
#     app.start(port=int(os.getenv("PORT", 3000)))

# interactivity is the one that determines the debug
# https://us-central1-rich-meridian-430023-j1.cloudfunctions.net/slackBot/slack/events
# https://seal-app-er6sr.ondigitalocean.app/slack/events

# use_prev_days_menu()
# get_soil_type_from_coords(36.754599, -120.453252)
# toggle_psi('Andrew', 'Andrew3101-3103', '3101-3101A-NW', 'off')
# change_logger_soil_type('BR-7A-C', 'Bays Ranch7A', 'Bays Ranch', new_soil_type='Clay Loam')
# growers = SharedPickle.open_pickle()
# SheetsHandler.billing_report_new_tab_v2(growers)
# TODO add feature to delete PSI
# TODO feature to delete days and rerun with PSI on
# TODO change BQ any value for any day so dont have to run queries every time