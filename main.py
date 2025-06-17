import os
from collections import defaultdict, deque

import requests
from dotenv import load_dotenv
from flask import Flask, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

import DBWriter
import SQLScripts
import SharedPickle
import SheetsHandler
import Soils

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
        if body['user_id'] in ['U06NJRAT1T2', 'U4KFKMH8C']:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Delete PSI values', 'Show Pickle', 'Use Previous Days VWC',
                            'Modify Values', 'Add Grower Billing', 'Get Field Location']
        else:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Delete PSI values', 'Show Pickle', 'Add Grower Billing', 'Get Field Location']
        main_menu(ack, respond, menu_options)
    except Exception as e:
        print(f"Error: {e}")
        respond("An error occurred while processing your request.")


def generate_options(list_of_items):
    return [
        {
            "text": {
                "text": item,
                "type": "plain_text"
            },
            "value": item
        } for item in list_of_items
    ]


# ---------------- Delete PSI helper & menus -----------------

def logger_delete_menu(logger_list, preselected=None):
    logger_action_id = 'logger_select_delete_psi'
    logger_block = logger_select_block(logger_list, logger_action_id, initial_selected=preselected)
    confirm_block = {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {"type": "plain_text", "text": "Confirm Delete"},
                "style": "danger",
                "action_id": "delete_confirm",
                "value": "confirm"
            }
        ]
    }
    return [logger_block, confirm_block]


# existing helper below
# ---------------- Delete PSI helper -----------------

def delete_psi_values_for_field(grower_name, field_name):
    """Set psi, sdd and canopy_temperature to NULL for every logger in the field."""
    growers = SharedPickle.open_pickle()
    dbw = DBWriter.DBWriter()
    response_lines = []
    project = SharedPickle.get_project(field_name, grower_name)
    for grower in growers:
        if grower.name != grower_name:
            continue
        for field in grower.fields:
            if field.name != field_name:
                continue
            field_dataset = dbw.remove_unwanted_chars_for_db_dataset(field.name)
            for logger in field.loggers:
                table_name = logger.name
                dml = (f"UPDATE `{project}.{field_dataset}.{table_name}` "
                       f"SET psi = NULL, sdd = NULL, canopy_temperature = NULL WHERE TRUE")
                try:
                    dbw.run_dml(dml, project=project)
                    response_lines.append(f"Cleared PSI for {logger.name} in {field_name}")
                except Exception as e:
                    response_lines.append(f"Error clearing {logger.name}: {e}")
    return '\n'.join(response_lines)

# ----------------------------------------------------

@app.action("logger_select_delete_psi")
@app.action("delete_confirm")
def handle_delete_psi(ack, body, respond):
    ack()
    user_id = body['user']['id']
    action_id = body['actions'][0]['action_id']
    if action_id == 'logger_select_delete_psi':
        selected_options = body['actions'][0]['selected_options']
        user_selections[user_id][action_id] = [opt['value'] for opt in selected_options]
    elif action_id == 'delete_confirm':
        user_selections[user_id][action_id] = True

    if 'logger_select_delete_psi' in user_selections[user_id] and 'delete_confirm' in user_selections[user_id]:
        grower = user_selections[user_id]['grower']
        fields = user_selections[user_id]['fields']
        loggers = user_selections[user_id]['logger_select_delete_psi']
        responses = []
        for field_name in fields:
            for logger in loggers:
                resp = delete_psi_values_for_specific_logger(grower.name, field_name, logger)
                responses.append(resp)
        respond(text='\n'.join(responses))
        SheetsHandler.log_request_to_sheet('Delete PSI Values', body['user']['name'], ', '.join(loggers))
        del user_selections[user_id]


def delete_psi_values_for_specific_logger(grower_name, field_name, logger_name):
    growers = SharedPickle.open_pickle()
    dbw = DBWriter.DBWriter()
    project = SharedPickle.get_project(field_name, grower_name)
    field_dataset = dbw.remove_unwanted_chars_for_db_dataset(field_name)
    dml = (f"UPDATE `{project}.{field_dataset}.{logger_name}` SET psi = NULL, sdd = NULL, canopy_temperature = NULL WHERE TRUE")
    try:
        dbw.run_dml(dml, project=project)
        # update pickle
        for grower in growers:
            if grower.name == grower_name:
                for field in grower.fields:
                    if field.name == field_name:
                        for logger in field.loggers:
                            if logger.name == logger_name:
                                logger.ir_active = False
                                logger.consecutive_ir_values = deque()
        SharedPickle.write_pickle(growers)
        return f"Cleared PSI for {logger_name} in {field_name}"
    except Exception as e:
        return f"Error clearing {logger_name}: {e}"

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
        field = user_selections[user_id]['field']
        grower = user_selections[user_id]['grower']
        grower_name = grower.name

        for logger in loggers:
            change_logger_soil_type(logger, field, grower_name, soil_type)
        response_text = f"Changing the following:\nLoggers: {', '.join(loggers)} to Soil Type: {soil_type}"
        respond(text=response_text)

        # Log the request to Google Sheets
        request_name = 'Change Soil Type'
        logger_str = ', '.join(loggers)
        info = f'{field} {logger_str} {soil_type}'
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

        grower = user_selections[user_id]['grower']
        field = user_selections[user_id]['field']
        grower_name = grower.name
        logger_list = user_selections[user_id]['logger_select_psi']
        on_or_off = user_selections[user_id].get('psi_on') or user_selections[user_id].get('psi_off')

        # Load pickle data and toggle PSI
        growers = SharedPickle.open_pickle()
        response_text = ''
        for logger in logger_list:
            response_text += toggle_psi(grower_name, field, logger, on_or_off, growers)

        respond(text=response_text)

        # Log request
        SheetsHandler.log_request_to_sheet(
            request_name='Toggle PSI',
            user=body['user']['name'],
            info=f'{field} {logger_list} {on_or_off}'
        )

        # Clean up state
        del user_selections[user_id]



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
        field = user_selections[user_id]['field']
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
def handle_field_select(ack, body, respond):
    ack()
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

    # Show logger menu based on callback_id
    if callback_id == 'field_select_change_soil':
        soil_types = ['Sand (10-5)', 'Loamy Sand (12-5)', 'Sandy Loam (18-8)', 'Sandy Clay Loam (27-17)',
                      'Loam (28-14)', 'Sandy Clay (36-25)', 'Silt Loam (31-11)', 'Silt (30-6)',
                      'Clay Loam (36-22)', 'Silty Clay Loam (38-22)', 'Silty Clay (41-27)', 'Clay (42-30)']
        logger_and_soil_list_menu(ack, respond, logger_list, soil_types)

    elif callback_id == 'field_select_psi':
        user_selections[user_id]['logger_select_psi'] = logger_list
        blocks = logger_and_toggle_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)

    elif callback_id == 'field_select_delete_psi':
        user_selections[user_id]['logger_select_delete_psi'] = logger_list
        blocks = logger_delete_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)

    elif callback_id == 'field_select_prev_day':
        logger_and_dates_menu(ack, respond, logger_list)

    elif callback_id == 'field_select_modify_values':
        user_selections[user_id]['logger_select_modify_values'] = logger_list
        blocks = logger_select_block(logger_list, action_id)
        respond(blocks=blocks)

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


def get_selected_value(state, block_id, action_id):
    """Helper function to get selected value from state."""
    return state[block_id][action_id]['selected_option']['value']

def get_selected_values(state, action_id):
    """Helper function to get multiple selected values from state."""
    for block in state.values():
        if action_id in block:
            return [opt['value'] for opt in block[action_id]['selected_options']]
    return []

def modify_value_selector_menu(respond, logger_list):
    """Show menu for selecting attribute to modify."""
    attributes = [
        "logger_id", "date", "time", "canopy_temperature", "ambient_temperature", "vpd",
        "vwc_1", "vwc_2", "vwc_3", "field_capacity", "wilting_point", "daily_gallons",
        "daily_switch", "daily_hours", "daily_pressure", "daily_inches", "psi",
        "psi_threshold", "psi_critical", "sdd", "rh", "eto", "kc", "etc", "et_hours"
    ]

    blocks = [
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "Choose loggers to modify"},
            "accessory": {
                "type": "multi_static_select",
                "action_id": "logger_select_modify_values",
                "placeholder": {"type": "plain_text", "text": "Select loggers"},
                "options": generate_options(logger_list)
            }
        },
        {
            "type": "input",
            "block_id": "attribute_select_block",
            "label": {"type": "plain_text", "text": "Select attribute to modify"},
            "element": {
                "type": "static_select",
                "action_id": "attribute_select_modify",
                "placeholder": {"type": "plain_text", "text": "Choose an attribute"},
                "options": generate_options(attributes)
            }
        },
        {
            "type": "input",
            "block_id": "value_input_block",
            "label": {"type": "plain_text", "text": "New value"},
            "element": {
                "type": "plain_text_input",
                "action_id": "value_input"
            }
        },
        {
            "type": "input",
            "block_id": "date_picker_block",
            "label": {"type": "plain_text", "text": "Pick date to apply this value"},
            "element": {
                "type": "datepicker",
                "action_id": "modify_date_select"
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Confirm"},
                    "style": "primary",
                    "action_id": "modify_confirm",
                    "value": "modify"
                }
            ]
        }
    ]
    respond(blocks=blocks)

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


@app.action("grower_select_psi")
@app.action("grower_select_change_soil")
@app.action("grower_select_show")
@app.action("grower_select_delete_psi")
@app.action("grower_select_prev_day")
@app.action("grower_select_billing")
@app.action("grower_select_modify_values")
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

    # elif grower_action_id == 'grower_select_billing':
    #     growers = SharedPickle.open_pickle()
    #     result = SheetsHandler.billing_report_new_tab(growers)
    #     link = 'https://docs.google.com/spreadsheets/d/137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k/edit?gid=0#gid=0'
    #     if result:
    #         respond(f'Added {grower.name} to the sheet\nView here: {link}')
    #     elif not result:
    #         respond(f'{grower.name} already in the sheet\nView here: {link}')
    #
    #     # Log the request to Google Sheets
    #     request_name = 'Add Grower Billing'
    #     info = grower.name
    #     SheetsHandler.log_request_to_sheet(request_name, username, info)

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

# def handle_billing(ack, body, respond):

def change_soil_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_change_soil'
    response = {
        "response_type": "in_channel",
        "text": "Change Soil Type",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


def field_list_menu(ack, respond, field_list, action_id):
    ack()
    
    # Use Block Kit for all cases
    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Choose a field"
            },
            "accessory": {
                "type": "static_select" if action_id not in ['field_select_psi', 'field_select_delete_psi'] else "multi_static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select a field" if action_id not in ['field_select_psi', 'field_select_delete_psi'] else "Select field(s)",
                    "emoji": True
                },
                "options": generate_options(field_list),
                "action_id": action_id
            }
        }
    ]
    
    respond(blocks=blocks)


def logger_and_soil_list_menu(ack, respond, logger_list, soil_types):
    ack()
    logger_action_id = 'logger_select_change_soil'
    blocks = [
        logger_select_block(logger_list, logger_action_id),
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Choose Soil Type"
            },
            "accessory": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select Soil Type",
                    "emoji": True
                },
                "options": generate_options(soil_types),
                "action_id": "soil_select"
            }
        }
    ]

    respond(blocks=blocks)


def logger_and_toggle_menu(logger_list, preselected=None, selected_state=None):
    # Define the logger block
    logger_action_id = 'logger_select_psi'
    logger_block = logger_select_block(logger_list, logger_action_id, initial_selected=preselected)

    # Define the toggle block for PSI On/Off
    toggle_block = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Turn PSI On:"
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "PSI On"
                },
                "action_id": "psi_on",
                "value": "on",
                "style": "primary" if selected_state == 'on' else "default"
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Turn PSI Off:"
            },
            "accessory": {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "PSI Off"
                },
                "action_id": "psi_off",
                "value": "off",
                "style": "danger" if selected_state == 'off' else "default"
            }
        }
    ]

    # Confirm button block
    confirm_block = {
        "type": "actions",
        "elements": [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Confirm"
                },
                "style": "primary",
                "action_id": "psi_confirm",
                "value": "confirm"
            }
        ]
    }

    # Combine logger block, toggle block, and confirm block
    blocks = [logger_block] + toggle_block + [confirm_block]
    return blocks


def date_picker_block():
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Choose Start Date"
            },
            "accessory": {
                "type": "datepicker",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select Start Date",
                    "emoji": True
                },
                "action_id": "start_date_select",
                "initial_date": "2025-01-01"  # Optional: initial date for the picker
            }
        },
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Choose End Date (can be the same as Start Date)"
            },
            "accessory": {
                "type": "datepicker",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Select End Date",
                    "emoji": True
                },
                "action_id": "end_date_select",
                "initial_date": "2025-01-02"  # Optional: initial date for the picker
            }
        }
    ]


def vwc_select_block(action_id):
    vwcs = ['VWC 1', 'VWC 2', 'VWC 3']
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "Choose VWC depths to change"
        },
        "accessory": {
            "type": "multi_static_select",
            "placeholder": {
                "type": "plain_text",
                "text": "Select VWCs",
                "emoji": True
            },
            "options": generate_options(vwcs),
            "action_id": action_id
        }
    }


def logger_and_dates_menu(ack, respond, logger_list):
    print("logger_and_dates_menu function called")
    ack()

    # Define the logger block
    logger_action_id = 'logger_select_prev_day'
    logger_block = logger_select_block(logger_list, logger_action_id)
    print(f"logger_block: {logger_block}")

    # Define the vwc picker block
    action_id = 'vwc_depth_select'
    vwc_block = vwc_select_block(action_id)

    # Define the date picker blocks
    date_range_blocks = date_picker_block()

    # Combine logger block and date picker blocks
    blocks = [logger_block] + [vwc_block] + date_range_blocks

    try:
        respond(blocks=blocks)
        print("Response sent successfully")

    except Exception as e:
        print(f"Error in respond function: {e}")


def logger_select_block(logger_list, action_id, initial_selected=None):
    return {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": "Choose logger(s) to change"
        },
        "accessory": {
            "type": "multi_static_select",
            "placeholder": {
                "type": "plain_text",
                "text": "Select logger(s)",
                "emoji": True
            },
            "options": generate_options(logger_list),
            "action_id": action_id,
            **({"initial_options": [opt for opt in generate_options(logger_list) if opt['value'] in initial_selected]} if initial_selected else {})
        }
    }


def main_menu(ack, respond, menu_options):
    ack()

    blocks = [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Menu"
            },
            "accessory": {
                "type": "static_select",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Choose an option",
                    "emoji": True
                },
                "options": generate_options(menu_options),
                "action_id": "menu_select"
            }
        }
    ]

    respond(blocks=blocks)


def get_soil_menu(ack, respond):
    ack()

    blocks = [
        {
            "type": "input",
            "block_id": "soil_input",
            "element": {
                "type": "plain_text_input",
                "action_id": "soil_coordinates_input",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Enter coordinates",
                    "emoji": True
                }
            },
            "label": {
                "type": "plain_text",
                "text": "Get Soil Type",
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
                    "value": "get_soil_type_coordinates",
                    "action_id": "get_soil_type"
                }
            ]
        }
    ]

    respond(
        blocks=blocks,
        text="Get Soil Type"
    )


def show_pickle_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_show'
    response = {
        "response_type": "in_channel",
        "text": "Show Grower Info",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


def use_prev_days_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_prev_day'
    response = {
        "response_type": "in_channel",
        "text": "Use Previous Days VWC",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


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


def grower_select_block(grower_names, action_id):
    return {
        "text": "Choose Grower",
        "fallback": "You are unable to choose an option",
        "color": "#3AA3E3",
        "attachment_type": "default",
        "callback_id": action_id,
        "actions": [
            {
                "name": "grower_list",
                "text": "Pick a grower...",
                "type": "select",
                "options": generate_options(grower_names)
            }
        ]
    }

def get_field_location_menu(ack, respond):
    ack()
    blocks = [
        {
            "type": "input",
            "block_id": "field_location_input",
            "element": {
                "type": "plain_text_input",
                "action_id": "submit_field_number",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Enter MS field number (example: 1416)"
                }
            },
            "label": {
                "type": "plain_text",
                "text": "Get Field Location"
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Submit"
                    },
                    "value": "get_field_location",
                    "action_id": "get_field_location"
                }
            ]
        }
    ]
    respond(blocks=blocks)

def get_coord_location_menu(ack, respond):
    ack()
    blocks = [
        {
            "type": "input",
            "block_id": "field_coord_input",
            "element": {
                "type": "plain_text_input",
                "action_id": "submit_field_coord",
                "placeholder": {
                    "type": "plain_text",
                    "text": "Enter coordinates like: 36.875752, -120.3441"
                }
            },
            "label": {
                "type": "plain_text",
                "text": "Find Field by Coordinate"
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {
                        "type": "plain_text",
                        "text": "Submit"
                    },
                    "value": "get_field_from_coord",
                    "action_id": "get_field_from_coord"
                }
            ]
        }
    ]
    respond(blocks=blocks)


def turn_on_psi_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_psi'

    response = {
        "response_type": "in_channel",
        "text": "Select PSI setting and Grower:",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


def toggle_psi(grower_name, field_name, logger_name, psi_toggle, growers):
    response_text = ''
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            if psi_toggle == 'off':  # if On
                                response_text += f'Turned Off IR for {logger.name}\n'
                                logger.ir_active = False
                            if psi_toggle == 'on':
                                response_text += f'Turned On IR for {logger.name}\n'
                                logger.ir_active = True
    SharedPickle.write_pickle(growers)
    return response_text


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


def change_logger_soil_type(logger_name: str, field_name: str, grower_name: str, new_soil_type: str):
    """
    Single function to change the soil type for a logger in both the pickle and the db

    :param logger_name:
    :param field_name:
    :param grower_name:
    :param new_soil_type:
    """
    print(f'Changing soil type for logger: {logger_name} to {new_soil_type}')

    growers = SharedPickle.open_pickle()
    dbw = DBWriter.DBWriter()
    crop_type = None

    # Change soil type in the pickle
    print('-Changing soil type in the pickle')
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            print('\tFound logger...changing')
                            old_soil_type = logger.soil.soil_type
                            logger.soil.set_soil_type(new_soil_type)
                            field_capacity = logger.soil.field_capacity
                            wilting_point = logger.soil.wilting_point
                            crop_type = logger.crop_type
    SharedPickle.write_pickle(growers)
    print('\tDone with pickle')

    # Change soil type parameters in the DB
    if crop_type:
        print('-Changing soil type in the db')
        field_name_db = dbw.remove_unwanted_chars_for_db_dataset(field_name)
        db_project = dbw.get_db_project(crop_type)
        dml = (f'UPDATE `{db_project}.{field_name_db}.{logger_name}` '
               f'SET field_capacity = {field_capacity}, wilting_point = {wilting_point} '
               f'WHERE TRUE')
        result = dbw.run_dml(dml)
        print(f'\tDone with DB')
        print()
        print(f'Soil type for {logger_name} changed from {old_soil_type} to {new_soil_type}')
        print()
    else:
        print('Error: problem finding logger')

# Entry point for Google Cloud Functions
@flask_app.route('/slack/events', methods=['POST'])
def slack_events():
    return handler.handle(request)


# The function that Google Cloud Functions will call
def slack_bot(request):
    return slack_events()


# If you want to run locally (not needed for Cloud Functions)
# if __name__ == "__main__":
#     flask_app.run(port=int(os.getenv("PORT", 3000)))

# if __name__ == "__main__":
#     app.start(port=int(os.getenv("PORT", 3000)))

# https://seal-app-er6sr.ondigitalocean.app/slack/events
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