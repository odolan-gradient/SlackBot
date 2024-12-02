import os
from collections import defaultdict

import requests
from dotenv import load_dotenv
from flask import Flask, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

import DBWriter
import SQLScripts
import SharedPickle
import SheetsHandler

load_dotenv()

app = App(
    token=os.getenv("SLACK_BOT_TOKEN"),
    signing_secret=os.getenv("SLACK_SIGNING_SECRET")
)
flask_app = Flask(__name__)
handler = SlackRequestHandler(app)

user_selections = defaultdict(dict)

# Example lists for dropdown options
growers = SharedPickle.open_pickle()
grower_names = [grower.name for grower in growers]
fields = [field.name for grower in growers for field in grower.fields]


@app.command("/menu")
@app.command("/test")
def main_menu_command(ack, body, respond):
    try:
        ack()
        menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Show Pickle', 'Use Previous Days VWC', 'Add Grower Billing']
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


@app.action("menu_select")
def handle_main_menu(ack, body, respond):
    ack()

    menu_option = body['actions'][0]['selected_option']['value']
    if menu_option == 'Change Soil Type':
        change_soil_menu(ack, respond)
    elif menu_option == 'Get Soil Type':
        get_soil_menu(ack, respond)
    elif menu_option == 'Toggle PSI':
        turn_on_psi_menu(ack, respond)
    elif menu_option == 'Show Pickle':
        show_pickle_menu(ack, respond)
    elif menu_option == 'Use Previous Days VWC':
        use_prev_days_menu(ack, respond)
    elif menu_option == 'Add Grower Billing':
        add_billing_menu(ack, respond)


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
        soil = get_soil_type_from_coords(lat, long)

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

        # Log the request to Google Sheets
        request_name = 'Change Soil Type'
        logger_str = ', '.join(loggers)
        info = f'{field} {logger_str} {soil_type}'
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)

        for logger in loggers:
            change_logger_soil_type(logger, field, grower_name, soil_type)
        response_text = f"Changing the following:\nLoggers: {', '.join(loggers)} to Soil Type: {soil_type}"
        respond(text=response_text)

        # Clear the selections for this user
        del user_selections[user_id]

    else:
        # If both selections aren't complete, don't respond yet
        pass


@app.action("logger_select_psi")
@app.action("psi_on")
@app.action("psi_off")
def handle_toggle_psi(ack, body, respond):
    ack()
    user_id = body['user']['id']
    action_id = ''
    if 'action_id' in body['actions'][0]:
        action_id = body['actions'][0]['action_id']

    if action_id == 'logger_select_psi':  # logger_select
        selected_options = body['actions'][0]['selected_options']
        user_selections[user_id][action_id] = [option['value'] for option in selected_options]
    elif action_id in ['psi_on', 'psi_off']:  # on or off
        user_selections[user_id][action_id] = body['actions'][0]['value']

    if 'logger_select_psi' in user_selections[user_id] and ('psi_on' in user_selections[user_id] or 'psi_off' in user_selections[user_id]):
        loggers = user_selections[user_id]['logger_select_psi']
        field = user_selections[user_id]['field']
        grower = user_selections[user_id]['grower']
        grower_name = grower.name
        if 'psi_on' in user_selections[user_id]:
            on_or_off = user_selections[user_id]['psi_on']
        elif 'psi_off' in user_selections[user_id]:
            on_or_off = user_selections[user_id]['psi_off']
        # Log the request to Google Sheets
        request_name = 'Toggle PSI'
        info = f'{field} {loggers} {on_or_off}'
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)

        response_text = ''
        for logger in loggers:
            response_text += toggle_psi(grower_name, field, logger, on_or_off)
        SharedPickle.write_pickle(growers)
        respond(text=response_text)
        # Clear the selections for this user
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
        print(vwcs)

        # Log the request to Google Sheets
        request_name = 'Use Previous Days VWC'
        info = ', '.join(loggers) + vwcs
        username = body['user']['name']
        SheetsHandler.log_request_to_sheet(request_name, username, info)

        for logger in loggers:
            SQLScripts.update_vwc_for_date_range(project, field, logger, start_date, end_date, vwcs)
        response_text = f"Using previous day VWC for the following:\nLoggers: {', '.join(loggers)} at {vwcs}"
        respond(text=response_text)

        # Clear the selections for this user
        del user_selections[user_id]

    else:
        # If both selections aren't complete, don't respond yet
        pass


@app.action("field_select_change_soil")
@app.action("field_select_psi")
@app.action("field_select_prev_day")
def handle_field_select(ack, body, respond):
    ack()
    # Extract values from the actions
    callback_id = body['callback_id']
    field = body['actions'][0]['selected_options'][0]['value']
    field = fix_ampersand(field)
    respond(f"You selected field: {field}")
    field_obj = SharedPickle.get_field(field)

    # Add to selections
    user_id = body['user']['id']
    user_selections[user_id]['field'] = field

    logger_list = [logger.name for logger in field_obj.loggers]
    if callback_id == 'field_select_change_soil':
        soil_types = ['Sand (10-5)', 'Loamy Sand (12-5)', 'Sandy Loam (18-8)', 'Sandy Clay Loam (27-17)', 'Loam (28-14)', 'Sandy Clay (36-25)', 'Silt Loam (31-11)', 'Silt (30-6)',
                      'Clay Loam (36-22)', 'Silty Clay Loam (38-22)', 'Silty Clay (41-27)', 'Clay (42-30)']
        logger_and_soil_list_menu(ack, respond, logger_list, soil_types)

    elif callback_id == 'field_select_psi':
        # psi_action_id = 'logger_select_psi'
        blocks = logger_and_toggle_menu(logger_list)
        response = {
            "response_type": "in_channel",
            "blocks": blocks
        }
        respond(response)
        # logger_and_toggle_menu(ack, respond, logger_list)
    elif callback_id == 'field_select_prev_day':
        logger_and_dates_menu(ack, respond, logger_list)


def fix_ampersand(text):
    return text.replace('&amp;', '&')


@app.action("grower_select_psi")
@app.action("grower_select_change_soil")
@app.action("grower_select_show")
@app.action("grower_select_prev_day")
@app.action("grower_select_billing")
def handle_grower_menu(ack, body, client, respond):
    ack()

    # Extract values from the actions
    grower_action_id = body['callback_id']
    selected_value = body['actions'][0]['selected_options'][0]['value']
    # Ampersands come in weird through slack
    selected_value = fix_ampersand(selected_value)
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

    elif grower_action_id == 'grower_select_prev_day':
        action_id = 'field_select_prev_day'
        field_list_menu(ack, respond, field_list, action_id)

    elif grower_action_id == 'grower_select_billing':
        result = SheetsHandler.billing_report(grower.name)
        link = 'https://docs.google.com/spreadsheets/d/137KpyvSKY_LCqiups4EAcwMQPYHV_a55bjwRQAMEX_k/edit?gid=0#gid=0'
        if result:
            respond(f'Added {grower.name} to the sheet\nView here: {link}')
        elif not result:
            respond(f'{grower.name} already in the sheet\nView here: {link}')

        # Log the request to Google Sheets
        request_name = 'Add Grower Billing'
        info = grower.name
        SheetsHandler.log_request_to_sheet(request_name, username, info)

    elif grower_action_id == 'grower_select_show':
        # Log the request to Google Sheets
        request_name = 'Show Pickle'
        info = selected_value
        SheetsHandler.log_request_to_sheet(request_name, username, info)

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


def change_soil_menu(ack, respond):
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

    response = {
        "response_type": "in_channel",
        "text": "Choose Grower Field",
        "attachments": [
            {
                "text": "Choose Grower Field",
                "fallback": "You are unable to choose an option",
                "color": "#3AA3E3",
                "attachment_type": "default",
                "callback_id": action_id,
                "actions": [
                    {
                        "name": "field_list",
                        "text": "Pick a field...",
                        "type": "select",
                        "options": generate_options(field_list)
                    }
                ]
            },
        ]
    }

    respond(response)


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


def logger_and_toggle_menu(logger_list):

    # Define the logger block
    logger_action_id = 'logger_select_psi'
    logger_block = logger_select_block(logger_list, logger_action_id)

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
                "style": "primary"
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
                "style": "danger"
            }
        }
    ]

    # Combine logger block and toggle block
    blocks = [logger_block] + toggle_block
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
                "initial_date": "2024-01-01"  # Optional: initial date for the picker
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
                "initial_date": "2024-01-02"  # Optional: initial date for the picker
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


def logger_select_block(logger_list, action_id):
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
            "action_id": action_id
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


def show_pickle_menu(ack, respond):
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


def use_prev_days_menu(ack, respond):
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

def add_billing_menu(ack, respond):
    ack()
    action_id = 'grower_select_billing'
    response = {
        "response_type": "in_channel",
        "text": "Add Grower to Billing Sheet",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


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


def turn_on_psi_menu(ack, respond):
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


def toggle_psi(grower_name, field_name, logger_name, psi_toggle):
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
                                continue
                            if psi_toggle == 'on':
                                response_text += f'Turned On IR for {logger.name}\n'
                                logger.ir_active = True
                                continue
    return response_text


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
    dbw = DBWriter()

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


def get_soil_type_from_coords(latitude, longitude):
    """
    Grabs soil type from ADA API given lat, long
    :param latitude:
    :param longitude:
    :return:
    """
    point_wkt = f"POINT({longitude} {latitude})"
    # SQL query to get soil texture information
    query = f"""
    SELECT mu.muname, c.localphase
    FROM mapunit AS mu
    JOIN component AS c ON c.mukey = mu.mukey
    JOIN chorizon AS ch ON ch.cokey = c.cokey
    WHERE mu.mukey IN (
            SELECT DISTINCT mukey
            FROM SDA_Get_Mukey_from_intersection_with_WktWgs84('{point_wkt}')
        )
    """

    # SDA request payload
    request_payload = {
        "format": "JSON+COLUMNNAME+METADATA",
        "query": query
    }

    sda_url = "https://sdmdataaccess.sc.egov.usda.gov/Tabular/SDMTabularService/post.rest"
    headers = {'Content-Type': 'application/json'}
    response = requests.post(sda_url, json=request_payload, headers=headers)

    # soil types intentionally formatted, longest to shortest length
    # so when we check if soil_type in response_string
    soil_types = ['Silty Clay Loam', 'Sandy Clay Loam',
                  'Sandy Clay', 'Silt Loam', 'Clay Loam',
                  'Silty Clay', 'Loamy Sand', 'Sandy Loam',
                  'Sand', 'Clay', 'Loam', 'Silt']

    if response.status_code == 200:
        data = response.json()
        if "Table" in data:
            # the row in the json containing soil texture information
            if data["Table"][2]:
                texture_line = data["Table"][2][0]
                second_texture_descrip = data["Table"][2][1]
                lowercase_texture = texture_line.lower()
                lowercase_second_texture = second_texture_descrip.lower()

                matched_soil_type = None

                # Iterate through the list of soil types and check for a match
                for soil_type in soil_types:
                    if soil_type.lower() in lowercase_texture:
                        matched_soil_type = soil_type
                        print(f'Found soil type: {lowercase_texture}')
                        break
                    elif soil_type.lower() in lowercase_second_texture:
                        matched_soil_type = soil_type
                        print(f'Found soil type: {lowercase_second_texture}')
                        break

                return matched_soil_type
        else:
            print("No soil information found for the given coordinates.")
    else:
        print(f"Error: {response.status_code}, {response.text}")




# Entry point for Google Cloud Functions
@flask_app.route('/slack/events', methods=['POST'])
def slack_events():
    return handler.handle(request)


# The function that Google Cloud Functions will call
def slack_bot(request):
    return slack_events()


# If you want to run locally (not needed for Cloud Functions)
if __name__ == "__main__":
    flask_app.run(port=int(os.getenv("PORT", 3000)))

# if __name__ == "__main__":
#     app.start(port=int(os.getenv("PORT", 3000)))

# https://seal-app-er6sr.ondigitalocean.app/slack/events
# interactivity is the one that determines the debug
