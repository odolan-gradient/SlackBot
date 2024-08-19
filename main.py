import os
from collections import defaultdict

import requests
from dotenv import load_dotenv
from flask import Flask, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

import DBWriter
import SharedPickle

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
def main_menu_command(ack, body, respond):
    # Acknowledge command request
    ack()

    # Get the user who invoked the command
    menu_options = ['Get Soil Type', 'Change Soil Type', 'Turn on PSI', 'Show Pickle']
    main_menu(ack, respond, menu_options)


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
        respond(f"Soil type at {coords}: \n\t{soil}")
    else:
        respond("Error: Coordinates should be in the format 'latitude longitude' or 'latitude,longitude'.")


@app.action("soil_select")
@app.action("logger_select_change_soil")
@app.action("logger_select_psi")
def handle_soil_change_selections(ack, body, respond):
    ack()
    user_id = body['user']['id']
    action_id = body['actions'][0]['action_id']

    if action_id == 'soil_select':
        selected_option = body['actions'][0]['selected_option']['value']
        user_selections[user_id][action_id] = selected_option
    elif action_id == 'logger_select_change_soil' or action_id == 'logger_select_psi':  # logger_select
        selected_options = body['actions'][0]['selected_options']
        user_selections[user_id][action_id] = [option['value'] for option in selected_options]

    # Check if both selections are complete
    if 'soil_select' in user_selections[user_id] and 'logger_select_change_soil' in user_selections[user_id]:
        loggers = user_selections[user_id]['logger_select_change_soil']
        soil_type = user_selections[user_id]['soil_select']
        field = user_selections[user_id]['field']
        grower = user_selections[user_id]['grower']
        grower_name = grower.name

        for logger in loggers:
            change_logger_soil_type(logger, field, grower_name, soil_type)
        response_text = f"Changing the following:\nLoggers: {', '.join(loggers)} to Soil Type: {soil_type}"
        respond(text=response_text)

        # Clear the selections for this user
        del user_selections[user_id]

    elif 'logger_select_psi' in user_selections[user_id]:
        loggers = user_selections[user_id]['logger_select_psi']
        field = user_selections[user_id]['field']
        grower = user_selections[user_id]['grower']
        grower_name = grower.name
        for logger in loggers:
            turn_on_psi(grower_name, field, logger)
        response_text = f"Activing IR on following loggers:\nLoggers: {', '.join(loggers)}"
        respond(text=response_text)
        # Clear the selections for this user
        del user_selections[user_id]
    else:
        # If both selections aren't complete, don't respond yet
        pass


@app.action("field_select_change_soil")
@app.action("field_select_psi")
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
        soil_types = ['Sand', 'Loamy Sand', 'Sandy Loam', 'Sandy Clay Loam', 'Loam', 'Sandy Clay', 'Silt Loam', 'Silt',
                      'Clay Loam', 'Silty Clay Loam', 'Silty Clay', 'Clay']
        logger_and_soil_list_menu(ack, respond, logger_list, soil_types)
    elif callback_id == 'field_select_psi':
        psi_action_id = 'logger_select_psi'
        response = {
            "response_type": "in_channel",
            "blocks": [
                logger_select_block(logger_list, psi_action_id)
            ]
        }
        respond(response)


def fix_ampersand(text):
    return text.replace('&amp;', '&')

@app.action("grower_select_psi")
@app.action("grower_select_change_soil")
def handle_grower_menu(ack, body, respond):
    ack()

    # Extract values from the actions
    grower_action_id = body['callback_id']
    selected_value = body['actions'][0]['selected_options'][0]['value']
    # Ampersands come in weird through slack
    selected_value = fix_ampersand(selected_value)
    grower = SharedPickle.get_grower(selected_value)
    field_list = [field.name for field in grower.fields]

    # Add to selections
    user_id = body['user']['id']
    user_selections[user_id]['grower'] = grower

    if grower_action_id == 'grower_select_psi':
        action_id = 'field_select_psi'
    elif grower_action_id == 'grower_select_change_soil':
        action_id = 'field_select_change_soil'

    field_list_menu(ack, respond, field_list, action_id)



@app.action("menu_select")
def handle_main_menu(ack, body, respond):
    ack()

    menu_option = body['actions'][0]['selected_option']['value']
    if menu_option == 'Change Soil Type':
        change_soil_menu(ack, respond)
    elif menu_option == 'Get Soil Type':
        get_soil_menu(ack, respond)
    elif menu_option == 'Turn on PSI':
        turn_on_psi_menu(ack, respond)
    elif menu_option == 'Show Pickle':
        show_pickle_menu(ack, respond)



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
                    "text": "Enter coordinates with a space separating",
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
        "text": "Turn on Logger PSI",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    }

    respond(response)


def show_pickle_menu(ack, respond):
    ack()

    # Get the pickle contents
    data = SharedPickle.open_pickle()
    pickle_contents = "PICKLE CONTENTS\n"

    for d in data:
        print(d)
        # Assuming each object has a to_string() method

        pickle_contents += d + "\n"

    # Send the pickle contents to the Slack channel
    response = {
        "response_type": "in_channel",
        "text": pickle_contents
    }
    respond(response)
def turn_on_psi(grower_name, field_name, logger_name):

    growers = SharedPickle.open_pickle()
    for grower in growers:
        if grower.name == grower_name:
            for field in grower.fields:
                if field.name == field_name:
                    for logger in field.loggers:
                        if logger.name == logger_name:
                            logger.ir_active = True
                            print(f'IR activated for {logger.name}')
    SharedPickle.write_pickle(growers)

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
    SELECT mu.muname, c.cokey
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
                lowercase_input = texture_line.lower()

                matched_soil_type = None

                # Iterate through the list of soil types and check for a match
                for soil_type in soil_types:
                    if soil_type.lower() in lowercase_input:
                        matched_soil_type = soil_type
                        print(f'Found soil type: {lowercase_input}')
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

# # If you want to run locally (not needed for Cloud Functions)
# if __name__ == "__main__":
#     flask_app.run(port=int(os.getenv("PORT", 3000)))

# if __name__ == "__main__":
#     app.start(port=int(os.getenv("PORT", 3000)))
