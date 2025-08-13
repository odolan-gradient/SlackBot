import json
import os
from collections import defaultdict
import re

from dotenv import load_dotenv
from flask import Flask, request
from slack_bolt import App
from slack_bolt.adapter.flask import SlackRequestHandler

import Decagon
import SQLScripts
import SharedPickle
import SheetsHandler
import Soils
import slackFunctions
from SharedPickle import get_project
from blocks import field_list_menu, logger_and_soil_list_menu, logger_and_toggle_menu, logger_and_dates_menu, \
    logger_select_block, main_menu, get_soil_menu, show_pickle_menu, use_prev_days_menu, grower_select_block, \
    get_field_location_menu, get_coord_location_menu, turn_on_psi_menu, change_soil_menu, logger_delete_menu, \
    modify_value_selector_menu, change_gpm_menu, logger_and_gpm_menu, run_field_menu, uninstall_field_menu, \
    uninstall_button_menu, update_irr_menu, generate_irrigation_row_blocks
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
def main_menu_command(ack, body, respond):
    try:
        ack()
        #  Ollie  U06NJRAT1T2
        # Javi 'U4KFKMH8C'
        if body['user_id'] in ['U06NJRAT1T2', 'U4KFKMH8C']:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Delete PSI values', 'Show Pickle', 'Use Previous Days VWC',
                            'Modify Values', 'Add Grower Billing', 'Get Field Location', 'Change GPM / Irr Acres', 'Run Field', 'Uninstall Fields',
                            'Update Irr. Hours']
        else:
            menu_options = ['Get Soil Type', 'Change Soil Type', 'Toggle PSI', 'Delete PSI values', 'Show Pickle', 'Add Grower Billing', 'Get Field Location',
                            'Change GPM / Irr Acres', 'Uninstall Fields', 'Update Irr. Hours']
        main_menu(ack, respond, menu_options)
    except Exception as e:
        print(f"Error: {e}")
        respond("An error occurred while processing your request.")


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
    elif menu_option == 'Uninstall Fields':
        uninstall_field_menu(ack, respond, grower_names)
    elif menu_option == 'Update Irr. Hours':
        update_irr_menu(ack, respond, grower_names)



@app.action("grower_select_psi")
@app.action("grower_select_change_soil")
@app.action("grower_select_show")
@app.action("grower_select_delete_psi")
@app.action("grower_select_prev_day")
@app.action("grower_select_billing")
@app.action("grower_select_modify_values")
@app.action("grower_select_gpm")
@app.action("grower_select_run_field")
@app.action("grower_select_uninstall_field")
@app.action('grower_select_update_irr')
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
    elif grower_action_id == 'grower_select_uninstall_field':
        all_fields = [f.name for f in grower.fields]
        active_fields = [f.name for f in grower.fields if f.active]
        action_id = 'field_select_uninstall_field'
        field_list_menu(ack, respond, field_list=all_fields, action_id=action_id, active_fields=active_fields)
        return
    elif grower_action_id == 'grower_select_update_irr':
        action_id = 'field_select_update_irr'
        field_list_menu(ack, respond, field_list, action_id)
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

@app.action("field_select_change_soil")
@app.action("field_select_psi")
@app.action("field_select_delete_psi")
@app.action("field_select_prev_day")
@app.action("field_select_modify_values")
@app.action("field_select_modify_values")
@app.action("field_select_gpm")
@app.action("field_select_run_field")
@app.action("field_select_update_irr")
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
        print('\tField Select Change Soil')
        soil_types = [
            'Sand (10-5)', 'Loamy Sand (12-5)', 'Sandy Loam (18-8)', 'Sandy Clay Loam (27-17)',
            'Loam (28-14)', 'Sandy Clay (36-25)', 'Silt Loam (31-11)', 'Silt (30-6)',
            'Clay Loam (36-22)', 'Silty Clay Loam (38-22)', 'Silty Clay (41-27)', 'Clay (42-30)'
        ]
        logger_and_soil_list_menu(ack, respond, logger_list, soil_types)

    elif action_id == 'field_select_psi':
        print('\tField Select PSI')
        user_selections[user_id]['logger_select_psi'] = logger_list
        blocks = logger_and_toggle_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)

    elif action_id == 'field_select_delete_psi':
        print('\tField Delete PSI')
        user_selections[user_id]['logger_select_delete_psi'] = logger_list
        blocks = logger_delete_menu(logger_list, preselected=logger_list)
        respond(blocks=blocks)

    elif action_id == 'field_select_prev_day':
        print('\tField Prev Day')
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
    elif action_id == 'field_select_update_irr':
        user_selections[user_id]['loggers'] = logger_list
        blocks = logger_select_block(logger_list, action_id='logger_select_update_irr')
        respond(blocks=[blocks])

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

    elif action_id in ['logger_select_change_soil']:  # logger_select
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
@app.action("single_date_picker")
@app.action("confirm_dates")
def handle_prev_day_selections(ack, body, respond):
    ack()
    user_id   = body["user"]["id"]
    action_id = body["actions"][0]["action_id"]
    # ensure we have a bucket for this user
    user_selections.setdefault(user_id, {})

    # 1) collect each selection
    if action_id == "logger_select_prev_day":
        opts = body["actions"][0]["selected_options"]
        user_selections[user_id][action_id] = [o["value"] for o in opts]

    elif action_id in ("start_date_select", "end_date_select"):
        user_selections[user_id][action_id] = body["actions"][0]["selected_date"]

    elif action_id == "vwc_depth_select":
        opts = body["actions"][0]["selected_options"]
        user_selections[user_id][action_id] = [o["value"] for o in opts]

    elif action_id == "single_date_picker":
        # store the one‑off override day
        user_selections[user_id]["optional_day"] = body["actions"][0]["selected_date"]

    # 2) if this wasn’t the Confirm button, bail out now
    if action_id != "confirm_dates":
        return

    # 3) Confirm was clicked → gather everything
    sel = user_selections[user_id]
    loggers      = sel.get("logger_select_prev_day", [])
    vwcs         = sel.get("vwc_depth_select", [])
    start_date   = sel.get("start_date_select")
    end_date     = sel.get("end_date_select")
    optional_day = sel.get("optional_day")  # may be None

    # simple validation
    if not loggers or not vwcs or not start_date or not end_date:
        respond(text="❗️ Please pick loggers, VWCs, and dates before confirming.")
        return

    # fetch project & metadata
    fields    = sel["fields"]
    field = fields[0]
    grower   = sel["grower"]
    project  = SharedPickle.get_project(field, grower.name)
    username = body["user"]["name"]

    # run the SQL updates
    for lg in loggers:
        SQLScripts.update_vwc_for_date_range(
            project,
            field,
            lg,
            start_date,
            end_date,
            vwcs,
            optional_day=optional_day  # pass it along if your function supports it
        )

    # final acknowledgement
    msg = (
        f"✔️ Applied VWCs {vwcs} for loggers {', '.join(loggers)} "
        f"from {start_date} to {end_date}"
    )
    if optional_day:
        msg += f"  (using {optional_day})"

    respond(text=msg)

    # log to Sheets
    info = f"{','.join(loggers)} @ {vwcs}"
    SheetsHandler.log_request_to_sheet("Use Previous Days VWC", username, info)

    # clean up
    del user_selections[user_id]


@app.action("field_select_uninstall_field")
def handle_field_select_uninstall(ack, body, respond):
    ack()
    user_id = body["user"]["id"]
    action  = body["actions"][0]
    selected = [opt["value"] for opt in action["selected_options"]]

    # Determine active/inactive for this grower
    grower = user_selections[user_id]["grower"]
    all_fields    = [f.name for f in grower.fields]
    active_fields = [f.name for f in grower.fields if f.active]

    # If they picked any inactive, error + re‑show menu
    invalid = [f for f in selected if f not in active_fields]
    if invalid:
        # a) error text
        err_text = (
                "⚠️ You can’t uninstall these inactive fields:\n"
                + "\n".join(f"• {f}" for f in invalid)
        )
        # b) show the summary + back/confirm buttons (using accumulated fields, unchanged)
        acc = user_selections[user_id].get("fields_accumulated", [])
        summary = "\n".join(f"• {f}" for f in acc) or "(no fields yet)"
        blocks = [{"type": "section", "text": {"type": "mrkdwn", "text": err_text}}] + uninstall_button_menu(ack, respond, summary)
        return respond(blocks=blocks)
    # 4) Accumulate only the valid picks
    user_selections[user_id].setdefault("fields_accumulated", []).extend(selected)
    acc = user_selections[user_id]["fields_accumulated"]
    # dedupe in-place
    acc[:] = list(dict.fromkeys(acc))

    # 5) Show summary + back/confirm buttons
    picked_md = "\n".join(f"• {f}" for f in acc)
    blocks = uninstall_button_menu(ack, respond, picked_md)
    respond(blocks=blocks)
    return None


@app.action("uninstall_add_grower")
def handle_uninstall_add_grower(ack, body, respond):
    ack()
    user_id = body["user"]["id"]
    # drop only the last grower/fields so they can pick a new grower
    user_selections[user_id].pop("grower", None)
    user_selections[user_id].pop("fields",  None)
    # re‑prompt grower (your existing uninstall_field_menu will work)
    uninstall_field_menu(ack, respond, [g.name for g in SharedPickle.open_pickle()])

@app.action("uninstall_finish")
def handle_uninstall_finish(ack, body, respond):
    ack()
    user_id = body["user"]["id"]
    fields = user_selections[user_id].get("fields_accumulated", [])
    if not fields:
        return respond("⚠️ No fields selected to uninstall.")
    results = slackFunctions.uninstall_fields(fields)
    respond(text="✅ Uninstalled:\n" + "\n".join(results))
    SheetsHandler.log_request_to_sheet(
        "Uninstall Fields",
        body["user"]["name"],
        f"{results}"
    )
    user_selections.pop(user_id, None)

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
        return None
    if action_id == "delete_end_date":
        user_selections[user_id]["delete_end_date"] = body["actions"][0]["selected_date"]
        return None

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
        return None
    return None


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
        # TODO make it so it returns what rows were added then log that instead of nothing
        SheetsHandler.log_request_to_sheet(request_name, 'need to fix', '')
    except Exception as e:
        respond(f'Error: {e} let Ollie know por favor')

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

@app.action("logger_select_update_irr")
def handle_logger_select_update_irr(ack, body, client, respond):
    ack()
    user_id = body["user"]["id"]
    user_name = body["user"]["name"]
    action = body["actions"][0]
    selected = [action["selected_option"]["value"]]
    user_selections[user_id]["loggers"] = selected

    # metadata needed for respond()
    channel_id = body["channel"]["id"]

    if "selected_option" in action:
        loggers = [action["selected_option"]["value"]]
    else:
        loggers = [opt["value"] for opt in action["selected_options"]]

    user_selections[user_id]["loggers"] = loggers
    metadata = json.dumps({
        "grower": user_selections[user_id]["grower"].name,
        "field": user_selections[user_id]["fields"][0],
        "loggers": loggers,
        "channel": channel_id,
        "user": user_id,
        "user_name": user_name,})

    # open modal
    client.views_open(
        trigger_id=body["trigger_id"],
        view={
            "type": "modal",
            "callback_id": "update_irr_hours_modal",
            "title": {"type": "plain_text", "text": "Update Irrigation Hours", "emoji": True},
            "submit": {"type": "plain_text", "text": "Submit", "emoji": True},
            "private_metadata": metadata,
            "blocks": [
                *generate_irrigation_row_blocks(0),
                {
                    "type": "actions",
                    "block_id": "add_row_block",
                    "elements": [
                        {
                            "type": "button",
                            "action_id": "add_irr_row",
                            "text": {"type": "plain_text", "text": "Add another date", "emoji": True},
                            "value": "0",
                        }
                    ],
                },
            ],
        },
    )

# Handle “Add another date” clicks
@app.action("add_irr_row")
def handle_add_irr_row(ack, body, client):
    ack()
    view = body["view"]
    blocks = view["blocks"]
    # find next index
    existing = [int(b["block_id"].split("_")[-1])
                for b in blocks if b.get("block_id", "").startswith("hours_block_")]
    next_idx = (max(existing) + 1) if existing else 0

    # insert new row before the add‑button block
    insert_at = next(i for i,b in enumerate(blocks) if b.get("block_id") == "add_row_block")
    new_blocks = generate_irrigation_row_blocks(next_idx)
    updated = blocks[:insert_at] + new_blocks + blocks[insert_at:]

    client.views_update(
        view_id=view["id"],
        hash=view["hash"],
        view={
            "type": view["type"],
            "callback_id": view["callback_id"],
            "private_metadata": view["private_metadata"],
            "title": view["title"],
            "submit": view["submit"],
            "blocks": updated,
        },
    )

# whenever any date_picker is used, just ack the action
@app.action(re.compile(r"date_picker_\d+"))
def handle_any_date_picker(ack, body):
    ack()

@app.view("update_irr_hours_modal")
def handle_update_irr_hours_submission(ack, body, client):
    ack()
    view = body["view"]
    meta = json.loads(view["private_metadata"])
    grower = meta["grower"]
    field = meta["field"]
    logger = meta["loggers"][0]
    channel = meta["channel"]
    user_id = meta["user"]
    user_name = meta["user_name"]
    state = view["state"]["values"]
    entries = []

    # gather all rows
    for block_id, action in state.items():
        if block_id.startswith("hours_block_"):
            idx = block_id.split("_")[-1] #checks which entry cuz there can be multiple dates
            hrs = float(action[f"hours_input_{idx}"]["value"])
            dt  = state[f"date_block_{idx}"][f"date_picker_{idx}"]["selected_date"]
            entries.append({"date": dt, "hours": hrs})

    # grower = user_selections[user_id]["grower"].name
    # field   = user_selections[user_id]["fields"][0]
    # logger = user_selections[user_id]["loggers"][0]
    project = get_project(field, grower)

    # TODO: actually write entries into your DB or pickle here
    for entry in entries:
        SQLScripts.update_irrigation_hours_for_date(project, field, logger, daily_hours=entry["hours"], date=entry["date"])

    summary = "\n".join(f"• {e['date']}: {e['hours']}h" for e in entries)

    client.chat_postEphemeral(channel = channel,user = user_id,text = f"*Updated irrigation for* `{grower}` / `{field}` / `{logger}`:\n{summary}")
    SheetsHandler.log_request_to_sheet('Update Irr Hours', user_name, f'{field} / {logger}: {summary}"')
    user_selections.pop(user_id, None)

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
