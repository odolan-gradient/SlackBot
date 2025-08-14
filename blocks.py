from datetime import date

'''
Holds all the json menu blocks for the slack bot, this is what will be shown to the user. The UI
'''

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
def generate_field_options(field_list, active_fields):
    options = []
    for f in field_list:
        if f in active_fields:
            # normal option
            text_obj = {"type": "plain_text", "text": f, "emoji": True}
        else:
            # strikethrough + “(inactive)” hint
            text_obj = {
                "type": "plain_text",
                "text": f"{f} (inactive)",
                "emoji": True
            }
        options.append({
            "text": text_obj,
            "value": f
        })
    return options

def make_confirm_block(action_id: str, button_text: str = "Confirm") -> dict:
    """
    Returns a Slack “Confirm” button wrapped in an `actions` block.
    You can pass in any action_id you like.
    """
    return {
        "type": "actions",
        "block_id": f"{action_id}_block",
        "elements": [
            {
                "type": "button",
                "action_id": action_id,
                "text": {
                    "type": "plain_text",
                    "text": button_text
                },
                "style": "primary"
            }
        ]
    }

def change_gpm_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_gpm'
    respond({
        "response_type": "in_channel",
        "text": "Change GPM / Irr Acres",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    })


def field_list_menu(ack, respond, field_list, action_id, active_fields=None, preselected=None):
    ack()
    # Which actions should be multi‑select?
    multi = ['field_select_psi', 'field_select_delete_psi', 'field_select_uninstall_field']
    is_multi = action_id in multi
    is_uninstall = (action_id == 'field_select_uninstall_field')

    # Build options: either normal or strikethrough for inactive
    if is_uninstall and active_fields is not None:
        options = generate_field_options(field_list, active_fields)
    else:
        options = generate_options(field_list)

    # Figure out which to pre‑select
    initial_opts = []
    if preselected:
        valid_values = {o['value'] for o in options}
        initial_opts = [
            o for o in options
            if o['value'] in preselected and o['value'] in valid_values
        ]

    accessory = {
        "type": "multi_static_select" if is_multi else "static_select",
        "action_id":  action_id,
        "placeholder":{
            "type":"plain_text",
            "text": "Select field(s)" if is_multi else "Select a field",
            "emoji": True
        },
        "options": options,
        **({"initial_options": initial_opts} if initial_opts else {})
    }

    blocks = [
        {
            "type":"section",
            "text":{"type":"mrkdwn","text": "Choose field(s)" if is_multi else "Choose a field"},
            "accessory": accessory
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


def _button(text, action_id, value, style=None):
    btn = {
        "type": "button",
        "text": {"type": "plain_text", "text": text},
        "action_id": action_id,
        "value": value
    }
    if style in ("primary", "danger"):
        btn["style"] = style
    return btn


def logger_and_toggle_menu(logger_list, preselected=None, selected_state=None):
    logger_block = logger_select_block(
        logger_list,
        action_id="logger_select_psi",
        initial_selected=preselected
    )

    blocks = [
        logger_block,
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "Turn PSI On:"},
            "accessory": _button(
                text="PSI On",
                action_id="psi_on",
                value="on",
                style="primary" if selected_state == "on" else None
            )
        },
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": "Turn PSI Off:"},
            "accessory": _button(
                text="PSI Off",
                action_id="psi_off",
                value="off",
                style="danger" if selected_state == "off" else None
            )
        },
        {
            "type": "actions",
            "elements": [
                _button("Confirm", "psi_confirm", "confirm", style="primary")
            ]
        }
    ]
    return blocks


def two_date_picker_block():
    today = date.today().isoformat()  # e.g. "2025-07-02"
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
                "initial_date": today
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
                "initial_date": today
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


# def logger_and_dates_menu(ack, respond, logger_list):
#     print("logger_and_dates_menu function called")
#     ack()
#
#     # Define the logger block
#     logger_action_id = 'logger_select_prev_day'
#     logger_block = logger_select_block(logger_list, logger_action_id)
#     print(f"logger_block: {logger_block}")
#
#     # Define the vwc picker block
#     action_id = 'vwc_depth_select'
#     vwc_block = vwc_select_block(action_id)
#
#     # Define the date picker blocks
#     date_range_blocks = two_date_picker_block()
#     optional_day_block = {
#         "type": "input",
#         "block_id": "use_different_day",
#         "optional": True,   # makes this field optional
#         "label": {
#             "type": "plain_text",
#             "text": "Use a different day"
#         },
#         "element": {
#             "type": "datepicker",
#             "action_id": "single_date_picker",
#             "initial_date": date.today().isoformat(),
#             "placeholder": {
#                 "type": "plain_text",
#                 "text": "Select a date"
#             }
#         }
#     }
#     # Combine logger block and date picker blocks
#     blocks = [logger_block] + [vwc_block] + date_range_blocks
#
#     try:
#         respond(blocks=blocks)
#         print("Response sent successfully")
#
#     except Exception as e:
#         print(f"Error in respond function: {e}")

def logger_and_dates_menu(ack, respond, logger_list):
    ack()

    # … your existing blocks …
    logger_block      = logger_select_block(logger_list, 'logger_select_prev_day')
    vwc_block         = vwc_select_block('vwc_depth_select')
    date_range_blocks = two_date_picker_block()

    # optional single‑day picker:
    optional_day_block = {
        "type": "input",
        "block_id": "use_different_day",
        "optional": True,
        "label": {"type": "plain_text", "text": "Use a different day"},
        "element": {
            "type": "datepicker",
            "action_id": "single_date_picker",
            "initial_date": date.today().isoformat(),
            "placeholder": {
                "type": "plain_text",
                "text": "Select a date"
            }
        }
    }

    # build your confirm block via the helper
    confirm_block = make_confirm_block("confirm_dates", "Confirm")

    blocks = [
        logger_block,
        vwc_block,
        *date_range_blocks,
        optional_day_block,
        confirm_block
    ]

    respond(blocks=blocks)

def logger_and_gpm_menu(ack, respond, logger_list, preselected=None):
    ack()

    # 1) Logger selector with preselection
    options = generate_options(logger_list)
    initial_opts = []
    if preselected:
        valid_values = {o["value"] for o in options}
        initial_opts = [
            o for o in options
            if o["value"] in preselected and o["value"] in valid_values
        ]

    logger_block = {
        "type": "section",
        "block_id": "logger_select_gpm_block",
        "text": {"type": "mrkdwn", "text": "Choose logger(s) to update:"},
        "accessory": {
            "type": "multi_static_select",
            "action_id": "logger_select_gpm",
            "placeholder": {"type": "plain_text", "text": "Select logger(s)", "emoji": True},
            "options": options,
            **({"initial_options": initial_opts} if initial_opts else {})
        }
    }

    # 2) choose which values to update
    fields_block = {
        "type": "input",
        "block_id": "update_fields_block",
        "label": {"type": "plain_text", "text": "Choose values to update", "emoji": True},
        "element": {
            "type": "checkboxes",
            "action_id": "update_fields_select",
            "options": [
                {"text": {"type": "plain_text", "text": "GPM"}, "value": "gpm"},
                {"text": {"type": "plain_text", "text": "Irr Acres"}, "value": "irr_acres"},
            ]
        }
    }

    # 3) new GPM input
    gpm_block = {
        "type": "input",
        "block_id": "gpm_input_block",
        "optional": True,
        "label": {"type": "plain_text", "text": "New GPM value", "emoji": True},
        "element": {
            "type": "plain_text_input",
            "action_id": "gpm_input",
            "placeholder": {"type": "plain_text", "text": "Enter integer GPM"}
        }
    }

    # 4) new Irr Acres input
    irr_block = {
        "type": "input",
        "block_id": "irr_input_block",
        "optional": True,
        "label": {"type": "plain_text", "text": "New Irr Acres value", "emoji": True},
        "element": {
            "type": "plain_text_input",
            "action_id": "irr_input",
            "placeholder": {"type": "plain_text", "text": "Enter integer acres"}
        }
    }

    # 5) confirm button
    confirm_block = make_confirm_block("confirm_gpm_update", "Confirm")

    respond(blocks=[
        logger_block,
        fields_block,
        gpm_block,
        irr_block,
        confirm_block
    ])





def logger_select_block(logger_list, action_id, initial_selected=None):
    # Generate the full list of options
    options = generate_options(logger_list)
    type = "multi_static_select"
    text1 = "Choose logger(s) to change"
    text2 = "Select logger(s)"
    if action_id in ['logger_select_update_irr']:
        type = 'static_select'
        text1 = "Choose logger to update"
        text2 = "Select logger"
    # Filter initial options to only include valid matches
    initial_opts = []
    if initial_selected:
        valid_values = {opt['value'] for opt in options}
        initial_opts = [
            opt for opt in options if opt['value'] in initial_selected and opt['value'] in valid_values
        ]

    block = {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": text1
        },
        "accessory": {
            "type": type,
            "placeholder": {
                "type": "plain_text",
                "text": text2,
                "emoji": True
            },
            "options": options,
            "action_id": action_id
        }
    }

    # Only include initial_options if non-empty and valid
    if initial_opts:
        block["accessory"]["initial_options"] = initial_opts

    return block


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


def logger_delete_menu(logger_list, preselected=None):
    # 1) Logger selector
    options = generate_options(logger_list)
    initial_opts = []
    if preselected:
        valid = {o["value"] for o in options}
        initial_opts = [o for o in options if o["value"] in preselected and o["value"] in valid]

    logger_block = {
        "type": "section",
        "block_id": "delete_logger_block",
        "text": {"type": "mrkdwn", "text": "Choose logger(s) whose PSI to delete:"},
        "accessory": {
            "type": "multi_static_select",
            "action_id": "logger_select_delete_psi",
            "placeholder": {"type": "plain_text", "text": "Select logger(s)", "emoji": True},
            "options": options,
            **({"initial_options": initial_opts} if initial_opts else {})
        }
    }

    # 2) Context/info
    info_block = {
        "type": "context",
        "elements": [
            {"type": "mrkdwn", "text": "*Optional:* specify a date range — leave both blank to wipe *all* PSI values."}
        ]
    }

    # 3) Start‐date picker
    start_date_block = {
        "type": "input",
        "block_id": "delete_start_date_block",
        "optional": True,
        "label": {"type": "plain_text", "text": "Start date", "emoji": True},
        "element": {
            "type": "datepicker",
            "action_id": "delete_start_date",
            "placeholder": {"type": "plain_text", "text": "Select a start date"}
        }
    }

    # 4) End‐date picker
    end_date_block = {
        "type": "input",
        "block_id": "delete_end_date_block",
        "optional": True,
        "label": {"type": "plain_text", "text": "End date", "emoji": True},
        "element": {
            "type": "datepicker",
            "action_id": "delete_end_date",
            "placeholder": {"type": "plain_text", "text": "Select an end date"}
        }
    }

    # 5) Confirm button
    confirm_block = {
        "type": "actions",
        "block_id": "delete_confirm_block",
        "elements": [
            {
                "type": "button",
                "action_id": "delete_confirm",
                "style": "danger",
                "text": {"type": "plain_text", "text": "Confirm Delete", "emoji": True},
                "value": "confirm"
            }
        ]
    }

    return [
        logger_block,
        info_block,
        start_date_block,
        end_date_block,
        confirm_block
    ]


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

def run_field_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_run_field'
    respond({
        "response_type": "in_channel",
        "text": "Run Field",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    })
def uninstall_field_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_uninstall_field'
    respond({
        "response_type": "in_channel",
        "text": "Uninstall Field",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    })

def update_irr_menu(ack, respond, grower_names):
    ack()
    action_id = 'grower_select_update_irr'
    respond({
        "response_type": "in_channel",
        "text": "Update Irr Hours",
        "attachments": [
            grower_select_block(grower_names, action_id)
        ]
    })

def uninstall_button_menu(ack, respond, picked_md):
    # now re‑render two buttons: Back to grower or Confirm uninstall
    return [
        {
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": f"*Fields selected so far:*\n{picked_md}"
            }
        },
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "← Add More Fields"},
                    "action_id": "uninstall_add_grower",
                    "value": "back"
                },
                {
                    "type": "button",
                    "text": {"type": "plain_text", "text": "Confirm Uninstall"},
                    "style": "danger",
                    "action_id": "uninstall_finish",
                    "value": "finish"
                }
            ]
        }
    ]

def generate_irrigation_row_blocks(idx: int):
    return [
        {
            "type": "input",
            "block_id": f"hours_block_{idx}",
            "label": {"type": "plain_text", "text": f"Irrigation Hours #{idx+1}", "emoji": True},
            "element": {
                "type": "plain_text_input",
                "action_id": f"hours_input_{idx}",
                "placeholder": {"type": "plain_text", "text": "e.g. 2.5"},
            },
        },
        {
            "type": "input",
            "block_id": f"date_block_{idx}",
            "label": {"type": "plain_text", "text": f"Date #{idx+1}", "emoji": True},
            "element": {
                "type": "datepicker",
                "action_id": f"date_picker_{idx}",
                "placeholder": {"type": "plain_text", "text": "Select a date"},
            },
        },
    ]
