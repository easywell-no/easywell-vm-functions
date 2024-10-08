# all_generate_report_functions/input_handler.py

import sys
import logging

def get_user_input():
    """
    Receive user input from command-line arguments.
    Returns:
        dict: A dictionary containing 'latitude' and 'longitude', or None if invalid input.
    """
    if len(sys.argv) >= 3:
        try:
            latitude = float(sys.argv[1])
            longitude = float(sys.argv[2])
            logging.info(f"Received coordinates from command line: ({latitude}, {longitude})")
        except ValueError:
            logging.error("Invalid latitude and longitude values provided.")
            return None
    else:
        logging.error("Latitude and longitude not provided as command-line arguments.")
        return None

    input_data = {
        'latitude': latitude,
        'longitude': longitude
    }
    return input_data
