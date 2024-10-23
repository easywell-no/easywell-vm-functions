# input_handler.py

import sys

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
            print(f"Received coordinates from command line: ({latitude}, {longitude})")
        except ValueError:
            print("Invalid latitude and longitude values provided.")
            return None
    else:
        print("Latitude and longitude not provided as command-line arguments.")
        return None

    input_data = {
        'latitude': latitude,
        'longitude': longitude
    }
    return input_data
