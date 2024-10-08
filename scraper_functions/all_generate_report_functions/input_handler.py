# all_generate_report_functions/input_handler.py

def get_user_input(latitude: float, longitude: float):
    """
    Receive user input from generate_report.py.
    Args:
        latitude (float): The latitude coordinate.
        longitude (float): The longitude coordinate.
    Returns:
        dict: A dictionary containing 'latitude' and 'longitude'.
    """
    input_data = {
        'latitude': latitude,
        'longitude': longitude
    }
    return input_data
