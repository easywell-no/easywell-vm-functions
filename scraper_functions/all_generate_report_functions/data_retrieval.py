# all_generate_report_functions/data_retrieval.py

import logging
import math
from utils.well_profiler import get_well_profiles

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great-circle distance between two points on the Earth using the Haversine formula.
    """
    R = 6371  # Earth radius in kilometers
    dlon = math.radians(lon2 - lon1)
    dlat = math.radians(lat2 - lat1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1))
        * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    distance = R * c
    return distance

def fetch_well_names(supabase, input_lat, input_lon, max_distance_km=50, max_wells=5):
    """
    Fetch well names within a specified distance from input coordinates.
    Args:
        supabase: Supabase client.
        input_lat (float): Latitude of the input location.
        input_lon (float): Longitude of the input location.
        max_distance_km (int): Maximum distance in kilometers.
        max_wells (int): Maximum number of wells to retrieve.
    Returns:
        List[str]: List of well names.
    """
    logging.info("Stage 2: Data Retrieval started.")
    try:
        # Fetch wells from 'wellbore_data' table
        response = supabase.table('wellbore_data').select(
            'wlbwellborename, wlbnsdecdeg, wlbewdecdeg'
        ).execute()
        all_wells = response.data
        logging.info(f"Fetched {len(all_wells)} wells from 'wellbore_data' table.")
    except Exception as e:
        logging.error(f"Error fetching well data: {e}")
        return []

    # Calculate distances and filter valid wells
    valid_wells = []
    for well in all_wells:
        if well['wlbnsdecdeg'] is not None and well['wlbewdecdeg'] is not None:
            try:
                well_lat = float(well['wlbnsdecdeg'])
                well_lon = float(well['wlbewdecdeg'])
                distance = haversine(input_lon, input_lat, well_lon, well_lat)
                if distance <= max_distance_km:
                    valid_wells.append({
                        'well_name': str(well['wlbwellborename']),
                        'distance_km': round(distance, 2)
                    })
            except Exception as e:
                logging.warning(f"Error calculating distance for well {well.get('wlbwellborename')}: {e}")
                continue
    logging.info(f"Found {len(valid_wells)} wells within {max_distance_km} km.")

    # Sort by distance and limit to max_wells
    closest_wells = sorted(valid_wells, key=lambda x: x['distance_km'])[:max_wells]
    logging.info(f"Selected {len(closest_wells)} closest wells.")

    well_names = [well['well_name'] for well in closest_wells]
    return well_names

def get_well_profiles(well_names, supabase):
    """
    Retrieve well profiles using the well_profiler utility.
    Args:
        well_names (List[str]): List of well names.
        supabase: Supabase client.
    Returns:
        Dict[str, Dict]: Dictionary of well profiles.
    """
    if not well_names:
        logging.error("No well names provided for profiling.")
        return {}
    well_profiles = get_well_profiles(well_names, supabase)
    return well_profiles
