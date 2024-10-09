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

def fetch_well_data(supabase, input_lat, input_lon):
    """
    Fetch wells from the database, calculate distances, and retrieve necessary information.
    Returns:
        dict: Contains 'well_profiles' and 'closest_wells'.
    """
    logging.info("Stage 2: Data Retrieval started.")
    try:
        # Fetch wells from 'wellbore_data' table
        response = supabase.table('wellbore_data').select(
            'wlbwellborename, wlbnsdecdeg, wlbewdecdeg, wlbwelltype, wlbfactpageurl, wlbtotaldepth'
        ).execute()
        all_wells = response.data
        logging.info(f"Fetched {len(all_wells)} wells from 'wellbore_data' table.")
    except Exception as e:
        logging.error(f"Error fetching well data: {e}")
        return None

    # Calculate distances and filter valid wells
    all_well_distances = []
    for well in all_wells:
        if well['wlbnsdecdeg'] is not None and well['wlbewdecdeg'] is not None:
            try:
                well_lat = float(well['wlbnsdecdeg'])
                well_lon = float(well['wlbewdecdeg'])
                distance = haversine(input_lon, input_lat, well_lon, well_lat)
                all_well_distances.append({
                    'well_name': str(well['wlbwellborename']),  # Ensure well_name is a string
                    'latitude': well_lat,
                    'longitude': well_lon,
                    'distance_km': round(distance, 2),
                    'well_type': well['wlbwelltype'],
                    'fact_page_url': well.get('wlbfactpageurl', 'N/A'),
                    'depth_m': well.get('wlbtotaldepth', 'N/A')
                })
            except Exception as e:
                logging.warning(f"Error calculating distance for well {well.get('wlbwellborename')}: {e}")
                continue
    logging.info(f"Calculated distances for {len(all_well_distances)} wells.")

    # Filter to only EXPLORATION wells
    exploration_wells = [well for well in all_well_distances if well['well_type'] == 'EXPLORATION']
    logging.info(f"Found {len(exploration_wells)} exploration wells.")

    # Sort by distance and get the closest wells (e.g., top 5)
    closest_exploration_wells = sorted(exploration_wells, key=lambda x: x['distance_km'])[:5]
    logging.info(f"Selected {len(closest_exploration_wells)} closest exploration wells.")

    well_names = [well['well_name'] for well in closest_exploration_wells]

    # Fetch well profiles for the selected wells
    well_profiles = get_well_profiles(well_names, supabase)
    logging.info(f"Retrieved well profiles for {len(well_profiles)} wells.")

    # Include distance in well profiles
    for well in closest_exploration_wells:
        well_name = well['well_name']
        if well_name in well_profiles:
            well_profiles[well_name]['distance_km'] = well['distance_km']
        else:
            logging.warning(f"Well name '{well_name}' not found in well profiles.")

    logging.info("Stage 2: Data Retrieval completed.")
    return {
        'closest_wells': closest_exploration_wells,
        'well_profiles': well_profiles
    }
