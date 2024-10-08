# all_generate_report_functions/data_retrieval.py

import logging
import math
import pandas as pd

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
        dict: Contains 'closest_wells', 'general_info', 'wellbore_history'.
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

    # Fetch general_info for the selected wells
    try:
        fields_to_display = [
            'wlbwellborename', 'type', 'boreoperatoer', 'totalt_vertikalt_dybde_tvd_m_rkb', 'borestart',
            'boreslutt', 'funn', 'eldste_penetrerte_formasjon', 'vanndybde_m_m',
            'avstand_boredekk_m_m', 'ns_grader', 'ov_grader', 'faktakart_i_nytt_vindu'
        ]
        response = supabase.table('general_info').select(', '.join(fields_to_display)).in_('wlbwellborename', well_names).execute()
        general_info_data = response.data
        logging.info(f"Fetched general info for {len(general_info_data)} wells.")
    except Exception as e:
        logging.error(f"Error fetching general info: {e}")
        general_info_data = []

    # Convert general_info to DataFrame
    df_general_info = pd.DataFrame(general_info_data)
    if df_general_info.empty:
        logging.warning("No general info data found for the selected wells.")
    else:
        df_general_info.fillna('N/A', inplace=True)
        df_general_info['wlbwellborename'] = df_general_info['wlbwellborename'].astype(str)

    # Fetch wellbore_history for the selected wells
    try:
        response = supabase.table('wellbore_history').select('*').in_('wlbwellborename', well_names).execute()
        history_data = response.data
        logging.info(f"Fetched wellbore history for {len(history_data)} records.")
    except Exception as e:
        logging.error(f"Error fetching wellbore history: {e}")
        history_data = []

    # Convert wellbore_history to DataFrame
    df_wellbore_history = pd.DataFrame(history_data)
    if df_wellbore_history.empty:
        logging.warning("No wellbore history data found for the selected wells.")
    else:
        df_wellbore_history.fillna('N/A', inplace=True)
        df_wellbore_history['wlbwellborename'] = df_wellbore_history['wlbwellborename'].astype(str)

    logging.info("Stage 2: Data Retrieval completed.")
    return {
        'closest_wells': closest_exploration_wells,
        'general_info': df_general_info,
        'wellbore_history': df_wellbore_history
    }
