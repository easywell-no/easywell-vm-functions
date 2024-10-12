# all_generate_report_functions/data_retrieval.py

import logging
import math
from utils.well_profiler import get_well_profiles

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great-circle distance between two points on the Earth using the Haversine formula.
    
    Args:
        lon1 (float): Longitude of the first point.
        lat1 (float): Latitude of the first point.
        lon2 (float): Longitude of the second point.
        lat2 (float): Latitude of the second point.
    
    Returns:
        float: Distance in kilometers between the two points.
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

def fetch_well_names(supabase, input_lat, input_lon, max_distance_km=50, max_wells=3, batch_size=1000):
    """
    Fetch well names within a specified distance from input coordinates.
    Implements pagination to fetch all wells in the database.
    
    Args:
        supabase (Client): Supabase client instance.
        input_lat (float): Latitude of the input location.
        input_lon (float): Longitude of the input location.
        max_distance_km (int, optional): Maximum distance in kilometers. Defaults to 50.
        max_wells (int, optional): Maximum number of wells to retrieve. Defaults to 3.
        batch_size (int, optional): Number of wells to fetch per request. Defaults to 1000.
    
    Returns:
        Tuple[List[str], Dict[str, float]]: A tuple containing a list of well names and a dictionary mapping well names to their distances.
    """
    logging.info("Stage 2: Data Retrieval started.")
    well_names = []
    distance_map = {}
    offset = 0
    while True:
        try:
            # Fetch a batch of wells from 'wellbore_data' table
            response = supabase.table('wellbore_data').select(
                'wlbwellborename, wlbnsdecdeg, wlbewdecdeg'
            ).range(offset, offset + batch_size - 1).execute()
            batch = response.data
            fetched = len(batch)
            
            if fetched == 0:
                logging.info("No more wells to fetch from the database.")
                break  # No more data to fetch
            
            logging.info(f"Fetched {fetched} wells from 'wellbore_data' table (offset {offset}).")
            
            for well in batch:
                well_name = well.get('wlbwellborename')
                well_nsdecdeg = well.get('wlbnsdecdeg')
                well_ewdecdeg = well.get('wlbewdecdeg')
                
                if well_nsdecdeg is None or well_ewdecdeg is None:
                    logging.warning(f"Well '{well_name}' has missing coordinates. Skipping.")
                    continue
                
                try:
                    well_lat = float(well_nsdecdeg)
                    well_lon = float(well_ewdecdeg)
                    distance = haversine(input_lon, input_lat, well_lon, well_lat)
                    
                    if distance <= max_distance_km:
                        if well_name not in well_names:
                            well_names.append(well_name)
                            distance_map[well_name] = round(distance, 2)
                            logging.info(f"Found well '{well_name}' within {distance_map[well_name]} km.")
                            
                            if len(well_names) >= max_wells:
                                logging.info(f"Reached maximum number of wells ({max_wells}). Stopping fetch.")
                                return well_names, distance_map
                except ValueError:
                    logging.warning(f"Invalid coordinates for well '{well_name}'. Skipping.")
                    continue
                except Exception as e:
                    logging.warning(f"Error processing well '{well_name}': {e}")
                    continue
            
            offset += batch_size  # Move to the next batch
        except Exception as e:
            logging.error(f"Error fetching wells from Supabase: {e}")
            break  # Exit the loop on error
    
    logging.info(f"Total wells found within {max_distance_km} km: {len(well_names)}.")
    if not well_names:
        logging.warning("No wells found within the specified distance.")
    
    return well_names, distance_map

def get_well_profiles_with_distance(well_names, distance_map, supabase):
    """
    Retrieve well profiles using the well_profiler utility and include distance information.
    
    Args:
        well_names (List[str]): List of well names.
        distance_map (Dict[str, float]): Dictionary mapping well names to their distances.
        supabase (Client): Supabase client instance.
    
    Returns:
        Dict[str, Dict]: Dictionary of well profiles with distance information included.
    """
    if not well_names:
        logging.error("No well names provided for profiling.")
        return {}
    
    # Retrieve well profiles
    well_profiles = get_well_profiles(well_names, supabase)
    
    # Add distance_km to each well profile
    for well_name in well_profiles:
        well_profiles[well_name]['distance_km'] = distance_map.get(well_name, 'N/A')
    
    logging.info(f"Retrieved well profiles for {len(well_profiles)} wells.")
    return well_profiles
