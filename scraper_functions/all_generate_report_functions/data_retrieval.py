# data_retrieval.py

import logging
import math
from typing import List, Dict

def haversine_distance(lat1, lon1, lat2, lon2):
    # Earth radius in kilometers
    R = 6371.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    delta_phi = math.radians(lat2 - lat1)
    delta_lambda = math.radians(lon2 - lon1)
    a = math.sin(delta_phi / 2) ** 2 + \
        math.cos(phi1) * math.cos(phi2) * \
        math.sin(delta_lambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

def get_nearby_wells(supabase, user_latitude, user_longitude, radius_km=50, limit=5) -> List[str]:
    """
    Retrieves wells within a specified radius from the user's location.
    Only includes wells of type 'EXPLORATION'.
    """
    try:
        # Fetch wells with coordinates where wlbwelltype is 'EXPLORATION'
        response = supabase.table('well_coordinates').select(
            'wlbwellborename, wlbnsdecdeg, wlbewdecdeg'
        ).eq('wlbwelltype', 'EXPLORATION').execute()

        # Check if the response has data
        wells = response.data
        if not wells:
            logging.error("No wells found in the well_coordinates table with type 'EXPLORATION'.")
            return []

    except Exception as e:
        logging.error(f"Error fetching well coordinates: {e}")
        return []

    nearby_wells = []
    for well in wells:
        well_lat = well['wlbnsdecdeg']
        well_lon = well['wlbewdecdeg']
        if well_lat is None or well_lon is None:
            continue  # Skip wells with missing coordinates
        distance = haversine_distance(user_latitude, user_longitude, well_lat, well_lon)
        if distance <= radius_km:
            nearby_wells.append({
                'wlbwellborename': well['wlbwellborename'],
                'distance': distance
            })

    if not nearby_wells:
        logging.error("No nearby wells found within the specified radius.")
        return []

    # Sort wells by distance and limit to the specified number
    nearby_wells.sort(key=lambda x: x['distance'])
    well_names = [well['wlbwellborename'] for well in nearby_wells[:limit]]

    return well_names

def get_similar_wells(supabase, well_names: List[str], top_k=5) -> List[str]:
    """
    Retrieves wells similar to the given wells using embeddings.
    Ensures that the returned wells are not in the original list.
    """
    try:
        # Get embeddings for the given wells
        response = supabase.table('well_profiles').select(
            'wlbwellborename, vector'
        ).in_('wlbwellborename', well_names).execute()

        embeddings = response.data
        if not embeddings:
            logging.error("No embeddings found for the given wells.")
            return []

    except Exception as e:
        logging.error(f"Error fetching embeddings for wells: {e}")
        return []

    # Calculate the average embedding
    import numpy as np
    vectors = [well['vector'] for well in embeddings if well['vector'] is not None]
    if not vectors:
        logging.error("No valid embeddings found for the given wells.")
        return []

    average_embedding = np.mean(vectors, axis=0).tolist()

    try:
        # Perform similarity search using the average embedding
        response = supabase.rpc('match_wells', {
            'query_embedding': average_embedding,
            'match_count': top_k + len(well_names)  # Ensure enough wells after excluding
        }).execute()

        similar_wells_data = response.data
        if not similar_wells_data:
            logging.error("No similar wells found using embeddings.")
            return []

    except Exception as e:
        logging.error(f"Error performing similarity search: {e}")
        return []

    similar_wells = [item['wlbwellborename'] for item in similar_wells_data if item['wlbwellborename'] not in well_names]

    # Limit to top_k wells
    return similar_wells[:top_k]

def get_well_profiles(supabase, well_names: List[str]) -> List[Dict]:
    """
    Retrieves well profiles for the given list of well names.
    """
    try:
        response = supabase.table('well_profiles').select(
            'wlbwellborename, well_profile'
        ).in_('wlbwellborename', well_names).execute()

        well_profiles = response.data
        if not well_profiles:
            logging.error("No well profiles found for the specified wells.")
            return []

    except Exception as e:
        logging.error(f"Error fetching well profiles: {e}")
        return []

    return well_profiles
