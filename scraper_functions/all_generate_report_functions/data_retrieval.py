# all_generate_report_functions/data_retrieval.py

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
    """
    # Fetch wells with coordinates
    response = supabase.table('well_coordinates').select(
        'wlbwellborename, wlbnsdecdeg, wlbewdecdeg'
    ).execute()
    if response.error:
        logging.error(f"Error fetching well coordinates: {response.error}")
        return []

    wells = response.data
    nearby_wells = []
    for well in wells:
        well_lat = well['wlbnsdecdeg']
        well_lon = well['wlbewdecdeg']
        distance = haversine_distance(user_latitude, user_longitude, well_lat, well_lon)
        if distance <= radius_km:
            nearby_wells.append({
                'wlbwellborename': well['wlbwellborename'],
                'distance': distance
            })

    # Sort wells by distance and limit to the specified number
    nearby_wells.sort(key=lambda x: x['distance'])
    well_names = [well['wlbwellborename'] for well in nearby_wells[:limit]]

    return well_names

def get_similar_wells(supabase, well_names: List[str], top_k=5) -> List[str]:
    """
    Retrieves wells similar to the given wells using embeddings.
    Ensures that the returned wells are not in the original list.
    """
    # Get embeddings for the given wells
    response = supabase.table('well_profiles').select(
        'wlbwellborename, vector'
    ).in_('wlbwellborename', well_names).execute()
    if response.error:
        logging.error(f"Error fetching embeddings for wells: {response.error}")
        return []

    embeddings = response.data

    # Calculate the average embedding
    import numpy as np
    vectors = [well['vector'] for well in embeddings if well['vector'] is not None]
    if not vectors:
        logging.error("No embeddings found for the given wells.")
        return []

    average_embedding = np.mean(vectors, axis=0).tolist()

    # Perform similarity search using the average embedding
    response = supabase.rpc('match_wells', {
        'query_embedding': average_embedding,
        'match_count': top_k + len(well_names)  # Add len(well_names) to ensure we have enough wells after excluding
    }).execute()
    if response.error:
        logging.error(f"Error performing similarity search: {response.error}")
        return []

    similar_wells = [item['wlbwellborename'] for item in response.data if item['wlbwellborename'] not in well_names]

    # Limit to top_k wells
    return similar_wells[:top_k]

def get_well_profiles(supabase, well_names: List[str]) -> List[Dict]:
    """
    Retrieves well profiles for the given list of well names.
    """
    response = supabase.table('well_profiles').select(
        'wlbwellborename, well_profile'
    ).in_('wlbwellborename', well_names).execute()
    if response.error:
        logging.error(f"Error fetching well profiles: {response.error}")
        return []

    well_profiles = response.data
    return well_profiles
