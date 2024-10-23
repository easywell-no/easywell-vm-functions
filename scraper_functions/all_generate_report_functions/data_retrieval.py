# data_retrieval.py

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

def get_nearby_wells(supabase, user_latitude, user_longitude, radius_km=100, limit=5) -> List[Dict]: #10k is a close well, 100k is far
    """
    Retrieves wells within a specified radius from the user's location.
    Only includes wells of type 'EXPLORATION'.
    Returns a list of dictionaries with well name and distance.
    """
    try:
        # Fetch wells with coordinates where wlbwelltype is 'EXPLORATION'
        response = supabase.table('well_coordinates').select(
            'wlbwellborename, wlbnsdecdeg, wlbewdecdeg'
        ).eq('wlbwelltype', 'EXPLORATION').execute()

        wells = response.data
        if not wells:
            print("No wells found in the well_coordinates table with type 'EXPLORATION'.")
            return []

    except Exception as e:
        print(f"Error fetching well coordinates: {e}")
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
        print("No nearby wells found within the specified radius.")
        return []

    # Sort wells by distance and limit to the specified number
    nearby_wells.sort(key=lambda x: x['distance'])
    nearby_wells = nearby_wells[:limit]

    return nearby_wells

def get_similar_wells(supabase, well_names: List[str], top_k=5) -> List[Dict]:
    """
    Retrieves wells similar to the given wells using embeddings.
    Ensures that the returned wells are not in the original list.
    Returns a list of dictionaries with well name and similarity score.
    """
    try:
        # Get embeddings for the given wells
        response = supabase.table('well_profiles').select(
            'wlbwellborename, vector'
        ).in_('wlbwellborename', well_names).execute()

        embeddings = response.data
        if not embeddings:
            print("No embeddings found for the given wells.")
            return []

    except Exception as e:
        print(f"Error fetching embeddings for wells: {e}")
        return []

    # Convert vectors to numpy arrays
    import numpy as np
    vectors = []
    for well in embeddings:
        vector = well['vector']
        if vector is not None:
            if isinstance(vector, list):
                vectors.append(np.array(vector, dtype=float))
            elif isinstance(vector, str):
                vector = vector.strip('[]').split(',')
                vector = [float(x.strip()) for x in vector]
                vectors.append(np.array(vector, dtype=float))
            else:
                print(f"Unexpected vector format for well {well['wlbwellborename']}: {type(vector)}")
                continue

    if not vectors:
        print("No valid embeddings found for the given wells.")
        return []

    # Calculate the average embedding
    try:
        average_embedding = np.mean(vectors, axis=0).tolist()
    except Exception as e:
        print(f"Error calculating average embedding: {e}")
        return []

    try:
        # Perform similarity search using the average embedding
        response = supabase.rpc('match_wells', {
            'query_embedding': average_embedding,
            'match_count': top_k + len(well_names)  # Ensure enough wells after excluding
        }).execute()

        similar_wells_data = response.data
        if not similar_wells_data:
            print("No similar wells found using embeddings.")
            return []

    except Exception as e:
        print(f"Error performing similarity search: {e}")
        return []

    similar_wells = []
    for item in similar_wells_data:
        if item['wlbwellborename'] not in well_names:
            similar_wells.append({
                'wlbwellborename': item['wlbwellborename'],
                'similarity_score': item.get('similarity', 0)  # Assuming 'similarity' is returned
            })

    # Limit to top_k wells
    similar_wells = similar_wells[:top_k]

    return similar_wells

def get_well_profiles(supabase, well_list: List[Dict]) -> List[Dict]:
    """
    Retrieves well profiles for the given list of wells.
    Each well in well_list is a dict with 'wlbwellborename' and other info.
    Returns a list of dicts including the well profile and other data.
    """
    well_names = [well['wlbwellborename'] for well in well_list]
    try:
        response = supabase.table('well_profiles').select(
            'wlbwellborename, well_profile'
        ).in_('wlbwellborename', well_names).execute()

        well_profiles = response.data
        if not well_profiles:
            print("No well profiles found for the specified wells.")
            return []

    except Exception as e:
        print(f"Error fetching well profiles: {e}")
        return []

    # Merge the well profiles with the original well_list data
    well_profiles_dict = {wp['wlbwellborename']: wp for wp in well_profiles}
    result = []
    for well in well_list:
        wlbwellborename = well['wlbwellborename']
        if wlbwellborename in well_profiles_dict:
            merged_well = {**well, **well_profiles_dict[wlbwellborename]}
            result.append(merged_well)

    return result
