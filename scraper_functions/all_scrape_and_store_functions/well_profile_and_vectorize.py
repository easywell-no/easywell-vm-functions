# utils/well_profile_and_vectorize.py

import logging
from supabase import Client
from utils.well_profiler import get_well_profiles
from utils.vectorize import vectorize_well_profiles

def well_profile_and_vectorize(supabase: Client):
    """
    Retrieve well profiles, paginate through well data, and store their embeddings.

    Args:
        supabase (Client): The Supabase client instance.
    """
    logging.info("Starting well profile and vectorization process.")
    page_size = 1000  # Define page size for pagination
    current_start = 0
    
    while True:
        try:
            # Fetch a page of well names with EXPLORATION type from wellbore_data
            response = supabase.table('wellbore_data')\
                .select('wlbwellborename')\
                .eq('wlbwelltype', 'EXPLORATION')\
                .range(current_start, current_start + page_size - 1)\
                .execute()
            
            well_names = [record['wlbwellborename'] for record in response.data]
            logging.info(f"Fetched {len(well_names)} well names in this batch.")
            
            if not well_names:
                break  # No more data to fetch, exit the loop
            
            # Get well profiles
            well_profiles = get_well_profiles(well_names, supabase)
            logging.info(f"Generated profiles for {len(well_profiles)} wells.")
            
            # Vectorize and store well profiles
            vectorize_well_profiles(well_profiles, supabase)

            # Increment the start point for the next batch
            current_start += page_size
        
        except Exception as e:
            logging.error(f"Error during well profile and vectorization process: {e}", exc_info=True)
            break

    logging.info("Completed well profile and vectorization process.")
