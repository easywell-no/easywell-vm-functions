# utils/vectorize.py

import logging
from supabase import Client
from utils.get_embedding import get_embedding

def vectorize_well_profiles(well_profiles, supabase: Client):
    """
    Vectorize the well profiles and store embeddings.

    Args:
        well_profiles (dict): A dictionary of well profiles.
        supabase (Client): The Supabase client instance.
    """
    logging.info("Starting vectorization of well profiles.")

    for well_name, profile in well_profiles.items():
        try:
            # Serialize the profile to a string (e.g., JSON or text summary)
            profile_str = str(profile)  # You may want to format this better depending on your use case

            # Generate embedding for the well profile
            embedding = get_embedding(profile_str)
            logging.info(f"Generated embedding for well {well_name}.")

            # Insert the profile and embedding into the profiled_wells table
            data = {
                'wlbwellborename': well_name,
                'well_profile': profile_str,
                'embedded_well_profile': embedding
            }

            supabase.table('profiled_wells').upsert(data).execute()
            logging.info(f"Stored well profile and embedding for {well_name}.")
        except Exception as e:
            logging.error(f"Error vectorizing profile for well {well_name}: {e}")

    logging.info("Completed vectorization of well profiles.")
