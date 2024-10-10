# utils/vectorize.py

import logging
import json
from supabase import Client
from utils.get_embedding import get_embedding

def vectorize_well_profiles(well_profiles, supabase: Client):
    logging.info("Starting vectorization of well profiles.")

    for well_name, profile in well_profiles.items():
        if not well_name:
            logging.warning("Encountered a well with no name. Skipping.")
            continue

        try:
            profile_str = json.dumps(profile)
            if not profile_str:
                logging.warning(f"Empty profile for well '{well_name}'. Skipping.")
                continue
            logging.debug(f"Serialized profile for well '{well_name}': {profile_str}")

            try:
                embedding = get_embedding(profile_str)
                if not embedding:
                    logging.warning(f"No embedding generated for well '{well_name}'. Skipping.")
                    continue
                logging.debug(f"Embedding for well '{well_name}': {embedding}")
                logging.info(f"Generated embedding for well '{well_name}'.")
            except Exception as e:
                logging.error(f"Error generating embedding for well '{well_name}': {e}")
                continue

            # Verify data types
            if not isinstance(well_name, str):
                logging.error(f"Invalid type for 'wlbwellborename': {well_name}")
                continue
            if not isinstance(profile_str, str):
                logging.error(f"Invalid type for 'well_profile': {profile_str}")
                continue
            if not isinstance(embedding, list) or not all(isinstance(x, (float, int)) for x in embedding):
                logging.error(f"Invalid type for 'embedded_well_profile': {embedding}")
                continue

            data = {
                'wlbwellborename': well_name,
                'well_profile': profile_str,
                'embedded_well_profile': embedding
            }

            try:
                response = supabase.table('profiled_wells').upsert(data).execute()
                logging.debug(f"Upsert response for well '{well_name}': {response}")
                if response.status_code in [200, 201]:
                    logging.info(f"Stored well profile and embedding for {well_name}.")
                else:
                    logging.error(f"Error inserting/updating {well_name}: {response.error}")
            except Exception as e:
                logging.error(f"Error inserting profile for well {well_name} into Supabase: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while processing well {well_name}: {e}")

    logging.info("Completed vectorization of well profiles.")
