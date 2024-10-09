# utils/vectorize.py

import logging
from supabase import Client
from utils.get_embedding import get_embedding

def vectorize_well_profiles(well_profiles, supabase: Client):
    logging.info("Starting vectorization of well profiles.")

    for well_name, profile in well_profiles.items():
        try:
            # Serialize the profile to a string (e.g., JSON or text summary)
            profile_str = str(profile)  # You may want to format this better depending on your use case

            logging.debug(f"Profile for well '{well_name}': {profile_str}")

            # Generate embedding for the well profile
            try:
                embedding = get_embedding(profile_str)
                logging.info(f"Generated embedding for well {well_name}.")
            except Exception as e:
                logging.error(f"Error generating embedding for well {well_name}: {e}")
                continue  # Skip this well if embedding fails

            # Insert the profile and embedding into the profiled_wells table
            data = {
                'wlbwellborename': well_name,
                'well_profile': profile_str,
                'embedded_well_profile': embedding
            }

            try:
                response = supabase.table('profiled_wells').upsert(data).execute()
                if response.status_code == 201:  # 201 is for insert success
                    logging.info(f"Stored well profile and embedding for {well_name}.")
                else:
                    logging.error(f"Error inserting/updating {well_name}: {response.error}")
            except Exception as e:
                logging.error(f"Error inserting profile for well {well_name} into Supabase: {e}")
        except Exception as e:
            logging.error(f"Unexpected error while processing well {well_name}: {e}")

    logging.info("Completed vectorization of well profiles.")
