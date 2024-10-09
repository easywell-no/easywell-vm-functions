# utils/well_profile_and_vectorize.py

def well_profile_and_vectorize(supabase: Client, test_mode=False):
    logging.info("Starting well profile and vectorization process.")
    page_size = 10 if test_mode else 1000  # Use smaller page size for testing
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
            logging.info(f"Fetched {len(well_names)} well names in this batch starting from {current_start}.")

            if not well_names:
                break  # No more data to fetch, exit the loop

            # Get well profiles
            well_profiles = get_well_profiles(well_names, supabase)
            logging.info(f"Generated profiles for {len(well_profiles)} wells.")

            if not well_profiles:
                logging.warning("No well profiles generated for the fetched well names.")
                current_start += page_size
                continue

            # Vectorize and store well profiles
            vectorize_well_profiles(well_profiles, supabase)

            # Increment the start point for the next batch
            current_start += page_size

            if test_mode:
                break  # Stop after first batch in test mode

        except Exception as e:
            logging.error(f"Error during well profile and vectorization process: {e}", exc_info=True)
            break

    logging.info("Completed well profile and vectorization process.")
