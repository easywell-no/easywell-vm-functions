# all_scrape_and_store_functions/well_profile_and_vectorize.py

import logging
import os
from supabase import Client, create_client
from utils.well_profiler import get_well_profiles
from utils.vectorize import vectorize_well_profiles
from dotenv import load_dotenv

def setup_logging():
    log_directory = os.path.join(os.path.dirname(__file__), '..', 'logs')
    os.makedirs(log_directory, exist_ok=True)  # Create logs directory if it doesn't exist

    log_file = os.path.join(log_directory, 'well_profile_and_vectorize.log')

    logging.basicConfig(
        level=logging.INFO,  # Change to DEBUG for more detailed logs
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()  # Also log to console
        ]
    )

def well_profile_and_vectorize(supabase: Client, test_mode=False):
    setup_logging()
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
                logging.info("No more well names to process. Exiting loop.")
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
                logging.info("Test mode enabled. Exiting after first batch.")
                break  # Stop after first batch in test mode

        except Exception as e:
            logging.error(f"Error during well profile and vectorization process: {e}", exc_info=True)
            break

    logging.info("Completed well profile and vectorization process.")

if __name__ == "__main__":
    load_dotenv()  # Load environment variables from a .env file if present

    SUPABASE_URL = os.getenv('SUPABASE_URL')
    SUPABASE_KEY = os.getenv('SUPABASE_KEY')
    OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')  # Add this line

    # Configure basic logging to capture environment variable status
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[
            logging.StreamHandler()  # Log to console
        ]
    )

    # Log the status of environment variables
    if SUPABASE_URL and SUPABASE_KEY:
        logging.info("Supabase credentials are loaded.")
    else:
        logging.error("Supabase credentials are not set in the environment variables.")

    if OPENAI_API_KEY:
        logging.info("OPENAI_API_KEY is loaded.")
    else:
        logging.error("OPENAI_API_KEY is not set.")

    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Supabase credentials are not set in the environment variables.")
        exit(1)
    if not OPENAI_API_KEY:
        print("OPENAI_API_KEY is not set in the environment variables.")
        exit(1)

    try:
        supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
        logging.info("Supabase client created successfully!")
    except Exception as e:
        logging.error(f"Failed to create Supabase client: {e}", exc_info=True)
        exit(1)

    # Set test_mode=True for testing with a small dataset
    well_profile_and_vectorize(supabase, test_mode=True)
