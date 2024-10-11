# scrape_and_store.py

import os
import logging
import sys
from dotenv import load_dotenv
from supabase import create_client, Client
from logging.handlers import RotatingFileHandler
from all_scrape_and_store_functions.update_wellbore import update_wellbore_data
from all_scrape_and_store_functions.scrape_factpages import scrape_factpages
from all_scrape_and_store_functions.well_profile_and_vectorize import well_profile_and_vectorize
from cleaner import cleanup

# Load environment variables
load_dotenv()

# Create log directory if it doesn't exist
log_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging for scrape_and_store.py
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = os.path.join(log_dir, "scrape_and_store.log")

rotating_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
rotating_handler.setFormatter(log_formatter)
rotating_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)

logging.basicConfig(
    level=logging.INFO,
    handlers=[rotating_handler, stream_handler]
)

# Load Supabase credentials from environment variables
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if SUPABASE_URL is None or SUPABASE_KEY is None:
    logging.error("Environment variables SUPABASE_URL or SUPABASE_KEY not set.")
    sys.exit(1)

# Create the Supabase client
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    logging.info("Supabase client created successfully!")
except Exception as e:
    logging.error(f"Failed to create Supabase client: {e}", exc_info=True)
    sys.exit(1)

def main():
    try:
        # Update and store wellbore data
        # update_wellbore_data(supabase)
        logging.info("Wellbore data update completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during wellbore data update: {e}", exc_info=True)
    
    try:
        # Scrape and store factpages
        # scrape_factpages(supabase)
        logging.info("Factpages scraped successfully.")
    except Exception as e:
        logging.error(f"An error occurred while scraping factpages: {e}", exc_info=True)

    try:
        # Make well-profiles, vectorize and store
        well_profile_and_vectorize(supabase)
        logging.info("Well profile and vectorization completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during well profile and vectorization: {e}", exc_info=True)
    
    try:
        # Cleanup process
        cleanup()
        logging.info("Cleanup process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during cleanup: {e}", exc_info=True)

if __name__ == "__main__":
    main()
