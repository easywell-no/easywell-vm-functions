import os
import logging
from dotenv import load_dotenv
from supabase import create_client, Client
from update_wellbore import update_wellbore_data
from cleaner import cleanup

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s',
                    handlers=[logging.FileHandler("main.log"), logging.StreamHandler()])

# Load environment variables
load_dotenv(dotenv_path='./scraper_functions/.env')

SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_KEY')

if SUPABASE_URL is None or SUPABASE_KEY is None:
    logging.error("Environment variables not loaded properly.")
    raise Exception("Environment variables not loaded properly.")

# Create the Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
logging.info("Supabase client created successfully!")

def main():
    try:
        # Call the update function
        update_wellbore_data(supabase)
        logging.info("Wellbore data update completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during wellbore data update: {e}")
    
    try:
        # Call the cleaner function
        cleanup()
        logging.info("Cleanup process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during cleanup: {e}")

if __name__ == "__main__":
    main()
