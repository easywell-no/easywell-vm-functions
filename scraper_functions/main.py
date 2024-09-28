import os
import logging
import sys
from dotenv import load_dotenv
from supabase import create_client, Client
from logging.handlers import RotatingFileHandler
from update_wellbore import update_wellbore_data
from scrape_factpages import scrape_factpages
from cleaner import cleanup

# Load environment variables
load_dotenv()  # Ensure this is called early

# Create log directory if it doesn't exist
log_dir = "/root/easywell-vm-functions/logs"
os.makedirs(log_dir, exist_ok=True)

# Configure logging for main.py
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = os.path.join(log_dir, "main.log")

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

# Append parent directory to sys.path if needed
sys.path.append("..")  # Adjust as necessary

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
        # Update wellbore data
        update_wellbore_data(supabase)
        logging.info("Wellbore data update completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during wellbore data update: {e}", exc_info=True)
    
    try:
        # Scrape factpages
        scrape_factpages(supabase)
        logging.info("Factpages scraped successfully.")
    except Exception as e:
        logging.error(f"An error occurred while scraping factpages: {e}", exc_info=True)
    
    try:
        # Cleanup process
        cleanup()
        logging.info("Cleanup process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during cleanup: {e}", exc_info=True)

if __name__ == "__main__":
    main()
