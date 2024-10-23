# process_well_info.py

import os
import sys
import logging
from logging.handlers import RotatingFileHandler
from dotenv import load_dotenv
from supabase import create_client, Client
import json

# Adjust sys.path to include utils directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(parent_dir, 'utils')
sys.path.append(utils_dir)

from get_embedding import get_embedding

# Import the create_well_profiles module
from create_well_profiles import get_well_profile

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

# Configure logging
log_dir = os.path.join(parent_dir, "logs")
os.makedirs(log_dir, exist_ok=True)
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = os.path.join(log_dir, "process_well_info.log")
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

# Create Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def main():
    # Get list of unique wlbwellborename from well_history
    try:
        response = supabase.table('well_history').select('wlbwellborename').execute()
        wellborenames = list(set(item['wlbwellborename'] for item in response.data))
        total_wells = len(wellborenames)
        logging.info(f"Total wells to process: {total_wells}")
    except Exception as e:
        logging.error(f"Error fetching wellborenames from 'well_history': {e}", exc_info=True)
        return

    batch_size = 100
    for i in range(0, total_wells, batch_size):
        batch_wellborenames = wellborenames[i:i+batch_size]
        logging.info(f"Processing wells {i+1} to {i+len(batch_wellborenames)}")

        for wlbwellborename in batch_wellborenames:
            try:
                # Get the well_profile
                well_profile = get_well_profile(wlbwellborename)

                # Convert well_profile to JSON string
                well_profile_json = json.dumps(well_profile)

                # Generate embedding vector for the well_profile
                vector = get_embedding(well_profile_json)

                # Prepare the data to insert
                data = {
                    'wlbwellborename': wlbwellborename,
                    'well_profile': well_profile,
                    'vector': vector
                }

                # Insert into 'well_profiles' table
                supabase.table('well_profiles').upsert(data).execute()
                logging.info(f"Processed well: {wlbwellborename}")
            except Exception as e:
                logging.error(f"Error processing well '{wlbwellborename}': {e}", exc_info=True)
                continue

    logging.info("All wells processed successfully.")

if __name__ == '__main__':
    main()
