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

# Import get_embedding function
from get_embedding import get_embedding

# Import the create_well_profiles module
from create_well_profiles import get_well_profile

# Load environment variables
env_path = os.path.join(parent_dir, '.env')
load_dotenv(dotenv_path=env_path)
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')
OPENAI_API_KEY = os.getenv('OPENAI_API_KEY')

# Set OpenAI API key
import openai
openai.api_key = OPENAI_API_KEY

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

def fetch_all_wellborenames():
    batch_size = 1000  # Adjust as needed
    offset = 0
    all_wellborenames = set()
    while True:
        response = supabase.table('well_history').select('wlbwellborename').range(offset, offset + batch_size - 1).execute()
        data = response.data
        if not data:
            break
        all_wellborenames.update(item['wlbwellborename'] for item in data)
        offset += batch_size
    return list(all_wellborenames)

def main():
    # Get list of unique wlbwellborename from well_history
    try:
        wellborenames = fetch_all_wellborenames()
        total_wells = len(wellborenames)
        logging.info(f"Total wells to process: {total_wells}")
    except Exception as e:
        logging.error(f"Error fetching wellborenames from 'well_history': {e}", exc_info=True)
        return

    batch_size = 50  # Adjust as needed
    for i in range(0, total_wells, batch_size):
        batch_wellborenames = wellborenames[i:i+batch_size]
        logging.info(f"Processing wells {i+1} to {i+len(batch_wellborenames)}")

        for wlbwellborename in batch_wellborenames:
            try:
                # Get the well_profile as a text string
                well_profile_dict = get_well_profile(wlbwellborename)
                well_profile_text = convert_well_profile_to_text(well_profile_dict)

                # Generate embedding vector for the well_profile
                vector = get_embedding(well_profile_text)

                # Prepare the data to insert
                data = {
                    'wlbwellborename': wlbwellborename,
                    'well_profile': well_profile_text,
                    'vector': vector
                }

                # Insert into 'well_profiles' table
                supabase.table('well_profiles').upsert(data).execute()
                logging.info(f"Processed well: {wlbwellborename}")
            except Exception as e:
                logging.error(f"Error processing well '{wlbwellborename}': {e}", exc_info=True)
                continue

    logging.info("All wells processed successfully.")

def convert_well_profile_to_text(well_profile_dict):
    """
    Converts the well_profile dictionary into a readable text format.
    """
    lines = []
    for key, value in well_profile_dict.items():
        if isinstance(value, list):
            lines.append(f"{key}:")
            for item in value:
                lines.append(json.dumps(item))
        else:
            lines.append(f"{key}: {value}")
    return "\n".join(lines)

if __name__ == '__main__':
    main()
