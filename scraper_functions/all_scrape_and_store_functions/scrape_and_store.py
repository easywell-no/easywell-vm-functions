import os
import sys
import logging
import requests
import pandas as pd
import io
from dotenv import load_dotenv
from supabase import create_client, Client
from logging.handlers import RotatingFileHandler
from dateutil import parser

# Adjust sys.path to include utils directory
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
utils_dir = os.path.join(parent_dir, 'utils')
sys.path.append(utils_dir)

from cleaner import cleanup

# Load environment variables
env_path = os.path.join(parent_dir, '.env')
load_dotenv(dotenv_path=env_path)

# Create log directory if it doesn't exist
log_dir = os.path.join(parent_dir, "logs")  # Ensure logs are in the correct directory
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
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

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

def fetch_csv(url):
    try:
        response = requests.get(url)
        response.raise_for_status()
        content = response.content.decode('utf-8')
        df = pd.read_csv(io.StringIO(content), encoding='utf-8', on_bad_lines='skip')
        return df
    except Exception as e:
        logging.error(f"Error fetching CSV from {url}: {e}", exc_info=True)
        return None

def parse_date_column(series):
    parsed_dates = []
    for date_str in series:
        try:
            if pd.isnull(date_str):
                parsed_dates.append(None)
            else:
                parsed_date = parser.parse(str(date_str), dayfirst=False)
                parsed_dates.append(parsed_date.strftime('%Y-%m-%d'))
        except Exception as e:
            logging.error(f"Error parsing date '{date_str}': {e}")
            parsed_dates.append(None)
    return pd.Series(parsed_dates)

def prepare_data(df):
    # Replace empty strings and whitespace-only strings with NaN
    df = df.replace(r'^\s*$', pd.NA, regex=True)
    # Make all column names lowercase to match the database
    df.columns = df.columns.str.lower()
    # Rename 'wlbname' to 'wlbwellborename' if it exists
    if 'wlbname' in df.columns:
        df = df.rename(columns={'wlbname': 'wlbwellborename'})
    # Identify date columns
    date_columns = [col for col in df.columns if 'date' in col.lower()]
    for date_col in date_columns:
        try:
            df[date_col] = parse_date_column(df[date_col])
        except Exception as e:
            logging.error(f"Error parsing date column '{date_col}': {e}", exc_info=True)
    # Replace NaN and NaT with None
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def replace_table_in_supabase(supabase: Client, table_name: str, df: pd.DataFrame):
    try:
        # Delete all existing data in the table using wlbwellborename
        supabase.table(table_name).delete().neq('wlbwellborename', '').execute()
        # Insert new data in chunks
        data = df.to_dict(orient='records')
        chunk_size = 500  # Adjust chunk size if necessary
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            # Insert data while handling missing values
            supabase.table(table_name).insert(chunk, upsert=False).execute()
        logging.info(f"Successfully updated table '{table_name}' in Supabase.")
    except Exception as e:
        logging.error(f"Error replacing table '{table_name}' in Supabase: {e}", exc_info=True)

def main():
    try:
        # Define URLs
        urls = {
            'well_history': 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_history&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false',
            'well_coordinates': 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_coordinates&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false',
            'well_mud': 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_mud&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false',
            'well_casings': 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_casing_and_lot&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false',
            'well_lito': 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_formation_top&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false'
        }

        for table_name, url in urls.items():
            logging.info(f"Processing '{table_name}'...")
            df = fetch_csv(url)
            if df is not None:
                df = prepare_data(df)
                replace_table_in_supabase(supabase, table_name, df)
            else:
                logging.warning(f"Skipping '{table_name}' due to fetch error.")
        logging.info("All CSV data updates completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during CSV data updates: {e}", exc_info=True)
    
    try:
        # Cleanup process
        cleanup()
        logging.info("Cleanup process completed successfully.")
    except Exception as e:
        logging.error(f"An error occurred during cleanup: {e}", exc_info=True)

if __name__ == "__main__":
    main()
