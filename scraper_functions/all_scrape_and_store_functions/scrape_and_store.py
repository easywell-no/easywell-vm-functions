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
import re
from fractions import Fraction
import urllib3

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

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
log_dir = os.path.join(parent_dir, "logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = os.path.join(log_dir, "scrape_and_store.log")

rotating_handler = RotatingFileHandler(log_file, maxBytes=5*1024*1024, backupCount=5)
rotating_handler.setFormatter(log_formatter)
rotating_handler.setLevel(logging.INFO)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(log_formatter)
stream_handler.setLevel(logging.INFO)  # Set to INFO to display successful scrape messages

logging.basicConfig(
    level=logging.INFO,
    handlers=[rotating_handler, stream_handler]
)

# Load Supabase credentials
SUPABASE_URL = os.getenv('SUPABASE_URL')
SUPABASE_KEY = os.getenv('SUPABASE_SERVICE_ROLE_KEY')

if SUPABASE_URL is None or SUPABASE_KEY is None:
    logging.error("Environment variables SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set.")
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
        response = requests.get(url, verify=False)
        response.raise_for_status()
        content = response.content.decode('utf-8')
        if content.lstrip().startswith('<!DOCTYPE html>'):
            logging.error(f"Received HTML content instead of CSV from {url}")
            return None
        df = pd.read_csv(io.StringIO(content), encoding='utf-8', on_bad_lines='skip')
        return df
    except requests.exceptions.SSLError as ssl_err:
        logging.error(f"SSL error fetching CSV from {url}: {ssl_err}", exc_info=True)
        return None
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
                parsed_date = parser.parse(str(date_str), dayfirst=True)
                parsed_dates.append(parsed_date.strftime('%Y-%m-%d'))
        except Exception as e:
            logging.error(f"Error parsing date '{date_str}': {e}")
            parsed_dates.append(None)
    return pd.Series(parsed_dates)

def convert_to_float(value):
    try:
        if pd.isnull(value):
            return None
        value = str(value).strip()
        # Handle fractions like '8 1/2'
        if re.match(r'^\d+\s+\d+/\d+$', value):
            whole_part, frac_part = value.split()
            return float(whole_part) + float(Fraction(frac_part))
        elif re.match(r'^\d+/\d+$', value):
            return float(Fraction(value))
        else:
            # Remove any extra spaces and commas
            value = value.replace(',', '').strip()
            return float(value)
    except Exception as e:
        logging.error(f"Error converting '{value}' to float: {e}")
        return None

def prepare_data(df, table_name):
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
    # Handle numeric columns
    if table_name == 'well_casings':
        float_columns = ['wlbcasingdiameter', 'wlbcasingdepth', 'wlbholediameter', 'wlbholedepth', 'wlblotmuddencity']
        for col in float_columns:
            if col in df.columns:
                df[col] = df[col].apply(convert_to_float)
    if table_name == 'well_lito':
        integer_columns = ['lsunpdidlithostrat', 'lsunpdidlithostratparent', 'wlbnpdidwellbore']
        for col in integer_columns:
            if col in df.columns:
                df[col] = df[col].apply(lambda x: int(float(x)) if pd.notnull(x) else None)
    if table_name == 'well_mud':
        # Remove duplicates based on primary key
        required_columns = ['wlbwellborename', 'wlbmd', 'wlbmuddatemeasured']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            logging.error(f"Missing columns {missing_columns} in 'well_mud' data")
        else:
            df = df.drop_duplicates(subset=required_columns)
    # Replace NaN and NaT with None
    df = df.astype(object).where(pd.notnull(df), None)
    return df

def replace_table_in_supabase(supabase: Client, table_name: str, df: pd.DataFrame):
    try:
        # Delete all existing data in the table
        delete_response = supabase.table(table_name).delete().neq('wlbwellborename', '').execute()
        logging.info(f"Deleted records from '{table_name}'")
        # Insert new data in chunks
        data = df.to_dict(orient='records')
        chunk_size = 500
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            insert_response = supabase.table(table_name).insert(chunk, upsert=False).execute()
            if insert_response.error:
                logging.error(f"Error inserting data into '{table_name}': {insert_response.error}")
                break
        logging.info(f"Successfully updated table '{table_name}' in Supabase.")
    except Exception as e:
        logging.error(f"Error replacing table '{table_name}' in Supabase: {e}", exc_info=True)

def main():
    try:
        # Updated URLs
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
                logging.info(f"Successfully fetched CSV data for '{table_name}'")
                df = prepare_data(df, table_name)
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
