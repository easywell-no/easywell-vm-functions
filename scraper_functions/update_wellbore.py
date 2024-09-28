import os
import logging
from logging.handlers import RotatingFileHandler
import sys
from dotenv import load_dotenv
import pandas as pd
from datetime import datetime, timedelta
from io import StringIO
import requests
from supabase import create_client, Client

# Load environment variables
load_dotenv()

# Create log directory if it doesn't exist
log_dir = "/root/easywell-vm-functions/logs"
os.makedirs(log_dir, exist_ok=True)

# Configure logging for update_wellbore.py
log_formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
log_file = os.path.join(log_dir, "update_wellbore.log")

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

# Constants
IGNORED_COLUMNS = ['last_scraped', 'status', 'needs_rescrape']
SUPABASE_COLUMNS = [
    'wlbwellborename', 'wlbwelltype', 'wlbwell', 'wlbdrillingoperator',
    'wlbproductionlicence', 'wlbpurpose', 'wlbstatus', 'wlbcontent',
    'wlbsubsea', 'wlbentrydate', 'wlbcompletiondate', 'wlbentrypredrilldate',
    'wlbcomppredrilldate', 'wlbfield', 'wlbdrillpermit', 'wlbdiscovery',
    'wlbdiscoverywellbore', 'wlbbottomholetemperature', 'wlbsitesurvey',
    'wlbseismiclocation', 'wlbmaxinclation', 'wlbkellybushelevation',
    'wlbfinalverticaldepth', 'wlbtotaldepth', 'wlbwaterdepth',
    'wlbkickoffpoint', 'wlbageattd', 'wlbformationattd', 'wlbmainarea',
    'wlbdrillingfacility', 'wlbfacilitytypedrilling',
    'wlbdrillingfacilityfixedormoveable', 'wlbproductionfacility',
    'wlblicensingactivity', 'wlbmultilateral', 'wlbpurposeplanned',
    'wlbcontentplanned', 'wlbentryyear', 'wlbcompletionyear',
    'wlbreclassfromwellbore', 'wlbreentryexplorationactivity',
    'wlbplotsymbol', 'wlbformationwithhc1', 'wlbagewithhc1',
    'wlbformationwithhc2', 'wlbagewithhc2', 'wlbformationwithhc3',
    'wlbagewithhc3', 'wlbdrillingdays', 'wlbreentry',
    'wlblicencetargetname', 'wlbpluggedabandondate', 'wlbpluggeddate',
    'wlbgeodeticdatum', 'wlbnsdeg', 'wlbnsmin', 'wlbnssec',
    'wlbnscode', 'wlbewdeg', 'wlbewmin', 'wlbewsec', 'wlbewcode',
    'wlbnsdecdeg', 'wlbewdecdeg', 'wlbnsutm', 'wlbewutm', 'wlbutmzone',
    'wlbnamepart1', 'wlbnamepart2', 'wlbdiskoswellboretype',
    'wlbnamepart3', 'wlbnamepart4', 'wlbnamepart5', 'wlbnamepart6',
    'wlbpressreleaseurl', 'wlbfactpageurl', 'wlbfactmapurl',
    'wlbdiskoswellboretype1', 'wlbdiskoswellboreparent',
    'wlbwdssqcdate', 'wlbreleaseddate', 'wlbdatereclass',
    'wlbnpdidwellbore', 'prlnpdidprodlicencetarget',
    'fclnpdidfacilityproducing', 'dscnpdiddiscovery', 'fldnpdidfield',
    'fclnpdidfacilitydrilling', 'wlbnpdidwellborereclass',
    'prlnpdidproductionlicence', 'wlbnpdidsitesurvey',
    'wlbaliasname', 'wlbdateupdated', 'wlbdateupdatedmax',
    'datesyncnpd'
]
DATE_COLUMNS = [
    'wlbentrydate', 'wlbcompletiondate', 'wlbentrypredrilldate',
    'wlbcomppredrilldate', 'wlbwdssqcdate', 'wlbreleaseddate',
    'wlbdatereclass', 'wlbpluggedabandondate', 'wlbpluggeddate',
    'wlbdateupdated', 'wlbdateupdatedmax', 'datesyncnpd'
]

def normalize_value(value):
    if pd.isna(value) or value == '' or (isinstance(value, str) and value.lower() in ['nan', 'nat', 'none', 'null']):
        return None
    return value

def convert_types(row):
    for key, value in row.items():
        if key in DATE_COLUMNS:
            if value:
                try:
                    row[key] = pd.to_datetime(value, dayfirst=True, errors='coerce').strftime('%Y-%m-%d')
                except Exception as e:
                    logging.warning(f"Date conversion error for {key}: {e}")
                    row[key] = None
            else:
                row[key] = None
        elif key == 'needs_rescrape':
            row[key] = bool(value) if isinstance(value, bool) else (
                value.lower() in ['true', '1', 'yes'] if isinstance(value, str) else bool(value)
            )
        elif value is not None and key not in SUPABASE_COLUMNS:
            try:
                row[key] = int(value)
            except:
                row[key] = None
    return row

def fetch_all_supabase_data(supabase_client: Client, table_name: str):
    all_data = []
    page_size = 1000
    current_page = 0
    while True:
        try:
            response = supabase_client.table(table_name)\
                .select('*')\
                .range(current_page * page_size, (current_page + 1) * page_size - 1)\
                .execute()
            if hasattr(response, 'data') and response.data:
                all_data.extend(response.data)
                logging.info(f"Fetched page {current_page + 1} with {len(response.data)} records.")
                if len(response.data) < page_size:
                    break
                current_page += 1
            else:
                break
        except Exception as e:
            logging.error(f"Error fetching data from Supabase: {e}")
            break
    return pd.DataFrame(all_data)

def clean_dict(d):
    return {k: (v if pd.notna(v) else None) for k, v in d.items()}

def update_wellbore_data(supabase_client: Client):
    logging.info("Starting update_wellbore_data process.")
    
    csv_url = 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_all_long&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false'
    
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        logging.info("CSV data fetched successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CSV data: {e}")
        return
    
    try:
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        logging.info("CSV data loaded into DataFrame successfully.")
    except Exception as e:
        logging.error(f"Error loading CSV data into DataFrame: {e}")
        return
    
    df.columns = df.columns.str.lower()
    logging.info("Column names standardized to lowercase.")
    
    df = df.apply(lambda x: x.map(normalize_value))
    logging.info("Data normalization completed.")
    
    df = df.apply(convert_types, axis=1)
    logging.info("Data type conversion completed.")
    
    for col in SUPABASE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    logging.info("Ensured all Supabase columns are present in DataFrame.")
    
    df = df[SUPABASE_COLUMNS]
    logging.info("Selected relevant Supabase columns.")
    
    # Replace NaN and NaT with None
    df = df.where(pd.notnull(df), None)
    logging.info("Replaced all invalid placeholder values with None.")
    
    if df.isnull().values.any():
        logging.info("DataFrame contains null values. They will be inserted as null in Supabase.")
    
    try:
        existing_data = fetch_all_supabase_data(supabase_client, 'wellbore_data')
        if not existing_data.empty:
            logging.info(f"Fetched {len(existing_data)} existing records from Supabase successfully.")
        else:
            logging.info("No existing data found in Supabase.")
    except Exception as e:
        logging.error(f"Exception occurred while fetching data from Supabase: {e}")
        return
    
    if existing_data.empty:
        existing_dict = {}
        logging.info("No existing data found in Supabase. All records will be inserted as new.")
    else:
        if 'wlbwellborename' in existing_data.columns:
            existing_dict = existing_data.set_index('wlbwellborename').to_dict('index')
            logging.info("'wlbwellborename' column found. Proceeding with update checks.")
        else:
            existing_dict = {}
            logging.error("'wlbwellborename' column not found in existing data. All records will be treated as new.")
    
    new_records = []
    records_to_update = []
    
    current_date = datetime.now().date()
    three_months_ago = current_date - timedelta(days=90)
    
    # Compare CSV with existing data
    for index, row in df.iterrows():
        well_name = row['wlbwellborename']
        if well_name in existing_dict:
            existing_record = existing_dict[well_name]
            row_dict = row.to_dict()
            relevant_columns = {k: v for k, v in row_dict.items() if k not in IGNORED_COLUMNS and k != 'wlbwellborename'}
            existing_relevant = {k: existing_record.get(k) for k in relevant_columns.keys()}
            
            needs_update = relevant_columns != existing_relevant
            last_scraped = pd.to_datetime(existing_record.get('last_scraped')).date() if existing_record.get('last_scraped') else None
            needs_rescrape = needs_update or (last_scraped and last_scraped < three_months_ago)
            
            if needs_rescrape:
                update_record = clean_dict(row_dict.copy())
                update_record['status'] = 'waiting'  # waiting -> reserved -> completed
                update_record['needs_rescrape'] = True
                update_record['last_scraped'] = current_date.strftime('%Y-%m-%d')
                records_to_update.append(update_record)
        else:
            new_record = clean_dict(row.to_dict())
            new_record['last_scraped'] = current_date.strftime('%Y-%m-%d')
            new_record['status'] = 'waiting'  # waiting -> reserved -> completed
            new_record['needs_rescrape'] = True
            new_records.append(new_record)
    
    # Determine if any rows were deleted
    csv_wellbores = set(df['wlbwellborename'].unique())
    db_wellbores = set(existing_data['wlbwellborename'].unique()) if not existing_data.empty else set()
    deleted_wellbores = db_wellbores - csv_wellbores
    total_deleted_rows = len(deleted_wellbores)
    
    # Insert new records
    total_new_records = len(new_records)
    if new_records:
        try:
            # Optionally log the first new record for debugging
            logging.info(f"First new record to insert: {new_records[0]}")
            chunks = [new_records[i:i + 1000] for i in range(0, total_new_records, 1000)]
            for chunk in chunks:
                response = supabase_client.table('wellbore_data').insert(chunk).execute()
                if response.error:
                    logging.error(f"Error inserting new records: {response.error}")
                else:
                    logging.info(f"Inserted {len(chunk)} new records successfully.")
        except Exception as e:
            logging.error(f"Exception occurred during insertion: {e}")
    
    # Update existing records
    total_update_records = len(records_to_update)
    if records_to_update:
        try:
            chunks = [records_to_update[i:i + 1000] for i in range(0, total_update_records, 1000)]
            for chunk in chunks:
                for record in chunk:
                    well_name = record['wlbwellborename']
                    update_dict = {k: v for k, v in record.items() if k != 'wlbwellborename'}
                    update_dict = clean_dict(update_dict)
                    
                    for key, value in update_dict.items():
                        if isinstance(value, pd.Timestamp):
                            update_dict[key] = value.strftime('%Y-%m-%d')
                    
                    response = supabase_client.table('wellbore_data').update(update_dict).eq('wlbwellborename', well_name).execute()
                    
                    if response.error:
                        logging.error(f"Error updating record {well_name}: {response.error}")
                    else:
                        logging.info(f"Updated record {well_name} successfully.")
            
            logging.info(f"Updated {total_update_records} records successfully.")
        except Exception as e:
            logging.error(f"Exception occurred during updates: {e}")
    
    logging.info(f"Update process completed: {total_new_records} new records, {total_update_records} updated records, {total_deleted_rows} rows deleted.")
