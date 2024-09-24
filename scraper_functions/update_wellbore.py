import pandas as pd
from datetime import datetime, timedelta
import logging
from io import StringIO
import requests
from supabase import create_client, Client

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("update_wellbore.log"),
        logging.StreamHandler()
    ]
)

# Constants
IGNORED_COLUMNS = ['last_scraped', 'status', 'needs_rescrape', 'datesyncNPD']
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

def update_wellbore_data(supabase_client: Client):
    logging.info("Starting update_wellbore_data process.")
    
    # URL for fetching CSV data
    csv_url = 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_all_long&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false'
    
    # Fetch CSV data from the source
    try:
        response = requests.get(csv_url)
        response.raise_for_status()
        logging.info("CSV data fetched successfully.")
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching CSV data: {e}")
        return
    
    # Load CSV data into a DataFrame
    try:
        csv_data = StringIO(response.text)
        df = pd.read_csv(csv_data)
        logging.info("CSV data loaded into DataFrame successfully.")
    except Exception as e:
        logging.error(f"Error loading CSV data into DataFrame: {e}")
        return
    
    # Standardize column names
    df.columns = df.columns.str.lower()
    logging.info("Column names standardized to lowercase.")
    
    # Normalize values (convert 'nan', 'NaT', etc. to None)
    df = df.applymap(normalize_value)
    logging.info("Data normalization completed.")
    
    # Convert data types (for dates, etc.)
    df = df.apply(convert_types, axis=1)
    logging.info("Data type conversion completed.")
    
    # Ensure all necessary columns for Supabase are present
    for col in SUPABASE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    logging.info("Ensured all Supabase columns are present in DataFrame.")
    
    # Select relevant columns
    df = df[SUPABASE_COLUMNS]
    logging.info("Selected relevant Supabase columns.")
    
    # Replace NaN values, 'None', 'NaT', and similar with None
    df = df.where(pd.notnull(df), None)
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].replace({'nan': None, 'NaN': None, 'NaT': None, 'None': None, 'null': None})
    logging.info("Replaced all invalid placeholder values with None.")
    
    # Fetch existing data from Supabase
    try:
        existing_data = fetch_all_supabase_data(supabase_client, 'wellbore_data')
        if not existing_data.empty:
            logging.info(f"Fetched {len(existing_data)} existing records from Supabase successfully.")
        else:
            logging.info("No existing data found in Supabase.")
    except Exception as e:
        logging.error(f"Exception occurred while fetching data from Supabase: {e}")
        return
    
    # Prepare new and updated records
    existing_dict = existing_data.set_index('wlbwellborename').to_dict('index') if not existing_data.empty else {}
    new_records = []
    records_to_update = []
    
    current_date = datetime.now().date()
    three_months_ago = current_date - timedelta(days=90)
    
    for index, row in df.iterrows():
        well_name = row['wlbwellborename']
        row_dict = row.to_dict()

        if well_name in existing_dict:
            # Compare with existing records
            existing_record = existing_dict[well_name]
            relevant_columns = {k: v for k, v in row_dict.items() if k not in IGNORED_COLUMNS}
            existing_relevant = {k: existing_record.get(k) for k in relevant_columns.keys()}
            
            needs_update = relevant_columns != existing_relevant
            last_scraped = pd.to_datetime(existing_record.get('last_scraped')).date() if existing_record.get('last_scraped') else None
            needs_rescrape = needs_update or (last_scraped and last_scraped < three_months_ago)
            
            if needs_rescrape:
                update_record = row_dict.copy()
                update_record['status'] = 'waiting'
                update_record['needs_rescrape'] = True
                update_record['last_scraped'] = current_date.strftime('%Y-%m-%d')
                records_to_update.append(update_record)
        else:
            # New record
            new_record = row_dict.copy()
            new_record['status'] = 'waiting'
            new_record['needs_rescrape'] = True
            new_record['last_scraped'] = current_date.strftime('%Y-%m-%d')
            new_records.append(new_record)
    
    # Handle insertion of new records
    if new_records:
        try:
            chunks = [new_records[i:i + 1000] for i in range(0, len(new_records), 1000)]
            for chunk in chunks:
                response = supabase_client.table('wellbore_data').insert(chunk).execute()
                if hasattr(response, 'data'):
                    logging.info(f"Inserted {len(chunk)} new records successfully.")
                else:
                    logging.error(f"Error inserting new records: {response}")
        except Exception as e:
            logging.error(f"Exception occurred during insertion: {e}")
    
    # Handle updates of existing records
    if records_to_update:
        try:
            chunks = [records_to_update[i:i + 1000] for i in range(0, len(records_to_update), 1000)]
            for chunk in chunks:
                for record in chunk:
                    well_name = record['wlbwellborename']
                    update_dict = {k: v for k, v in record.items() if k != 'wlbwellborename'}
                    response = supabase_client.table('wellbore_data').update(update_dict).eq('wlbwellborename', well_name).execute()
                    if hasattr(response, 'data'):
                        logging.info(f"Updated record {well_name} successfully.")
                    else:
                        logging.error(f"Error updating record {well_name}: {response}")
            logging.info(f"Updated {len(records_to_update)} records successfully.")
        except Exception as e:
            logging.error(f"Exception occurred during updates: {e}")
    
    logging.info(f"Update process completed: {len(new_records)} new records, {len(records_to_update)} updated records.")
