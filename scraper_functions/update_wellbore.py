# update_wellbore.py

import pandas as pd
from datetime import datetime
from supabase import Client
import logging
from io import StringIO
import requests

# Configure logging
logging.basicConfig(
    level=logging.INFO,  # Set to INFO or DEBUG for more verbosity
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("update_wellbore.log"),
        logging.StreamHandler()
    ]
)

# Columns to ignore when checking for updates
IGNORED_COLUMNS = [
    'last_scraped', 
    'status', 
    'needs_rescrape', 
    'date_last_updated_csv'
]

# List of all Supabase table columns excluding the additional ones
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

# List of date columns
DATE_COLUMNS = [
    'wlbentrydate', 'wlbcompletiondate', 'wlbentrypredrilldate',
    'wlbcomppredrilldate', 'wlbwdssqcdate', 'wlbreleaseddate',
    'wlbdatereclass', 'wlbpluggedabandondate', 'wlbpluggeddate',
    'wlbdateupdated', 'wlbdateupdatedmax', 'datesyncnpd'
]

def normalize_value(value):
    """Converts empty strings or NaNs to None."""
    if pd.isnull(value) or str(value).strip() == '':
        return None
    return value

def convert_types(row):
    """
    Convert row values to appropriate types based on Supabase schema.
    Adjust this function as needed to handle specific conversions.
    """
    for key in row.keys():
        # Handle dates
        if key in DATE_COLUMNS:
            if row[key]:
                try:
                    row[key] = pd.to_datetime(row[key], dayfirst=True, errors='coerce').strftime('%Y-%m-%d')
                except Exception as e:
                    logging.warning(f"Date conversion error for {key}: {e}")
                    row[key] = None
            else:
                row[key] = None
        # Handle booleans (if any specific columns)
        elif key == 'needs_rescrape':
            if isinstance(row[key], bool):
                pass
            elif isinstance(row[key], str):
                row[key] = row[key].lower() in ['true', '1', 'yes']
            else:
                row[key] = bool(row[key])
        # Handle integers
        elif row[key] is not None and key not in SUPABASE_COLUMNS:
            try:
                row[key] = int(row[key])
            except:
                row[key] = None
    return row

def update_wellbore_data(supabase_client: Client):
    logging.info("Starting update_wellbore_data process.")
    
    # URL of the CSV file
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
    
    # Standardize column names to lowercase
    df.columns = df.columns.str.lower()
    logging.info("Column names standardized to lowercase.")
    
    # Normalize data using apply instead of applymap to avoid FutureWarning
    df = df.apply(lambda col: col.map(normalize_value))
    logging.info("Data normalization completed.")
    
    # Convert data types
    df = df.apply(convert_types, axis=1)
    logging.info("Data type conversion completed.")
    
    # Ensure all Supabase columns are present
    for col in SUPABASE_COLUMNS:
        if col not in df.columns:
            df[col] = None
    logging.info("Ensured all Supabase columns are present in DataFrame.")
    
    # Select only the Supabase columns
    df = df[SUPABASE_COLUMNS]
    logging.info("Selected relevant Supabase columns.")
    
    # Set additional columns
    df['last_scraped'] = None
    df['status'] = 'active'
    df['needs_rescrape'] = False
    df['date_last_updated_csv'] = df['wlbdateupdatedmax']
    
    # Convert 'date_last_updated_csv' to 'YYYY-MM-DD'
    df['date_last_updated_csv'] = pd.to_datetime(df['date_last_updated_csv'], dayfirst=True, errors='coerce').dt.strftime('%Y-%m-%d')
    
    # Fetch existing wellbore_data from Supabase
    try:
        response = supabase_client.table('wellbore_data').select('*').execute()
        # Updated error handling based on supabase-py response structure
        if response.status_code >= 400:
            logging.error(f"Error fetching existing data from Supabase: {response.status_code} {response.status_message}")
            return
        existing_data = pd.DataFrame(response.data)
        logging.info("Fetched existing data from Supabase successfully.")
    except Exception as e:
        logging.error(f"Exception occurred while fetching data from Supabase: {e}")
        return
    
    # Create a dictionary for quick lookup
    existing_dict = existing_data.set_index('wlbwellborename').to_dict('index')
    
    # Prepare lists for new records and updates
    new_records = []
    records_to_update = []
    
    for index, row in df.iterrows():
        well_name = row['wlbwellborename']
        if well_name in existing_dict:
            # Compare each relevant column to determine if an update is needed
            existing_record = existing_dict[well_name]
            row_dict = row.to_dict()
            # Exclude ignored columns and additional columns
            relevant_columns = {k: v for k, v in row_dict.items() if k not in IGNORED_COLUMNS and k != 'wlbwellborename'}
            existing_relevant = {k: existing_record.get(k) for k in relevant_columns.keys()}
            if relevant_columns != existing_relevant:
                # Prepare the update record
                update_record = row_dict.copy()
                update_record['status'] = 'pending'
                update_record['needs_rescrape'] = True
                update_record['last_scraped'] = None
                records_to_update.append(update_record)
        else:
            # Prepare the new record
            new_record = row.to_dict()
            new_record['last_scraped'] = datetime.utcnow().strftime('%Y-%m-%d')
            new_records.append(new_record)
    
    # Insert new records
    if new_records:
        try:
            response = supabase_client.table('wellbore_data').insert(new_records).execute()
            # Check if Supabase returned any errors in the response
            if response.status_code >= 400:
                logging.error(f"Error inserting new records: {response.status_code} {response.status_message}")
            else:
                logging.info(f"Inserted {len(new_records)} new records successfully.")
        except Exception as e:
            logging.error(f"Exception occurred during insertion: {e}")
    
    # Update existing records
    if records_to_update:
        try:
            for record in records_to_update:
                well_name = record['wlbwellborename']
                # Remove the primary key from the update dictionary
                update_dict = record.copy()
                del update_dict['wlbwellborename']
                response = supabase_client.table('wellbore_data').update(update_dict).eq('wlbwellborename', well_name).execute()
                # Check if Supabase returned any errors in the response
                if response.status_code >= 400:
                    logging.error(f"Error updating record {well_name}: {response.status_code} {response.status_message}")
            logging.info(f"Updated {len(records_to_update)} records successfully.")
        except Exception as e:
            logging.error(f"Exception occurred during updates: {e}")
    
    logging.info("Update process completed.")
