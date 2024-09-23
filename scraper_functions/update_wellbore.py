import pandas as pd
from datetime import datetime
from supabase import Client
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("update_wellbore.log"),
        logging.StreamHandler()
    ]
)

# Columns to ignore when checking for updates
IGNORED_COLUMNS = [
    'datesyncNPD', 'last_scraped', 'status', 
    'needs_rescrape', 'date_last_updated_csv'
]

# Mapping from CSV columns to SQL columns (if names differ)
CSV_TO_SQL_MAPPING = {
    'wlbWellboreName': 'wlbwellborename',
    'wlbFactPageUrl': 'wlbfactpageurl',
    'wlbDateUpdatedMax': 'wlbdateupdatedmax',
    # Add additional mappings if column names differ
}

# List of all Supabase table columns
SUPABASE_COLUMNS = [
    'wlbwellborename', 'last_scraped', 'status', 
    'needs_rescrape', 'date_last_updated_csv', 'wlbwelltype', 
    'wlbwell', 'wlbdrillingoperator', 'wlbproductionlicence', 
    'wlbpurpose', 'wlbstatus', 'wlbcontent', 'wlbsubsea', 
    'wlbentrydate', 'wlbcompletiondate', 'wlbentrypredrilldate', 
    'wlbcomppredrilldate', 'wlbfield', 'wlbdrillpermit', 
    'wlbdiscovery', 'wlbdiscoverywellbore', 'wlbbottomholetemperature', 
    'wlbsitesurvey', 'wlbseismiclocation', 'wlbmaxinclation', 
    'wlbkellybushelevation', 'wlbfinalverticaldepth', 'wlbtotaldepth', 
    'wlbwaterdepth', 'wlbkickoffpoint', 'wlbageattd', 
    'wlbformationattd', 'wlbmainarea', 'wlbdrillingfacility', 
    'wlbfacilitytypedrilling', 'wlbdrillingfacilityfixedormoveable', 
    'wlbproductionfacility', 'wlblicensingactivity', 'wlbmultilateral', 
    'wlbpurposeplanned', 'wlbcontentplanned', 'wlbentryyear', 
    'wlbcompletionyear', 'wlbreclassfromwellbore', 
    'wlbreentryexplorationactivity', 'wlbplotsymbol', 
    'wlbformationwithhc1', 'wlbagewithhc1', 'wlbformationwithhc2', 
    'wlbagewithhc2', 'wlbformationwithhc3', 'wlbagewithhc3', 
    'wlbdrillingdays', 'wlbreentry', 'wlblicencetargetname', 
    'wlbpluggedabandondate', 'wlbpluggeddate', 'wlbgeodeticdatum', 
    'wlbnsdeg', 'wlbnsmin', 'wlbnssec', 'wlbnscode', 'wlbewdeg', 
    'wlbewmin', 'wlbewsec', 'wlbewcode', 'wlbnsdecdeg', 
    'wlbewdecdeg', 'wlbnsutm', 'wlbewutm', 'wlbutmzone', 
    'wlbnamepart1', 'wlbnamepart2', 'wlbdiskoswellboretype', 
    'wlbnamepart3', 'wlbnamepart4', 'wlbnamepart5', 
    'wlbnamepart6', 'wlbpressreleaseurl', 'wlbfactpageurl', 
    'wlbfactmapurl', 'wlbdiskoswellboretype1', 
    'wlbdiskoswellboreparent', 'wlbwdssqcdate', 'wlbreleaseddate', 
    'wlbdatereclass', 'wlbnpdidwellbore', 
    'prlnpdidprodlicencetarget', 'fclnpdidfacilityproducing', 
    'dscnpdiddiscovery', 'fldnpdidfield', 'fclnpdidfacilitydrilling', 
    'wlbnpdidwellborereclass', 'prlnpdidproductionlicence', 
    'wlbnpdidsitesurvey', 'wlbaliasname', 'wlbdateupdated', 
    'wlbdateupdatedmax', 'datesyncnpd'
    # Add any additional columns if necessary
]

def normalize_value(value):
    """Converts empty values to None."""
    if pd.isnull(value) or value == '':
        return None
    return value

def convert_types(row):
    """
    Convert row values to appropriate types based on Supabase schema.
    Adjust this function as needed to handle specific conversions.
    """
    for key, value in row.items():
        # Handle dates
        if key in [
            'last_scraped', 'date_last_updated_csv', 'wlbentrydate',
            'wlbcompletiondate', 'wlbentrypredrilldate', 'wlbcomppredrilldate',
            'wlbwdssqcdate', 'wlbreleaseddate', 'wlbdatereclass',
            'wlbpluggedabandondate', 'wlbpluggeddate'
        ]:
            try:
                row[key] = pd.to_datetime(value).date() if value else None
            except Exception as e:
                logging.warning(f"Date conversion error for {key}: {e}")
                row[key] = None
        # Handle floats
        elif key in [
            'wlbbottomholetemperature', 'wlbmaxinclation', 'wlbkellybushelevation',
            'wlbfinalverticaldepth', 'wlbtotaldepth', 'wlbwaterdepth',
            'wlbnsdeg', 'wlbnsmin', 'wlbnssec', 'wlbewdeg', 'wlbewmin',
            'wlbewsec', 'wlbnsdecdeg', 'wlbewdecdeg'
        ]:
            try:
                row[key] = float(value) if value is not None else None
            except Exception as e:
                logging.warning(f"Float conversion error for {key}: {e}")
                row[key] = None
        # Handle booleans
        elif key in ['wlbsubsea', 'wlbdiscoverywellbore', 'wlbmultilateral', 'wlbreentry']:
            if isinstance(value, bool):
                pass
            elif isinstance(value, str):
                row[key] = value.lower() in ['true', '1', 'yes']
            else:
                row[key] = bool(value)
        # Handle integers
        elif key in ['wlbentryyear', 'wlbcompletionyear', 'wlbdrillingdays']:
            try:
                row[key] = int(value) if value is not None else None
            except Exception as e:
                logging.warning(f"Integer conversion error for {key}: {e}")
                row[key] = None
    return row

def update_wellbore_data(supabase_client: Client):
    logging.info("Starting daily update of wellbore_data")
    
    try:
        # Load the CSV data from Sokkeldirektoratet
        csv_url = 'https://factpages.sodir.no/public?/Factpages/external/tableview/wellbore_all_long&rs:Command=Render&rc:Toolbar=false&rc:Parameters=f&IpAddress=not_used&CultureCode=nb-no&rs:Format=CSV&Top100=false'
        df = pd.read_csv(csv_url, delimiter=",")
        logging.info("CSV data loaded successfully.")
    except Exception as e:
        logging.error(f"Failed to load CSV data: {e}")
        return

    try:
        # Normalize data to avoid empty values causing errors
        df = df.transform(normalize_value)
        logging.info("Data normalization completed.")
    except Exception as e:
        logging.error(f"Data normalization failed: {e}")
        return

    try:
        # Map CSV columns to SQL columns if necessary
        df.rename(columns=CSV_TO_SQL_MAPPING, inplace=True)
        logging.info("Column mapping completed.")
    except Exception as e:
        logging.error(f"Column mapping failed: {e}")
